"""Isotonic recalibration of Kalshi prices with empirical-Bayes shrinkage.

Fit: per (category_group × horizon_bucket) weighted pool-adjacent-violators
isotonic regression of outcome y on market price p (hand-rolled, pure
python — no sklearn/numpy). Each group curve is blended toward the GLOBAL
curve with weight n/(n+K) (empirical-Bayes shrinkage); groups below
MIN_GROUP_N are not stored at all and resolve through the fallback chain
category×horizon → horizon-only → global at evaluation time.

Validation is a TIME-based split (no shuffling): train close_ts < T_split,
test close_ts >= T_split, T_split chosen so ~25% of observations are test.
Splitting on close_ts keeps every market wholly on one side (all of a
market's horizon observations share its close_ts). Ship gate: overall test
Brier(model) < Brier(market). After validation the production artifact is
refit on ALL data (standard practice — the split exists to measure, not to
train the shipped model); the validation report travels inside the artifact.

Monotonicity: PAV guarantees a nondecreasing fit; a convex combination of
nondecreasing curves is nondecreasing; evaluation is linear interpolation
between knots, flat beyond the ends, clipped to [0.001, 0.999].

Floats everywhere here — model-internal math. The artifact is JSON (floats),
but anything that reaches SQLite or the UI goes through score.py as decimal
strings (data invariants). Validation output is NOT trading authorization
(data invariants).
"""

from __future__ import annotations

import json
import logging
import os
import re
import time
from collections import defaultdict
from pathlib import Path
from typing import Any, Iterable, Sequence

from .dataset import GROUPS, HORIZON_BUCKETS, Observation, build_training_frame

logger = logging.getLogger(__name__)

MODEL_FAMILY = "kalshi-flb"
DEFAULT_K = 2000.0
MIN_GROUP_N = 500
TEST_FRACTION = 0.25
CLIP_LO = 0.001
CLIP_HI = 0.999
GLOBAL_KEY = "*|*"

Curve = list[tuple[float, float]]  # knots [(p, calibrated_p)], p strictly increasing


# --------------------------------------------------------------------------
# Pool-adjacent-violators isotonic regression (weighted, pure python)
# --------------------------------------------------------------------------

def pav_isotonic(points: Iterable[tuple[float, float, float]]) -> Curve:
    """Weighted isotonic (nondecreasing) regression via PAV.

    ``points``: (x, y, w) triples, any order. Exact-tie x values are pooled
    first. Returns knots [(x, fitted)] where x is the weighted mean x of each
    final block and fitted values are nondecreasing (strictly increasing
    between blocks, since equal-value neighbors merge).
    """
    by_x: dict[float, list[float]] = defaultdict(lambda: [0.0, 0.0])  # x -> [sw, swy]
    for x, y, w in points:
        if w <= 0:
            raise ValueError(f"nonpositive weight {w!r}")
        acc = by_x[float(x)]
        acc[0] += w
        acc[1] += w * y
    if not by_x:
        raise ValueError("pav_isotonic: empty input")

    # blocks: [sum_w, sum_wy, sum_wx]; mean y = sum_wy/sum_w
    blocks: list[list[float]] = []
    for x in sorted(by_x):
        sw, swy = by_x[x]
        blocks.append([sw, swy, sw * x])
        # merge while the previous block's mean >= current's (violator);
        # merging on equality keeps block values strictly increasing.
        while len(blocks) >= 2 and (
            blocks[-2][1] * blocks[-1][0] >= blocks[-1][1] * blocks[-2][0]
        ):
            b = blocks.pop()
            blocks[-1][0] += b[0]
            blocks[-1][1] += b[1]
            blocks[-1][2] += b[2]
    return [(swx / sw, swy / sw) for sw, swy, swx in blocks]


def interpolate(curve: Sequence[Sequence[float]], x: float) -> float:
    """Evaluate a knot list at x: linear between knots, flat beyond the ends,
    clipped to [CLIP_LO, CLIP_HI]."""
    if not curve:
        raise ValueError("empty curve")
    lo, hi = 0, len(curve) - 1
    if x <= curve[0][0]:
        y = curve[0][1]
    elif x >= curve[hi][0]:
        y = curve[hi][1]
    else:
        while hi - lo > 1:  # binary search for the bracketing knots
            mid = (lo + hi) // 2
            if curve[mid][0] <= x:
                lo = mid
            else:
                hi = mid
        x0, y0 = curve[lo][0], curve[lo][1]
        x1, y1 = curve[hi][0], curve[hi][1]
        y = y0 if x1 == x0 else y0 + (y1 - y0) * (x - x0) / (x1 - x0)
    return min(max(y, CLIP_LO), CLIP_HI)


def blend_curves(a: Curve, b: Curve, weight_a: float) -> Curve:
    """weight_a·a + (1-weight_a)·b, evaluated on the union of knot x's.
    Both inputs nondecreasing ⇒ result nondecreasing."""
    xs = sorted({x for x, _ in a} | {x for x, _ in b})
    return [
        (x, weight_a * interpolate(a, x) + (1.0 - weight_a) * interpolate(b, x))
        for x in xs
    ]


# --------------------------------------------------------------------------
# Mincer-Zarnowitz bias monitor:  y - p = alpha + psi·p
# --------------------------------------------------------------------------

def mincer_zarnowitz(obs: Sequence[Observation]) -> dict[str, float | int]:
    """OLS of (y − p) on p. ψ>0 with α<0 is the favorite-longshot-bias
    signature (paper: ψ=0.034, α=−1.736 cents ≈ −0.0174 prob; here α is in
    probability units). This is the bias TIME-SERIES monitor — record every
    fit, expect decay (2025 already halved vs 2024 in the paper)."""
    n = len(obs)
    if n < 2:
        return {"psi": 0.0, "alpha": 0.0, "n": n}
    mean_p = sum(o.p_market for o in obs) / n
    mean_r = sum(o.y - o.p_market for o in obs) / n
    var_p = sum((o.p_market - mean_p) ** 2 for o in obs)
    if var_p == 0.0:
        return {"psi": 0.0, "alpha": mean_r, "n": n}
    cov = sum((o.p_market - mean_p) * ((o.y - o.p_market) - mean_r) for o in obs)
    psi = cov / var_p
    alpha = mean_r - psi * mean_p
    return {"psi": psi, "alpha": alpha, "n": n}


# --------------------------------------------------------------------------
# Fit: global + horizon-only + category×horizon curves with shrinkage
# --------------------------------------------------------------------------

def group_key(category: str, horizon: str) -> str:
    return f"{category}|{horizon}"


def fit_curves(
    frame: Sequence[Observation],
    *,
    k: float = DEFAULT_K,
    min_group_n: int = MIN_GROUP_N,
) -> tuple[dict[str, Curve], dict[str, str]]:
    """Returns (curves, fallback_map).

    curves: '*|*' (global, always), '*|<horizon>' and '<group>|<horizon>' for
    every group with >= min_group_n observations, each shrunk toward the
    global curve with weight n/(n+k). fallback_map records, for every
    (category, horizon) combination OBSERVED in training, which curve key it
    resolves to (documentation + artifact introspection; resolution logic
    itself is resolve_curve, which also covers unseen combos).
    """
    if not frame:
        raise ValueError("fit_curves: empty training frame")
    global_curve = pav_isotonic((o.p_market, float(o.y), 1.0) for o in frame)
    curves: dict[str, Curve] = {GLOBAL_KEY: global_curve}

    by_horizon: dict[str, list[Observation]] = defaultdict(list)
    by_group: dict[tuple[str, str], list[Observation]] = defaultdict(list)
    for o in frame:
        by_horizon[o.horizon_bucket].append(o)
        by_group[(o.category_group, o.horizon_bucket)].append(o)

    for horizon, obs in by_horizon.items():
        if len(obs) >= min_group_n:
            raw = pav_isotonic((o.p_market, float(o.y), 1.0) for o in obs)
            lam = len(obs) / (len(obs) + k)
            curves[group_key("*", horizon)] = blend_curves(raw, global_curve, lam)

    for (category, horizon), obs in by_group.items():
        if len(obs) >= min_group_n:
            raw = pav_isotonic((o.p_market, float(o.y), 1.0) for o in obs)
            lam = len(obs) / (len(obs) + k)
            curves[group_key(category, horizon)] = blend_curves(raw, global_curve, lam)

    fallback_map = {
        group_key(c, h): _resolve_key(curves, c, h) for (c, h) in sorted(by_group)
    }
    return curves, fallback_map


def _resolve_key(curves: dict[str, Curve], category: str, horizon: str) -> str:
    for key in (group_key(category, horizon), group_key("*", horizon), GLOBAL_KEY):
        if key in curves:
            return key
    raise KeyError("no global curve in artifact")  # unreachable on valid artifacts


def resolve_curve(curves: dict[str, Curve], category: str, horizon: str) -> Curve:
    """Fallback chain: category×horizon → horizon-only → global."""
    return curves[_resolve_key(curves, category, horizon)]


def predict(curves: dict[str, Curve], o: Observation) -> float:
    return interpolate(resolve_curve(curves, o.category_group, o.horizon_bucket), o.p_market)


# --------------------------------------------------------------------------
# Time-split validation
# --------------------------------------------------------------------------

def time_split(
    frame: Sequence[Observation], test_fraction: float = TEST_FRACTION
) -> tuple[list[Observation], list[Observation], int]:
    """(train, test, t_split): train close_ts < t_split <= test close_ts.
    NO shuffling — the split key is settled close_ts, so every market (all of
    its horizon observations share one close_ts) lands wholly on one side.
    The realized test fraction can deviate from the target when many markets
    share the boundary close_ts."""
    if len(frame) < 8:
        raise ValueError(f"not enough observations to split: {len(frame)}")
    ordered = sorted(frame, key=lambda o: o.close_ts)
    t_split = ordered[int(len(ordered) * (1.0 - test_fraction))].close_ts
    train = [o for o in ordered if o.close_ts < t_split]
    test = [o for o in ordered if o.close_ts >= t_split]
    if not train or not test:
        raise ValueError(
            "degenerate time split (all observations share close_ts region); "
            f"t_split={t_split} train={len(train)} test={len(test)}"
        )
    return train, test, t_split


def _brier(pairs: Iterable[tuple[float, int]]) -> float:
    pairs = list(pairs)
    return sum((p - y) ** 2 for p, y in pairs) / len(pairs)


def evaluate(
    curves: dict[str, Curve],
    test: Sequence[Observation],
    train: Sequence[Observation],
) -> dict[str, Any]:
    """Test-set validation report: model-vs-market Brier overall and per
    category_group×horizon, a 10-bin reliability table, and the naive
    constant-baseline harness check (a degenerate always-p̄ predictor must
    NOT beat the market — if it does, the harness itself is broken)."""
    model_p = [predict(curves, o) for o in test]
    brier_model = _brier(zip(model_p, (o.y for o in test)))
    brier_market = _brier((o.p_market, o.y) for o in test)

    per_group: dict[str, dict[str, Any]] = {}
    by_group: dict[str, list[tuple[Observation, float]]] = defaultdict(list)
    for o, mp in zip(test, model_p):
        by_group[group_key(o.category_group, o.horizon_bucket)].append((o, mp))
    for key, items in sorted(by_group.items()):
        per_group[key] = {
            "n": len(items),
            "brier_model": _brier((mp, o.y) for o, mp in items),
            "brier_market": _brier((o.p_market, o.y) for o, mp in items),
        }

    reliability = []
    bins: dict[int, list[tuple[Observation, float]]] = defaultdict(list)
    for o, mp in zip(test, model_p):
        bins[min(int(o.p_market * 10), 9)].append((o, mp))
    for b in range(10):
        items = bins.get(b, [])
        reliability.append(
            {
                "bin": f"[{b / 10:.1f},{(b + 1) / 10:.1f})",
                "n": len(items),
                "mean_p": sum(o.p_market for o, _ in items) / len(items) if items else None,
                "mean_y": sum(o.y for o, _ in items) / len(items) if items else None,
                "mean_model_p": sum(mp for _, mp in items) / len(items) if items else None,
            }
        )

    p_const = sum(o.y for o in train) / len(train)
    brier_const = _brier((p_const, o.y) for o in test)

    return {
        "n_test": len(test),
        "brier_model": brier_model,
        "brier_market": brier_market,
        "brier_delta": brier_model - brier_market,  # negative = model wins
        "gate_pass": brier_model < brier_market,
        "per_group": per_group,
        "reliability": reliability,
        "baseline_constant": {
            "p": p_const,
            "brier": brier_const,
            "beats_market": brier_const < brier_market,  # must be False
        },
        "mz_test": mincer_zarnowitz(test),
    }


# --------------------------------------------------------------------------
# Artifact IO
# --------------------------------------------------------------------------

def models_dir(data_dir: Path) -> Path:
    return Path(data_dir) / "models"


def _next_version(data_dir: Path) -> int:
    pattern = re.compile(rf"{MODEL_FAMILY}-v(\d+)\.json$")
    existing = [
        int(m.group(1))
        for p in models_dir(data_dir).glob(f"{MODEL_FAMILY}-v*.json")
        if (m := pattern.match(p.name))
    ]
    return max(existing, default=0) + 1


def validate_artifact(artifact: dict[str, Any]) -> dict[str, Any]:
    """Shape check on load — a malformed artifact must fail loudly, not score."""
    for key in ("version", "model_version", "trained_at", "train_window",
                "n_train", "n_test", "k", "curves", "fallback_map", "mz", "metrics"):
        if key not in artifact:
            raise ValueError(f"artifact missing key {key!r}")
    curves = artifact["curves"]
    if not isinstance(curves, dict) or GLOBAL_KEY not in curves:
        raise ValueError("artifact curves must be a dict containing the global '*|*' curve")
    for key, curve in curves.items():
        if not curve:
            raise ValueError(f"curve {key!r} is empty")
        xs = [pt[0] for pt in curve]
        ys = [pt[1] for pt in curve]
        if any(len(pt) != 2 for pt in curve):
            raise ValueError(f"curve {key!r} has a non-pair knot")
        if any(x1 <= x0 for x0, x1 in zip(xs, xs[1:])):
            raise ValueError(f"curve {key!r} knot x's not strictly increasing")
        if any(y1 < y0 for y0, y1 in zip(ys, ys[1:])):
            raise ValueError(f"curve {key!r} not monotone nondecreasing")
        if any(not (0.0 <= v <= 1.0) for v in xs + ys):
            raise ValueError(f"curve {key!r} has values outside [0,1]")
    if not isinstance(artifact["model_version"], str) or not artifact["model_version"]:
        raise ValueError("model_version must be a non-empty string")
    return artifact


def write_artifact(data_dir: Path, artifact: dict[str, Any]) -> Path:
    """JSON artifact + CURRENT pointer file (atomic .tmp renames). CURRENT
    holds the artifact FILENAME — a pointer file, not a symlink, so it
    survives naive copies/rsync."""
    out_dir = models_dir(data_dir)
    out_dir.mkdir(parents=True, exist_ok=True)
    path = out_dir / f"{MODEL_FAMILY}-v{artifact['version']}.json"
    tmp = path.with_suffix(".json.tmp")
    tmp.write_text(json.dumps(artifact, indent=1, sort_keys=True))
    os.replace(tmp, path)
    cur_tmp = out_dir / "CURRENT.tmp"
    cur_tmp.write_text(path.name + "\n")
    os.replace(cur_tmp, out_dir / "CURRENT")
    return path


def load_artifact(path: Path) -> dict[str, Any]:
    artifact = validate_artifact(json.loads(Path(path).read_text()))
    # JSON turns tuples into lists; normalize knots to tuples for hashing-free use
    artifact["curves"] = {
        k: [(float(x), float(y)) for x, y in v] for k, v in artifact["curves"].items()
    }
    return artifact


def current_artifact_path(data_dir: Path) -> Path | None:
    pointer = models_dir(data_dir) / "CURRENT"
    if not pointer.exists():
        return None
    name = pointer.read_text().strip()
    path = models_dir(data_dir) / name
    return path if path.exists() else None


# --------------------------------------------------------------------------
# train — the whole pipeline
# --------------------------------------------------------------------------

def train(
    data_dir: Path,
    *,
    k: float = DEFAULT_K,
    min_group_n: int = MIN_GROUP_N,
    test_fraction: float = TEST_FRACTION,
    frame: Sequence[Observation] | None = None,
) -> dict[str, Any]:
    """Build dataset → time-split validate → refit on ALL data → artifact.

    If the validation gate fails (test Brier(model) >= Brier(market)) the artifact
    is still written but model_version is tagged 'shadow-…': scoring logs
    forecasts for self-grading, and the UI join excludes shadow versions.
    ``frame`` injects a prebuilt dataset (tests / reuse across K sweeps).
    Returns the artifact dict (already written to disk).
    """
    from .. import lake  # late import: duckdb surface only needed here

    data_dir = Path(data_dir)
    if frame is None:
        conn = lake.connect(data_dir)
        try:
            frame = build_training_frame(conn)
        finally:
            conn.close()
    if not frame:
        raise RuntimeError("training frame is empty — no settled markets with tape?")

    train_obs, test_obs, t_split = time_split(frame, test_fraction)
    fit_train, _ = fit_curves(train_obs, k=k, min_group_n=min_group_n)
    metrics = evaluate(fit_train, test_obs, train_obs)
    metrics["t_split"] = t_split
    metrics["t_split_iso"] = _iso(t_split)
    metrics["n_train"] = len(train_obs)
    metrics["mz_train"] = mincer_zarnowitz(train_obs)

    gate_pass = metrics["gate_pass"]
    if metrics["baseline_constant"]["beats_market"]:
        # constant predictor beating the market means the harness is broken;
        # never ship that as a live model.
        gate_pass = False
        logger.error("harness check FAILED: constant baseline beat market Brier")

    # Production artifact: refit on ALL data (validation measured the recipe;
    # the shipped curves use every observation).
    curves, fallback_map = fit_curves(frame, k=k, min_group_n=min_group_n)
    version = _next_version(data_dir)
    base_version = f"{MODEL_FAMILY}-v{version}"
    model_version = base_version if gate_pass else f"shadow-{base_version}"

    artifact = {
        "version": version,
        "model_version": model_version,
        "shadow": not gate_pass,
        "trained_at": int(time.time()),
        "train_window": [
            min(o.close_ts for o in frame), max(o.close_ts for o in frame)
        ],
        "train_window_iso": [
            _iso(min(o.close_ts for o in frame)), _iso(max(o.close_ts for o in frame))
        ],
        "n_train": len(train_obs),
        "n_test": len(test_obs),
        "n_total": len(frame),
        "k": k,
        "min_group_n": min_group_n,
        "horizons": [b for b, _ in HORIZON_BUCKETS],
        "groups": GROUPS,
        "group_n": _group_counts(frame),
        "curves": curves,
        "fallback_map": fallback_map,
        "mz": mincer_zarnowitz(frame),  # full-data bias monitor (vs paper ψ=0.034)
        "metrics": metrics,
    }
    validate_artifact(artifact)
    path = write_artifact(data_dir, artifact)
    logger.info(
        "trained %s: n=%d (train %d / test %d), gate %s, "
        "test Brier model %.5f vs market %.5f, psi=%.4f -> %s",
        model_version, len(frame), len(train_obs), len(test_obs),
        "PASS" if gate_pass else "FAIL (shadow)",
        metrics["brier_model"], metrics["brier_market"],
        artifact["mz"]["psi"], path,
    )
    return artifact


def _group_counts(frame: Sequence[Observation]) -> dict[str, int]:
    counts: dict[str, int] = defaultdict(int)
    for o in frame:
        counts[group_key(o.category_group, o.horizon_bucket)] += 1
    return dict(sorted(counts.items()))


def _iso(ts: int) -> str:
    return time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime(ts))
