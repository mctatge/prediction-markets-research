"""PAV isotonic correctness, shrinkage, time-split, MZ, artifact roundtrip."""

import json

import pytest

from pmkt.model.calibrate import (
    CLIP_HI,
    CLIP_LO,
    GLOBAL_KEY,
    blend_curves,
    current_artifact_path,
    evaluate,
    fit_curves,
    interpolate,
    load_artifact,
    mincer_zarnowitz,
    pav_isotonic,
    resolve_curve,
    time_split,
    train,
    validate_artifact,
    write_artifact,
)
from pmkt.model.dataset import Observation


def _obs(p, y, *, cat="sports", h="24h", close_ts=1_000_000, mid="M"):
    return Observation(
        venue_market_id=mid, horizon_bucket=h, category_group=cat,
        p_market=p, y=y, close_ts=close_ts,
    )


# ---------------------------------------------------------------------------
# (a) PAV: known solutions, weights, ties, monotonicity
# ---------------------------------------------------------------------------

def test_pav_known_small_case():
    # y means by x: 1, 0, 1, 1 -> (1,0) pool to 0.5; (1,1) pool to 1.0
    knots = pav_isotonic([(1, 1, 1), (2, 0, 1), (3, 1, 1), (4, 1, 1)])
    assert knots == [(1.5, 0.5), (3.5, 1.0)]


def test_pav_weighted_correctness():
    # violation with weights: pooled mean = (1*1 + 3*0)/4, pooled x likewise
    assert pav_isotonic([(0, 1, 1), (1, 0, 3)]) == [(0.75, 0.25)]


def test_pav_pools_tied_x():
    knots = pav_isotonic([(0.5, 1, 1), (0.5, 0, 1), (0.5, 0, 2)])
    assert knots == [(0.5, 0.25)]


def test_pav_already_isotonic_is_identity():
    pts = [(0.1, 0.0, 1), (0.4, 0.5, 2), (0.9, 1.0, 1)]
    assert pav_isotonic(pts) == [(0.1, 0.0), (0.4, 0.5), (0.9, 1.0)]


def test_pav_monotone_on_noisy_input():
    # deterministic pseudo-noise, values strictly increase between blocks
    pts = [(i / 100, ((i * 7919) % 100) / 100, 1.0 + (i % 3)) for i in range(100)]
    knots = pav_isotonic(pts)
    assert all(y1 > y0 for (_, y0), (_, y1) in zip(knots, knots[1:]))
    assert all(x1 > x0 for (x0, _), (x1, _) in zip(knots, knots[1:]))


def test_pav_rejects_bad_input():
    with pytest.raises(ValueError):
        pav_isotonic([])
    with pytest.raises(ValueError):
        pav_isotonic([(0.5, 1.0, 0.0)])


# ---------------------------------------------------------------------------
# interpolation + blending
# ---------------------------------------------------------------------------

def test_interpolate_linear_flat_ends_and_clip():
    curve = [(0.2, 0.1), (0.8, 0.7)]
    assert interpolate(curve, 0.5) == pytest.approx(0.4)
    assert interpolate(curve, 0.0) == pytest.approx(0.1)   # flat left
    assert interpolate(curve, 1.0) == pytest.approx(0.7)   # flat right
    assert interpolate([(0.5, 0.0)], 0.5) == CLIP_LO       # clipped low
    assert interpolate([(0.5, 1.0)], 0.5) == CLIP_HI       # clipped high


def test_blend_curves_is_convex_combination_and_monotone():
    a = [(0.0, 0.0), (1.0, 1.0)]
    b = [(0.0, 0.2), (0.5, 0.3), (1.0, 0.8)]
    blended = blend_curves(a, b, 0.25)
    assert interpolate(blended, 0.5) == pytest.approx(0.25 * 0.5 + 0.75 * 0.3)
    ys = [y for _, y in blended]
    assert ys == sorted(ys)


# ---------------------------------------------------------------------------
# Mincer-Zarnowitz OLS
# ---------------------------------------------------------------------------

def test_mincer_zarnowitz_known_line():
    # E[y-p | p=0.2] = 0.2 (4/10 ones), E[y-p | p=0.4] = 0.1 (5/10 ones)
    # -> psi = -0.5, alpha = 0.3
    obs = [_obs(0.2, 1) for _ in range(4)] + [_obs(0.2, 0) for _ in range(6)]
    obs += [_obs(0.4, 1) for _ in range(5)] + [_obs(0.4, 0) for _ in range(5)]
    mz = mincer_zarnowitz(obs)
    assert mz["psi"] == pytest.approx(-0.5)
    assert mz["alpha"] == pytest.approx(0.3)
    assert mz["n"] == 20


def test_mincer_zarnowitz_unbiased_is_zero():
    obs = [_obs(0.5, 1) for _ in range(5)] + [_obs(0.5, 0) for _ in range(5)]
    obs += [_obs(0.2, 1) for _ in range(2)] + [_obs(0.2, 0) for _ in range(8)]
    mz = mincer_zarnowitz(obs)
    assert mz["psi"] == pytest.approx(0.0, abs=1e-12)
    assert mz["alpha"] == pytest.approx(0.0, abs=1e-12)


# ---------------------------------------------------------------------------
# (c) shrinkage: tiny extreme group lands near the global curve
# ---------------------------------------------------------------------------

def _calibrated_global_frame(n=4000, per_level=40):
    """Well-calibrated global data: 100 price levels x per_level obs each,
    empirical y rate at each level ~= p."""
    frame = []
    for i in range(n):
        level = (i // per_level) % 100
        p = 0.1 + 0.8 * level / 100
        y = 1 if (i % per_level) / per_level < p else 0
        frame.append(_obs(round(p, 3), y, cat="financials", h="24h", mid=f"g{i}"))
    return frame


def test_shrinkage_pulls_tiny_extreme_group_to_global():
    frame = _calibrated_global_frame()
    # tiny group: 40 obs at p=0.3 that ALL resolved yes (empirical rate 1.0)
    tiny = [_obs(0.3, 1, cat="crypto", h="1h", mid=f"t{i}") for i in range(40)]
    curves, _ = fit_curves(frame + tiny, k=2000, min_group_n=10)
    g = interpolate(curves[GLOBAL_KEY], 0.3)
    c = interpolate(curves["crypto|1h"], 0.3)
    # weight = 40/2040 ≈ 2%: nudged toward 1.0 but must stay near global
    assert c > g
    assert abs(c - g) < 0.05
    # sanity: unshrunk empirical rate would be ~1.0, far from the fitted estimate
    assert c < 0.5


def test_small_groups_fall_back_horizon_then_global():
    frame = _calibrated_global_frame()
    tiny = [_obs(0.3, 1, cat="crypto", h="1h", mid=f"t{i}") for i in range(40)]
    curves, fallback_map = fit_curves(frame + tiny, k=2000, min_group_n=500)
    # 40 < 500: no crypto|1h curve is stored
    assert "crypto|1h" not in curves
    assert fallback_map["crypto|1h"] == GLOBAL_KEY  # no *|1h either (n=40)
    assert fallback_map["financials|24h"] == "financials|24h"
    # resolve_curve applies the same chain for unseen combos
    assert resolve_curve(curves, "politics", "24h") == curves["*|24h"]
    assert resolve_curve(curves, "politics", "168h") == curves[GLOBAL_KEY]


# ---------------------------------------------------------------------------
# (d) time split: chronological, market-disjoint
# ---------------------------------------------------------------------------

def test_time_split_is_chronological_and_market_disjoint():
    frame = []
    for i in range(100):
        close = 1_000_000 + i * 3600  # one market per close_ts
        for h in ("1h", "24h"):       # two horizon obs per market
            frame.append(_obs(0.5, i % 2, h=h, close_ts=close, mid=f"m{i}"))
    train_obs, test_obs, t_split = time_split(frame, test_fraction=0.25)
    assert max(o.close_ts for o in train_obs) < t_split
    assert min(o.close_ts for o in test_obs) >= t_split
    train_mkts = {o.venue_market_id for o in train_obs}
    test_mkts = {o.venue_market_id for o in test_obs}
    assert not (train_mkts & test_mkts)
    assert len(test_obs) / len(frame) == pytest.approx(0.25, abs=0.02)


def test_time_split_rejects_tiny_frames():
    with pytest.raises(ValueError):
        time_split([_obs(0.5, 1)] * 4)


# ---------------------------------------------------------------------------
# evaluate: gate + naive-baseline harness check
# ---------------------------------------------------------------------------

def _biased_frame(n, start_ts, *, longshot_rate=0.05, favorite_rate=0.85):
    """FLB-shaped data: p=0.2 longshots resolve below price, p=0.7 favorites
    above — the correction the model must learn."""
    frame = []
    for i in range(n):
        close = start_ts + i * 60
        if i % 2:
            frame.append(_obs(0.2, 1 if (i // 2) % 20 < 20 * longshot_rate else 0,
                              close_ts=close, mid=f"b{i}"))
        else:
            frame.append(_obs(0.7, 1 if (i // 2) % 20 < 20 * favorite_rate else 0,
                              close_ts=close, mid=f"b{i}"))
    return frame


def test_evaluate_gate_passes_on_consistent_bias():
    frame = _biased_frame(4000, 1_000_000)
    train_obs, test_obs, _ = time_split(frame)
    curves, _ = fit_curves(train_obs, k=100, min_group_n=100)
    metrics = evaluate(curves, test_obs, train_obs)
    assert metrics["gate_pass"]
    assert metrics["brier_model"] < metrics["brier_market"]
    assert metrics["baseline_constant"]["beats_market"] is False
    bins = {b["bin"]: b for b in metrics["reliability"]}
    assert bins["[0.2,0.3)"]["n"] > 0
    assert bins["[0.2,0.3)"]["mean_p"] == pytest.approx(0.2)
    assert bins["[0.2,0.3)"]["mean_y"] == pytest.approx(0.05, abs=0.03)


# ---------------------------------------------------------------------------
# (f) artifact save/load roundtrip + validation + shadow tagging
# ---------------------------------------------------------------------------

def test_train_artifact_roundtrip(tmp_path):
    frame = _biased_frame(4000, 1_000_000)
    artifact = train(tmp_path, k=100, min_group_n=100, frame=frame)
    assert artifact["shadow"] is False
    assert artifact["model_version"] == "kalshi-flb-v1"
    path = current_artifact_path(tmp_path)
    assert path is not None and path.name == "kalshi-flb-v1.json"
    loaded = load_artifact(path)
    assert loaded["model_version"] == artifact["model_version"]
    assert loaded["curves"].keys() == artifact["curves"].keys()
    for key in loaded["curves"]:
        for (x0, y0), (x1, y1) in zip(loaded["curves"][key], artifact["curves"][key]):
            assert x0 == pytest.approx(x1) and y0 == pytest.approx(y1)
    assert loaded["metrics"]["gate_pass"] is True
    assert "t_split_iso" in loaded["metrics"]

    # retrain bumps the version and repoints CURRENT
    artifact2 = train(tmp_path, k=100, min_group_n=100, frame=frame)
    assert artifact2["model_version"] == "kalshi-flb-v2"
    assert current_artifact_path(tmp_path).name == "kalshi-flb-v2.json"


def test_train_tags_shadow_when_gate_fails(tmp_path):
    # bias REVERSES between train and test: the learned correction hurts
    train_part = _biased_frame(3000, 1_000_000, longshot_rate=0.05, favorite_rate=0.85)
    test_part = _biased_frame(1000, 2_000_000, longshot_rate=0.45, favorite_rate=0.55)
    artifact = train(tmp_path, k=100, min_group_n=100, frame=train_part + test_part)
    assert artifact["shadow"] is True
    assert artifact["model_version"].startswith("shadow-kalshi-flb-v")
    # still written + loadable: shadow models log forecasts and grade themselves
    assert current_artifact_path(tmp_path) is not None


def test_validate_artifact_rejects_malformed(tmp_path):
    artifact = train(tmp_path, k=100, min_group_n=100,
                     frame=_biased_frame(2000, 1_000_000))
    good = json.loads((current_artifact_path(tmp_path)).read_text())

    missing = dict(good)
    del missing["curves"]
    with pytest.raises(ValueError, match="missing key"):
        validate_artifact(missing)

    non_monotone = json.loads(json.dumps(good))
    non_monotone["curves"][GLOBAL_KEY] = [[0.1, 0.9], [0.9, 0.1]]
    with pytest.raises(ValueError, match="monotone"):
        validate_artifact(non_monotone)

    no_global = json.loads(json.dumps(good))
    del no_global["curves"][GLOBAL_KEY]
    with pytest.raises(ValueError, match="global"):
        validate_artifact(no_global)

    bad_x = json.loads(json.dumps(good))
    bad_x["curves"][GLOBAL_KEY] = [[0.5, 0.4], [0.5, 0.6]]
    with pytest.raises(ValueError, match="strictly increasing"):
        validate_artifact(bad_x)
