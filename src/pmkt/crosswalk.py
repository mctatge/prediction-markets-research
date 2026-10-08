"""Cross-venue structural matcher — pair a Kalshi binary market with the
Polymarket token/market for the SAME outcome, for live paper-trading.

Used by the dual-venue shadow ledger. Adapted from
backtests/transfer_probe.py::rebuild_crosswalk, which
matched a frozen July-2026 slate; three changes make it a rolling service:

  1. hardcoded July-2026 date globs → a rolling [min_date, max_date] window;
  2. resolution agreement can't confirm a match LIVE (a game hasn't settled
     at decision time) → it becomes a POST-SETTLEMENT QA signal;
  3. a two-tier confirm model — ``confirmed`` (usable by decide) vs
     ``candidate`` (surfaced for manual review).

This module is deliberately I/O-free: it matches over already-loaded rows so it
is trivially unit-testable and never touches hot.sqlite or DuckDB. lake.py does
the reads (sqlite mode=ro + pm_resolutions parquet) and calls in here.

**Precision >> coverage.** One wrong confirmed match logs paper trades against
the WRONG Kalshi market and silently poisons the track record; a missed match
just costs coverage. So only the unambiguous structural case auto-confirms
(exact slug↔ticker code via alias, single non-doubleheader game, 1:1 sides,
sport in AUTO_CONFIRM_SPORTS); World Cup (3-way draw) and tennis (surname-prefix,
fuzzy) stay candidate-only behind a manual gate.
"""

from __future__ import annotations

import re
from collections import defaultdict
from typing import Any, Iterable

from .crosswalk_aliases import (
    AUTO_CONFIRM_SPORTS,
    MLB_ALIAS,
    WC_ALIAS,
    WNBA_ALIAS,
    _MONTHS,
)

# Slug parsers, generalized off rebuild_crosswalk's `2026-07-\d{2}` to any date.
_ML_RE = re.compile(r"^(mlb|wnba)-([a-z0-9]+)-([a-z0-9]+)-(\d{4}-\d{2}-\d{2})$")
_WC_RE = re.compile(
    r"^fifwc-([a-z]{3})-([a-z]{3})-(\d{4}-\d{2}-\d{2})-([a-z]{3}|draw)$"
)
_TN_RE = re.compile(r"^(atp|wta)-([a-z0-9]+)-([a-z0-9]+)-(\d{4}-\d{2}-\d{2})$")

# Kalshi event ticker: <SERIES>-<YY><MON><DD>[<HHMM>]<CODES>[G<n>]
# e.g. KXMLBGAME-26JUL10DETPHI, KXWCGAME-26JUL10ARGSUI, ...-G2 (doubleheader).
_EVENT_RE = re.compile(
    r"^[A-Z]+-(\d{2})([A-Z]{3})(\d{2})(\d{4})?([A-Z0-9]+?)(G\d)?$"
)


def parse_event_ticker(
    event_ticker: str, suffixes: set[str]
) -> tuple[str, str, str, bool] | None:
    """(game_date_iso, away_code, home_code, is_doubleheader) from a Kalshi
    sports event ticker, or None if it can't be parsed / split cleanly.

    ``suffixes`` are the market-ticker code suffixes within the event (minus
    ``TIE``); the away/home split point is the one that makes both halves of
    the concatenated ``CODES`` valid suffixes — the same trick as
    rebuild_crosswalk (no fixed code length; works for 2- and 3-letter codes).
    """
    m = _EVENT_RE.match(event_ticker)
    if not m:
        return None
    yy, mon, dd, _hhmm, codes, dh = m.groups()
    month = _MONTHS.get(mon)
    if month is None:
        return None
    date = f"20{yy}-{month:02d}-{dd}"
    teams = next(
        (
            (a, codes[len(a):])
            for a in sorted(suffixes, key=len, reverse=True)
            if codes.startswith(a) and codes[len(a):] in suffixes
        ),
        None,
    )
    if teams is None:
        return None
    return date, teams[0], teams[1], bool(dh)


def _in_window(date: str, min_date: str | None, max_date: str | None) -> bool:
    if min_date is not None and date < min_date:
        return False
    if max_date is not None and date > max_date:
        return False
    return True


def _index_pm(pm_inst_rows: Iterable[dict[str, Any]]):
    """Group PM instrument rows into per-market dicts keyed by slug structure.

    Returns (pm_ml, pm_wc, pm_tn) where pm_ml/pm_wc map a parsed slug key to a
    market dict and pm_tn maps (tour, p1, p2) → list of tennis market dicts
    (surname collisions resolved later by date proximity)."""
    pm_ml: dict[tuple, dict] = {}
    pm_wc: dict[tuple, dict] = {}
    pm_tn: dict[tuple, list] = defaultdict(list)
    seen: dict[tuple, dict] = {}
    for r in pm_inst_rows:
        slug = r.get("slug") or ""
        for rx, book in ((_ML_RE, pm_ml), (_WC_RE, pm_wc), (_TN_RE, None)):
            m = rx.match(slug)
            if not m:
                continue
            key = m.groups()
            skey = (rx.pattern, key)
            if skey not in seen:
                d = seen[skey] = {
                    "cid": r["cid"], "slug": slug, "title": r.get("title"),
                    "status": r.get("status_norm"), "outcomes": {},
                    "date": key[-1] if rx is _TN_RE else None,
                }
                if book is not None:
                    book[key] = d
                else:
                    pm_tn[key[:3]].append(d)
            seen[skey]["outcomes"][r["oi"]] = (
                r.get("tok"), r.get("oname"), r.get("is_primary")
            )
            break
    return pm_ml, pm_wc, pm_tn


def match_crosswalk(
    kalshi_rows: Iterable[dict[str, Any]],
    pm_inst_rows: Iterable[dict[str, Any]],
    pm_res: dict[str, tuple[int | None, str | None]],
    *,
    min_date: str | None = None,
    max_date: str | None = None,
) -> list[dict[str, Any]]:
    """Structural cross-venue candidate rows (one per contract-side).

    ``kalshi_rows``: dicts {series, event, ticker, title, status_norm, result,
    close_ts}. ``pm_inst_rows``: instrument-level dicts {cid, slug, title,
    status_norm, tok, oi, oname, is_primary}. ``pm_res``: conditionId →
    (winning_outcome_index, winning_label) for QA. Rows are NOT yet confirmed —
    call apply_confirmation() next.
    """
    kev: dict[str, list] = defaultdict(list)
    for r in kalshi_rows:
        kev[r["event"]].append(r)
    pm_ml, pm_wc, pm_tn = _index_pm(pm_inst_rows)

    out: list[dict[str, Any]] = []

    def emit(ev, sport, date, kmkt, side, pmd, oi, method, note):
        tok, oname, is_prim = pmd["outcomes"].get(oi, (None, None, None))
        prim = next(
            (t for _i, (t, _n, ip) in pmd["outcomes"].items() if ip == 1), None
        )
        win_idx, _win_label = pm_res.get(pmd["cid"], (None, None))
        agree = ""
        if kmkt["status_norm"] == "settled" and win_idx is not None:
            agree = "1" if ((kmkt["result"] == "yes") == (win_idx == oi)) else "0"
        out.append({
            "event_key": ev, "sport": sport, "game_date": date,
            "kalshi_ticker": kmkt["ticker"], "kalshi_side": side,
            "kalshi_title": kmkt.get("title"),
            "kalshi_status": kmkt["status_norm"],
            "kalshi_result": kmkt.get("result") or "",
            "pm_condition_id": pmd["cid"], "pm_slug": pmd["slug"],
            "pm_token_id": tok or "", "pm_outcome_index": oi,
            "pm_outcome_name": oname or "",
            "pm_primary_token_id": prim or "",
            "pm_status": pmd.get("status") or "",
            "pm_winning_index": win_idx,
            "resolution_agree": agree,
            "match_method": method, "notes": note,
        })

    for ev, mkts in kev.items():
        series = mkts[0]["series"]
        suffixes = {t["ticker"].rsplit("-", 1)[1] for t in mkts} - {"TIE"}
        parsed = parse_event_ticker(ev, suffixes)
        if parsed is None:
            continue
        date, away, home, doubleheader = parsed
        if not _in_window(date, min_date, max_date):
            continue
        if doubleheader:
            # PM slugs carry no game number → ambiguous. Skip (logged as
            # unmatched by the builder's coverage diff).
            continue

        if series in ("KXMLBGAME", "KXWNBAGAME", "KXWCGAME"):
            if series == "KXWCGAME":
                a_pm = WC_ALIAS.get(away, away.lower())
                h_pm = WC_ALIAS.get(home, home.lower())
                for kmkt in mkts:
                    side = kmkt["ticker"].rsplit("-", 1)[1]
                    leg = "draw" if side == "TIE" else WC_ALIAS.get(side, side.lower())
                    pmd = pm_wc.get((a_pm, h_pm, date, leg))
                    if pmd:
                        yes_oi = next(
                            (i for i, (_t, n, _p) in pmd["outcomes"].items()
                             if n == "Yes"), 0
                        )
                        emit(ev, "wc", date, kmkt, side, pmd, yes_oi,
                             "slug-code", f"leg={leg}")
            else:
                sport = "mlb" if series == "KXMLBGAME" else "wnba"
                alias = MLB_ALIAS if sport == "mlb" else WNBA_ALIAS
                key = (sport, alias.get(away, away.lower()),
                       alias.get(home, home.lower()), date)
                pmd = pm_ml.get(key)
                if pmd:
                    for kmkt in mkts:
                        side = kmkt["ticker"].rsplit("-", 1)[1]
                        oi = 0 if alias.get(side, side.lower()) == key[1] else 1
                        emit(ev, sport, date, kmkt, side, pmd, oi, "slug-code",
                             f"away={away} home={home}")
        else:
            _match_tennis(ev, series, mkts, date, pm_tn, emit)

    return out


def _match_tennis(ev, series, mkts, date, pm_tn, emit) -> None:
    """Tennis surname-prefix matching (fuzzy → candidate-only). Ported from
    rebuild_crosswalk: parse the two surnames from the Kalshi event title,
    find the PM tennis market whose slug tokens prefix them, disambiguate by
    date proximity, and map each Kalshi leg to the PM outcome by surname."""
    tour = "atp" if series == "KXATPMATCH" else "wta"
    mm = re.search(r"win the (.+?) vs (.+?): ", mkts[0].get("title") or "")
    if not mm:
        return
    s1, s2 = (s.strip().lower().replace(" ", "") for s in mm.groups())
    cands = []
    for (t, p1, p2), lst in pm_tn.items():
        if t == tour and (
            (s1.startswith(p1) and s2.startswith(p2))
            or (s1.startswith(p2) and s2.startswith(p1))
        ):
            for d in lst:
                dd = abs(int(d["date"][-2:]) - int(date[-2:]))
                if dd <= 1:
                    cands.append((dd, id(d), d))
    cands.sort(key=lambda x: x[:2])
    if not cands or (
        len(cands) > 1 and cands[0][0] == cands[1][0]
        and cands[0][2] is not cands[1][2]
    ):
        return
    pmd = cands[0][2]
    for kmkt in mkts:
        mfull = re.match(r"Will (.+?) win the ", kmkt.get("title") or "")
        if not mfull:
            continue
        full = mfull.group(1).strip().lower()
        oi = next(
            (i for i, (_t, n, _p) in pmd["outcomes"].items()
             if n and (n.lower() in full or full in n.lower()
                       or full.split()[-1] in n.lower())),
            None,
        )
        if oi is None:
            continue
        emit(ev, tour, date, kmkt, kmkt["ticker"].rsplit("-", 1)[1], pmd, oi,
             "surname", f"kalshi_date={date} pm_date={pmd['date']}")


def _auto_confirm(row: dict[str, Any]) -> int:
    """The unambiguous structural case: exact slug-code alias match, an
    auto-confirm sport (clean 2-way moneyline), a mapped token, a mapped
    outcome index. World Cup / tennis never auto-confirm."""
    return int(
        row["match_method"] == "slug-code"
        and row["sport"] in AUTO_CONFIRM_SPORTS
        and bool(row["pm_token_id"])
        and row["pm_outcome_index"] is not None
    )


def apply_confirmation(
    rows: list[dict[str, Any]], overrides: dict[tuple[str, str], int]
) -> list[dict[str, Any]]:
    """Set ``confirmed``/``auto_confirmed`` per row (mutates + returns rows).

    ``overrides`` maps (kalshi_ticker, pm_token_id) → confirmed (0/1), the
    merged human decisions + QA quarantines from data/crosswalk_confirmed.csv.
    An override always wins over the auto rule (that is how a human confirms a
    tennis match or a QA quarantine kills a bad auto-confirm); auto_confirmed
    records only rows the machine confirmed on its own.
    """
    for row in rows:
        auto = _auto_confirm(row)
        key = (row["kalshi_ticker"], row["pm_token_id"])
        if key in overrides:
            row["confirmed"] = int(overrides[key])
            row["auto_confirmed"] = 0
        else:
            row["confirmed"] = auto
            row["auto_confirmed"] = auto
    return rows


def qa_disagreements(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    """Confirmed matches whose BOTH-settled resolutions disagree across venues
    — a matcher bug. The builder quarantines these (override confirmed=0) and
    alerts. This is the trust-builder that replaces the missing live signal:
    auto-confirm earns wider scope only as this loop stays clean."""
    return [
        r for r in rows
        if r.get("confirmed") == 1 and r.get("resolution_agree") == "0"
    ]
