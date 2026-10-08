"""Cross-venue structural matcher: alias matching, two-tier confirmation
(auto-confirm precision), doubleheader skip, post-settlement QA disagreement,
and Kalshi orderbook top-of-book normalization."""

from decimal import Decimal

from pmkt.crosswalk import (
    apply_confirmation,
    match_crosswalk,
    parse_event_ticker,
    qa_disagreements,
)
from pmkt.model.decide import _kalshi_best_quote


def _kalshi_game(event, ticker_suffixes, *, status="open", result="",
                 series="KXMLBGAME"):
    return [
        {"series": series, "event": event, "ticker": f"{event}-{suf}",
         "title": f"Will {suf} win", "status_norm": status,
         "result": result if suf == ticker_suffixes[0] else
                   ("no" if result == "yes" else "yes" if result else ""),
         "close_ts": 1}
        for suf in ticker_suffixes
    ]


def _pm_market(cid, slug, outcomes, *, status="open", title="x"):
    """outcomes: list of (oi, token, name, is_primary)."""
    return [
        {"cid": cid, "slug": slug, "title": title, "status_norm": status,
         "tok": tok, "oi": oi, "oname": name, "is_primary": prim}
        for (oi, tok, name, prim) in outcomes
    ]


def test_parse_event_ticker_split_and_doubleheader():
    # away/home split found via the market-suffix set (variable code lengths)
    assert parse_event_ticker(
        "KXMLBGAME-26JUL20DETPHI", {"DET", "PHI"}
    ) == ("2026-07-20", "DET", "PHI", False)
    # World Cup 3-letter codes
    assert parse_event_ticker(
        "KXWCGAME-26JUL20ARGSUI", {"ARG", "SUI"}
    ) == ("2026-07-20", "ARG", "SUI", False)
    # doubleheader flagged
    assert parse_event_ticker(
        "KXMLBGAME-26JUL20DETPHIG2", {"DET", "PHI"}
    ) == ("2026-07-20", "DET", "PHI", True)
    # unsplittable (only one side present) → None
    assert parse_event_ticker("KXMLBGAME-26JUL20DETPHI", {"DET"}) is None


def test_mlb_alias_match_autoconfirms():
    # CHW -> cws alias; both sides map 1:1 -> auto-confirm
    kalshi = _kalshi_game("KXMLBGAME-26JUL20CHWNYY", ["CHW", "NYY"])
    pm = _pm_market("0xcwsnyy", "mlb-cws-nyy-2026-07-20",
                    [(0, "tokCWS", "CWS", 1), (1, "tokNYY", "NYY", 0)])
    rows = match_crosswalk(kalshi, pm, {})
    apply_confirmation(rows, {})
    assert len(rows) == 2
    chw = next(r for r in rows if r["kalshi_side"] == "CHW")
    assert chw["pm_token_id"] == "tokCWS" and chw["pm_outcome_index"] == 0
    assert chw["match_method"] == "slug-code"
    assert all(r["confirmed"] == 1 and r["auto_confirmed"] == 1 for r in rows)


def test_wc_and_tennis_are_candidates_not_autoconfirmed():
    # World Cup (3-way / draw) — slug-code but NOT an auto-confirm sport
    wc_k = _kalshi_game("KXWCGAME-26JUL20ARGSUI", ["ARG", "SUI", "TIE"],
                        series="KXWCGAME")
    wc_pm = (
        _pm_market("0xarg", "fifwc-arg-che-2026-07-20-arg",
                   [(0, "tokY", "Yes", 1), (1, "tokN", "No", 0)])
        + _pm_market("0xche", "fifwc-arg-che-2026-07-20-che",
                     [(0, "tokY2", "Yes", 1), (1, "tokN2", "No", 0)])
        + _pm_market("0xdraw", "fifwc-arg-che-2026-07-20-draw",
                     [(0, "tokY3", "Yes", 1), (1, "tokN3", "No", 0)])
    )
    rows = match_crosswalk(wc_k, wc_pm, {})
    apply_confirmation(rows, {})
    assert rows, "WC should still MATCH structurally"
    assert all(r["match_method"] == "slug-code" for r in rows)
    assert all(r["auto_confirmed"] == 0 and r["confirmed"] == 0 for r in rows),\
        "WC is candidate-only until manually confirmed"


def test_override_confirms_a_candidate():
    # Both team suffixes must be present for the away/home code split; PM slug
    # uses the aliased home code (SUI -> che).
    wc_k = _kalshi_game("KXWCGAME-26JUL20ARGSUI", ["ARG", "SUI", "TIE"],
                        series="KXWCGAME")
    wc_pm = _pm_market("0xarg", "fifwc-arg-che-2026-07-20-arg",
                       [(0, "tokY", "Yes", 1), (1, "tokN", "No", 0)])
    rows = match_crosswalk(wc_k, wc_pm, {})
    assert rows and all(r["confirmed"] == 0 for r in
                        apply_confirmation(rows, {}))  # candidate by default
    key = (rows[0]["kalshi_ticker"], rows[0]["pm_token_id"])
    apply_confirmation(rows, {key: 1})  # human confirm
    assert rows[0]["confirmed"] == 1 and rows[0]["auto_confirmed"] == 0


def test_qa_disagreement_flagged_and_quarantined():
    # Kalshi says CHW won (yes), PM resolution says index 1 (NYY) won → disagree
    kalshi = _kalshi_game("KXMLBGAME-26JUL20CHWNYY", ["CHW", "NYY"],
                          status="settled", result="yes")
    pm = _pm_market("0xcwsnyy", "mlb-cws-nyy-2026-07-20",
                    [(0, "tokCWS", "CWS", 1), (1, "tokNYY", "NYY", 0)],
                    status="settled")
    rows = match_crosswalk(kalshi, pm, {"0xcwsnyy": (1, "NYY")})
    apply_confirmation(rows, {})
    assert all(r["resolution_agree"] == "0" for r in rows)
    qa = qa_disagreements(rows)
    assert len(qa) == 2
    # quarantine → confirmed flips to 0
    overrides = {(r["kalshi_ticker"], r["pm_token_id"]): 0 for r in qa}
    apply_confirmation(rows, overrides)
    assert all(r["confirmed"] == 0 for r in rows)


def test_qa_agreement_when_both_settle_consistently():
    kalshi = _kalshi_game("KXMLBGAME-26JUL20CHWNYY", ["CHW", "NYY"],
                          status="settled", result="yes")  # CHW won
    pm = _pm_market("0xcwsnyy", "mlb-cws-nyy-2026-07-20",
                    [(0, "tokCWS", "CWS", 1), (1, "tokNYY", "NYY", 0)],
                    status="settled")
    rows = match_crosswalk(kalshi, pm, {"0xcwsnyy": (0, "CWS")})  # index 0 won
    apply_confirmation(rows, {})
    assert all(r["resolution_agree"] == "1" for r in rows)
    assert qa_disagreements(rows) == []


def test_window_filters_out_of_range_dates():
    kalshi = _kalshi_game("KXMLBGAME-26JUL20DETPHI", ["DET", "PHI"])
    pm = _pm_market("0xdetphi", "mlb-det-phi-2026-07-20",
                    [(0, "tokDET", "DET", 1), (1, "tokPHI", "PHI", 0)])
    assert match_crosswalk(kalshi, pm, {}, min_date="2026-08-01") == []
    assert len(match_crosswalk(kalshi, pm, {}, min_date="2026-07-01",
                               max_date="2026-07-31")) == 2


def test_kalshi_best_quote_normalization():
    # bids-only, ascending, best bid LAST; yes_ask = 1 − best_no_bid
    payload = {"orderbook_fp": {
        "yes_dollars": [["0.47", "100"], ["0.48", "68.75"]],
        "no_dollars": [["0.50", "10"], ["0.51", "560468.22"]],
    }}
    q = _kalshi_best_quote(payload)
    assert q["bid"] == Decimal("0.48")           # best (last) yes bid
    assert q["ask"] == Decimal(1) - Decimal("0.51")  # 1 − best no bid = 0.49
    assert q["bid_size"] == Decimal("68.75")
    assert q["ask_size"] == Decimal("560468.22")  # depth resting at the ask


def test_kalshi_best_quote_empty_book():
    q = _kalshi_best_quote({"orderbook_fp": {"yes_dollars": [], "no_dollars": []}})
    assert q == {"bid": None, "ask": None, "bid_size": None, "ask_size": None}
    assert _kalshi_best_quote({})["bid"] is None
