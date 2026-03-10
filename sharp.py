"""
SharpCollector — обрабатывает входящие данные от Sharp (WS и score) и пишет в БД.
"""
from __future__ import annotations
import json
import time
import logging
from typing import Any, Dict, Tuple

from config import _SHARP_KEEP_MARKETS, SHARP_MAX_LEVEL
from db import BatchWriter, HBTracker
from logger import _log

log = logging.getLogger(__name__)


class SharpCollector:
    def __init__(self, writer: BatchWriter, hb: HBTracker):
        self._writer = writer
        self._hb     = hb
        self._market_runners: Dict[str, Dict[str, str]] = {}
        self._event_names:    Dict[str, Tuple[str, str]] = {}
        self._dedup_ws: dict = {}
        self._dedup_sc: dict = {}

    def process_ws(self, raw: Any):
        if not isinstance(raw, dict): return
        ts = raw.get("ts", time.time() * 1000)
        if ts > 1e12: ts /= 1000.0
        try:
            payload = json.loads(raw["raw"])
        except: return

        mdef = payload.get("marketDefinition")
        if isinstance(mdef, dict):
            mid = str(payload.get("id", ""))
            runners = mdef.get("runners") or []
            if mid and runners: self._index_runners(mid, runners)

        market_name = payload.get("marketNameWithParents", "")
        event_name  = payload.get("mainEventName", "")
        rc          = payload.get("rc", [])
        mid         = str(payload.get("id", ""))
        event_id    = str(payload.get("mainEventId", ""))

        # ── FIX D: SHARP MARKET FILTER ───────────────────────────────────────
        # АНАЛИЗ БД: проникали horse racing ("2m3f Hrd - Win"), предматчевые
        # фикстуры ("Fixtures 10 Mar - Atalanta v Bayern Munich - Match Odds").
        # Они никогда не матчатся с теннисными событиями Thrill.
        # Пустой market_name пропускаем — это может быть init-пакет без имени.
        if market_name and market_name not in _SHARP_KEEP_MARKETS:
            return
        # ─────────────────────────────────────────────────────────────────────

        betting     = payload.get("bettingEnabled", False)
        inplay      = payload.get("img", False)
        status      = "live" if betting and inplay else ("prematch" if betting else "suspended")
        st          = payload.get("mainEventStartTime")
        if st and st > 1e12: st /= 1000.0
        comp = payload.get("competition")
        tournament = comp.get("name", "") if isinstance(comp, dict) else ""
        overround   = payload.get("overround")
        underround  = payload.get("underround")
        try: market_volume = float(payload.get("tv") or 0) or None
        except: market_volume = None

        p1, p2 = self._names(event_id, event_name)
        if not rc: return

        by_id   = {str(r["id"]): r for r in rc}
        mapping = self._market_runners.get(mid)
        if mapping and len(rc) >= 2:
            ordered = [
                by_id.get(mapping["p1"]) or rc[0],
                by_id.get(mapping["p2"]) or rc[1],
            ]
        else:
            ordered = rc[:2]

        base = {
            "ts": ts, "event_id": event_id, "market_id": mid,
            "player1": p1 or None, "player2": p2 or None,
            "tournament": tournament or None,
            "status": status,
            "betting_enabled": int(betting),
            "in_play": int(inplay),
            "market_name": market_name,
            "start_time": st,
            "overround": overround,
            "underround": underround,
            "market_volume": market_volume,
        }

        count    = 0
        log_best = {}
        for idx, runner in enumerate(ordered):
            rid = runner.get("id")
            try: rvol = float(runner.get("tv") or 0) or None
            except: rvol = None
            for side, key in [("back", "bdatb"), ("lay", "bdatl")]:
                for level_entry in (runner.get(key) or []):
                    try:
                        odds_val   = float(level_entry["odds"])
                        amount_val = float(level_entry["amount"])
                        level_idx  = int(level_entry["index"])
                    except (KeyError, ValueError, TypeError):
                        continue

                    if level_idx > SHARP_MAX_LEVEL:
                        continue

                    dk = (event_id, mid, idx, side, level_idx)
                    if self._dedup_ws.get(dk) == (odds_val, amount_val):
                        continue
                    self._dedup_ws[dk] = (odds_val, amount_val)

                    self._writer.write("sharp_ws", {
                        **base,
                        "runner_id":     rid,
                        "runner_idx":    idx,
                        "runner_volume": rvol,
                        "side":          side,
                        "level":         level_idx,
                        "odds":          odds_val,
                        "amount":        amount_val,
                    })
                    count += 1
                    if level_idx == 0:
                        log_best[f"p{idx+1}_{side}"] = odds_val

        if event_id:
            self._hb.ping("sharp", event_id, ts)

        if count:
            names = f"{p1} v {p2}" if p1 else f"eid={event_id}"
            _log("sharp", f"{names} | mkt={market_name} | {status} | "
                          f"p1={log_best.get('p1_back')}/{log_best.get('p1_lay')} "
                          f"p2={log_best.get('p2_back')}/{log_best.get('p2_lay')} | "
                          f"{count} levels written")

    def process_score(self, raw: Any):
        if not isinstance(raw, dict): return
        ts = raw.get("ts", time.time() * 1000)
        if ts > 1e12: ts /= 1000.0
        try:
            payload = json.loads(raw.get("raw", "{}"))
        except:
            return

        updates = payload.get("EVENT_INFO_UPDATES") or []
        if not updates:
            updates = [payload] if payload.get("eventId") else []

        for upd in updates:
            eid  = str(upd.get("eventId", ""))
            sc   = upd.get("score") or {}
            home = sc.get("home") or {}
            away = sc.get("away") or {}

            def get_name(side):
                ln = side.get("localizedNames") or {}
                return ln.get("global|en") or side.get("name") or ""

            p1 = get_name(home).strip()
            p2 = get_name(away).strip()
            if eid and p1 and p2:
                self._event_names[eid] = (p1, p2)

            p1_hl = home.get("highlight", False)
            p2_hl = away.get("highlight", False)
            if p1_hl:   server = 1
            elif p2_hl: server = 2
            else:       server = None

            def to_int(v):
                try: return int(v)
                except: return None

            row = {
                "ts":       ts,
                "event_id": eid,
                "player1":  p1 or None,
                "player2":  p2 or None,
                "p1_score": home.get("score"),
                "p2_score": away.get("score"),
                "p1_games": to_int(home.get("games")),
                "p2_games": to_int(away.get("games")),
                "p1_sets":  to_int(home.get("sets")),
                "p2_sets":  to_int(away.get("sets")),
                "server":   server,
                "period":   str(upd.get("period")) if upd.get("period") is not None else None,
                "status":   str(upd.get("status")) if upd.get("status") is not None else None,
            }

            sig = (row["p1_score"], row["p2_score"], row["p1_games"],
                   row["p2_games"], row["p1_sets"], row["p2_sets"],
                   row["server"], row["period"], row["status"])
            if self._dedup_sc.get(eid) == sig:
                continue
            self._dedup_sc[eid] = sig

            self._writer.write("sharp_score", row)
            _log("sharp-score",
                 f"eid={eid} | {p1} v {p2} | "
                 f"score={home.get('score')}:{away.get('score')} "
                 f"games={home.get('games')}:{away.get('games')} "
                 f"sets={home.get('sets')}:{away.get('sets')} "
                 f"srv={server}")

    def _names(self, eid: str, event_name: str) -> Tuple[str, str]:
        cached = self._event_names.get(eid)
        if cached: return cached
        parts = event_name.split(" v ")
        if len(parts) == 2:
            p1, p2 = parts[0].strip(), parts[1].strip()
            self._event_names[eid] = (p1, p2)
            return p1, p2
        return "", ""

    def _index_runners(self, mid: str, runners: list):
        ordered = sorted(
            [r for r in runners if isinstance(r, dict)],
            key=lambda r: r.get("sortPriority", r.get("sort", 99))
        )
        if len(ordered) < 2: return
        p1 = str(ordered[0].get("selectionId", ordered[0].get("id", "")))
        p2 = str(ordered[1].get("selectionId", ordered[1].get("id", "")))
        if p1 and p2: self._market_runners[mid] = {"p1": p1, "p2": p2}
