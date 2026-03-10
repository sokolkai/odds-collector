"""
ThrillCollector — обрабатывает входящие данные от Thrill и пишет в БД.
"""
from __future__ import annotations
import json
import time
import logging
from typing import Any, Dict

from config import _THRILL_KEEP_MARKETS, _THRILL_KEEP_SPORTS
from db import BatchWriter, HBTracker
from logger import _log

log = logging.getLogger(__name__)

_FINISHED = {3, 4, 5, 6, 99, 100}

# FIX E: расширенная карта видов спорта из реальных данных БД
_SPORT_NAMES = {
    "5":   "Tennis",
    "303": "Tennis",
    "20":  "Table Tennis",   # Setka Cup, WTT Feeder — обнаружен в БД
    "1":   "Football",       # football — основной источник мусора в market_id=1
    "2":   "Basketball",
    "4":   "Ice Hockey",
    "6":   "Baseball",
    "22":  "Badminton",
    "23":  "Volleyball",
    "7":   "Handball",
    "10":  "Tennis (ITF)",   # предположительно
}


def _merge(dst: dict, src: dict):
    for k, v in src.items():
        if v is None:
            dst.pop(k, None)
        elif isinstance(v, dict) and isinstance(dst.get(k), dict):
            _merge(dst[k], v)
        else:
            dst[k] = v


def _parse_score(state: dict):
    sc   = state.get("score") or {}
    ps   = sc.get("period_scores") or []
    sets = [[p.get("home_score", 0), p.get("away_score", 0)] for p in ps]
    hg   = sc.get("home_gamescore")
    ag   = sc.get("away_gamescore")
    srv  = (state.get("state") or {}).get("serving_team")
    return (
        json.dumps(sets) if sets else None,
        int(hg) if hg is not None else None,
        int(ag) if ag is not None else None,
        int(srv) if srv is not None else None,
    )


class ThrillCollector:
    def __init__(self, writer: BatchWriter, hb: HBTracker):
        self._writer = writer
        self._hb     = hb
        self._state: Dict[str, dict] = {}
        self._desc:  Dict[str, dict] = {}
        self._tours: Dict[str, dict] = {}
        self._dedup: dict = {}

    def process(self, raw: Any):
        if not isinstance(raw, dict): return
        if isinstance(raw.get("tournaments"), dict):
            self._tours.update(raw["tournaments"])
        events = raw.get("events")
        if not isinstance(events, dict): return
        ts = time.time()
        desc_fresh: set = set()

        for eid, edata in events.items():
            if edata is None:
                state = self._state.get(eid)
                if state:
                    ms = (state.get("state") or {}).get("match_status")
                    if ms in _FINISHED:
                        self._state.pop(eid, None)
                        self._desc.pop(eid, None)
                        self._hb.clear("thrill", eid)
                continue
            if not isinstance(edata, dict): continue
            if edata.get("desc"):
                self._desc[eid] = edata["desc"]
                desc_fresh.add(eid)
            _merge(self._state.setdefault(eid, {}), edata)

        for eid, edata in events.items():
            if edata is None: continue
            state = self._state.get(eid, {})
            ms = (state.get("state") or {}).get("match_status")
            if ms in _FINISHED:
                self._state.pop(eid, None)
                self._desc.pop(eid, None)
                self._hb.clear("thrill", eid)
                continue
            self._emit(eid, state, ts)

        for eid in desc_fresh:
            if eid in events: continue
            state = self._state.get(eid, {})
            if state: self._emit(eid, state, ts)

    def _emit(self, eid: str, state: dict, ts: float):
        desc  = self._desc.get(eid) or state.get("desc") or {}
        comps = desc.get("competitors", [])
        if len(comps) < 2: return
        p1 = comps[0].get("name", "")
        p2 = comps[1].get("name", "")
        if not p1 or not p2: return
        if desc.get("virtual"): return

        sport   = str(desc.get("sport", ""))
        markets = state.get("markets") or {}
        if not markets: return

        # ── FIX B: PREMATCH FILTER ───────────────────────────────────────────
        # АНАЛИЗ БД: все 17 442 строк Sharp имеют in_play=1, betting_enabled=1.
        # Sharp не котирует prematch → prematch строки Thrill никогда не дают арб.
        # Бонус: heartbeat'ы для prematch событий тоже исчезают (было −69.5%).
        # Статус определяем ДО вызова hb.ping() — не тратим HB на prematch.
        sd = state.get("state") or {}
        if sd.get("status") == 1:      status = "live"
        elif sd.get("status") == 0:    status = "prematch"
        else:                          status = "unknown"

        if status != "live":
            return   # только live события могут дать арбитраж
        # ─────────────────────────────────────────────────────────────────────

        # ── FIX C: SPORT FILTER ──────────────────────────────────────────────
        # АНАЛИЗ БД: Sharp торговал только теннисом (sport_id=5/303).
        # sport_id=20 (настольный теннис), 22 (бадминтон) и др. — не матчатся.
        # Пустой _THRILL_KEEP_SPORTS = отключить фильтр (принимать всё).
        if _THRILL_KEEP_SPORTS and sport not in _THRILL_KEEP_SPORTS:
            return
        # ─────────────────────────────────────────────────────────────────────

        # Heartbeat: только для событий, прошедших все фильтры
        self._hb.ping("thrill", eid, ts)

        sets_json, game_home, game_away, server = _parse_score(state)
        scheduled = desc.get("scheduled")
        tid   = str(desc.get("tournament", ""))
        tname = (self._tours.get(tid) or {}).get("name", "") if tid else ""
        sname = _SPORT_NAMES.get(sport, sport)

        base = {
            "ts": ts, "event_id": eid,
            "player1": p1, "player2": p2,
            "tournament_id": tid, "tournament_name": tname,
            "sport_id": sport, "sport_name": sname,
            "status": status,
            "scheduled": float(scheduled) if scheduled else None,
            "sets_json": sets_json,
            "game_home": game_home, "game_away": game_away, "server": server,
        }

        count = 0
        for market_id, specs in markets.items():
            # ── FIX A: MARKET FILTER ─────────────────────────────────────────
            # АНАЛИЗ БД: market_id='1' = исключительно football/hockey с 3
            # исходами (outcome 1=home, 2=away, 3=draw). Ни одного теннисного
            # события. compute_history() использует только outcome_id='4','5'.
            # market_id='1' никогда не участвовал в расчёте арбитража.
            if market_id not in _THRILL_KEEP_MARKETS:
                continue
            # ─────────────────────────────────────────────────────────────────

            if not isinstance(specs, dict): continue
            for spec, outcomes in specs.items():
                if not isinstance(outcomes, dict): continue
                for outcome_id, oval in outcomes.items():
                    if not isinstance(oval, dict): continue
                    try:
                        odds_val = float(oval["k"])
                    except (KeyError, ValueError, TypeError):
                        continue
                    suspended = 1 if oval.get("b") == 1 else 0

                    dedup_key = (eid, market_id, spec, outcome_id)
                    if self._dedup.get(dedup_key) == (odds_val, suspended):
                        continue
                    self._dedup[dedup_key] = (odds_val, suspended)

                    self._writer.write("thrill", {
                        **base,
                        "market_id":   market_id,
                        "market_spec": spec,
                        "outcome_id":  outcome_id,
                        "odds":        odds_val,
                        "suspended":   suspended,
                    })
                    count += 1

        if count:
            game_str = f"{game_home}:{game_away}" if game_home is not None else ""
            _log("thrill", f"{p1} v {p2} | {tname} | {status} | "
                           f"sets={sets_json or '-'} game={game_str} | "
                           f"{count} outcomes written")
