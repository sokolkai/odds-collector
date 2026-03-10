"""
История арбитража: матчинг имён, расчёт арба, обнаружение фазы,
воспроизведение таймлайна из БД.

ИСПРАВЛЕНИЯ (аудит):
  FIX 1: Убран check abs(tb1_ts - tb2_ts) > freshness_sec.
         Thrill обновляет outcomes асинхронно — 23% апдейтов одиночные.
         Heartbeat уже доказывает liveness источника.
  FIX 2: Pipeline больше НЕ требует has_arb=True на каждом тике.
         Waiting фаза: просто ждёт sharp_delay, не проверяя текущий арб.
         Tracking фаза: вычисляет profit по frozen sharp, не зависит
         от текущего has_arb. Убивается только при невалидных данных
         (stale, betting off, bad spread).
  FIX 3: Убран continue после создания pipeline — первый тик может
         сразу перейти в tracking если elapsed >= sharp_delay.
  FIX 4: Tracking фаза начинает мерить min_profit сразу (с elapsed=0),
         а не только после window_start. window_start/window_end
         теперь определяют только когда pipeline может быть закрыт.
"""
from __future__ import annotations
import bisect
import json
import logging
import sqlite3
import time
import unicodedata as _ucd
from collections import defaultdict
from typing import Dict, List, Optional, Tuple

log = logging.getLogger(__name__)

_SHARP_COMM   = 0.029
_THRILL_STALE = 180.0


# ══════════════════════════════════════════════════════════════════════════════
# ── ARB MATH ──────────────────────────────────────────────────────────────────
# ══════════════════════════════════════════════════════════════════════════════

def _bb(b1, b2):
    if not b1 or not b2 or b1 <= 1 or b2 <= 1: return None
    c  = _SHARP_COMM
    s1 = (1.0 + (b2 - 1.0) * (1.0 - c)) / b1
    profit = s1 * (b1 - 1.0) - 1.0
    total  = s1 + 1.0
    return (profit / total) * 100.0 if total > 0 else None

def _bl(back, lay):
    if not back or not lay or back <= 1 or lay <= 1: return None
    c      = _SHARP_COMM
    s_back = (lay - c) / back
    if s_back <= 0: return None
    profit = (1.0 - c) - s_back
    total  = s_back + 1.0
    return (profit / total) * 100.0 if total > 0 else None

def _calc_arbs(tb1, tb2, sb1, sl1, sb2, sl2):
    return [
        _bb(tb1, sb2),
        _bb(tb2, sb1),
        _bl(tb1, sl1),
        _bl(tb2, sl2),
    ]

def _best_arb(arbs):
    valid = [v for v in (arbs or []) if v is not None and v > -900]
    return max(valid) if valid else None


# ══════════════════════════════════════════════════════════════════════════════
# ── NAME MATCHING ─────────────────────────────────────────────────────────────
# ══════════════════════════════════════════════════════════════════════════════

def _nm_norm(s: str) -> str:
    s = _ucd.normalize("NFD", s)
    return "".join(c for c in s if _ucd.category(c) != "Mn").lower().strip()

def _nm_clean(s: str) -> str:
    return " ".join(s.replace(".", " ").split())

_NM_SUFFIXES  = {"jr", "sr", "ii", "iii", "iv"}
_NM_PARTICLES = {"de","di","du","van","von","la","le","el","al","da","dos","del","den"}

def _nm_strip_suf(surname: str) -> str:
    words = surname.split()
    while words and words[-1] in _NM_SUFFIXES:
        words.pop()
    return " ".join(words) if words else surname

def _nm_surname_from_single(raw: str) -> str:
    s = _nm_clean(_nm_norm(raw))
    if "," in s:
        return _nm_strip_suf(s.split(",")[0].strip())
    parts = s.split()
    if not parts: return s
    if len(parts) == 1: return parts[0]
    def _is_lead(tok):
        if tok in _NM_PARTICLES: return False
        t = tok.replace("-", "")
        return len(t) == 1 and t.isalpha()
    def _is_trail(tok):
        if tok in _NM_PARTICLES: return False
        if tok in _NM_SUFFIXES: return True
        t = tok.replace("-", "")
        return len(t) <= 2 and t.isalpha()
    if _is_lead(parts[0]):
        return " ".join(parts[1:])
    end = len(parts)
    while end > 1 and _is_trail(parts[end - 1]):
        end -= 1
    if end < len(parts):
        r = " ".join(parts[:end])
        if len(r) >= 2: return r
    return s

def _nm_is_pair(name: str) -> bool:
    return "/" in name or " & " in name

def _nm_pair_key(name: str) -> str:
    sep = " & " if " & " in name else "/"
    halves = [h.strip() for h in name.split(sep, 1)]
    surnames = sorted(_nm_surname_from_single(h).replace("-", " ") for h in halves)
    return "+".join(surnames)

def _extract_surname(name: str) -> str:
    name = name.strip()
    if _nm_is_pair(name): return _nm_pair_key(name)
    return _nm_surname_from_single(_nm_norm(name)).replace("-", " ")

def _nm_singles_match(a: str, b: str) -> bool:
    if a == b: return True
    a_last = a.split()[-1]; b_last = b.split()[-1]
    if a_last == b_last and len(a) != len(b) and len(a_last) >= 3: return True
    la, lb = len(a), len(b); diff = abs(la - lb)
    if diff <= 2 and min(la, lb) >= 7:
        short = a if la <= lb else b; long_ = b if la <= lb else a
        if long_.startswith(short): return True
    if diff == 1 and min(la, lb) >= 4:
        short, long_ = (a, b) if la < lb else (b, a)
        if not long_.startswith(short):
            for i in range(len(long_)):
                if long_[:i] + long_[i+1:] == short: return True
    return False

def _nm_surnames_match(a: str, b: str) -> bool:
    if a == b: return True
    ap, bp = "+" in a, "+" in b
    if ap and bp:
        pa, pb = a.split("+"), b.split("+")
        return _nm_singles_match(pa[0], pb[0]) and _nm_singles_match(pa[1], pb[1])
    if ap or bp: return False
    return _nm_singles_match(a, b)

def _match_pair(p1a: str, p2a: str, p1b: str, p2b: str) -> Tuple[bool, bool]:
    a1 = _extract_surname(p1a); a2 = _extract_surname(p2a)
    b1 = _extract_surname(p1b); b2 = _extract_surname(p2b)
    if _nm_surnames_match(a1, b1) and _nm_surnames_match(a2, b2): return True, False
    if _nm_surnames_match(a1, b2) and _nm_surnames_match(a2, b1): return True, True
    return False, False


# ══════════════════════════════════════════════════════════════════════════════
# ── PHASE DETECTION ───────────────────────────────────────────────────────────
# ══════════════════════════════════════════════════════════════════════════════

def _set_complete(h, a):
    if h == 6 and a <= 4: return True
    if a == 6 and h <= 4: return True
    if h == 7 and a in (5, 6): return True
    if a == 7 and h in (5, 6): return True
    return False

def _detect_phase(sets_json, game_home, game_away, server):
    try:
        sets = json.loads(sets_json) if sets_json else []
    except: sets = []
    if not sets: return None
    completed = [s for s in sets if _set_complete(s[0], s[1])]
    current   = next((s for s in reversed(sets) if not _set_complete(s[0], s[1])), sets[-1])
    g1, g2 = current[0], current[1]
    games_total = g1 + g2
    if g1 == 0 and g2 == 0 and (game_home == 0 or game_home is None) and (game_away == 0 or game_away is None) and completed:
        return {"phase": "SET_BREAK", "label": "Set break"}
    if g1 == 6 and g2 == 6:
        return {"phase": "TIEBREAK", "label": "Tiebreak"}
    if game_home == 40 and game_away == 40:
        return {"phase": "DEUCE", "label": "Deuce"}
    if game_home is not None and game_away is not None:
        if server == 1:
            is_bp = (game_away == 40 and game_home < 40) or (game_away == 50 and game_home == 40)
        elif server == 2:
            is_bp = (game_home == 40 and game_away < 40) or (game_home == 50 and game_away == 40)
        else:
            is_bp = False
        if is_bp:
            return {"phase": "BREAK_POINT", "label": "Break point"}
        if (game_home == 50 and game_away == 40) or (game_home == 40 and game_away == 50):
            return {"phase": "ADVANTAGE", "label": "Advantage"}
    pts_zero = (game_home == 0 or game_home is None) and (game_away == 0 or game_away is None)
    if pts_zero and games_total > 0:
        if games_total % 2 == 1:
            return {"phase": "CHANGEOVER", "label": "Changeover"}
        return {"phase": "POST_GAME", "label": "Post game"}
    return {"phase": "IN_POINT", "label": "In play"}


# ══════════════════════════════════════════════════════════════════════════════
# ── HISTORY COMPUTATION ───────────────────────────────────────────────────────
# ══════════════════════════════════════════════════════════════════════════════

_ARB_LABELS = [
    "T.P1↑ / S.P2↑",
    "S.P1↑ / T.P2↑",
    "T.P1↑ / S.P1 lay",
    "T.P2↑ / S.P2 lay",
]


def _data_valid(thrill_age, sharp_age, freshness_sec,
                s_meta, sb1, sl1, sb2, sl2,
                tb1, tb2, max_thrill_odds):
    """Проверяет, что данные свежие и валидные (без проверки арба)."""
    # Свежесть
    if thrill_age > freshness_sec or sharp_age > freshness_sec:
        return False
    # Betting
    if not s_meta.get("betting", 1):
        return False
    # Spread validity
    if sb1 is not None and sl1 is not None and sl1 <= sb1:
        return False
    if sb2 is not None and sl2 is not None and sl2 <= sb2:
        return False
    # Thrill odds cap
    tb1_ok = tb1 is not None and tb1 <= max_thrill_odds
    tb2_ok = tb2 is not None and tb2 <= max_thrill_odds
    if not tb1_ok and not tb2_ok:
        return False
    return True


def compute_history(db_path: str,
                    limit:           int   = 100,
                    min_arb:         float = 2.0,
                    sharp_delay:     float = 3.0,
                    window_start:    float = 15.0,
                    window_end:      float = 20.0,
                    cooldown:        float = 60.0,
                    freshness_sec:   float = 45.0,
                    max_thrill_odds: float = 30.0,
                    lookback_hours:  float = 0.0) -> list:
    conn = sqlite3.connect(db_path, check_same_thread=False)
    cur  = conn.cursor()

    ts_cutoff = (time.time() - lookback_hours * 3600) if lookback_hours > 0 else 0.0
    ts_filter = "AND ts >= ?" if ts_cutoff > 0 else ""
    ts_params = (ts_cutoff,) if ts_cutoff > 0 else ()

    # ── 1. Load heartbeat timelines ──────────────────────────────────────────
    cur.execute(
        f"SELECT ts, source, event_id FROM heartbeat "
        f"WHERE 1=1 {ts_filter} ORDER BY ts",
        ts_params
    )
    thrill_hb: Dict[str, list] = defaultdict(list)
    sharp_hb:  Dict[str, list] = defaultdict(list)

    for hb_ts, source, eid in cur.fetchall():
        if source == "thrill": thrill_hb[eid].append(hb_ts)
        else:                  sharp_hb[eid].append(hb_ts)

    def _last_hb_before(hb_list: list, ts: float) -> float:
        if not hb_list: return 0.0
        idx = bisect.bisect_right(hb_list, ts) - 1
        return hb_list[idx] if idx >= 0 else 0.0

    # ── 2. Load Thrill timeline ──────────────────────────────────────────────
    cur.execute(f"""
        SELECT ts, event_id, player1, player2, tournament_name, status,
               outcome_id, odds, sets_json, game_home, game_away, server
        FROM thrill
        WHERE market_id='186' AND suspended=0
          AND player1 IS NOT NULL AND player2 IS NOT NULL
          {ts_filter}
        ORDER BY ts
    """, ts_params)
    thrill_rows = cur.fetchall()

    th_changes: dict = defaultdict(dict)
    for (ts, eid, p1, p2, tname, status,
         oid, odds, sets_json, game_home, game_away, server) in thrill_rows:
        slot = th_changes[eid].setdefault(ts, {
            "p1": p1, "p2": p2, "tname": tname, "status": status,
            "sets_json": sets_json, "game_home": game_home,
            "game_away": game_away, "server": server,
        })
        slot[oid] = odds

    thrill_tl: Dict[str, list] = {}
    for eid, ts_dict in th_changes.items():
        p1_back = p2_back = None
        tl = []
        for ts in sorted(ts_dict):
            slot = ts_dict[ts]
            if "4" in slot: p1_back = slot["4"]
            if "5" in slot: p2_back = slot["5"]
            meta = {k: slot[k] for k in ("p1","p2","tname","status","sets_json",
                                          "game_home","game_away","server") if k in slot}
            tl.append((ts, p1_back, p2_back, dict(meta)))
        thrill_tl[eid] = tl

    # ── 3. Load Sharp timeline ───────────────────────────────────────────────
    cur.execute(f"""
        SELECT ts, event_id, market_id, player1, player2, status, in_play, betting_enabled,
               runner_idx, side, odds
        FROM sharp_ws
        WHERE level=0 AND market_name='Match Odds'
          AND player1 IS NOT NULL AND player2 IS NOT NULL
          {ts_filter}
        ORDER BY ts
    """, ts_params)
    sharp_rows = cur.fetchall()

    sh_changes: dict = defaultdict(dict)
    for (ts, eid, mid, p1, p2, status, in_play, betting,
         runner_idx, side, odds) in sharp_rows:
        slot = sh_changes[eid].setdefault(ts, {
            "p1": p1, "p2": p2, "status": status,
            "in_play": in_play, "betting": int(betting or 0), "mid": mid
        })
        key = f"p{runner_idx+1}_{side}"
        slot[key] = odds

    sharp_tl: Dict[str, list] = {}
    for eid, ts_dict in sh_changes.items():
        p1_back = p1_lay = p2_back = p2_lay = None
        tl = []
        for ts in sorted(ts_dict):
            slot = ts_dict[ts]
            if not slot.get("betting", 1):
                p1_back = p1_lay = p2_back = p2_lay = None
            else:
                if "p1_back" in slot: p1_back = slot["p1_back"]
                if "p1_lay"  in slot: p1_lay  = slot["p1_lay"]
                if "p2_back" in slot: p2_back = slot["p2_back"]
                if "p2_lay"  in slot: p2_lay  = slot["p2_lay"]
            meta = {k: slot[k] for k in ("p1","p2","status","in_play","betting","mid") if k in slot}
            tl.append((ts, p1_back, p1_lay, p2_back, p2_lay, dict(meta)))
        sharp_tl[eid] = tl

    conn.close()

    # ── 4. Match Thrill ↔ Sharp events ──────────────────────────────────────
    th_names: Dict[str, Tuple[str, str]] = {}
    for eid, tl in thrill_tl.items():
        if tl:
            meta = tl[-1][3]
            th_names[eid] = (meta.get("p1",""), meta.get("p2",""))

    sh_names: Dict[str, Tuple[str, str]] = {}
    for eid, tl in sharp_tl.items():
        if tl:
            meta = tl[-1][5]
            sh_names[eid] = (meta.get("p1",""), meta.get("p2",""))

    matched: List[Tuple[str, str, bool]] = []
    used_sh: set = set()
    for th_eid, (t_p1, t_p2) in th_names.items():
        if not t_p1 or not t_p2: continue
        for sh_eid, (s_p1, s_p2) in sh_names.items():
            if sh_eid in used_sh: continue
            if not s_p1 or not s_p2: continue
            ok, sw = _match_pair(t_p1, t_p2, s_p1, s_p2)
            if ok:
                matched.append((th_eid, sh_eid, sw))
                used_sh.add(sh_eid)
                break

    # ── 5. Simulate for each matched pair ─────────────────────────────────────
    history_entries: list = []

    for th_eid, sh_eid, swapped in matched:
        t_tl = thrill_tl[th_eid]
        s_tl = sharp_tl[sh_eid]

        t_hb_list = thrill_hb.get(th_eid, [])
        s_hb_list = sharp_hb.get(sh_eid, [])

        all_ts = sorted(set(r[0] for r in t_tl) | set(r[0] for r in s_tl))

        t_idx = s_idx = 0
        tb1 = tb2 = None
        sb1 = sl1 = sb2 = sl2 = None
        t_meta: dict = {}
        s_meta: dict = {}

        pipeline     = None
        cooldown_until = 0.0

        for ts in all_ts:
            while t_idx < len(t_tl) and t_tl[t_idx][0] <= ts:
                _, tb1_, tb2_, meta_ = t_tl[t_idx]
                tb1, tb2 = tb1_, tb2_
                t_meta = meta_
                t_idx += 1

            while s_idx < len(s_tl) and s_tl[s_idx][0] <= ts:
                _, sb1_, sl1_, sb2_, sl2_, meta_ = s_tl[s_idx]
                if swapped:
                    sb1, sl1, sb2, sl2 = sb2_, sl2_, sb1_, sl1_
                else:
                    sb1, sl1, sb2, sl2 = sb1_, sl1_, sb2_, sl2_
                s_meta = meta_
                s_idx += 1

            status    = t_meta.get("status", "unknown")
            sets_json = t_meta.get("sets_json")
            game_home = t_meta.get("game_home")
            game_away = t_meta.get("game_away")
            server    = t_meta.get("server")

            # Базовые проверки: live матч с данными
            if status != "live": continue
            if not sets_json or sets_json == "[]": continue
            if tb1 is None and tb2 is None: continue
            if sb1 is None and sb2 is None: continue

            # Freshness
            last_t_hb = _last_hb_before(t_hb_list, ts)
            last_s_hb = _last_hb_before(s_hb_list, ts)
            thrill_age = ts - last_t_hb if last_t_hb > 0 else freshness_sec + 1
            sharp_age  = ts - last_s_hb if last_s_hb > 0 else freshness_sec + 1

            # Thrill odds с учётом cap
            tb1_u = tb1 if (tb1 is not None and tb1 <= max_thrill_odds) else None
            tb2_u = tb2 if (tb2 is not None and tb2 <= max_thrill_odds) else None

            valid = _data_valid(thrill_age, sharp_age, freshness_sec,
                                s_meta, sb1, sl1, sb2, sl2,
                                tb1, tb2, max_thrill_odds)

            # ── FIX 2: Текущий арб проверяется только для СОЗДАНИЯ pipeline ──
            arbs    = _calc_arbs(tb1_u, tb2_u, sb1, sl1, sb2, sl2)
            best_v  = _best_arb(arbs)
            has_arb = best_v is not None and best_v >= min_arb

            if ts < cooldown_until:
                continue

            # ── Pipeline не существует ────────────────────────────────────────
            if pipeline is None:
                # Создаём только если данные валидны И арб есть
                if not valid or not has_arb:
                    continue
                arb_type_idx = 0
                arb_type_val = None
                for _i, _v in enumerate(arbs):
                    if _v is not None and (arb_type_val is None or _v > arb_type_val):
                        arb_type_val = _v
                        arb_type_idx = _i
                pipeline = {
                    "detected_ts":        ts,
                    "phase":              "waiting",
                    "frozen_sharp":       None,
                    "arb_type_idx":       arb_type_idx,
                    "min_profit":         None,
                    "min_arbs":           None,
                    "min_snap":           None,
                    "detected_sets_json": sets_json,
                    "last_thrill_age":    thrill_age,
                    "last_sharp_age":     sharp_age,
                }
                # FIX 3: не делаем continue — позволяем сразу перейти в tracking

            # ── Pipeline существует ───────────────────────────────────────────
            elapsed = ts - pipeline["detected_ts"]

            # Невалидные данные убивают pipeline
            if not valid:
                # Но если tracking и есть результат — эмитим
                if (pipeline["phase"] == "tracking"
                        and pipeline.get("frozen_sharp")
                        and pipeline.get("min_profit") is not None
                        and pipeline["min_profit"] > 0):
                    history_entries.append(
                        _make_entry(pipeline, ts, th_eid, sh_eid, t_meta,
                                    thrill_age, sharp_age))
                    cooldown_until = ts + cooldown
                pipeline = None
                continue

            # ── Waiting phase ─────────────────────────────────────────────────
            if pipeline["phase"] == "waiting":
                if elapsed >= sharp_delay:
                    pipeline["phase"]        = "tracking"
                    pipeline["frozen_sharp"] = {"p1_back": sb1, "p1_lay": sl1,
                                                 "p2_back": sb2, "p2_lay": sl2}
                else:
                    continue

            # ── Tracking phase ────────────────────────────────────────────────
            if pipeline["phase"] == "tracking" and pipeline["frozen_sharp"]:
                pipeline["last_thrill_age"] = thrill_age
                pipeline["last_sharp_age"]  = sharp_age

                # FIX 4: Вычисляем profit по frozen sharp на КАЖДОМ тике
                fs = pipeline["frozen_sharp"]
                arbs_f = _calc_arbs(tb1_u, tb2_u,
                                    fs.get("p1_back"), fs.get("p1_lay"),
                                    fs.get("p2_back"), fs.get("p2_lay"))
                profit_f = arbs_f[pipeline["arb_type_idx"]]
                if (profit_f is not None and
                        (pipeline["min_profit"] is None or profit_f < pipeline["min_profit"])):
                    pipeline["min_profit"] = profit_f
                    pipeline["min_arbs"]   = arbs_f
                    pipeline["min_snap"]   = _snap(ts, t_meta, tb1_u, tb2_u,
                                                   sb1, sl1, sb2, sl2,
                                                   arbs_f, sets_json,
                                                   game_home, game_away, server)

                if elapsed >= window_end:
                    if (pipeline.get("min_profit") is not None and
                            pipeline["min_profit"] > 0):
                        history_entries.append(
                            _make_entry(pipeline, ts, th_eid, sh_eid, t_meta,
                                        thrill_age, sharp_age))
                    cooldown_until = ts + cooldown
                    pipeline = None

        # Закрытие оставшегося pipeline в конце
        if pipeline is not None and pipeline["phase"] == "tracking":
            if (pipeline.get("min_profit") is not None and
                    pipeline["min_profit"] > 0):
                last_ts = all_ts[-1] if all_ts else 0
                history_entries.append(
                    _make_entry(pipeline, last_ts, th_eid, sh_eid, t_meta,
                                pipeline.get("last_thrill_age", 0),
                                pipeline.get("last_sharp_age", 0)))

    history_entries.sort(key=lambda e: e["recorded_ts"], reverse=True)
    return history_entries[:limit]


def _snap(ts, t_meta, tb1, tb2, sb1, sl1, sb2, sl2,
          arbs, sets_json, game_home, game_away, server):
    try:
        ps_raw = json.loads(sets_json) if sets_json else []
    except: ps_raw = []
    period_scores = [{"home_score": s[0], "away_score": s[1]} for s in ps_raw]
    score = {
        "period_scores":  period_scores,
        "home_gamescore": game_home,
        "away_gamescore": game_away,
        "current_server": server,
    } if period_scores else {}
    phase = _detect_phase(sets_json, game_home, game_away, server)
    return {
        "player1":        t_meta.get("p1", ""),
        "player2":        t_meta.get("p2", ""),
        "tournament":     t_meta.get("tname", ""),
        "score":          score,
        "score_ts":       ts,
        "thrill_p1_back": tb1,
        "thrill_p2_back": tb2,
        "phase":          phase,
    }


def _make_entry(pipeline, recorded_ts, th_eid, sh_eid, t_meta,
                thrill_age: float, sharp_age: float):
    arbs = pipeline.get("min_arbs") or [None, None, None, None]
    arb_type_idx = pipeline.get("arb_type_idx", 0)
    score_changed = (pipeline.get("detected_sets_json") is not None and
                     t_meta.get("sets_json") != pipeline.get("detected_sets_json"))
    return {
        "snap":           pipeline["min_snap"],
        "frozen_sharp":   pipeline["frozen_sharp"],
        "arbs":           arbs,
        "profit":         pipeline.get("min_profit"),
        "recorded_ts":    recorded_ts,
        "thrill_age":     round(thrill_age, 1),
        "sharp_age":      round(sharp_age, 1),
        "arb_type":       _ARB_LABELS[arb_type_idx],
        "arb_type_idx":   arb_type_idx,
        "score_changed":  score_changed,
        "th_event_id":    th_eid,
        "sh_event_id":    sh_eid,
    }


# ── History JSON response builder ─────────────────────────────────────────────
def build_history_response(db_path: str, params: dict) -> dict:
    import time as _time
    t0 = _time.time()
    try:
        entries = compute_history(
            db_path,
            limit           = int(params.get("limit",            100)),
            min_arb         = float(params.get("min_arb",         2.0)),
            sharp_delay     = float(params.get("sharp_delay",     3.0)),
            window_start    = float(params.get("window_start",   15.0)),
            window_end      = float(params.get("window_end",     20.0)),
            cooldown        = float(params.get("cooldown",       60.0)),
            freshness_sec   = float(params.get("freshness_sec",  45.0)),
            max_thrill_odds = float(params.get("max_thrill_odds",30.0)),
            lookback_hours  = float(params.get("lookback_hours",  0.0)),
        )
    except Exception as exc:
        log.error("history compute error: %s", exc, exc_info=True)
        entries = []

    try:
        conn = sqlite3.connect(db_path, check_same_thread=False)
        cur  = conn.cursor()
        t_rows  = cur.execute("SELECT COUNT(*) FROM thrill").fetchone()[0]
        s_rows  = cur.execute("SELECT COUNT(*) FROM sharp_ws").fetchone()[0]
        hb_rows = cur.execute("SELECT COUNT(*) FROM heartbeat").fetchone()[0]
        t_min, t_max = cur.execute("SELECT MIN(ts),MAX(ts) FROM thrill").fetchone()
        conn.close()
        time_range = ""
        if t_min and t_max:
            import datetime as _dt
            tfmt = lambda ts: _dt.datetime.fromtimestamp(ts).strftime("%Y-%m-%d %H:%M")
            time_range = f"{tfmt(t_min)} → {tfmt(t_max)}"
    except:
        t_rows = s_rows = hb_rows = 0
        time_range = ""

    try:
        conn2 = sqlite3.connect(db_path, check_same_thread=False)
        cur2  = conn2.cursor()
        thrill_eids = {r[0] for r in cur2.execute(
            "SELECT DISTINCT event_id FROM thrill WHERE player1 IS NOT NULL").fetchall()}
        sharp_eids  = {r[0] for r in cur2.execute(
            "SELECT DISTINCT event_id FROM sharp_ws WHERE player1 IS NOT NULL AND market_name='Match Odds'").fetchall()}
        conn2.close()
    except:
        thrill_eids = sharp_eids = set()

    return {
        "history": entries,
        "stats": {
            "thrill_events":  len(thrill_eids),
            "sharp_events":   len(sharp_eids),
            "matched_pairs":  len({e["th_event_id"] for e in entries}),
            "thrill_rows":    t_rows,
            "sharp_rows":     s_rows,
            "hb_rows":        hb_rows,
            "total_rows":     t_rows + s_rows,
            "db_time_range":  time_range,
            "computed_in_ms": round((_time.time() - t0) * 1000),
        }
    }