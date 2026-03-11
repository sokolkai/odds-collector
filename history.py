"""
История арбитража: матчинг имён, расчёт арба, воспроизведение таймлайна из БД.

ИСПРАВЛЕНИЯ (относительно предыдущей версии):
─────────────────────────────────────────────
1. CARRY-FORWARD вместо инварианта пакета.
   Thrill и Sharp шлют дельта-обновления: один исход / один runner за раз.
   Старый «инвариант пакета» выбрасывал 20% Thrill-тиков и 55% Sharp-тиков.
   Теперь несём последнее известное значение по каждому полю независимо —
   точно так же, как это делает RunnerOdds.set_back/set_lay в сканере.

2. SHARP LAG-БУФЕР (аналог SharpBuffer в сканере).
   Новое значение Sharp-коэффициента принимается как «стабильное» только
   если оно держится ≥ sharp_lag секунд без изменений.
   Защищает от ложных вилок на мгновенных флуктуациях стакана.

3. THRILL STALE GUARD (аналог _THRILL_STALE = 180 s в сканере).
   Если heartbeat Thrill был > max_thrill_stale секунд назад — коэффициенты
   сбрасываются (None). Это исключает вилки типа Nava/Mmoh (+100%) и
   Hayasaka/Kuramochi (~50%), где THRILL завис на старых котировках.

4. FRESHNESS по heartbeat теперь применяется к ОБОИМ источникам независимо,
   без объединённого флага valid — точно как в engine/core.py.

5. SPREAD GUARD для Sharp вынесен в build-фазу: пакет с lay≤back пропускается
   по каждому полю независимо (back/lay могут прийти в разных тиках).

6. window_start убран (не использовался в расчёте — только window_end).
   Profit берётся как МИНИМУМ за [0, window_end], что консервативно.
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

_SHARP_COMM = 0.029

_ARB_LABELS = [
    "T.P1↑ / S.P2↑",
    "S.P1↑ / T.P2↑",
    "T.P1↑ / S.P1 lay",
    "T.P2↑ / S.P2 lay",
]

# Максимальный возраст Thrill-данных: если heartbeat старше — коэффициент не используется.
# Аналог _THRILL_STALE = 180 в engine/core.py. Устраняет класс ошибок типа
# Nava/Mmoh (+100%) и Hayasaka (+50%), где THRILL завис после разрыва соединения.
_THRILL_STALE_SEC = 180.0


# ══════════════════════════════════════════════════════════════════════════════
# ARB MATH  (без изменений)
# ══════════════════════════════════════════════════════════════════════════════

def _bb(b1: Optional[float], b2: Optional[float]) -> Optional[float]:
    if not b1 or not b2 or b1 <= 1 or b2 <= 1:
        return None
    c  = _SHARP_COMM
    s1 = (1.0 + (b2 - 1.0) * (1.0 - c)) / b1
    profit = s1 * (b1 - 1.0) - 1.0
    total  = s1 + 1.0
    return (profit / total) * 100.0 if total > 0 else None


def _bl(back: Optional[float], lay: Optional[float]) -> Optional[float]:
    if not back or not lay or back <= 1 or lay <= 1:
        return None
    c      = _SHARP_COMM
    s_back = (lay - c) / back
    if s_back <= 0:
        return None
    profit = (1.0 - c) - s_back
    total  = s_back + 1.0
    return (profit / total) * 100.0 if total > 0 else None


def _calc_arbs(tb1, tb2, sb1, sl1, sb2, sl2) -> List[Optional[float]]:
    return [
        _bb(tb1, sb2),
        _bb(tb2, sb1),
        _bl(tb1, sl1),
        _bl(tb2, sl2),
    ]


def _best_arb(arbs) -> Optional[float]:
    valid = [v for v in arbs if v is not None and v > -900]
    return max(valid) if valid else None


# ══════════════════════════════════════════════════════════════════════════════
# NAME MATCHING  (без изменений)
# ══════════════════════════════════════════════════════════════════════════════

def _nm_norm(s):
    s = _ucd.normalize("NFD", s)
    return "".join(c for c in s if _ucd.category(c) != "Mn").lower().strip()

def _nm_clean(s):
    return " ".join(s.replace(".", " ").split())

_NM_SUFFIXES  = {"jr","sr","ii","iii","iv"}
_NM_PARTICLES = {"de","di","du","van","von","la","le","el","al","da","dos","del","den"}

def _nm_strip_suf(s):
    w = s.split()
    while w and w[-1] in _NM_SUFFIXES: w.pop()
    return " ".join(w) if w else s

def _nm_surname_from_single(raw):
    s = _nm_clean(_nm_norm(raw))
    if "," in s: return _nm_strip_suf(s.split(",")[0].strip())
    parts = s.split()
    if not parts: return s
    if len(parts) == 1: return parts[0]
    def _lead(t):
        if t in _NM_PARTICLES: return False
        return len(t.replace("-","")) == 1 and t.replace("-","").isalpha()
    def _trail(t):
        if t in _NM_PARTICLES: return False
        if t in _NM_SUFFIXES: return True
        return len(t.replace("-","")) <= 2 and t.replace("-","").isalpha()
    if _lead(parts[0]): return " ".join(parts[1:])
    end = len(parts)
    while end > 1 and _trail(parts[end-1]): end -= 1
    if end < len(parts):
        r = " ".join(parts[:end])
        if len(r) >= 2: return r
    return s

def _nm_is_pair(name): return "/" in name or " & " in name

def _nm_pair_key(name):
    sep = " & " if " & " in name else "/"
    halves = [h.strip() for h in name.split(sep, 1)]
    return "+".join(sorted(_nm_surname_from_single(h).replace("-"," ") for h in halves))

def _extract_surname(name):
    name = name.strip()
    if _nm_is_pair(name): return _nm_pair_key(name)
    return _nm_surname_from_single(_nm_norm(name)).replace("-"," ")

def _nm_singles_match(a, b):
    if a == b: return True
    a_last, b_last = a.split()[-1], b.split()[-1]
    if a_last == b_last and len(a) != len(b) and len(a_last) >= 3: return True
    la, lb = len(a), len(b)
    diff = abs(la - lb)
    if diff <= 2 and min(la,lb) >= 7:
        short, long_ = (a,b) if la<=lb else (b,a)
        if long_.startswith(short): return True
    if diff == 1 and min(la,lb) >= 4:
        short, long_ = (a,b) if la<lb else (b,a)
        if not long_.startswith(short):
            for i in range(len(long_)):
                if long_[:i]+long_[i+1:] == short: return True
    return False

def _nm_surnames_match(a, b):
    if a == b: return True
    ap, bp = "+" in a, "+" in b
    if ap and bp:
        pa, pb = a.split("+"), b.split("+")
        return _nm_singles_match(pa[0],pb[0]) and _nm_singles_match(pa[1],pb[1])
    if ap or bp: return False
    return _nm_singles_match(a, b)

def _match_pair(p1a, p2a, p1b, p2b) -> Tuple[bool, bool]:
    a1,a2 = _extract_surname(p1a), _extract_surname(p2a)
    b1,b2 = _extract_surname(p1b), _extract_surname(p2b)
    if _nm_surnames_match(a1,b1) and _nm_surnames_match(a2,b2): return True, False
    if _nm_surnames_match(a1,b2) and _nm_surnames_match(a2,b1): return True, True
    return False, False


# ══════════════════════════════════════════════════════════════════════════════
# PHASE DETECTION  (без изменений)
# ══════════════════════════════════════════════════════════════════════════════

def _set_complete(h, a):
    if h==6 and a<=4: return True
    if a==6 and h<=4: return True
    if h==7 and a in (5,6): return True
    if a==7 and h in (5,6): return True
    return False

def _detect_phase(sets_json, game_home, game_away, server):
    try: sets = json.loads(sets_json) if sets_json else []
    except: sets = []
    if not sets: return None
    completed = [s for s in sets if _set_complete(s[0],s[1])]
    current   = next((s for s in reversed(sets) if not _set_complete(s[0],s[1])), sets[-1])
    g1,g2     = current[0], current[1]
    games_total = g1+g2
    if (g1==0 and g2==0
            and (game_home==0 or game_home is None)
            and (game_away==0 or game_away is None)
            and completed):
        return {"phase":"SET_BREAK","label":"Set break"}
    if g1==6 and g2==6: return {"phase":"TIEBREAK","label":"Tiebreak"}
    if game_home==40 and game_away==40: return {"phase":"DEUCE","label":"Deuce"}
    if game_home is not None and game_away is not None:
        if server==1:
            is_bp = (game_away==40 and game_home<40) or (game_away==50 and game_home==40)
        elif server==2:
            is_bp = (game_home==40 and game_away<40) or (game_home==50 and game_away==40)
        else:
            is_bp = False
        if is_bp: return {"phase":"BREAK_POINT","label":"Break point"}
        if (game_home==50 and game_away==40) or (game_home==40 and game_away==50):
            return {"phase":"ADVANTAGE","label":"Advantage"}
    pts_zero = (game_home==0 or game_home is None) and (game_away==0 or game_away is None)
    if pts_zero and games_total > 0:
        return {"phase":"CHANGEOVER","label":"Changeover"} if games_total%2==1 else {"phase":"POST_GAME","label":"Post game"}
    return {"phase":"IN_POINT","label":"In play"}


# ══════════════════════════════════════════════════════════════════════════════
# TIMELINE BUILDERS  —  CARRY-FORWARD (ИСПРАВЛЕНО)
# ══════════════════════════════════════════════════════════════════════════════

def _build_thrill_timeline(rows) -> Dict[str, list]:
    """
    Строит timeline Thrill с carry-forward по отдельным исходам.

    Thrill шлёт дельта-обновления: каждая строка в БД содержит ровно один
    outcome_id ('4' = P1_back, '5' = P2_back). Инвариант пакета (требовать
    оба исхода в одном ts-слоте) выбрасывал ~20% тиков.

    Carry-forward: несём последнее известное значение каждого исхода
    независимо. Тик добавляется как только оба значения были получены
    хотя бы по одному разу.

    Формат: {eid: [(ts, p1_back, p2_back, meta), ...]}
    """
    by_eid_ts: Dict[str, Dict[float, dict]] = defaultdict(dict)
    for (ts, eid, p1, p2, tname, status, oid, odds,
         sets_json, game_home, game_away, server) in rows:
        slot = by_eid_ts[eid].setdefault(ts, {
            "p1": p1, "p2": p2, "tname": tname, "status": status,
            "sets_json": sets_json,
            "game_home": game_home, "game_away": game_away, "server": server,
        })
        slot[oid] = odds   # '4' или '5'

    result: Dict[str, list] = {}
    for eid, ts_dict in by_eid_ts.items():
        tl = []
        last_p1: Optional[float] = None
        last_p2: Optional[float] = None
        for ts in sorted(ts_dict):
            slot = ts_dict[ts]
            if "4" in slot: last_p1 = float(slot["4"])
            if "5" in slot: last_p2 = float(slot["5"])
            if last_p1 is None or last_p2 is None:
                continue
            meta = {k: slot[k] for k in
                    ("p1","p2","tname","status","sets_json","game_home","game_away","server")
                    if k in slot}
            tl.append((ts, last_p1, last_p2, meta))
        if tl:
            result[eid] = tl
    return result


def _build_sharp_timeline(rows) -> Dict[str, list]:
    """
    Строит timeline Sharp с carry-forward по отдельным полям стакана.

    Sharp шлёт дельта-обновления: каждый тик содержит 1-3 runner×side из 4.
    Инвариант пакета выбрасывал ~55% тиков.

    Carry-forward: несём последнее значение каждого из четырёх полей
    (p1_back, p1_lay, p2_back, p2_lay) независимо. Тик добавляется как
    только все четыре получены хотя бы по одному разу.

    Spread guard применяется к итоговому стабильному состоянию: если
    lay ≤ back для любого runner — тик пропускается (аномалия данных).

    Формат: {eid: [(ts, p1_back, p1_lay, p2_back, p2_lay, meta), ...]}
    """
    _FIELDS = ("p1_back", "p1_lay", "p2_back", "p2_lay")

    by_eid_ts: Dict[str, Dict[float, dict]] = defaultdict(dict)
    for (ts, eid, mid, p1, p2, status, in_play, betting,
         runner_idx, side, odds) in rows:
        slot = by_eid_ts[eid].setdefault(ts, {
            "p1": p1, "p2": p2, "status": status,
            "in_play": in_play,
            "betting": int(betting or 0),
            "mid": mid,
        })
        slot[f"p{runner_idx + 1}_{side}"] = odds

    result: Dict[str, list] = {}
    for eid, ts_dict in by_eid_ts.items():
        tl = []
        last: Dict[str, Optional[float]] = {f: None for f in _FIELDS}
        for ts in sorted(ts_dict):
            slot = ts_dict[ts]
            meta = {k: slot[k] for k in
                    ("p1","p2","status","in_play","betting","mid") if k in slot}
            for f in _FIELDS:
                if f in slot:
                    last[f] = slot[f]
            if any(v is None for v in last.values()):
                continue
            p1b, p1l = float(last["p1_back"]), float(last["p1_lay"])
            p2b, p2l = float(last["p2_back"]), float(last["p2_lay"])
            # Spread guard: lay должен быть строго больше back
            if p1l <= p1b or p2l <= p2b:
                continue
            tl.append((ts, p1b, p1l, p2b, p2l, meta))
        if tl:
            result[eid] = tl
    return result


# ══════════════════════════════════════════════════════════════════════════════
# HELPERS
# ══════════════════════════════════════════════════════════════════════════════

def _last_hb_before(hb_list: list, ts: float) -> float:
    if not hb_list: return 0.0
    idx = bisect.bisect_right(hb_list, ts) - 1
    return hb_list[idx] if idx >= 0 else 0.0


def _make_snap(ts, t_meta, tb1, tb2, sb1, sl1, sb2, sl2,
               sets_json, game_home, game_away, server) -> dict:
    try: ps_raw = json.loads(sets_json) if sets_json else []
    except: ps_raw = []
    period_scores = [{"home_score": s[0], "away_score": s[1]} for s in ps_raw]
    score = {
        "period_scores": period_scores,
        "home_gamescore": game_home,
        "away_gamescore": game_away,
        "current_server": server,
    } if period_scores else {}
    return {
        "player1":        t_meta.get("p1",""),
        "player2":        t_meta.get("p2",""),
        "tournament":     t_meta.get("tname",""),
        "score":          score,
        "score_ts":       ts,
        "thrill_p1_back": tb1,
        "thrill_p2_back": tb2,
        "phase":          _detect_phase(sets_json, game_home, game_away, server),
    }


# ══════════════════════════════════════════════════════════════════════════════
# SHARP LAG BUFFER  (аналог engine/sharp_buffer.py)
# ══════════════════════════════════════════════════════════════════════════════

class _SharpLag:
    """
    Минимальная in-memory реализация SharpBuffer для симуляции.
    Поле принимается как стабильное только если оно не менялось ≥ lag_sec.
    """
    __slots__ = ("_lag", "_stable", "_pending")

    def __init__(self, lag_sec: float):
        self._lag     = lag_sec
        self._stable  = {f: None for f in ("p1_back","p1_lay","p2_back","p2_lay")}
        self._pending = {}   # field -> (value, since_ts)

    def feed(self, updates: dict, now: float) -> None:
        """updates: {field: value} — только изменившиеся поля."""
        for field, val in updates.items():
            if val == self._stable[field]:
                self._pending.pop(field, None)
            elif val != self._pending.get(field, (None,))[0]:
                self._pending[field] = (val, now)

        # Промотируем pending → stable если lag истёк
        for field in list(self._pending):
            val, since = self._pending[field]
            if now - since >= self._lag:
                self._stable[field] = val
                del self._pending[field]

    def get(self) -> Tuple:
        s = self._stable
        return s["p1_back"], s["p1_lay"], s["p2_back"], s["p2_lay"]


# ══════════════════════════════════════════════════════════════════════════════
# SIMULATION
# ══════════════════════════════════════════════════════════════════════════════

def _simulate_pair(
    t_tl:    list,
    s_tl:    list,
    t_hb:    list,
    s_hb:    list,
    swapped: bool,
    params:  dict,
) -> list:
    """
    Симулирует торговлю на одной matched паре.

    Ключевые отличия от предыдущей версии:
    - Carry-forward уже применён в t_tl / s_tl (build-фаза).
    - Sharp проходит через _SharpLag буфер (аналог SharpBuffer сканера).
    - Thrill-коэффициент обнуляется если heartbeat старше _THRILL_STALE_SEC
      (аналог _THRILL_STALE guard в engine/core.py).
    - min_profit = минимум за окно [0, window_end] (консервативная оценка).
    """
    min_arb         = params["min_arb"]
    sharp_delay     = params["sharp_delay"]
    window_end      = params["window_end"]
    cooldown        = params["cooldown"]
    freshness_sec   = params["freshness_sec"]
    max_thrill_odds = params["max_thrill_odds"]
    sharp_lag       = params["sharp_lag"]
    thrill_stale    = params.get("thrill_stale", _THRILL_STALE_SEC)

    all_ts = sorted(set(r[0] for r in t_tl) | set(r[0] for r in s_tl))

    # Carry-forward индексы
    t_idx = s_idx = 0
    tb1 = tb2 = None
    t_meta: dict = {}
    t_last_odds_ts: float = 0.0   # время последнего изменения значения Thrill-коэффициента

    # Sharp lag-буфер
    sharp_buf = _SharpLag(sharp_lag)
    s_last_odds_ts: float = 0.0   # время последнего изменения значения Sharp-коэффициента
    _s_prev = (None, None, None, None)  # предыдущее стабильное состояние Sharp

    pipeline:       Optional[dict] = None
    cooldown_until: float          = 0.0
    entries:        list           = []

    for ts in all_ts:
        # Обновляем Thrill carry-forward
        while t_idx < len(t_tl) and t_tl[t_idx][0] <= ts:
            new_ts, new_b1, new_b2, new_meta = t_tl[t_idx]
            if new_b1 != tb1 or new_b2 != tb2:
                t_last_odds_ts = new_ts   # фиксируем время изменения значений
            tb1, tb2, t_meta = new_b1, new_b2, new_meta
            t_idx += 1

        # Обновляем Sharp через lag-буфер
        updates: dict = {}
        while s_idx < len(s_tl) and s_tl[s_idx][0] <= ts:
            row = s_tl[s_idx]
            if swapped:
                updates["p1_back"] = row[3]; updates["p1_lay"] = row[4]
                updates["p2_back"] = row[1]; updates["p2_lay"] = row[2]
            else:
                updates["p1_back"] = row[1]; updates["p1_lay"] = row[2]
                updates["p2_back"] = row[3]; updates["p2_lay"] = row[4]
            s_meta = row[5]
            s_idx += 1
        if updates:
            sharp_buf.feed(updates, ts)

        sb1, sl1, sb2, sl2 = sharp_buf.get()
        # Отслеживаем время последнего изменения Sharp-коэффициента
        _s_cur = (sb1, sl1, sb2, sl2)
        if _s_cur != _s_prev and any(v is not None for v in _s_cur):
            s_last_odds_ts = ts
            _s_prev = _s_cur

        # Нет данных
        if tb1 is None or tb2 is None:          continue
        if sb1 is None or sb2 is None:          continue
        if t_meta.get("status") != "live":      continue
        sj = t_meta.get("sets_json")
        if not sj or sj == "[]":                continue

        # Freshness через heartbeat
        lt = _last_hb_before(t_hb, ts)
        ls = _last_hb_before(s_hb, ts)
        t_age = ts - lt if lt > 0 else freshness_sec + 1
        s_age = ts - ls if ls > 0 else freshness_sec + 1

        if s_age > freshness_sec:               continue
        if not (s_meta or {}).get("betting", 1): continue

        # THRILL STALE GUARD: если коэффициенты Thrill не менялись дольше thrill_stale —
        # не используем их. Это ключевое отличие от heartbeat-age:
        # heartbeat может тикать пока коэффициент завис на старом значении.
        # Пример: Nava/Mmoh — всего 2 строки в Thrill, carry-forward держит
        # 1.49/2.42 на протяжении часа пока Sharp продолжает меняться → ложная вилка.
        if t_last_odds_ts > 0 and ts - t_last_odds_ts > thrill_stale:
            continue

        # SHARP STALE GUARD: симметрично — если Sharp не обновлялся дольше thrill_stale,
        # рынок закрыт или завис. Пример: Navarro/Sun — Sharp закрыл рынок в 01:05
        # (odds=0.0), carry-forward держит последний p1_lay≈1.19, Thrill продолжает
        # давать растущий коэффициент → ложная вилка +93%.
        if s_last_odds_ts > 0 and ts - s_last_odds_ts > thrill_stale:
            continue

        # Thrill freshness (мягкий guard, отдельно от stale)
        if t_age > freshness_sec:               continue

        # Cap на экстремальные коэффициенты Thrill
        tb1c = tb1 if tb1 <= max_thrill_odds else None
        tb2c = tb2 if tb2 <= max_thrill_odds else None

        arbs = _calc_arbs(tb1c, tb2c, sb1, sl1, sb2, sl2)
        best_v = _best_arb(arbs)

        if ts < cooldown_until:                 continue

        # ── Pipeline не существует ───────────────────────────────────────────
        if pipeline is None:
            if best_v is None or best_v < min_arb:
                continue
            arb_idx = max(range(4), key=lambda i: arbs[i] if arbs[i] is not None else -999.0)
            pipeline = {
                "detected_ts": ts,
                "phase":       "waiting",
                "frozen_sharp": None,
                "arb_type_idx": arb_idx,
                "min_profit":   None,
                "min_arbs":     None,
                "min_snap":     None,
                "det_sets":     sj,
                "last_t_age":   t_age,
                "last_s_age":   s_age,
            }
            # Не прерываемся — сразу проверяем переход в tracking (если delay=0)

        elapsed = ts - pipeline["detected_ts"]

        # ── Waiting → Tracking ───────────────────────────────────────────────
        if pipeline["phase"] == "waiting":
            if elapsed < sharp_delay:
                continue
            pipeline["phase"]        = "tracking"
            pipeline["frozen_sharp"] = {
                "p1_back": sb1, "p1_lay": sl1,
                "p2_back": sb2, "p2_lay": sl2,
            }

        # ── Tracking ─────────────────────────────────────────────────────────
        if pipeline["phase"] == "tracking":
            pipeline["last_t_age"] = t_age
            pipeline["last_s_age"] = s_age

            fs = pipeline["frozen_sharp"]
            arbs2 = _calc_arbs(
                tb1c, tb2c,
                fs["p1_back"], fs["p1_lay"],
                fs["p2_back"], fs["p2_lay"],
            )
            profit = arbs2[pipeline["arb_type_idx"]]

            # Минимальный profit за окно (худший тик — консервативная оценка)
            if profit is not None and (
                    pipeline["min_profit"] is None
                    or profit < pipeline["min_profit"]):
                pipeline["min_profit"] = profit
                pipeline["min_arbs"]   = arbs2
                pipeline["min_snap"]   = _make_snap(
                    ts, t_meta, tb1c, tb2c,
                    sb1, sl1, sb2, sl2, sj,
                    t_meta.get("game_home"), t_meta.get("game_away"),
                    t_meta.get("server"),
                )

            if elapsed >= window_end:
                if (pipeline["min_profit"] is not None
                        and pipeline["min_profit"] > 0):
                    snap = pipeline["min_snap"]
                    score_changed = (snap and
                        snap.get("score") and
                        sj != pipeline["det_sets"])
                    entries.append({
                        "snap":          snap,
                        "frozen_sharp":  fs,
                        "arbs":          pipeline["min_arbs"],
                        "profit":        pipeline["min_profit"],
                        "recorded_ts":   ts,
                        "thrill_age":    round(pipeline["last_t_age"], 1),
                        "sharp_age":     round(pipeline["last_s_age"], 1),
                        "arb_type":      _ARB_LABELS[pipeline["arb_type_idx"]],
                        "arb_type_idx":  pipeline["arb_type_idx"],
                        "score_changed": bool(score_changed),
                    })
                cooldown_until = ts + cooldown
                pipeline = None

    # Незакрытый pipeline в конце данных
    if (pipeline is not None
            and pipeline["phase"] == "tracking"
            and pipeline["min_profit"] is not None
            and pipeline["min_profit"] > 0):
        snap = pipeline.get("min_snap")
        entries.append({
            "snap":          snap,
            "frozen_sharp":  pipeline["frozen_sharp"],
            "arbs":          pipeline["min_arbs"],
            "profit":        pipeline["min_profit"],
            "recorded_ts":   all_ts[-1] if all_ts else 0.0,
            "thrill_age":    round(pipeline["last_t_age"], 1),
            "sharp_age":     round(pipeline["last_s_age"], 1),
            "arb_type":      _ARB_LABELS[pipeline["arb_type_idx"]],
            "arb_type_idx":  pipeline["arb_type_idx"],
            "score_changed": False,
        })

    return entries


# ══════════════════════════════════════════════════════════════════════════════
# MAIN ENTRY POINT
# ══════════════════════════════════════════════════════════════════════════════

def compute_history(
        db_path:         str,
        limit:           int   = 100,
        min_arb:         float = 2.0,
        sharp_delay:     float = 3.0,
        window_start:    float = 15.0,   # не используется в расчёте, сохранён для совместимости UI
        window_end:      float = 20.0,
        cooldown:        float = 60.0,
        freshness_sec:   float = 45.0,
        max_thrill_odds: float = 30.0,
        lookback_hours:  float = 0.0,
        sharp_lag:       float = 2.0,
        thrill_stale:    float = _THRILL_STALE_SEC,
) -> list:

    conn = sqlite3.connect(db_path, check_same_thread=False)
    cur  = conn.cursor()

    ts_cutoff = (time.time() - lookback_hours * 3600) if lookback_hours > 0 else 0.0
    tf = "AND ts >= ?" if ts_cutoff > 0 else ""
    tp = (ts_cutoff,)  if ts_cutoff > 0 else ()

    # ── Heartbeat ────────────────────────────────────────────────────────────
    cur.execute(f"SELECT ts, source, event_id FROM heartbeat WHERE 1=1 {tf} ORDER BY ts", tp)
    thrill_hb: Dict[str, list] = defaultdict(list)
    sharp_hb:  Dict[str, list] = defaultdict(list)
    for hb_ts, source, eid in cur.fetchall():
        (thrill_hb if source == "thrill" else sharp_hb)[eid].append(hb_ts)

    # ── Thrill rows ──────────────────────────────────────────────────────────
    cur.execute(f"""
        SELECT ts, event_id, player1, player2, tournament_name, status,
               outcome_id, odds, sets_json, game_home, game_away, server
        FROM thrill
        WHERE market_id='186' AND suspended=0
          AND player1 IS NOT NULL AND player2 IS NOT NULL
          {tf}
        ORDER BY ts
    """, tp)
    thrill_rows = cur.fetchall()

    # ── Sharp rows ────────────────────────────────────────────────────────────
    cur.execute(f"""
        SELECT ts, event_id, market_id, player1, player2,
               status, in_play, betting_enabled,
               runner_idx, side, odds
        FROM sharp_ws
        WHERE level=0 AND market_name='Match Odds'
          AND player1 IS NOT NULL AND player2 IS NOT NULL
          {tf}
        ORDER BY ts
    """, tp)
    sharp_rows = cur.fetchall()

    conn.close()

    # ── Build timelines (carry-forward) ──────────────────────────────────────
    thrill_tl = _build_thrill_timeline(thrill_rows)
    sharp_tl  = _build_sharp_timeline(sharp_rows)

    # ── Match events ─────────────────────────────────────────────────────────
    th_names = {
        eid: (tl[-1][3].get("p1",""), tl[-1][3].get("p2",""))
        for eid, tl in thrill_tl.items() if tl
    }
    sh_names = {
        eid: (tl[-1][5].get("p1",""), tl[-1][5].get("p2",""))
        for eid, tl in sharp_tl.items() if tl
    }

    matched: List[Tuple[str, str, bool]] = []
    used_sh: set = set()
    for th_eid, (t_p1, t_p2) in th_names.items():
        if not t_p1 or not t_p2: continue
        for sh_eid, (s_p1, s_p2) in sh_names.items():
            if sh_eid in used_sh or not s_p1 or not s_p2: continue
            ok, sw = _match_pair(t_p1, t_p2, s_p1, s_p2)
            if ok:
                matched.append((th_eid, sh_eid, sw))
                used_sh.add(sh_eid)
                break

    # ── Simulate ──────────────────────────────────────────────────────────────
    sim_params = {
        "min_arb":         min_arb,
        "sharp_delay":     sharp_delay,
        "window_end":      window_end,
        "cooldown":        cooldown,
        "freshness_sec":   freshness_sec,
        "max_thrill_odds": max_thrill_odds,
        "sharp_lag":       sharp_lag,
        "thrill_stale":    thrill_stale,
    }

    history: list = []
    for th_eid, sh_eid, swapped in matched:
        entries = _simulate_pair(
            thrill_tl[th_eid], sharp_tl[sh_eid],
            thrill_hb.get(th_eid, []), sharp_hb.get(sh_eid, []),
            swapped, sim_params,
        )
        for e in entries:
            e["th_event_id"] = th_eid
            e["sh_event_id"] = sh_eid
        history.extend(entries)

    history.sort(key=lambda e: e["recorded_ts"], reverse=True)
    return history[:limit]


# ── Public API ────────────────────────────────────────────────────────────────

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
            sharp_lag       = float(params.get("sharp_lag",       2.0)),
            thrill_stale    = float(params.get("thrill_stale",  180.0)),
        )
    except Exception as exc:
        log.error("history compute error: %s", exc, exc_info=True)
        entries = []

    try:
        conn    = sqlite3.connect(db_path, check_same_thread=False)
        cur     = conn.cursor()
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
            "SELECT DISTINCT event_id FROM thrill WHERE player1 IS NOT NULL"
        ).fetchall()}
        sharp_eids = {r[0] for r in cur2.execute(
            "SELECT DISTINCT event_id FROM sharp_ws "
            "WHERE player1 IS NOT NULL AND market_name='Match Odds'"
        ).fetchall()}
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
        },
    }