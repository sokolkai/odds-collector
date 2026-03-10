"""
История арбитража: матчинг имён, расчёт арба, обнаружение фазы,
воспроизведение таймлайна из БД.

ИНВАРИАНТ ПАКЕТА (ключевое изменение):
  Thrill: P1_back и P2_back должны присутствовать в ОДНОМ ts-слоте.
          Если в пакете пришёл только один исход — тик пропускается.
  Sharp:  p1_back, p1_lay, p2_back, p2_lay — все четыре в ОДНОМ ts-слоте.
          Неполный пакет (только один runner или только back/lay) — пропускается.

  Carry-forward применяется только к ПОЛНЫМ состояниям, не к отдельным полям.
  Это исключает ситуацию, когда P1 от T1 и P2 от T0 образуют
  «невозможную» вилку внутри одной конторы.

Pipeline-фазы:
  None → waiting (sharp_delay сек) → tracking (до window_end) → закрытие.
  В tracking фиксируем Sharp и ищем минимальный profit за окно.
  Записывается только если min_profit > 0.
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


# ══════════════════════════════════════════════════════════════════════════════
# ARB MATH
# ══════════════════════════════════════════════════════════════════════════════

def _bb(b1: Optional[float], b2: Optional[float]) -> Optional[float]:
    """Back b1 у Thrill + Back b2 у Sharp (с комиссией Sharp)."""
    if not b1 or not b2 or b1 <= 1 or b2 <= 1:
        return None
    c  = _SHARP_COMM
    s1 = (1.0 + (b2 - 1.0) * (1.0 - c)) / b1
    profit = s1 * (b1 - 1.0) - 1.0
    total  = s1 + 1.0
    return (profit / total) * 100.0 if total > 0 else None


def _bl(back: Optional[float], lay: Optional[float]) -> Optional[float]:
    """Back у Thrill + Lay у Sharp (с комиссией Sharp)."""
    if not back or not lay or back <= 1 or lay <= 1:
        return None
    c      = _SHARP_COMM
    s_back = (lay - c) / back
    if s_back <= 0:
        return None
    profit = (1.0 - c) - s_back
    total  = s_back + 1.0
    return (profit / total) * 100.0 if total > 0 else None


def _calc_arbs(tb1: Optional[float], tb2: Optional[float],
               sb1: Optional[float], sl1: Optional[float],
               sb2: Optional[float], sl2: Optional[float],
               ) -> List[Optional[float]]:
    """
    Четыре арб-стратегии:
      [0] T.P1↑ / S.P2↑  — Back P1 Thrill + Back P2 Sharp
      [1] S.P1↑ / T.P2↑  — Back P2 Thrill + Back P1 Sharp
      [2] T.P1↑ / S.P1lay — Back P1 Thrill + Lay P1 Sharp
      [3] T.P2↑ / S.P2lay — Back P2 Thrill + Lay P2 Sharp
    """
    return [
        _bb(tb1, sb2),
        _bb(tb2, sb1),
        _bl(tb1, sl1),
        _bl(tb2, sl2),
    ]


def _best_arb(arbs: List[Optional[float]]) -> Optional[float]:
    valid = [v for v in arbs if v is not None and v > -900]
    return max(valid) if valid else None


# ══════════════════════════════════════════════════════════════════════════════
# NAME MATCHING
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
    if not parts:
        return s
    if len(parts) == 1:
        return parts[0]

    def _is_lead(tok):
        if tok in _NM_PARTICLES:
            return False
        t = tok.replace("-", "")
        return len(t) == 1 and t.isalpha()

    def _is_trail(tok):
        if tok in _NM_PARTICLES:
            return False
        if tok in _NM_SUFFIXES:
            return True
        t = tok.replace("-", "")
        return len(t) <= 2 and t.isalpha()

    if _is_lead(parts[0]):
        return " ".join(parts[1:])
    end = len(parts)
    while end > 1 and _is_trail(parts[end - 1]):
        end -= 1
    if end < len(parts):
        r = " ".join(parts[:end])
        if len(r) >= 2:
            return r
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
    if _nm_is_pair(name):
        return _nm_pair_key(name)
    return _nm_surname_from_single(_nm_norm(name)).replace("-", " ")


def _nm_singles_match(a: str, b: str) -> bool:
    if a == b:
        return True
    a_last = a.split()[-1]
    b_last = b.split()[-1]
    if a_last == b_last and len(a) != len(b) and len(a_last) >= 3:
        return True
    la, lb = len(a), len(b)
    diff = abs(la - lb)
    if diff <= 2 and min(la, lb) >= 7:
        short = a if la <= lb else b
        long_ = b if la <= lb else a
        if long_.startswith(short):
            return True
    if diff == 1 and min(la, lb) >= 4:
        short, long_ = (a, b) if la < lb else (b, a)
        if not long_.startswith(short):
            for i in range(len(long_)):
                if long_[:i] + long_[i + 1:] == short:
                    return True
    return False


def _nm_surnames_match(a: str, b: str) -> bool:
    if a == b:
        return True
    ap, bp = "+" in a, "+" in b
    if ap and bp:
        pa, pb = a.split("+"), b.split("+")
        return _nm_singles_match(pa[0], pb[0]) and _nm_singles_match(pa[1], pb[1])
    if ap or bp:
        return False
    return _nm_singles_match(a, b)


def _match_pair(p1a: str, p2a: str, p1b: str, p2b: str) -> Tuple[bool, bool]:
    """Возвращает (matched, swapped)."""
    a1 = _extract_surname(p1a); a2 = _extract_surname(p2a)
    b1 = _extract_surname(p1b); b2 = _extract_surname(p2b)
    if _nm_surnames_match(a1, b1) and _nm_surnames_match(a2, b2):
        return True, False
    if _nm_surnames_match(a1, b2) and _nm_surnames_match(a2, b1):
        return True, True
    return False, False


# ══════════════════════════════════════════════════════════════════════════════
# PHASE DETECTION
# ══════════════════════════════════════════════════════════════════════════════

def _set_complete(h: int, a: int) -> bool:
    if h == 6 and a <= 4: return True
    if a == 6 and h <= 4: return True
    if h == 7 and a in (5, 6): return True
    if a == 7 and h in (5, 6): return True
    return False


def _detect_phase(sets_json, game_home, game_away, server):
    try:
        sets = json.loads(sets_json) if sets_json else []
    except:
        sets = []
    if not sets:
        return None
    completed = [s for s in sets if _set_complete(s[0], s[1])]
    current   = next((s for s in reversed(sets) if not _set_complete(s[0], s[1])), sets[-1])
    g1, g2    = current[0], current[1]
    games_total = g1 + g2

    if (g1 == 0 and g2 == 0
            and (game_home == 0 or game_home is None)
            and (game_away == 0 or game_away is None)
            and completed):
        return {"phase": "SET_BREAK",   "label": "Set break"}
    if g1 == 6 and g2 == 6:
        return {"phase": "TIEBREAK",    "label": "Tiebreak"}
    if game_home == 40 and game_away == 40:
        return {"phase": "DEUCE",       "label": "Deuce"}
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
            return {"phase": "ADVANTAGE",   "label": "Advantage"}
    pts_zero = (game_home == 0 or game_home is None) and (game_away == 0 or game_away is None)
    if pts_zero and games_total > 0:
        if games_total % 2 == 1:
            return {"phase": "CHANGEOVER", "label": "Changeover"}
        return {"phase": "POST_GAME",      "label": "Post game"}
    return {"phase": "IN_POINT", "label": "In play"}


# ══════════════════════════════════════════════════════════════════════════════
# TIMELINE BUILDERS  —  ИНВАРИАНТ ПАКЕТА
# ══════════════════════════════════════════════════════════════════════════════

def _build_thrill_timeline(rows) -> Dict[str, list]:
    """
    Строит timeline Thrill из строк БД.

    ИНВАРИАНТ: тик добавляется ТОЛЬКО если оба исхода "4" (P1_back)
    и "5" (P2_back) присутствуют в одном ts-слоте.
    Одиночные обновления (только P1 или только P2) пропускаются.

    Carry-forward в фазе симуляции применяется к ПОЛНЫМ состояниям —
    всегда берём последний известный полный пакет, никогда не смешиваем
    P1 из одного времени с P2 из другого.

    Формат: {eid: [(ts, p1_back, p2_back, meta), ...]}
    """
    # Группируем по (eid, ts)
    by_eid_ts: Dict[str, Dict[float, dict]] = defaultdict(dict)
    for (ts, eid, p1, p2, tname, status, oid, odds,
         sets_json, game_home, game_away, server) in rows:
        slot = by_eid_ts[eid].setdefault(ts, {
            "p1": p1, "p2": p2, "tname": tname, "status": status,
            "sets_json": sets_json,
            "game_home": game_home, "game_away": game_away, "server": server,
        })
        slot[oid] = odds  # "4" = P1_back, "5" = P2_back

    result: Dict[str, list] = {}
    for eid, ts_dict in by_eid_ts.items():
        tl = []
        for ts in sorted(ts_dict):
            slot = ts_dict[ts]
            # ИНВАРИАНТ: пропускаем неполные пакеты
            if "4" not in slot or "5" not in slot:
                continue
            meta = {k: slot[k] for k in
                    ("p1", "p2", "tname", "status", "sets_json",
                     "game_home", "game_away", "server")
                    if k in slot}
            tl.append((ts, float(slot["4"]), float(slot["5"]), meta))
        if tl:
            result[eid] = tl
    return result


def _build_sharp_timeline(rows) -> Dict[str, list]:
    """
    Строит timeline Sharp из строк БД.

    ИНВАРИАНТ: тик добавляется ТОЛЬКО если все четыре значения
    (p1_back, p1_lay, p2_back, p2_lay) присутствуют в одном ts-слоте.
    Частичные обновления (только один runner, только back или только lay)
    пропускаются.

    Дополнительно: spread обязан быть валиден в момент пакета (lay > back).
    Невалидный спред в пакете означает аномалию данных — пакет пропускается.

    Carry-forward в симуляции применяется к полному состоянию (все 4 значения).

    Формат: {eid: [(ts, p1_back, p1_lay, p2_back, p2_lay, meta), ...]}
    """
    _NEED = frozenset(("p1_back", "p1_lay", "p2_back", "p2_lay"))

    by_eid_ts: Dict[str, Dict[float, dict]] = defaultdict(dict)
    for (ts, eid, mid, p1, p2, status, in_play, betting,
         runner_idx, side, odds) in rows:
        slot = by_eid_ts[eid].setdefault(ts, {
            "p1": p1, "p2": p2, "status": status,
            "in_play": in_play,
            "betting": int(betting or 0),
            "mid": mid,
        })
        key = f"p{runner_idx + 1}_{side}"  # p1_back / p1_lay / p2_back / p2_lay
        slot[key] = odds

    result: Dict[str, list] = {}
    for eid, ts_dict in by_eid_ts.items():
        tl = []
        for ts in sorted(ts_dict):
            slot = ts_dict[ts]
            # ИНВАРИАНТ: пропускаем неполные пакеты
            if not _NEED.issubset(slot):
                continue
            p1b, p1l = float(slot["p1_back"]), float(slot["p1_lay"])
            p2b, p2l = float(slot["p2_back"]), float(slot["p2_lay"])
            # Спред должен быть физически корректен в рамках одного пакета
            if p1l <= p1b or p2l <= p2b:
                continue
            meta = {k: slot[k] for k in
                    ("p1", "p2", "status", "in_play", "betting", "mid")
                    if k in slot}
            tl.append((ts, p1b, p1l, p2b, p2l, meta))
        if tl:
            result[eid] = tl
    return result


# ══════════════════════════════════════════════════════════════════════════════
# HELPERS
# ══════════════════════════════════════════════════════════════════════════════

def _last_hb_before(hb_list: list, ts: float) -> float:
    """Последний heartbeat строго до ts (бинарный поиск)."""
    if not hb_list:
        return 0.0
    idx = bisect.bisect_right(hb_list, ts) - 1
    return hb_list[idx] if idx >= 0 else 0.0


def _data_valid(thrill_age: float, sharp_age: float,
                freshness_sec: float, s_meta: dict,
                tb1: float, tb2: float,
                max_thrill_odds: float) -> bool:
    """
    Данные пригодны для арбитража если:
    - оба источника свежие (по heartbeat)
    - betting включён на Sharp
    - хотя бы один коэффициент Thrill не превышает cap
    """
    if thrill_age > freshness_sec or sharp_age > freshness_sec:
        return False
    if not s_meta.get("betting", 1):
        return False
    if tb1 > max_thrill_odds and tb2 > max_thrill_odds:
        return False
    return True


def _make_snap(ts, t_meta: dict,
               tb1: Optional[float], tb2: Optional[float],
               sb1: float, sl1: float, sb2: float, sl2: float,
               arbs: list,
               sets_json, game_home, game_away, server) -> dict:
    try:
        ps_raw = json.loads(sets_json) if sets_json else []
    except:
        ps_raw = []
    period_scores = [{"home_score": s[0], "away_score": s[1]} for s in ps_raw]
    score = {
        "period_scores":  period_scores,
        "home_gamescore": game_home,
        "away_gamescore": game_away,
        "current_server": server,
    } if period_scores else {}
    return {
        "player1":        t_meta.get("p1", ""),
        "player2":        t_meta.get("p2", ""),
        "tournament":     t_meta.get("tname", ""),
        "score":          score,
        "score_ts":       ts,
        "thrill_p1_back": tb1,
        "thrill_p2_back": tb2,
        "phase":          _detect_phase(sets_json, game_home, game_away, server),
    }


def _build_entry(pipeline: dict, recorded_ts: float,
                 t_meta: dict,
                 thrill_age: float, sharp_age: float) -> dict:
    arbs         = pipeline.get("min_arbs") or [None, None, None, None]
    arb_type_idx = pipeline.get("arb_type_idx", 0)
    score_changed = (
        pipeline.get("detected_sets_json") is not None
        and t_meta.get("sets_json") != pipeline.get("detected_sets_json")
    )
    return {
        "snap":          pipeline["min_snap"],
        "frozen_sharp":  pipeline["frozen_sharp"],
        "arbs":          arbs,
        "profit":        pipeline["min_profit"],
        "recorded_ts":   recorded_ts,
        "thrill_age":    round(thrill_age, 1),
        "sharp_age":     round(sharp_age, 1),
        "arb_type":      _ARB_LABELS[arb_type_idx],
        "arb_type_idx":  arb_type_idx,
        "score_changed": score_changed,
        # th_event_id / sh_event_id добавляет вызывающая сторона
    }


# ══════════════════════════════════════════════════════════════════════════════
# SIMULATION
# ══════════════════════════════════════════════════════════════════════════════

def _simulate_pair(
    t_tl: list,
    s_tl: list,
    t_hb_list: list,
    s_hb_list: list,
    swapped: bool,
    params: dict,
) -> list:
    """
    Симулирует торговлю на одной matched паре.

    Аргументы
    ---------
    t_tl       Thrill timeline [(ts, p1_back, p2_back, meta), ...]
               Каждый элемент — ПОЛНЫЙ пакет (оба исхода из одного ts-слота).
    s_tl       Sharp timeline  [(ts, p1_back, p1_lay, p2_back, p2_lay, meta), ...]
               Каждый элемент — ПОЛНЫЙ пакет (все 4 значения из одного ts-слота).
    t_hb_list  Отсортированный список ts heartbeat Thrill для этого event_id.
    s_hb_list  Отсортированный список ts heartbeat Sharp для этого event_id.
    swapped    Если True — P1↔P2 у Sharp перестановлены (матч в обратном порядке).
    params     Словарь параметров симуляции.

    Возвращает список записей истории (без th/sh event_id).
    """
    min_arb         = params["min_arb"]
    sharp_delay     = params["sharp_delay"]
    window_end      = params["window_end"]
    cooldown        = params["cooldown"]
    freshness_sec   = params["freshness_sec"]
    max_thrill_odds = params["max_thrill_odds"]

    # Объединённая временная шкала всех тиков обоих источников
    all_ts = sorted(set(r[0] for r in t_tl) | set(r[0] for r in s_tl))

    # Текущие полные состояния (carry-forward полного пакета, не отдельных полей)
    t_idx = s_idx = 0
    tb1 = tb2 = None                   # Thrill P1_back, P2_back (всегда из одного пакета)
    sb1 = sl1 = sb2 = sl2 = None       # Sharp p1_back, p1_lay, p2_back, p2_lay
    t_meta: dict = {}
    s_meta: dict = {}

    pipeline:       Optional[dict] = None
    cooldown_until: float          = 0.0
    entries:        list           = []

    for ts in all_ts:
        # Обновляем состояние Thrill (carry-forward ПОЛНОГО пакета)
        while t_idx < len(t_tl) and t_tl[t_idx][0] <= ts:
            _, tb1, tb2, t_meta = t_tl[t_idx]
            t_idx += 1

        # Обновляем состояние Sharp (carry-forward ПОЛНОГО пакета)
        while s_idx < len(s_tl) and s_tl[s_idx][0] <= ts:
            row = s_tl[s_idx]
            if swapped:
                sb1, sl1, sb2, sl2 = row[3], row[4], row[1], row[2]
            else:
                sb1, sl1, sb2, sl2 = row[1], row[2], row[3], row[4]
            s_meta = row[5]
            s_idx += 1

        # Базовые проверки — нужны данные обоих источников
        if tb1 is None or tb2 is None:
            continue
        if sb1 is None or sb2 is None:
            continue
        if t_meta.get("status") != "live":
            continue

        sets_json = t_meta.get("sets_json")
        if not sets_json or sets_json == "[]":
            continue

        game_home = t_meta.get("game_home")
        game_away = t_meta.get("game_away")
        server    = t_meta.get("server")

        # Freshness через heartbeat
        last_t_hb  = _last_hb_before(t_hb_list, ts)
        last_s_hb  = _last_hb_before(s_hb_list, ts)
        thrill_age = ts - last_t_hb if last_t_hb > 0 else freshness_sec + 1
        sharp_age  = ts - last_s_hb if last_s_hb > 0 else freshness_sec + 1

        # Thrill с cap на максимальный коэффициент
        tb1_c = tb1 if tb1 <= max_thrill_odds else None
        tb2_c = tb2 if tb2 <= max_thrill_odds else None

        valid = _data_valid(thrill_age, sharp_age, freshness_sec,
                            s_meta, tb1, tb2, max_thrill_odds)

        if ts < cooldown_until:
            continue

        # ── Pipeline не существует ───────────────────────────────────────────
        if pipeline is None:
            if not valid:
                continue
            arbs   = _calc_arbs(tb1_c, tb2_c, sb1, sl1, sb2, sl2)
            best_v = _best_arb(arbs)
            if best_v is None or best_v < min_arb:
                continue

            arb_type_idx = max(
                range(4),
                key=lambda i: arbs[i] if arbs[i] is not None else -999.0,
            )
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
            # Не делаем continue — сразу проверяем переход в tracking (если delay=0)

        # ── Pipeline активен ─────────────────────────────────────────────────
        elapsed = ts - pipeline["detected_ts"]

        # Невалидные данные убивают pipeline
        if not valid:
            if (pipeline["phase"] == "tracking"
                    and pipeline["min_profit"] is not None
                    and pipeline["min_profit"] > 0):
                entries.append(_build_entry(
                    pipeline, ts, t_meta, thrill_age, sharp_age))
                cooldown_until = ts + cooldown
            pipeline = None
            continue

        # ── Waiting → Tracking ───────────────────────────────────────────────
        if pipeline["phase"] == "waiting":
            if elapsed < sharp_delay:
                continue
            # Фиксируем Sharp котировки в момент перехода
            pipeline["phase"]        = "tracking"
            pipeline["frozen_sharp"] = {
                "p1_back": sb1, "p1_lay": sl1,
                "p2_back": sb2, "p2_lay": sl2,
            }

        # ── Tracking ─────────────────────────────────────────────────────────
        if pipeline["phase"] == "tracking":
            pipeline["last_thrill_age"] = thrill_age
            pipeline["last_sharp_age"]  = sharp_age

            # Profit считается по FROZEN Sharp + текущий Thrill
            fs   = pipeline["frozen_sharp"]
            arbs = _calc_arbs(
                tb1_c, tb2_c,
                fs["p1_back"], fs["p1_lay"],
                fs["p2_back"], fs["p2_lay"],
            )
            profit = arbs[pipeline["arb_type_idx"]]

            # Запоминаем МИНИМАЛЬНЫЙ profit (худший тик за окно)
            if profit is not None and (
                    pipeline["min_profit"] is None
                    or profit < pipeline["min_profit"]):
                pipeline["min_profit"] = profit
                pipeline["min_arbs"]   = arbs
                pipeline["min_snap"]   = _make_snap(
                    ts, t_meta, tb1_c, tb2_c,
                    sb1, sl1, sb2, sl2,
                    arbs, sets_json, game_home, game_away, server,
                )

            # Окно закрыто
            if elapsed >= window_end:
                if (pipeline["min_profit"] is not None
                        and pipeline["min_profit"] > 0):
                    entries.append(_build_entry(
                        pipeline, ts, t_meta, thrill_age, sharp_age))
                cooldown_until = ts + cooldown
                pipeline = None

    # Незакрытый pipeline в конце данных
    if (pipeline is not None
            and pipeline["phase"] == "tracking"
            and pipeline["min_profit"] is not None
            and pipeline["min_profit"] > 0):
        last_ts = all_ts[-1] if all_ts else 0.0
        entries.append(_build_entry(
            pipeline, last_ts, t_meta,
            pipeline["last_thrill_age"],
            pipeline["last_sharp_age"],
        ))

    return entries


# ══════════════════════════════════════════════════════════════════════════════
# MAIN ENTRY POINT
# ══════════════════════════════════════════════════════════════════════════════

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
    tf = "AND ts >= ?" if ts_cutoff > 0 else ""
    tp = (ts_cutoff,) if ts_cutoff > 0 else ()

    # ── 1. Heartbeat timelines ───────────────────────────────────────────────
    cur.execute(
        f"SELECT ts, source, event_id FROM heartbeat WHERE 1=1 {tf} ORDER BY ts",
        tp,
    )
    thrill_hb: Dict[str, list] = defaultdict(list)
    sharp_hb:  Dict[str, list] = defaultdict(list)
    for hb_ts, source, eid in cur.fetchall():
        if source == "thrill":
            thrill_hb[eid].append(hb_ts)
        else:
            sharp_hb[eid].append(hb_ts)

    # ── 2. Thrill rows ───────────────────────────────────────────────────────
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

    # ── 3. Sharp WS rows ─────────────────────────────────────────────────────
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

    # ── 4. Build timelines (инвариант пакета применяется здесь) ─────────────
    thrill_tl = _build_thrill_timeline(thrill_rows)
    sharp_tl  = _build_sharp_timeline(sharp_rows)

    # ── 5. Match events by player names ──────────────────────────────────────
    th_names: Dict[str, Tuple[str, str]] = {
        eid: (tl[-1][3].get("p1", ""), tl[-1][3].get("p2", ""))
        for eid, tl in thrill_tl.items() if tl
    }
    sh_names: Dict[str, Tuple[str, str]] = {
        eid: (tl[-1][5].get("p1", ""), tl[-1][5].get("p2", ""))
        for eid, tl in sharp_tl.items() if tl
    }

    matched: List[Tuple[str, str, bool]] = []
    used_sh: set = set()
    for th_eid, (t_p1, t_p2) in th_names.items():
        if not t_p1 or not t_p2:
            continue
        for sh_eid, (s_p1, s_p2) in sh_names.items():
            if sh_eid in used_sh or not s_p1 or not s_p2:
                continue
            ok, sw = _match_pair(t_p1, t_p2, s_p1, s_p2)
            if ok:
                matched.append((th_eid, sh_eid, sw))
                used_sh.add(sh_eid)
                break

    # ── 6. Simulate each matched pair ────────────────────────────────────────
    sim_params = {
        "min_arb":         min_arb,
        "sharp_delay":     sharp_delay,
        "window_end":      window_end,
        "cooldown":        cooldown,
        "freshness_sec":   freshness_sec,
        "max_thrill_odds": max_thrill_odds,
    }

    history: list = []
    for th_eid, sh_eid, swapped in matched:
        entries = _simulate_pair(
            thrill_tl[th_eid],
            sharp_tl[sh_eid],
            thrill_hb.get(th_eid, []),
            sharp_hb.get(sh_eid, []),
            swapped,
            sim_params,
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
            tfmt       = lambda ts: _dt.datetime.fromtimestamp(ts).strftime("%Y-%m-%d %H:%M")
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