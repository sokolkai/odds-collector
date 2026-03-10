"""
Odds Collector — записывает данные от Thrill и Sharp в SQLite.
Дедупликация в памяти: одинаковые строки не пишутся повторно.

Запуск:  python collector.py
Deps:    pip install flask

Routes:
  POST /thrill, /sharp-ws, /sharp-score  — userscript ingestion
  GET  /health                           — status
  GET  /log                              — live log page
  GET  /history                          — arb history dashboard
  GET  /history-data                     — history JSON

━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
ОПТИМИЗАЦИИ v3  (изменения на основе анализа реальной БД 2025-03-10)
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

Анализ реальной базы (26 минут, 10834 thrill / 17442 sharp_ws):
  ┌─────────────────────────────────────────────────────────────────┐
  │ БЫЛО: 92.8% строк Thrill — абсолютный мусор                    │
  │                                                                 │
  │  market_id=1    →  54.6%  (football/hockey, outcome=1/2/3)     │
  │  prematch       →  12.8%  (Sharp никогда не цитирует prematch) │
  │  non-tennis     →  25.5%  (sport_id≠5/303, Sharp не торгует)   │
  │  useful         →   7.2%  ← только это реально нужно           │
  └─────────────────────────────────────────────────────────────────┘

FIX A. УДАЛЕНИЕ market_id='1' из _THRILL_KEEP_MARKETS
  Анализ: market_id=1 используется ИСКЛЮЧИТЕЛЬНО не-теннисными
  видами спорта (football sport_id=1, NHL sport_id=4, etc.).
  Все теннисные события используют market_id=186 (outcome 4=P1, 5=P2).
  compute_history() ищет только outcome_id IN ('4','5') → market_id=1
  никогда не использовался в расчётах арбитража.
  Экономия: −54.6% строк Thrill.

FIX B. PREMATCH FILTER в _emit()
  Анализ: все 17 442 строк Sharp имеют in_play=1, betting_enabled=1.
  Sharp не котирует prematch события от слова совсем.
  Prematch строки Thrill никогда не дают арбитраж.
  Бонус: heartbeats для prematch событий тоже исчезают (было −69.5%).
  Экономия: −12.8% строк Thrill, −69.5% строк heartbeat.

FIX C. SPORT FILTER в _emit()
  Анализ: sport_id=20 = настольный теннис (Setka Cup, WTT Feeder).
  Другие sport_id = esports-симуляции, бадминтон, и т.д.
  Sharp в данной сессии торговал ТОЛЬКО теннисом (all 83 events).
  Конфигурируемый список _THRILL_KEEP_SPORTS — пустой = пропускать всё.
  Экономия: −25.5% строк Thrill (или больше с настольным теннисом).

FIX D. SHARP MARKET FILTER в process_ws()
  Анализ: в Sharp проникают horse racing ("2m3f Hrd - Win"),
  предматчевые фикстуры ("Fixtures 10 Mar - Atalanta v Bayern Munich
  - Match Odds"). Они занимают место и никогда не матчатся с Thrill.
  Решение: пропускать всё кроме рынков в _SHARP_KEEP_MARKETS.
  Экономия: −0.9% строк Sharp.

FIX E. РАСШИРЕННАЯ КАРТА ВИДОВ СПОРТА (_SPORT_NAMES)
  Из реальных данных определены: sport_id=20 (Table Tennis),
  sport_id=4 (Ice Hockey), sport_id=1 (Football), sport_id=2 (Basketball).
  Улучшает читаемость логов.

FIX F. COMPUTE_HISTORY ОПТИМИЗАЦИЯ
  Убран market_id='1' из WHERE clause (данных с ним больше нет).
  Это ускоряет загрузку timeline при больших БД.

━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
ОПТИМИЗАЦИИ v2 (оригинальные)
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

1. MARKET FILTER (Thrill) — только рынок 186 (Match Winner)
2. SHARP LEVEL FILTER — только top-of-book (level=0)
3. HEARTBEAT TABLE — proof-of-life без дублирования коэфа
4. BATCH WRITER — один commit вместо тысячи
5. AUTO-PRUNE — фоновое удаление устаревших данных
6. ИНДЕКСЫ — покрывающие индексы для compute_history()
"""
from __future__ import annotations
import json, time, logging, sqlite3, threading, queue
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

from flask import Flask, request, Response

# ── CONFIG ────────────────────────────────────────────────────────────────────
HOST    = "0.0.0.0"
PORT    = 5556
DB_PATH = "odds.db"

# Thrill: ТОЛЬКО рынок 186 (Match Winner, теннис).
# АНАЛИЗ БД: market_id='1' использовался ИСКЛЮЧИТЕЛЬНО для футбола/хоккея
# с тремя исходами (1=home, 2=away, 3=draw). Ни одного теннисного события.
# compute_history() искал только outcome_id IN ('4','5') — market_id='1'
# никогда не использовался. Удаление: −54.6% строк.
_THRILL_KEEP_MARKETS: frozenset = frozenset({"186"})

# Thrill: фильтр по виду спорта. Пустой = пропускать всё.
# АНАЛИЗ БД: sport_id=5/303=Tennis, sport_id=20=Table Tennis (Setka Cup, WTT),
# sport_id=1=Football, sport_id=4=Ice Hockey, sport_id=2=Basketball,
# sport_id=22=Badminton, sport_id=23=Volleyball и т.д.
# Sharp в реальности торгует только теннисом → остальное мусор.
# Если добавить настольный теннис: добавь "20" в список.
_THRILL_KEEP_SPORTS: frozenset = frozenset({"5", "303"})

# Sharp: принимать только эти рынки.
# АНАЛИЗ БД: попадались horse racing ("2m3f Hrd - Win"),
# футбольные фикстуры ("Fixtures 10 Mar - Atalanta v Bayern Munich - Match Odds").
# Они никогда не матчатся с теннисными событиями Thrill.
_SHARP_KEEP_MARKETS: frozenset = frozenset({"Match Odds"})

# Sharp: только top-of-book (level=0). Уровни 1,2 — глубина стакана, не нужна.
SHARP_MAX_LEVEL: int = 0

# Heartbeat: один тик на (source, event_id) каждые N секунд
HB_INTERVAL_SEC: float = 5.0

# BatchWriter: коммит при достижении N строк или N секунд
BATCH_MAX_ROWS: int   = 150
BATCH_MAX_SEC:  float = 0.5

# Auto-prune: удалять данные старше N секунд (0 = не удалять)
DB_TTL_SEC:  float = 6 * 3600   # 6 часов
PRUNE_EVERY: float = 600         # каждые 10 минут
# ─────────────────────────────────────────────────────────────────────────────

logging.basicConfig(level=logging.INFO,
                    format="%(asctime)s [%(levelname)s] %(message)s",
                    datefmt="%H:%M:%S")
log = logging.getLogger(__name__)

# ── LIVE LOG ──────────────────────────────────────────────────────────────────
_log_lock = threading.Lock()
_log_q:   List[str] = []
_log_subs: List[queue.Queue] = []

def _log(source: str, line: str):
    ts  = time.strftime("%H:%M:%S")
    msg = f"{ts}\t{source.upper()}\t{line}"
    with _log_lock:
        _log_q.append(msg)
        if len(_log_q) > 1000:
            _log_q.pop(0)
    dead = []
    for q in _log_subs:
        try: q.put_nowait(msg)
        except: dead.append(q)
    for q in dead:
        try: _log_subs.remove(q)
        except: pass


# ── DATABASE ──────────────────────────────────────────────────────────────────
def init_db(path: str) -> sqlite3.Connection:
    conn = sqlite3.connect(path, check_same_thread=False)
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA synchronous=NORMAL")
    conn.execute("PRAGMA wal_autocheckpoint=1000")
    conn.execute("PRAGMA cache_size=-32000")   # 32 MB page cache
    conn.executescript("""
        CREATE TABLE IF NOT EXISTS thrill (
            id              INTEGER PRIMARY KEY AUTOINCREMENT,
            ts              REAL    NOT NULL,
            event_id        TEXT    NOT NULL,
            player1         TEXT,
            player2         TEXT,
            tournament_id   TEXT,
            tournament_name TEXT,
            sport_id        TEXT,
            sport_name      TEXT,
            status          TEXT,
            scheduled       REAL,
            sets_json       TEXT,
            game_home       INTEGER,
            game_away       INTEGER,
            server          INTEGER,
            market_id       TEXT,
            market_spec     TEXT,
            outcome_id      TEXT,
            odds            REAL,
            suspended       INTEGER
        );
        CREATE TABLE IF NOT EXISTS sharp_ws (
            id              INTEGER PRIMARY KEY AUTOINCREMENT,
            ts              REAL    NOT NULL,
            event_id        TEXT,
            market_id       TEXT,
            player1         TEXT,
            player2         TEXT,
            tournament      TEXT,
            status          TEXT,
            betting_enabled INTEGER,
            in_play         INTEGER,
            market_name     TEXT,
            start_time      REAL,
            overround       REAL,
            underround      REAL,
            market_volume   REAL,
            runner_id       INTEGER,
            runner_idx      INTEGER,
            runner_volume   REAL,
            side            TEXT,
            level           INTEGER,
            odds            REAL,
            amount          REAL
        );
        CREATE TABLE IF NOT EXISTS sharp_score (
            id              INTEGER PRIMARY KEY AUTOINCREMENT,
            ts              REAL    NOT NULL,
            event_id        TEXT,
            player1         TEXT,
            player2         TEXT,
            p1_score        TEXT,
            p2_score        TEXT,
            p1_games        INTEGER,
            p2_games        INTEGER,
            p1_sets         INTEGER,
            p2_sets         INTEGER,
            server          INTEGER,
            period          TEXT,
            status          TEXT
        );

        -- Heartbeat: доказательство жизни источника без изменения коэфа.
        CREATE TABLE IF NOT EXISTS heartbeat (
            id       INTEGER PRIMARY KEY AUTOINCREMENT,
            ts       REAL NOT NULL,
            source   TEXT NOT NULL,
            event_id TEXT NOT NULL
        );

        -- Базовые индексы
        CREATE INDEX IF NOT EXISTS idx_th_ts   ON thrill(ts);
        CREATE INDEX IF NOT EXISTS idx_th_eid  ON thrill(event_id);
        CREATE INDEX IF NOT EXISTS idx_sw_ts   ON sharp_ws(ts);
        CREATE INDEX IF NOT EXISTS idx_sw_eid  ON sharp_ws(event_id);
        CREATE INDEX IF NOT EXISTS idx_ss_ts   ON sharp_score(ts);
        CREATE INDEX IF NOT EXISTS idx_ss_eid  ON sharp_score(event_id);

        -- Составные индексы для compute_history() запросов
        CREATE INDEX IF NOT EXISTS idx_th_eid_ts_mkt
            ON thrill(event_id, ts, market_id, outcome_id, suspended);
        CREATE INDEX IF NOT EXISTS idx_sw_eid_ts_lvl
            ON sharp_ws(event_id, ts, level, market_name, betting_enabled);
        CREATE INDEX IF NOT EXISTS idx_hb_src_eid_ts
            ON heartbeat(source, event_id, ts);
    """)
    conn.commit()
    log.info("DB ready: %s", path)
    return conn


# ── BATCH WRITER ──────────────────────────────────────────────────────────────
class BatchWriter:
    """
    Асинхронная запись в БД. Накапливает строки и делает один INSERT+COMMIT
    раз в BATCH_MAX_SEC секунд или при BATCH_MAX_ROWS строках.
    """
    def __init__(self, conn: sqlite3.Connection,
                 max_rows: int = BATCH_MAX_ROWS,
                 max_sec: float = BATCH_MAX_SEC):
        self._conn    = conn
        self._lock    = threading.Lock()
        self._q: queue.Queue = queue.Queue()
        self._max_rows = max_rows
        self._max_sec  = max_sec
        self._t = threading.Thread(target=self._run, daemon=True, name="BatchWriter")
        self._t.start()

    def write(self, table: str, row: dict) -> None:
        self._q.put((table, row))

    def flush(self) -> None:
        done = threading.Event()
        self._q.put(done)
        done.wait(timeout=5.0)

    def _run(self) -> None:
        pending: list = []
        last_flush: float = time.monotonic()

        while True:
            deadline = last_flush + self._max_sec
            wait = max(0.0, deadline - time.monotonic())
            try:
                item = self._q.get(timeout=wait)
            except queue.Empty:
                item = None

            if isinstance(item, threading.Event):
                if pending:
                    self._flush(pending)
                    pending = []
                    last_flush = time.monotonic()
                item.set()
                continue

            if item is not None:
                pending.append(item)

            now = time.monotonic()
            if pending and (len(pending) >= self._max_rows
                            or now - last_flush >= self._max_sec):
                self._flush(pending)
                pending = []
                last_flush = now

    def _flush(self, items: list) -> None:
        try:
            with self._lock:
                for table, row in items:
                    cols = ", ".join(row.keys())
                    vals = ", ".join(f":{k}" for k in row)
                    self._conn.execute(
                        f"INSERT INTO {table} ({cols}) VALUES ({vals})", row)
                self._conn.commit()
        except Exception as e:
            log.error("BatchWriter flush error (%d rows): %s", len(items), e)


# ── HEARTBEAT TRACKER ────────────────────────────────────────────────────────
class HBTracker:
    """
    Дедuplicated heartbeat writer.
    Пишет один тик (ts, source, event_id) раз в HB_INTERVAL_SEC.
    """
    def __init__(self, writer: BatchWriter, interval: float = HB_INTERVAL_SEC):
        self._writer   = writer
        self._interval = interval
        self._last: Dict[tuple, float] = {}

    def ping(self, source: str, event_id: str, ts: float) -> None:
        key = (source, event_id)
        if ts - self._last.get(key, 0.0) >= self._interval:
            self._last[key] = ts
            self._writer.write("heartbeat", {
                "ts": ts, "source": source, "event_id": event_id
            })

    def clear(self, source: str, event_id: str) -> None:
        self._last.pop((source, event_id), None)


# ── DATABASE PRUNER ───────────────────────────────────────────────────────────
class PruneThread(threading.Thread):
    def __init__(self, db_path: str, writer: BatchWriter,
                 ttl_sec: float = DB_TTL_SEC,
                 every_sec: float = PRUNE_EVERY):
        super().__init__(daemon=True, name="PruneThread")
        self._db_path  = db_path
        self._writer   = writer
        self._ttl      = ttl_sec
        self._every    = every_sec
        self._vacuum_every = 3600
        self._last_vacuum  = 0.0

    def run(self) -> None:
        if self._ttl <= 0:
            log.info("PruneThread: TTL=0, pruning disabled")
            return
        while True:
            time.sleep(self._every)
            self._prune()

    def _prune(self) -> None:
        cutoff = time.time() - self._ttl
        self._writer.flush()
        try:
            conn = sqlite3.connect(self._db_path, check_same_thread=False)
            conn.execute("PRAGMA journal_mode=WAL")
            total = 0
            for table in ("thrill", "sharp_ws", "sharp_score", "heartbeat"):
                n = conn.execute(
                    f"DELETE FROM {table} WHERE ts < ?", (cutoff,)
                ).rowcount
                total += n
            conn.commit()
            if total > 0:
                log.info("Pruned %d rows older than %.1fh",
                         total, self._ttl / 3600)
            now = time.time()
            if now - self._last_vacuum >= self._vacuum_every:
                conn.execute("VACUUM")
                self._last_vacuum = now
                log.info("VACUUM complete")
            conn.close()
        except Exception as e:
            log.error("PruneThread error: %s", e)


# ── THRILL ────────────────────────────────────────────────────────────────────
_FINISHED    = {3, 4, 5, 6, 99, 100}

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


# ── SHARP ─────────────────────────────────────────────────────────────────────
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


# ══════════════════════════════════════════════════════════════════════════════
# ── HISTORY ENGINE ────────────────────────────────────────────────────────────
# ══════════════════════════════════════════════════════════════════════════════

_SHARP_COMM  = 0.029
_THRILL_STALE = 180.0

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
        _bb(sb1, tb2),
        _bl(tb1, sl1),
        _bl(tb2, sl2),
    ]

def _best_arb(arbs):
    valid = [v for v in (arbs or []) if v is not None and v > -900]
    return max(valid) if valid else None


# ── Name matching ─────────────────────────────────────────────────────────────
import unicodedata as _ucd
from typing import Tuple as _Tuple

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

def _match_pair(p1a: str, p2a: str, p1b: str, p2b: str) -> _Tuple[bool, bool]:
    a1 = _extract_surname(p1a); a2 = _extract_surname(p2a)
    b1 = _extract_surname(p1b); b2 = _extract_surname(p2b)
    if _nm_surnames_match(a1, b1) and _nm_surnames_match(a2, b2): return True, False
    if _nm_surnames_match(a1, b2) and _nm_surnames_match(a2, b1): return True, True
    return False, False


# ── Phase detection ───────────────────────────────────────────────────────────
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


# ── History computation ───────────────────────────────────────────────────────
_ARB_LABELS = [
    "T.P1↑ / S.P2↑",
    "S.P1↑ / T.P2↑",
    "T.P1↑ / S.P1 lay",
    "T.P2↑ / S.P2 lay",
]


def compute_history(db_path: str,
                    limit:           int   = 100,
                    min_arb:         float = 2.0,
                    sharp_delay:     float = 3.0,
                    window_start:    float = 15.0,
                    window_end:      float = 20.0,
                    cooldown:        float = 60.0,
                    freshness_sec:   float = 45.0,
                    lag_sec:         float = 2.0,
                    max_thrill_odds: float = 30.0,
                    lookback_hours:  float = 0.0) -> list:
    """
    Воспроизводит данные из DB и находит арбитражные возможности.

    FIX F: убран market_id='1' из WHERE clause Thrill timeline —
    данных с ним больше нет (исключён в ThrillCollector._emit()).
    Ускоряет загрузку при больших БД.
    """
    import bisect
    from collections import defaultdict

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
    # FIX F: market_id='186' только — убран '1' (данных с ним больше нет).
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

    # ── 5. Simulate HistoryTracker for each matched pair ──────────────────────
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

            while s_idx < len(s_tl) and s_tl[s_idx][0] <= ts - lag_sec:
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

            if status != "live": continue
            if not sets_json or sets_json == "[]": continue
            if tb1 is None and tb2 is None: continue
            if sb1 is None and sb2 is None: continue

            last_t_hb = _last_hb_before(t_hb_list, ts)
            last_s_hb = _last_hb_before(s_hb_list, ts)
            thrill_age = ts - last_t_hb if last_t_hb > 0 else freshness_sec + 1
            sharp_age  = ts - last_s_hb if last_s_hb > 0 else freshness_sec + 1

            if thrill_age > freshness_sec or sharp_age > freshness_sec:
                if pipeline: pipeline = None
                continue

            if not s_meta.get("betting", 1):
                if pipeline: pipeline = None
                continue

            if sb1 is not None and sl1 is not None and sl1 <= sb1:
                if pipeline: pipeline = None
                continue
            if sb2 is not None and sl2 is not None and sl2 <= sb2:
                if pipeline: pipeline = None
                continue

            tb1_u = tb1 if (tb1 is None or tb1 <= max_thrill_odds) else None
            tb2_u = tb2 if (tb2 is None or tb2 <= max_thrill_odds) else None
            if tb1_u is None and tb2_u is None:
                if pipeline: pipeline = None
                continue

            arbs    = _calc_arbs(tb1_u, tb2_u, sb1, sl1, sb2, sl2)
            best_v  = _best_arb(arbs)
            has_arb = best_v is not None and best_v >= min_arb

            if ts < cooldown_until:
                continue

            if not has_arb:
                if pipeline is not None:
                    entry = pipeline
                    if (entry["phase"] == "tracking"
                            and entry.get("frozen_sharp")
                            and _best_arb(entry["max_arbs"]) is not None
                            and _best_arb(entry["max_arbs"]) > 0):
                        history_entries.append(
                            _make_entry(entry, ts, th_eid, sh_eid, t_meta,
                                        thrill_age, sharp_age))
                        cooldown_until = ts + cooldown
                    pipeline = None
                continue

            if pipeline is None:
                pipeline = {
                    "detected_ts":        ts,
                    "phase":              "waiting",
                    "frozen_sharp":       None,
                    "max_arbs":           [None, None, None, None],
                    "max_snap":           None,
                    "detected_sets_json": sets_json,
                    "last_thrill_age":    thrill_age,
                    "last_sharp_age":     sharp_age,
                }
                pipeline["max_snap"] = _snap(ts, t_meta, tb1_u, tb2_u,
                                              sb1, sl1, sb2, sl2,
                                              arbs, sets_json, game_home, game_away, server)
                continue

            elapsed = ts - pipeline["detected_ts"]

            if pipeline["phase"] == "waiting":
                if elapsed >= sharp_delay:
                    pipeline["phase"]        = "tracking"
                    pipeline["frozen_sharp"] = {"p1_back": sb1, "p1_lay": sl1,
                                                 "p2_back": sb2, "p2_lay": sl2}
                else:
                    continue

            if pipeline["phase"] == "tracking" and pipeline["frozen_sharp"]:
                pipeline["last_thrill_age"] = thrill_age
                pipeline["last_sharp_age"]  = sharp_age

                if elapsed >= window_start:
                    fs = pipeline["frozen_sharp"]
                    arbs_f = _calc_arbs(tb1_u, tb2_u,
                                        fs.get("p1_back"), fs.get("p1_lay"),
                                        fs.get("p2_back"), fs.get("p2_lay"))
                    if (_best_arb(arbs_f) is not None and
                            (_best_arb(pipeline["max_arbs"]) is None or
                             _best_arb(arbs_f) > _best_arb(pipeline["max_arbs"]))):
                        pipeline["max_arbs"] = arbs_f
                        pipeline["max_snap"] = _snap(ts, t_meta, tb1_u, tb2_u,
                                                      sb1, sl1, sb2, sl2,
                                                      arbs_f, sets_json,
                                                      game_home, game_away, server)

                if elapsed >= window_end:
                    if (_best_arb(pipeline["max_arbs"]) is not None and
                            _best_arb(pipeline["max_arbs"]) > 0):
                        history_entries.append(
                            _make_entry(pipeline, ts, th_eid, sh_eid, t_meta,
                                        thrill_age, sharp_age))
                    cooldown_until = ts + cooldown
                    pipeline = None

        if pipeline is not None and pipeline["phase"] == "tracking":
            if (_best_arb(pipeline["max_arbs"]) is not None and
                    _best_arb(pipeline["max_arbs"]) > 0):
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
    arbs = pipeline["max_arbs"]
    best_idx = 0
    best_val = None
    for i, v in enumerate(arbs):
        if v is not None and (best_val is None or v > best_val):
            best_val = v
            best_idx = i
    score_changed = (pipeline.get("detected_sets_json") is not None and
                     t_meta.get("sets_json") != pipeline.get("detected_sets_json"))
    return {
        "snap":           pipeline["max_snap"],
        "frozen_sharp":   pipeline["frozen_sharp"],
        "max_arbs":       arbs,
        "recorded_ts":    recorded_ts,
        "thrill_age":     round(thrill_age, 1),
        "sharp_age":      round(sharp_age, 1),
        "arb_type":       _ARB_LABELS[best_idx],
        "arb_type_idx":   best_idx,
        "score_changed":  score_changed,
        "th_event_id":    th_eid,
        "sh_event_id":    sh_eid,
    }


# ── History Dashboard HTML ────────────────────────────────────────────────────
_HISTORY_HTML = r"""<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<title>&#9889; Arb History</title>
<style>
  @import url('https://fonts.googleapis.com/css2?family=JetBrains+Mono:wght@400;600;700&display=swap');
  :root{--bg:#0a0a0f;--bg2:#12121a;--border:#1e1e2e;--border2:#161620;
    --tx:#c9d1d9;--tx2:#9ca3af;--tx3:#6b7280;--dim:#3a3a4a;
    --green:#00ff87;--purple:#c084fc;--blue:#60a5fa;--yellow:#fbbf24;--red:#ff6b6b;--orange:#fb923c;}
  .light{--bg:#f5f6f8;--bg2:#fff;--border:#d4d8e0;--border2:#e4e8ee;
    --tx:#1a1a2e;--tx2:#4a5068;--tx3:#7a8098;--dim:#b0b8c8;
    --green:#0a9;--purple:#7c3aed;--blue:#2563eb;--yellow:#b45309;--red:#dc2626;--orange:#ea580c;}
  *{margin:0;padding:0;box-sizing:border-box;}
  body{background:var(--bg);color:var(--tx);font-family:'JetBrains Mono','Fira Code',monospace;font-size:12px;padding:10px 14px 60px;}
  .tw{overflow-x:auto;}
  table{width:100%;border-collapse:separate;border-spacing:0;font-size:11px;}
  th{padding:8px 5px;text-align:center;color:var(--tx3);font-size:9px;font-weight:600;
    text-transform:uppercase;letter-spacing:.05em;border-bottom:1px solid var(--border);
    position:sticky;top:0;background:var(--bg);z-index:2;white-space:nowrap;}
  th.l{text-align:left;}
  td{padding:3px 5px;white-space:nowrap;text-align:center;vertical-align:middle;}
  td.l{text-align:left;}
  td.sp{width:12px;min-width:12px;padding:0;}
  tr.rw td{background:linear-gradient(180deg,#181825 0%,#0f0f17 60%,#0c0c14 100%);
    border-top:1px solid rgba(255,255,255,.045);border-bottom:1px solid rgba(0,0,0,.45);}
  tr.rw td:first-child{border-radius:4px 0 0 4px;border-left:1px solid rgba(255,255,255,.04);}
  tr.rw td:last-child{border-radius:0 4px 4px 0;border-right:1px solid rgba(0,0,0,.3);}
  tr.rw+tr.rw td{box-shadow:0 -12px 0 var(--bg);}
  .light tr.rw td{background:linear-gradient(180deg,#fff 0%,#f0f2f5 100%);
    border-top:1px solid rgba(255,255,255,.9);border-bottom:1px solid rgba(0,0,0,.08);}
  .p1{font-weight:600;font-size:11px;}
  .p2{font-weight:600;font-size:11px;color:var(--tx2);}
  .ov{font-weight:700;font-variant-numeric:tabular-nums;font-size:13px;}
  .o-t{color:var(--purple);}
  .o-s{color:var(--blue);}
  .o-l{color:var(--red);font-size:12px;}
  .arb{font-weight:700;font-size:12px;font-variant-numeric:tabular-nums;padding:2px 3px;border-radius:3px;}
  .arb.pos{color:var(--green);background:rgba(0,255,135,.09);}
  .arb.neg{color:var(--dim);}
  .arb.near{color:var(--yellow);}
  .best-arb{font-weight:700;font-size:14px;font-variant-numeric:tabular-nums;
    padding:3px 7px;border-radius:4px;color:var(--green);background:rgba(0,255,135,.1);
    border:1px solid rgba(0,255,135,.2);}
  .ph-strip{display:flex;align-items:baseline;gap:5px;margin-top:3px;padding-top:3px;border-top:1px solid var(--border2);}
  .ph-lbl{font-size:10px;font-weight:700;}
  .ph-BREAK_POINT{color:#f87171;} .ph-TIEBREAK{color:#818cf8;}
  .ph-DEUCE,.ph-ADVANTAGE{color:#fbbf24;} .ph-CHANGEOVER,.ph-SET_BREAK{color:#00ff87;}
  .ph-POST_GAME,.ph-IN_POINT{color:#8899aa;}
  .sc-wrap{display:inline-block;background:var(--bg2);border:1px solid var(--border);border-radius:5px;overflow:hidden;}
  .sc-row{display:flex;align-items:center;height:20px;padding:0 5px;}
  .sc-row+.sc-row{border-top:1px solid var(--border2);}
  .sc-set{width:16px;text-align:center;font-size:12px;font-variant-numeric:tabular-nums;}
  .sc-gs{min-width:20px;height:15px;padding:0 3px;border-radius:2px;background:var(--bg);
    border:1px solid var(--border);display:inline-flex;align-items:center;justify-content:center;
    font-size:10px;font-weight:700;margin-left:5px;}
  .sc-dot{width:6px;height:6px;border-radius:50%;background:var(--green);
    box-shadow:0 0 5px var(--green);margin:0 3px;flex-shrink:0;}
  .sc-dot-h{width:6px;height:6px;border-radius:50%;margin:0 3px;flex-shrink:0;}
  .badge{display:inline-block;padding:1px 5px;border-radius:3px;font-size:9px;
    font-weight:700;letter-spacing:.03em;text-transform:uppercase;white-space:nowrap;}
  .badge-bl{color:#60a5fa;background:rgba(96,165,250,.12);border:1px solid rgba(96,165,250,.25);}
  .badge-bb{color:#c084fc;background:rgba(192,132,252,.12);border:1px solid rgba(192,132,252,.25);}
  .badge-warn{color:var(--orange);background:rgba(251,146,60,.1);border:1px solid rgba(251,146,60,.25);}
  .age-ok{color:var(--green);font-size:10px;} .age-warn{color:var(--yellow);font-size:10px;}
  .age-stale{color:var(--red);font-size:10px;}
  .empty{text-align:center;padding:40px;color:var(--dim);}
  .params{background:var(--bg2);border:1px solid var(--border);border-radius:6px;
    padding:8px 12px;margin-bottom:10px;display:flex;gap:8px;align-items:center;flex-wrap:wrap;}
  .params label{display:flex;align-items:center;gap:3px;color:var(--tx2);font-size:11px;}
  .params input{width:44px;padding:2px 3px;background:var(--bg);border:1px solid var(--border);
    border-radius:3px;color:var(--tx);font-family:inherit;font-size:11px;text-align:center;}
  .ptitle{color:var(--tx3);font-size:9px;text-transform:uppercase;letter-spacing:.06em;padding-right:4px;}
  .pgrp{display:flex;gap:8px;align-items:center;padding:0 8px;border-left:1px solid var(--border);}
  .reload-btn{padding:3px 10px;border-radius:4px;font-size:11px;font-family:inherit;cursor:pointer;
    border:1px solid var(--green);background:rgba(0,255,135,.07);color:var(--green);}
  .reload-btn:hover{background:rgba(0,255,135,.15);}
  .reset-btn{padding:3px 9px;border-radius:4px;font-size:11px;font-family:inherit;cursor:pointer;
    border:1px solid var(--border);background:transparent;color:var(--tx3);}
  .sbar{position:fixed;bottom:0;left:0;right:0;display:flex;align-items:center;
    justify-content:space-between;padding:5px 14px;opacity:.3;transition:opacity .2s;background:var(--bg);}
  .sbar:hover{opacity:.9;}
  .sbar-l{display:flex;gap:12px;font-size:9px;color:var(--tx3);}
  .tbtn{padding:2px 6px;border-radius:3px;cursor:pointer;font-size:11px;
    border:1px solid var(--border);background:transparent;color:var(--tx3);}
  .tour-hdr td{background:linear-gradient(90deg,rgba(251,191,36,.07) 0%,transparent 70%);
    border-left:3px solid var(--yellow);border-bottom:1px solid var(--border);
    border-top:2px solid var(--border);padding:6px 12px;text-align:left;}
  .tour-name{color:var(--yellow);font-size:11px;font-weight:700;}
  .tour-cnt{color:var(--dim);margin-left:8px;font-size:10px;}
</style>
</head>
<body>
<div id="root"></div>
<script>
'use strict';
const _DEF={min_arb:2.0,sharp_delay:3.0,window_start:15.0,window_end:20.0,
            cooldown:60.0,freshness_sec:45.0,lag_sec:2.0,max_thrill_odds:30.0,
            lookback_hours:0,limit:100};
let S={data:null,theme:'dark',params:Object.assign({},_DEF)};
try{if(localStorage.getItem('arb_t')==='light'){S.theme='light';document.body.classList.add('light');}}catch(e){}
try{const p=localStorage.getItem('arb_hp');if(p)S.params=Object.assign({},_DEF,JSON.parse(p));}catch(e){}

const F=(v,d=2)=>v!=null?Number(v).toFixed(d):'--';
const E=s=>s?String(s).replace(/&/g,'&amp;').replace(/</g,'&lt;').replace(/>/g,'&gt;'):'';
const Ago=ts=>{const s=Math.round(Date.now()/1000-ts);return s<60?s+'s':Math.floor(s/60)+'m'+String(s%60).padStart(2,'0')+'s';};
const AgeCls=s=>s==null?'':(s<20?'age-ok':s<40?'age-warn':'age-stale');
const arbCls=v=>v==null?'arb neg':v>0?'arb pos':v>-2?'arb near':'arb neg';
const arbTxt=v=>v==null?'--':(v>0?'+':'')+Number(v).toFixed(2)+'%';

function fmtScore(sc){
  if(!sc)return '';
  const ps=sc.period_scores||[];
  if(!ps.length)return '';
  const hg=sc.home_gamescore,ag=sc.away_gamescore,srv=sc.current_server;
  const row=isH=>{
    let r='<div class="sc-row">';
    for(let i=0;i<ps.length;i++){
      const p=ps[i],v=isH?+p.home_score:+p.away_score,o=isH?+p.away_score:+p.home_score,cur=i===ps.length-1;
      const c=cur||v>o?'var(--tx)':'var(--dim)',fw=cur||v>o?700:400;
      r+=`<span class="sc-set" style="color:${c};font-weight:${fw}">${v}</span>`;
    }
    r+=srv===(isH?1:2)?'<span class="sc-dot"></span>':'<span class="sc-dot-h"></span>';
    const gv=(isH?hg:ag)!=null?String(isH?hg:ag):'0';
    return r+`<span class="sc-gs">${gv}</span></div>`;
  };
  return `<div class="sc-wrap">${row(true)}${row(false)}</div>`;
}

function phBadge(ph){
  if(!ph)return '';
  return `<div class="ph-strip"><span class="ph-lbl ph-${ph.phase||''}">${E(ph.label||ph.phase)}</span></div>`;
}

function buildRow(entry){
  const m=entry.snap,fs=entry.frozen_sharp||{},arbs=entry.max_arbs||[null,null,null,null];
  const sc=fmtScore(m.score);
  const best=arbs.reduce((b,v)=>v!=null&&(b===null||v>b)?v:b,null);
  const isBL=entry.arb_type_idx===2||entry.arb_type_idx===3;
  const typeBadge=`<span class="badge ${isBL?'badge-bl':'badge-bb'}">${E(entry.arb_type||'')}</span>`;
  const scWarn=entry.score_changed?'<span class="badge badge-warn" title="Score changed during window">sc!</span> ':'';
  const ta=entry.thrill_age!=null?Math.round(entry.thrill_age):null;
  const sa=entry.sharp_age!=null?Math.round(entry.sharp_age):null;
  let h='<tr class="rw">';
  h+=`<td class="l"><span class="p1">${E(m.player1)}</span><br><span class="p2">${E(m.player2)}</span>${phBadge(m.phase)}</td>`;
  h+='<td class="sp"></td>';
  h+=`<td>${sc||'<span style="color:var(--dim)">--</span>'}</td>`;
  h+='<td class="sp"></td>';
  h+=`<td><span class="ov o-t">${F(m.thrill_p1_back)}</span></td>`;
  h+=`<td><span class="ov o-t">${F(m.thrill_p2_back)}</span></td>`;
  h+='<td class="sp"></td>';
  h+=`<td><span class="ov o-s">${F(fs.p1_back)}</span></td>`;
  h+=`<td><span class="ov o-l">${F(fs.p1_lay)}</span></td>`;
  h+=`<td><span class="ov o-s">${F(fs.p2_back)}</span></td>`;
  h+=`<td><span class="ov o-l">${F(fs.p2_lay)}</span></td>`;
  h+='<td class="sp"></td>';
  for(let i=0;i<4;i++) h+=`<td><span class="${arbCls(arbs[i])}">${arbTxt(arbs[i])}</span></td>`;
  h+='<td class="sp"></td>';
  h+=`<td><span class="best-arb">${arbTxt(best)}</span></td>`;
  h+=`<td>${scWarn}${typeBadge}</td>`;
  h+=`<td><span class="${AgeCls(ta)}">T:${ta!=null?ta+'s':'--'}</span><br><span class="${AgeCls(sa)}">S:${sa!=null?sa+'s':'--'}</span></td>`;
  h+=`<td style="color:var(--tx3);font-variant-numeric:tabular-nums">${Ago(entry.recorded_ts)}</td>`;
  return h+'</tr>';
}

function renderParams(){
  const p=S.params;
  const PI=(k,l,v,u,st='0.5')=>`<label>${l}<input type="number" step="${st}" value="${v}"
    oninput="S.params['${k}']=parseFloat(this.value)||0">${u}</label>`;
  return `<div class="params">
    <span class="ptitle">Simulation</span>
    <div class="pgrp">
      ${PI('min_arb','Min arb',p.min_arb,'%')}
      ${PI('sharp_delay','Delay',p.sharp_delay,'s')}
      ${PI('window_start','Win start',p.window_start,'s')}
      ${PI('window_end','Win end',p.window_end,'s')}
      ${PI('cooldown','Cooldown',p.cooldown,'s')}
    </div>
    <span class="ptitle">Quality</span>
    <div class="pgrp">
      ${PI('freshness_sec','Freshness',p.freshness_sec,'s')}
      ${PI('lag_sec','Lag',p.lag_sec,'s','0.5')}
      ${PI('max_thrill_odds','MaxOdds',p.max_thrill_odds,'x','1')}
    </div>
    <div class="pgrp">
      ${PI('lookback_hours','Lookback',p.lookback_hours,'h','1')}
      ${PI('limit','Limit',p.limit,'','1')}
    </div>
    <button class="reload-btn" onclick="reload()">&#8635; Recalculate</button>
    <button class="reset-btn" onclick="resetP()">&#8634; Reset</button>
    <span id="spinner" style="color:var(--tx3);font-size:11px;display:none">&#8987;</span>
  </div>`;
}

function render(){
  const hist=(S.data&&S.data.history)||[];
  const stats=(S.data&&S.data.stats)||{};
  let h='';
  h+=`<div style="display:flex;align-items:center;gap:10px;margin-bottom:8px;flex-wrap:wrap">
    <span style="color:var(--yellow);font-size:15px;font-weight:700">&#9889; Arb History</span>
    <span style="color:var(--tx3);font-size:11px">${E(stats.db_time_range||'')}</span>
  </div>`;
  if(S.data){
    const warned=hist.filter(e=>e.score_changed).length;
    const blCount=hist.filter(e=>e.arb_type_idx===2||e.arb_type_idx===3).length;
    h+=`<div style="margin-bottom:8px;display:flex;gap:14px;flex-wrap:wrap;font-size:11px;color:var(--tx3)">
      <span>Thrill <b style="color:var(--tx)">${stats.thrill_events||0}</b></span>
      <span>Sharp <b style="color:var(--tx)">${stats.sharp_events||0}</b></span>
      <span>Matched <b style="color:var(--tx)">${stats.matched_pairs||0}</b></span>
      <span>Arbs <b style="color:var(--green)">${hist.length}</b></span>
      ${blCount?`<span>back-lay <b style="color:var(--blue)">${blCount}</b></span>`:''}
      ${warned?`<span style="color:var(--orange)">score changed &#9888; ${warned}</span>`:''}
      <span style="color:var(--dim)">${stats.computed_in_ms||0}ms</span>
    </div>`;
  }
  h+=renderParams();
  if(!S.data){
    h+='<div class="empty">Loading...</div>';
  } else if(!hist.length){
    h+='<div class="empty">No arb opportunities found with current params</div>';
  } else {
    const byTour={};
    hist.forEach(e=>{const t=e.snap.tournament||'Unknown';(byTour[t]=byTour[t]||[]).push(e);});
    h+=`<div class="tw"><table><thead><tr>
      <th class="l">Match</th><th class="sp"></th><th>Score</th><th class="sp"></th>
      <th colspan="2" style="color:var(--purple)">Thrill P1/P2</th><th class="sp"></th>
      <th style="color:var(--blue)">P1 back</th><th style="color:var(--red)">P1 lay</th>
      <th style="color:var(--blue)">P2 back</th><th style="color:var(--red)">P2 lay</th>
      <th class="sp"></th>
      <th style="color:var(--dim)" title="Back T.P1 + Back S.P2">bb1</th>
      <th style="color:var(--dim)" title="Back S.P1 + Back T.P2">bb2</th>
      <th style="color:var(--blue)" title="Back T.P1 + Lay S.P1">bl1</th>
      <th style="color:var(--blue)" title="Back T.P2 + Lay S.P2">bl2</th>
      <th class="sp"></th>
      <th style="color:var(--green)">Best %</th>
      <th>Type</th><th>Ages</th><th>Ago</th>
    </tr></thead><tbody>`;
    for(const [tour,rows] of Object.entries(byTour).sort((a,b)=>a[0].localeCompare(b[0]))){
      h+=`<tr class="tour-hdr"><td colspan="21">
        <span class="tour-name">${E(tour)}</span>
        <span class="tour-cnt">${rows.length} entry${rows.length!==1?'s':''}</span>
      </td></tr>`;
      rows.forEach(e=>h+=buildRow(e));
    }
    h+='</tbody></table></div>';
  }
  h+=`<div class="sbar"><div class="sbar-l">
    <span>DB ${stats.total_rows||0} rows</span>
    <span>Thrill ${stats.thrill_rows||0}</span>
    <span>Sharp ${stats.sharp_rows||0}</span>
    <span>HB ${stats.hb_rows||0}</span>
  </div><button class="tbtn" onclick="TT()">${S.theme==='dark'?'&#9728;':'&#9790;'}</button></div>`;
  document.getElementById('root').innerHTML=h;
}

function TT(){
  S.theme=S.theme==='dark'?'light':'dark';
  document.body.classList.toggle('light');
  try{localStorage.setItem('arb_t',S.theme);}catch(e){}
  render();
}
function resetP(){
  S.params=Object.assign({},_DEF);
  try{localStorage.removeItem('arb_hp');}catch(e){}
  reload();
}
function reload(){
  try{localStorage.setItem('arb_hp',JSON.stringify(S.params));}catch(e){}
  document.querySelectorAll('#spinner').forEach(el=>el.style.display='inline');
  const q=new URLSearchParams(S.params).toString();
  fetch('/history-data?'+q)
    .then(r=>r.json())
    .then(d=>{S.data=d;render();})
    .catch(e=>{console.error(e);document.querySelectorAll('#spinner').forEach(el=>el.style.display='none');});
}
reload();
</script>
</body>
</html>"""


# ── History JSON endpoint ─────────────────────────────────────────────────────
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
            lag_sec         = float(params.get("lag_sec",         2.0)),
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


# ── GIT AUTO-PUSH ─────────────────────────────────────────────────────────────
def auto_push():
    import subprocess
    root = Path(__file__).resolve().parent

    def run(cmd):
        return subprocess.run(cmd, cwd=root, capture_output=True, text=True, timeout=30)

    try:
        if run(["git", "rev-parse", "--is-inside-work-tree"]).returncode != 0:
            log.debug("git: not a repo, skipping"); return
        if not run(["git", "remote", "get-url", "origin"]).stdout.strip():
            log.warning("git: no origin remote"); return
        run(["git", "update-index", "--refresh"])
        if not run(["git", "status", "--porcelain"]).stdout.strip():
            log.debug("git: nothing to push"); return
        run(["git", "add", "-A"])
        ts = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        r = run(["git", "commit", "-m", f"auto: {ts}"])
        if r.returncode != 0 and "nothing to commit" not in r.stdout: return
        branch = run(["git", "branch", "--show-current"]).stdout.strip() or "main"
        run(["git", "pull", "--rebase", "origin", branch])
        r = run(["git", "push", "origin", branch])
        if r.returncode == 0: log.info("git: pushed to origin/%s", branch)
        else: log.error("git push failed: %s", r.stderr.strip())
    except Exception as e:
        log.error("auto_push: %s", e)


# ── DEBUG PAGE ────────────────────────────────────────────────────────────────
_LOG_HTML = """<!DOCTYPE html>
<html>
<head>
<meta charset="utf-8">
<title>Collector Log</title>
<style>
*{box-sizing:border-box;margin:0;padding:0}
body{background:#0d0d0d;color:#ccc;font:12px/1.5 'Courier New',monospace}
#bar{position:fixed;top:0;left:0;right:0;background:#111;border-bottom:1px solid #1e1e1e;
     padding:8px 16px;display:flex;gap:16px;align-items:center;z-index:9}
#bar h1{font-size:13px;color:#fff;letter-spacing:.05em}
.cnt{color:#555;font-size:11px}.cnt b{color:#aaa}
#pause{margin-left:auto;background:#1a1a1a;border:1px solid #2a2a2a;color:#aaa;
       padding:4px 14px;cursor:pointer;border-radius:3px;font:inherit}
#pause:hover{color:#fff}
#dot{width:8px;height:8px;border-radius:50%;background:#ef4444;flex-shrink:0}
#dot.on{background:#22c55e;box-shadow:0 0 6px #22c55e}
#log{margin-top:38px}
.row{display:grid;grid-template-columns:68px 110px 1fr;gap:0 10px;
     padding:3px 16px;border-bottom:1px solid #111}
.row:hover{background:#131313}
.ts{color:#3f3f3f}.src{font-weight:700;white-space:nowrap}
.src-THRILL{color:#a78bfa}.src-SHARP{color:#38bdf8}
.src-SHARPSCORE{color:#6ee7b7}.src-SHARPCATALOG{color:#555}
.detail{color:#52525b;word-break:break-all}
.detail .v{color:#86efac}.detail .name{color:#e2e8f0}
.detail .mkt{color:#f59e0b}.detail .tour{color:#818cf8}
</style>
</head>
<body>
<div id="bar">
  <span id="dot"></span>
  <h1>ODDS COLLECTOR</h1>
  <div class="cnt">thrill <b id="ct">0</b></div>
  <div class="cnt">sharp <b id="cs">0</b></div>
  <div class="cnt">score <b id="csc">0</b></div>
  <div class="cnt">total <b id="ctot">0</b></div>
  <button id="pause" onclick="toggle()">&#9646;&#9646; Pause</button>
</div>
<div id="log"></div>
<script>
let paused=false, ct=0, cs=0, csc=0;
const logEl=document.getElementById('log');
const dot=document.getElementById('dot');
function toggle(){paused=!paused;document.getElementById('pause').textContent=paused?'&#9654; Resume':'&#9646;&#9646; Pause';}
function colorize(s){
  return s
    .replace(/(\\d+\\.\\d+)/g,'<span class="v">$1</span>')
    .replace(/\\|([^|]+)\\|/g,'|<span class="name">$1</span>|')
    .replace(/mkt=([^|]+)/,'mkt=<span class="mkt">$1</span>')
    .replace(/tour=([^|]+)/,'tour=<span class="tour">$1</span>');
}
function addRow(line){
  const parts=line.split('\\t');
  if(parts.length<3) return;
  const [ts,src,detail]=parts;
  const k=src.trim().replace(/-/g,'');
  if(k==='THRILL') ct++;
  else if(k==='SHARP') cs++;
  else if(k==='SHARPSCORE') csc++;
  document.getElementById('ct').textContent=ct;
  document.getElementById('cs').textContent=cs;
  document.getElementById('csc').textContent=csc;
  document.getElementById('ctot').textContent=ct+cs+csc;
  const div=document.createElement('div');
  div.className='row';
  div.innerHTML=`<span class="ts">${ts}</span><span class="src src-${k}">[${src.trim()}]</span><span class="detail">${colorize(detail)}</span>`;
  logEl.prepend(div);
  while(logEl.children.length>400) logEl.removeChild(logEl.lastChild);
}
const es=new EventSource('/log-stream');
es.onopen=()=>dot.classList.add('on');
es.onerror=()=>dot.classList.remove('on');
es.onmessage=e=>{if(!paused) addRow(e.data);};
fetch('/log-history').then(r=>r.json()).then(lines=>{lines.slice(-150).forEach(addRow);});
</script>
</body>
</html>"""


# ── MAIN ──────────────────────────────────────────────────────────────────────
if __name__ == "__main__":
    auto_push()

    conn   = init_db(DB_PATH)
    writer = BatchWriter(conn)
    hb     = HBTracker(writer)

    thrill_col = ThrillCollector(writer, hb)
    sharp_col  = SharpCollector(writer, hb)

    if DB_TTL_SEC > 0:
        PruneThread(DB_PATH, writer).start()
        log.info("PruneThread: TTL=%.1fh, interval=%.0fs", DB_TTL_SEC / 3600, PRUNE_EVERY)

    app = Flask(__name__)

    @app.post("/thrill")
    def ingest_thrill():
        thrill_col.process(request.get_json(force=True, silent=True) or {})
        return {"ok": True}

    @app.post("/sharp-ws")
    def ingest_sharp_ws():
        data = request.get_json(force=True, silent=True) or {}
        if isinstance(data, list):
            for item in data: sharp_col.process_ws(item)
        else: sharp_col.process_ws(data)
        return {"ok": True}

    @app.post("/sharp-score")
    def ingest_sharp_score():
        data = request.get_json(force=True, silent=True) or {}
        if isinstance(data, list):
            for item in data: sharp_col.process_score(item)
        else: sharp_col.process_score(data)
        return {"ok": True}

    @app.post("/sharp-catalog")
    def ingest_sharp_catalog():
        return {"ok": True}

    @app.get("/health")
    def health():
        rconn = sqlite3.connect(DB_PATH, check_same_thread=False)
        t  = rconn.execute("SELECT COUNT(*) FROM thrill").fetchone()[0]
        sw = rconn.execute("SELECT COUNT(*) FROM sharp_ws").fetchone()[0]
        ss = rconn.execute("SELECT COUNT(*) FROM sharp_score").fetchone()[0]
        hb_n = rconn.execute("SELECT COUNT(*) FROM heartbeat").fetchone()[0]
        rconn.close()
        return {"ok": True, "thrill": t, "sharp_ws": sw,
                "sharp_score": ss, "heartbeat": hb_n}

    @app.get("/log")
    def log_page():
        return Response(_LOG_HTML, mimetype="text/html")

    @app.get("/log-history")
    def log_history():
        with _log_lock:
            return json.dumps(_log_q[-200:])

    @app.get("/log-stream")
    def log_stream():
        q = queue.Queue(maxsize=200)
        _log_subs.append(q)
        def gen():
            try:
                while True:
                    try:    msg = q.get(timeout=25); yield f"data: {msg}\n\n"
                    except queue.Empty: yield ": ping\n\n"
            finally:
                try: _log_subs.remove(q)
                except: pass
        return Response(gen(), mimetype="text/event-stream",
                        headers={"Cache-Control": "no-cache", "X-Accel-Buffering": "no"})

    @app.get("/history")
    def history_page():
        return Response(_HISTORY_HTML, mimetype="text/html")

    @app.get("/history-data")
    def history_data():
        params = {k: v for k, v in request.args.items()}
        result = build_history_response(DB_PATH, params)
        return Response(json.dumps(result, separators=(",", ":")),
                        mimetype="application/json",
                        headers={"Access-Control-Allow-Origin": "*"})

    log.info("Collector → http://%s:%d  |  DB: %s  |  Log: http://localhost:%d/log",
             HOST, PORT, DB_PATH, PORT)
    app.run(host=HOST, port=PORT, threaded=True, use_reloader=False, debug=False)