"""
Работа с БД: инициализация, асинхронный BatchWriter,
HBTracker (дедуплицированные heartbeat'ы), PruneThread (авто-очистка).
"""
from __future__ import annotations
import logging
import queue
import sqlite3
import threading
import time
from typing import Dict

from config import (
    BATCH_MAX_ROWS, BATCH_MAX_SEC,
    DB_TTL_SEC, PRUNE_EVERY,
    HB_INTERVAL_SEC,
)

log = logging.getLogger(__name__)


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
