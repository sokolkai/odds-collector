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

Структура проекта:
  collector.py   — точка входа, Flask-роуты, auto_push
  config.py      — все константы конфигурации
  logger.py      — live-log (_log, _log_q, _log_subs)
  db.py          — init_db, BatchWriter, HBTracker, PruneThread
  thrill.py      — ThrillCollector
  sharp.py       — SharpCollector
  history.py     — name matching, arb math, compute_history, build_history_response
  templates.py   — HTML страницы (_HISTORY_HTML, _LOG_HTML)
"""
from __future__ import annotations
import json
import logging
import queue
import sqlite3
import threading
from datetime import datetime
from pathlib import Path

from flask import Flask, Response, request

from config import HOST, PORT, DB_PATH, DB_TTL_SEC, PRUNE_EVERY
from db import init_db, BatchWriter, HBTracker, PruneThread
from history import build_history_response
from logger import _log_lock, _log_q, _log_subs
from sharp import SharpCollector
from templates import _HISTORY_HTML, _LOG_HTML
from thrill import ThrillCollector

log = logging.getLogger(__name__)


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
