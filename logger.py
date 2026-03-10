"""
Live-log: in-memory буфер + SSE-подписчики.
"""
import time
import queue
import threading
from typing import List

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
