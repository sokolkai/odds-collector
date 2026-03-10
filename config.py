"""
Конфигурация коллектора — все константы в одном месте.
"""
import logging

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
