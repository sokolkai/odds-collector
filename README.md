# Odds Collector

Собирает данные от Thrill (SSE) и Sharp (WebSocket) в локальную SQLite базу.

## Установка

```bash
pip install -r requirements.txt
```

## Запуск

```bash
python collector.py
```

База создаётся автоматически как `odds.db` в той же папке.

## Настройки (верхушка collector.py)

| Параметр | По умолчанию | Описание |
|---|---|---|
| `DB_PATH` | `odds.db` | путь к базе |
| `TTL_HOURS` | `48` | записи старше N часов удаляются |
| `THRILL_URL` | — | SSE endpoint Thrill |
| `SHARP_WS_URL` | — | WebSocket URL Sharp |

## Схема базы

### Таблица `thrill`

| Колонка | Тип | Описание |
|---|---|---|
| `ts` | REAL | unix timestamp события |
| `event_id` | TEXT | ID матча на Thrill |
| `player1` | TEXT | имя первого игрока |
| `player2` | TEXT | имя второго игрока |
| `tournament` | TEXT | название турнира |
| `status` | TEXT | live / prematch / unknown |
| `p1_back` | REAL | коэффициент на P1 |
| `p2_back` | REAL | коэффициент на P2 |
| `score_sets` | TEXT | JSON: `[[6,4],[3,2]]` |
| `score_game` | TEXT | текущий гейм: `"40:30"` |
| `server` | INT | кто подаёт: 1 или 2 |

### Таблица `sharp`

| Колонка | Тип | Описание |
|---|---|---|
| `ts` | REAL | unix timestamp |
| `event_id` | TEXT | ID матча на Sharp |
| `player1` | TEXT | |
| `player2` | TEXT | |
| `tournament` | TEXT | |
| `status` | TEXT | live / prematch |
| `p1_back` | REAL | Back P1 |
| `p1_lay` | REAL | Lay P1 |
| `p2_back` | REAL | Back P2 |
| `p2_lay` | REAL | Lay P2 |

## Адаптация Sharp

В `SharpCollector.process()` подставь реальные поля из WS-пакетов Sharp.
Thrill работает из коробки — логика взята из основного сканера.

## Примеры запросов

```sql
-- последние 100 записей Thrill
SELECT ts, player1, player2, p1_back, p2_back, score_game
FROM thrill ORDER BY ts DESC LIMIT 100;

-- все live матчи прямо сейчас
SELECT * FROM thrill
WHERE status='live' AND ts > unixepoch() - 60
ORDER BY ts DESC;

-- история коэффициентов по матчу
SELECT ts, p1_back, p2_back FROM thrill
WHERE player1 LIKE '%Djokovic%'
ORDER BY ts;
```
