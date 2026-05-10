# Broker History

## 2026-05-09: Фикс лог-наводнения (шаги 1-3 из инцидента 2026-05-06)

Закрыты три из четырёх корневых дефектов прошлого инцидента. Шаг 4 (диагностика
почему overflow вообще постоянный — нет читателей? мелкий буфер? фоновый
писатель в PTY?) выделен в отдельную задачу.

### Что сделано в `broker.py`

1. **Ротация логов.** `setup_logger` теперь использует
   `logging.handlers.RotatingFileHandler` с `maxBytes=10 MiB`,
   `backupCount=3`. Жёсткий потолок ~40 MiB на сессию даже при худшем
   сценарии. Константы — `LOG_MAX_BYTES`, `LOG_BACKUP_COUNT`.
2. **Агрегация overflow-warning'ов.** В `SessionDaemon` добавлены счётчики
   `_overflow_bytes`, `_overflow_events`, `_overflow_last_flush`. На каждый
   overflow `_read_pty` инкрементирует счётчики и вызывает
   `_maybe_flush_overflow()`, который раз в `OVERFLOW_FLUSH_INTERVAL = 1.0 s`
   эмитит одну строку:
   `Ring buffer overflow: dropped X bytes in last 1.0s (M events)`.
   Хвост добивается из `_cleanup` принудительным флашем (`force=True`),
   чтобы при штатной остановке не терять последние накопленные события.
3. **Удаление логов в `kill <name>`.** После SIGTERM ждём до 2 сек реальной
   смерти процесса (`os.kill(pid, 0)`-пинг в цикле), затем чистим
   `logs/<name>.log*` (включая ротированные backup'ы). Аварийная сессия
   больше не оставляет гигабайты после себя.

### Контракт после фикса

- Верхняя граница диска на сессию: ~40 MiB (4 файла × 10 MiB).
- При устойчивом overflow: ≤86 400 строк лога/сутки/сессия (1 строка/сек),
  даже если PTY непрерывно тонет.
- При `broker kill <name>`: логи удаляются автоматически. Чтобы оставить
  пост-мортем — не вызывать `kill`, а копировать `/tmp/devolution-broker/logs/<name>.log*`
  до kill.

### Что НЕ сделано (вынесено отдельной задачей)

- Найти первопричину постоянного overflow: что пишет в PTY быстрее, чем
  ring buffer (256 KiB) успевает читаться, когда нет клиентов.
- Возможно, при `not self.clients` стоит увеличить буфер или приостановить
  чтение PTY (хотя у нас уже есть suspend-after — почему он не сработал?).

---

## 2026-05-06: Unbounded `Ring buffer overflow` log → 74 GiB, диск VPS забит (повторно)

Long-run сессия `main` на VPS `31.59.58.81` (контейнер `debian13-docker`) за ~5 дней
сгенерировала `/tmp/devolution-broker/logs/main.log` размером **79 551 832 346 байт (~74 GiB)**,
заполнив `/dev/vda3` до 100%. Это **второй такой инцидент** — первый был в рамках
прошлого long-run теста, корневая причина тогда не была зафиксирована.

### Что в логе
Файл целиком — повторение одной строки:
`YYYY-MM-DD HH:MM:SS [WARNING] Ring buffer overflow: 111 bytes dropped`
~1.13 миллиарда строк за период `2026-05-04 07:54:23` → `2026-05-06 11:38`.
Образцы 100 KiB head/tail сохранены в `findings/_head_100KB_2026-05-06.log` и
`findings/_tail_100KB_2026-05-06.log`. Подробный разбор: `findings/2026-05-06_disk_overflow_loop.md`.

### Корневые баги (минимум два)
1. **`broker.py:486-488`** — WARNING пишется на каждый overflow ring buffer'a без
   rate-limit'а. В установившемся режиме «poller выгребает данные быстрее клиентов»
   это даёт тысячи строк/сек.
2. **`broker.py:94-106`** — `setup_logger()` использует обычный `logging.FileHandler`,
   без `RotatingFileHandler` / без `maxBytes`. Файл растёт неограниченно.

### Предлагаемые фиксы (отдельная задача)
- Минимум: `RotatingFileHandler(maxBytes=10*1024*1024, backupCount=3)` в `setup_logger`.
- Качественно: дроссель warning'a — агрегировать byte-count и логировать одно
  «dropped X bytes in last Ns (M events)» раз в N секунд.
- Заодно — найти, почему overflow вообще постоянный (нет клиентов? слишком мелкий буфер?
  фоновый писатель в PTY?). Сама по себе ротация — пластырь, не решение.
- В `broker.py kill <name>` (или `prune`) — убирать лог-файлы после остановки сессии.

### Очистка после инцидента
- Сессия `main` остановлена через `broker kill main` (pid 100966 + child 100968).
- `/tmp/devolution-broker/` целиком удалён → освобождено ~74 GiB.
- `df -h /` после: `25G/99G` (было `99G/99G`).
- OmniRoute (соседний контейнер) не пострадал, патчи на месте.

---

## 2026-04-19: Active `claude` sessions missing from `list --json`

Observed on host in UTC timezone.

### What was observed

- `ps` showed three running `claude` processes started via broker:
  - session `broker` (`python3 /usr/local/bin/broker session broker claude` -> child `claude`)
  - session `devolution` (`python3 /usr/local/bin/broker session devolution claude` -> child `claude`)
  - session `tax` (`python3 /usr/local/bin/broker session tax claude` -> child `claude`)
- Each `claude` child had broker env vars:
  - `DEVOLUTION_BROKER_SESSION` set to `broker` / `devolution` / `tax`
  - `DEVOLUTION_BROKER_META` set to `/tmp/devolution-broker/<session>.json`

### JSON/list mismatch

- `python3 broker.py list --json` returned only one session: `codex`.
- In `/tmp/devolution-broker`, only `codex.json`/`codex.pid`/`codex.sock` were present.
- Files for `broker`, `devolution`, `tax` (`.json`, `.pid`, `.sock`) were absent.
- `broker`, `devolution`, `tax` log file descriptors were visible as deleted files in `/proc/<pid>/fd`.

### Conclusion

`list --json` currently does **not** return those three active `claude` sessions because it enumerates `*.json` files from `/tmp/devolution-broker` and those metadata files were missing at check time.

## 2026-04-19: Connection delete consistency requirement (Hub)

User-reported behavior:
- After pressing "delete connection" in Hub, session metadata files disappeared.
- Broker daemons and child `claude` processes stayed alive.
- Result: sessions became invisible in Hub JSON while still consuming memory.

Expected behavior:
- Either remove session processes together with connection metadata,
- or keep/recover metadata so active sessions are still visible in Hub.

Implemented fix in `broker.py`:
- `list --json` now discovers live daemon sessions directly from `/proc` if metadata is missing.
- Missing `.json`/`.pid` metadata is recreated for discovered live sessions.
- `kill <name>` now has fallback lookup by live daemon process when `.pid` is missing.

## 2026-04-19: Orphan daemons + "empty session" + uptime reset

Follow-up: the discovery-based recovery above had three issues exposed in production.

### Symptoms
- After Hub wiped `/tmp/devolution-broker`, daemons stayed alive but their socket files
  were unlinked. `broker session devolution` then opened a NEW empty daemon on top of
  the ghost, so the app showed an empty terminal while the orphan kept the Claude child.
- `list --json` reported wrong `uptime`: a session running 26h showed 5h. `ps` showed the
  real age; metadata said otherwise.
- A client `broker session devolution` could hang in `connect()` for 45+ min when the
  socket file was missing but a stale pidfile or metadata made the session look alive.

### Root causes
1. `_discover_live_daemon_sessions()` read `proc_entry.stat().st_ctime` as process start
   time. On `/procfs`, inode times reflect *access* time, not process creation — so every
   metadata rebuild reset `created` to near-now.
2. Recovery returned daemons even when their socket file was gone, letting `list` report
   unreachable sessions as healthy.
3. Daemons had no mechanism to notice that their own socket file had been deleted and
   self-terminate.
4. `attach_session` had no connect timeout, so a stale `.sock` from a terminating daemon
   could hang a client indefinitely.

### Fixes
- New `_proc_start_time(pid)` reads `/proc/<pid>/stat` field 22 (starttime in jiffies)
  plus `/proc/stat btime` for a real process start time.
- `_discover_live_daemon_sessions()` uses that start time and skips entries without a
  socket file on disk — orphans are invisible to `list` and cannot be attached to.
- `SessionDaemon` captures the inode of its socket after bind and checks every 5 s that
  the file still points to the same inode. If it has been unlinked or replaced, the
  daemon self-terminates via its normal `_cleanup` path.
- `cleanup_dead_session()` now SIGTERMs any live orphan daemon for the same name when
  no socket file exists (so re-creating a session cleans up the ghost).
- `attach_session()` uses a 5 s `connect()` timeout and raises `ConnectionRefusedError`
  on failure, which triggers the normal cleanup path.

### Manual cleanup performed
- Killed pre-fix orphan daemons for `devolution` (744606) and `tax` (784445) plus their
  Claude children and a hanging client (795269). New daemons created after the fix will
  self-terminate via the new watchdog path.
