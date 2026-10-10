# KedrDB: установка, резервные копии и обслуживание

Этап 7, 10 октября 2026. Development-сборка `0.1.0`, не выпуск 1.0. Все примеры создают новые каталоги. Проверки на синтетических данных не заменяют независимый пилот. Перенос PostgreSQL описан в [MIGRATION.md](MIGRATION.md).

Для обычной локальной работы после сборки достаточно запустить `kedr-studio`: он запускает свою KedrDB и подключается автоматически. Расположение постоянных данных и отдельный сервер: [CONNECTIONS.md](CONNECTIONS.md). Перед offline backup своей базы нужно закрыть Studio.

## Свежая установка

Нужны Git, Rust/rustup и существующий каталог, доступный текущему пользователю. Бесплатные CI проверки выполняются на обычных публичных GitHub runners; скорость CI не используется для сравнения производительности.

```sh
git clone https://github.com/irgegh2/kedrdb.git
cd kedrdb
git switch codex/server-and-studio
cargo fetch --locked
cargo build --workspace --release --locked --offline
./target/release/kedr doctor
./target/release/kedr storage-check .
./target/release/kedr create-db shop
./target/release/kedr sql shop 'CREATE TABLE products (id BIGINT PRIMARY KEY, name TEXT, stock INT)'
./target/release/kedr sql shop "INSERT INTO products (id,name,stock) VALUES (1,'Кедр',10)"
./target/release/kedr sql shop 'SELECT * FROM products ORDER BY id'
```

На Linux для Studio нужны библиотеки X11/Wayland/OpenGL, libxkbcommon; для файлового диалога — libdbus и подходящий XDG Desktop Portal либо Zenity. Пакеты Ubuntu для сборки перечислены в [CI](../.github/workflows/ci.yml). На macOS диалог системный. Установочный пакет без Rust готовится на этапе 10.

Создание SCRAM verifier и запуск сервера — [CONNECTIONS.md](CONNECTIONS.md). Пароль вводится скрыто, не передается аргументом. Приложение: `./target/release/kedr-studio`. Подключиться, создать таблицу через SQL, обновить/выбрать структуру, INSERT, SELECT, BEGIN/ROLLBACK. «Открыть…» читает UTF-8 `.sql` до 64 КиБ, «Сохранить…» сохраняет документ через системный диалог (синхронизация файла и atomic rename; гарантия database commit/directory durability к документам редактора не относится). Файл не выполняется при открытии. Один запуск — один оператор, либо выделенный оператор; многокомандные скрипты целиком не поддерживаются. Полный SQL export больших баз предназначен для CLI-импортера, не редактора с лимитом 64 КиБ.

## Резервная копия и восстановление

1. Закрыть запись приложения и остановить сервер через Ctrl+C/SIGTERM. Дождаться завершения процесса. CLI требует эксклюзивный OS lock и не делает online backup работающего сервера.
2. Выполнить backup в новый каталог вне каталога базы. Писатели остаются остановленными, пока команда не завершится: открытие/recovery → полная физическая проверка → checkpoint → копирование identity/data/пустого активного WAL → fsync файлов/каталогов → manifest последним. Ошибки не дают сообщения `backup_complete`.
3. Проверить копию, перенести **весь каталог** и восстановить в новый путь. Существующий путь, даже пустой каталог, не перезаписывается.

```sh
./target/release/kedr backup shop shop-backup-001
./target/release/kedr verify-backup shop-backup-001
./target/release/kedr restore shop-backup-001 shop-restored-001
./target/release/kedr integrity-check shop-restored-001
./target/release/kedr export-sql shop source.sql
./target/release/kedr export-sql shop-restored-001 restored.sql
cmp source.sql restored.sql
```

Перед возобновлением записи выбрать **одну** рабочую копию и проверить прикладные запросы. Backup сохраняет database identity; две независимые активные копии одного identity нельзя считать репликами. Source backup не содержит SCRAM-файл, TLS private key или профили Studio: их подготовить отдельно по инструкции подключения.

Manifest версии 1 содержит database ID, data/page/WAL format, checkpoint LSN, segment ID, raw record count, размеры и SHA-256 трех файлов. Проверяется полный граф страниц/свободного списка, CRC, identity и пустой WAL с той же границей. Неожиданные файлы, пути `..`, symlink-члены, неизвестные версии и неполная копия отклоняются. SHA-256 обнаруживает изменение относительно manifest; изменяемый вместе с данными manifest не является подписью доверенного отправителя. Каталоги создаются 0700, копии/manifest — 0600. Ограничение одного файла 64 ГиБ; буфер копирования 64 КиБ.

Незавершенная копия сохраняется для диагностики. Физическое восстановление не имеет команды продолжения: выбрать новый путь и повторить из проверенной backup. Не удалять quarantine marker вручную для обхода проверки.

## SQL export/import и продолжение

```sh
./target/release/kedr export-sql shop shop.sql
./target/release/kedr import-sql shop.sql shop-imported
# Если процесс импорта прервался:
./target/release/kedr resume-import shop.sql shop-imported
```

Импортер принимает собственный **канонический** export v1 с заголовком/финальным footer. Обычный pg_dump, произвольный SQL и исполняемые psql команды не принимаются. До создания target читается весь документ и проверяются размеры/допустимые CREATE TABLE/INDEX и literal INSERT. SQL исходник не меняется. Для продолжения нужен тот же файл с тем же SHA-256 и числом операторов; свежий snapshot не подставляется.

Оператор и ordinal/progress фиксируются одной WAL-транзакцией. Уже примененные операторы пропускаются. Каждые 256 операторов выводится `import_progress`; это отметка durable прогресса, не обещание окончания. Обычные клиенты отклоняют `.import-incomplete`, в том числе symlink marker. На карантинной копии FK проверяются после загрузки всех строк, что позволяет self-reference циклы. UNIQUE/PK/NULL/type проверяются при вставке. Перед публикацией проверяются все FK и индексы, **непосредственно все байты** канонического экспорта схемы и значений, физическая целостность и checkpoint. Только затем снимается marker и выводится `import_complete`.

Предел 64 КиБ относится к одному оператору, а не всему экспорту. Строки читаются пакетами по 64; весь набор не помещается в память. Слишком большая строка/индекс, ошибка диска, неверная FK или несовпадение оставляют target закрытым. Только нормализованный логический SQL переносится; физические номера таблиц и внутренние имена UNIQUE-индексов не сохраняются. Raw KV база не экспортируется как пустая SQL база.

## Статус и диагностический пакет

Через подключенный SQL-клиент или Studio:

```sql
SHOW KEDR STATUS;
SHOW KEDR METRICS;
```

STATUS показывает API/версию и I/T/E транзакции. METRICS возвращает одну TEXT колонку с JSON v1 и не ожидает writer lock. Накопленные метрики процесса: выполненные запросы/ошибки, число соединений/отказов/auth errors, активные сессии; фиксированная гистограмма execution latency, **верхние границы** p50/p95/p99; readers/writer/очередь и суммарное/максимальное ожидание lock; байты чтения/data/WAL, fsync calls/time/errors, checkpoint attempts/time/errors. Замер запроса включает lock wait и storage I/O, исключает передачу результата; Parse ошибки входят в protocol_errors, не в выполненные запросы. Нет per-query текста и идентификаторов таблиц. Storage counters относятся к процессу, включают startup/recovery и другие его FileStorage операции. Ошибки open/resize/metadata отдельно счетчиком IO не охватываются.

Системный sampler выполняется только при запросе METRICS: RSS процесса, размеры data/retained WAL и доступное место (`df -Pk`). Недоступное измерение — JSON null, а не ноль. Это ориентир наблюдаемости; процентили из фиксированных buckets не являются точными benchmark-результатами. Статистика сбрасывается при перезапуске.

Сервер пишет JSON строки в stderr: `query_error` с SQLSTATE, `slow_query` при execution >=250 мс, `connection_error` с кодом, `startup_or_server_error` и краткое текстовое объяснение запуска. SQL, параметры, пароль, SCRAM proof и ключи в структурированные события не включаются. Существующая stdout строка listening сохраняется для скриптов. Сохранить stderr в приватный файл при запуске.

Offline, после остановки сервера:

```sh
./target/release/kedr diagnostics shop diagnostic-001.json
```

JSON содержит только платформу/версию/форматы/identity/LSN и размеры/граф дерева. Без SQL, строк, названий таблиц, credentials и ключей. Файл создается новым 0600. Пакет не автоматически отправляется куда-либо.

## Инциденты

| Сценарий | Действие и ожидаемое поведение |
|---|---|
| Закончилось место / fsync error | Не повторять неоднозначную запись. При KD3002 / SQLSTATE 08007 commit мог состояться; остановить сервер, сохранить каталог, освободить место вне файлов базы, открыть/recovery, проверить бизнес-ключ операции и integrity. Не удалять WAL, чтобы «запустить». Backup без complete manifest не считать завершенным. |
| Зависший клиент держит транзакцию | METRICS покажет writer/readers/ожидание. Сервер: lock wait 5 с, idle-in-transaction 60 с, transaction lifetime 300 с. Cancel/разрыв освобождает private transaction; уже достигнутый durable commit не отменяется. Автоповтора commit нет. |
| Плохие права | Проверить владельца каталогов/auth/TLS key; auth/key должны быть приватными. Использовать того же OS-пользователя. Не исправлять через всем-доступные права; failed open не дает listening. |
| Неизвестный формат | Сохранить исходник. Запустить подходящий бинарный файл или утвержденный мигратор на копии. Ручное изменение version/checksum не является конвертацией. |
| Поврежденная страница/журнал | Остановить запись; сохранить поврежденную копию и диагностические логи; восстановить последнюю проверенную backup в новый путь. CRC/SHA error не игнорируется. «Исправление» удалением WAL запрещено. |
| Обрыв импорта | Приложения к target не подключать; `resume-import` с точным сохраненным export. Не создавать новый source snapshot под старым job. |
| Studio потеряла связь | Повторное подключение вручную. Проверить результат последней записи по устойчивому ID. Ни SELECT/DDL, ни commit не повторяются автоматически. |

В тестах инъецируются ENOSPC при SQL export и ошибки короткого I/O/fsync в StorageIo campaign; также проверяются timeout/cancel/disconnect и SIGKILL import. Подробности описываются в отчете этапа. Аппаратное отключение питания и заполнение настоящего пользовательского диска этим не доказаны.

## Окно обслуживания

`backup_complete` содержит bytes/checkpoint_lsn/elapsed_ms; миграционный manifest — snapshot_ms. Это измерения **конкретного набора/диска**, не гарантия для заказчика. Пилот должен записать: допустимый простой, число/размер строк, диск, время stop→verified backup→restart, сроки переносов/сверки, запас свободного места и результат восстановления. Файл для заполнения: [PILOT_MAINTENANCE.md](PILOT_MAINTENANCE.md). Реального пилота и независимого человека еще нет; их приемку нельзя заменить тестом на пяти строках.
