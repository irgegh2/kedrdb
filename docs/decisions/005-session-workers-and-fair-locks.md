# ADR 005: сессии, справедливая очередь и ограничение времени

Дата: 9 октября 2026 года. Статус: реализация проходит приемку этапа 4.

SharedDatabase владеет каталогом через FileDatabase и глобальным Gate. Каждая сессия имеет один Rust worker thread и bounded channel емкостью 1. Transaction живет в стеке worker внутри FileDatabase.transaction; это позволяет сохранять borrow безопасно между командами без unsafe и self-referential структуры. Lifetime/idle deadline проверяется worker, даже когда клиент ничего не отправляет. При disconnect Drop закрывает очередь, ставит cancellation и дожидается выхода worker.

READ ONLY открывает отдельный read view того же файла под shared permit, с отдельным cache. Краткий mutex базы используется только при открытии view и не удерживается во время чтения. Writer держит exclusive permit и mutex FileDatabase, чтобы один владелец WAL/data выполнял commit. Блокировка каталога ОС остается у SharedDatabase весь срок работы сессий. Сессии admission-bound (до 64), thread-per-session выбран для первого ограниченного профиля, не как окончательная архитектура большого сервера.

Gate использует monotonic tickets и очередь. Writer получает доступ первым в очереди, только при нуле readers; reader проходит лишь если перед ним нет writer. Таймаут/отмена удаляют ticket и пробуждают остальных. Cache access проверяет cooperative interrupt, чтобы теплый кэш не обходил deadline/cancel. При commit последняя проверка стоит перед WAL; дальнейшие устойчивые действия завершаются без ошибочного обещания rollback.

Ошибки внутри explicit transaction оставляют aborted до ROLLBACK. Cancel/expiry освобождают ресурсы и оставляют aborted уже без lock. Состояние и лимиты — в [ISOLATION.md](../ISOLATION.md).

Проверка будущего SQL-каталога и вторичных индексов использует этот же атомарный набор ключей; реальные SQL histories добавляются на этапе 5. Утверждение serializable пока проверяется для текущего key/value API. Сеть не реализована этим ADR: disconnect сейчас означает закрытие локальной сессии, а EOF сокета должен вызывать тот же Drop на этапе 6.
