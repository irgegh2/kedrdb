# ADR 004: WAL и приватные изменения

Дата: 9 октября 2026 года. Статус: реализовано в ветке этапа 3; окончательная приемка в [отчете](../milestones/03.md).

## No-steal без зависимости от cache eviction

Tree store выполняется поверх private StorageIo overlay. Write/resize меняют только ограниченный набор полных страниц и logical file length. Read объединяет исходный файл и overlay; sync overlay не синхронизирует исходный файл. Поэтому даже dirty cache eviction и metadata checkpoint дерева не меняют исходные данные до COMMIT.

Начальный лимит — 256 modified page images, включая metadata (около 4 МиБ). Превышение или ошибка команды до записи WAL откатывает весь overlay. Ошибка внутри будущей явной транзакции переводит сессию в aborted. Для read-only операции durable WAL не требуется; SQL/session policy определяется этапом 4.

Перед commit: завершить tree checkpoint в overlay, получить полный ограниченный write set, logical length и проверить индексы/каталог на прикладном уровне. Затем записать PAGE_IMAGE records и COMMIT, выполнить WAL sync, применить подтвержденные страницы к data file. В этой начальной реализации apply/data sync выполняются до ответа; ошибка после WAL durable point сообщает неопределенный исход и прекращает работу handle. Recovery повторяет commit полностью. WAL сохраняется до отдельного безопасного checkpoint/rotation.

Rollback не выполняет writes/sync исходного файла. После ошибки write/sync WAL запрещено продолжать append поверх неоднозначного хвоста. Не возвращать «rolled back», если commit мог сохраниться.

## Сегмент WAL

Codec работает с одним открытым сегментом; файловый facade управляет созданием, ротацией, checkpoint и удалением покрытой истории. Точный порядок — в [DURABILITY.md](../DURABILITY.md).

Header 128 bytes: magic[8]=KEDRWAL1 @0; version:u16=1 @8; header length:u16=128 @10; reserved:u32=0 @12; database ID[16] @16; segment ID:u64 @32; first LSN:u64 @40; previous transaction ID:u64 @48; previous commit LSN:u64 @56; CRC32C:u32 @64; reserved[60]=0 @68. Все integer little-endian. При CRC checksum bytes зануляются.

Database/segment ID nonzero, first LSN=previous commit LSN+1. Сегменты сменяются только на commit boundary. Имя нового файла и каталоги синхронизируются до подтверждения использующего их commit; abstract StorageIo этого не обеспечивает. Файловый facade владеет lock/paths/directory sync. Без него компонент проверяет только заданные гарантии открытых I/O.

Data metadata сохраняет расположение полей ADR 003, но version:u16=2 @8, applied commit LSN:u64 @64 и flags:u64=1 @72. CRC остается @80 и покрывает полную страницу. Две копии имеют generation и CRC. Recovery сначала проверяет WAL без публикации, UUID и максимальный LSN корректных metadata, затем выполняет redo. Неизвестная версия одной копии отклоняется, не маскируется fallback. Прямой tree API отклоняет v2; только private transaction I/O открывает его. Legacy format v1 остается отдельным development store без WAL.

Record header 48 bytes как ADR 002: magic[8]=KEDRWREC, version:u16=1, kind:u16, header length:u32=48, total length:u32, flags:u32=0, LSN:u64, transaction ID:u64, CRC32C:u32, reserved:u32=0. CRC покрывает всю record с нулевым checksum. Проверять total length <=16440 до выделения/чтения payload.

PAGE_IMAGE kind=1: page ID:u64 и ровно 16384 bytes; metadata pages 0/1 тоже журналируются. COMMIT kind=2: first image LSN:u64, last image LSN:u64, image count:u32, reserved:u32=0, final data length:u64. В группе 1..256 images, уникальные page IDs, один transaction ID; LSN строго последовательны, COMMIT непосредственно завершает набор. Target length кратна page size, минимум 3 pages; все image IDs входят в target length. Повтор transaction ID или другой database ID отклоняется.

## Recovery и хвост

Recovery разбирает журнал последовательно, удерживая не более одной transaction group. Callback получает только полную проверенную COMMIT-группу. Redo пишет полные образы, включая metadata; повтор того же redo допустим. После последнего примененного commit data sync выполняется до очистки неподтвержденного хвоста журнала. Сбой при redo оставляет WAL для повторного полного восстановления.

Неполный header/body на физическом EOF или полные PAGE_IMAGE без COMMIT — неподтвержденный хвост. На него не опираются данные; он удаляется только после успешной проверки и применения подтвержденного префикса. CRC/структурная ошибка в полной record — диагностируемое повреждение, не молчаливое игнорирование. Автовосстановление поврежденного полного tail с неверной CRC не обещается этим консервативным протоколом; crash campaign должна явно отличать этот отказ от потери подтвержденной истории.

Не проверять только факт успешного restart. Эталон фиксирует полученные клиентом acknowledgements, ambiguous outcomes и отдельно содержимое после recovery. Нужны прерывания каждого write/sync/resize/read, short I/O, persistence-before-error, несинхронизированная частичная сохранность и повторный crash при redo. Для нескольких сегментов также проверяются directory metadata, создание/удаление и удержание журнала backup.
