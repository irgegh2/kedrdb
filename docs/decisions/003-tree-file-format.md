# ADR 003: файл, кэш, строки и B+ tree этапа 2

Дата: 9 октября 2026 года. Статус: реализованы primitives хранения; WAL и транзакции отдельно. Формат до alpha может меняться с записью решения.

## Файл

Размер страницы 16384. Страницы 0 и 1 — две metadata copies. Начальная пустая база содержит также leaf page 2. Поля little-endian, все неиспользованные байты нулевые. Header:

| Смещение | Размер | Поле |
|---:|---:|---|
| 0 | 8 | `KEDRFILE` |
| 8 | 2 | format version=1 |
| 10 | 2 | header size=96 |
| 12 | 4 | page size=16384 |
| 16 | 16 | nonzero database ID |
| 32 | 8 | generation |
| 40 | 8 | root page |
| 48 | 8 | page count, включая metadata |
| 56 | 8 | free-list head, 0 если пуст |
| 64 | 16 | reserved=0, checkpoint/WAL определяются этапом 3 |
| 80 | 4 | CRC32C полной страницы с нулем в checksum |
| 84 | до конца | reserved=0 |

Открытие выбирает валидную копию с большим generation. Разные database ID или одинаковый generation с разным содержимым — отказ. Размер файла должен точно совпадать с page count; неизвестный формат, короткий файл и незавершенное расширение не скрываются. Это правило для штатного checkpoint, не обещание crash recovery.

Checkpoint этапа 2: dirty pages → data sync → inactive metadata с новым generation → data sync. Он не делает группу изменений атомарной при сбое: грязные страницы могут вытесняться раньше. Любая ошибка mutating I/O/checkpoint переводит handle в recovery-required. До WAL инструменты объявлены development-only.

## Кэш и владение

Количество frames ограничено byte budget с учетом размера Frame, память резервируется один раз. С оптимизации этапа 8 frames создаются только при загрузке страниц; емкость остается ограниченной вычисленным пределом, свободный frame после ошибочного чтения используется повторно. Статистика `frames` отражает число созданных frames, `allocated_bytes` — всю зарезервированную емкость. LRU по возрастающему clock; вытеснение сначала записывает dirty page. Ошибочная/частичная загрузка не оставляет ID у недочитанного frame. PageGuard удерживает исключительный Rust borrow и снимает pin при Drop; Drop не выполняет I/O. Этот интерфейс обеспечивает безопасность последовательного владельца, а не многопоточность или server transaction isolation.

CLI удерживает неблокирующую advisory OS lock файла всю операцию; создание использует create_new, случайный database ID из /dev/urandom и sync parent directory. Lock автоматически освобождается ОС при закрытии процесса. Это защита от других использующих тот же протокол владельцев; сторонний процесс с прямой записью может игнорировать advisory lock. Основание: [Rust File::try_lock](https://doc.rust-lang.org/std/fs/struct.File.html#method.try_lock).

## Узлы

У дерева собственный packed codec, отдельно от generic slotted-page primitive ADR 002. Magic `KEDRNODE`; version:u16@8=1; kind:u16@10 (1 leaf, 2 branch, 3 free); header size:u16@12=64; entry count:u16@14; page ID:u64@16; reserved:u64@24=0; CRC32C:u32@32; payload end:u16@36; level:u16@38; first child/free next:u64@40; reserved[16]@48=0.

Leaf level=0, first child=0. Каждая запись: key length:u16, value length:u16, key bytes, value bytes. Key length 1..512; value до 8192. Строка в 8 КиБ помещается вместе с ключом и заголовком в 16-КиБ страницу. Keys строго возрастают.

Branch level 1..63, first child задает левую ветку. Payload: key length:u16, separator bytes, next child:u64. Children=keys+1. Child level должен быть parent level-1. Сепараторы — нижние границы: слева `<separator`, справа `>=separator`; после удаления они могут не совпадать с фактическим минимальным ключом. Range traversal использует границы и обход дерева, без отдельной linked-leaf цепочки.

Split выбирает границу по закодированному объему, а не количеству ключей, чтобы длинные/короткие записи не создавали слишком большой узел. Для branch separator поднимается в родителя; для leaf остается первым ключом справа. Новый root повышает высоту. Максимальная глубина 64 ограничивает поврежденные ссылки/recursion.

Удаление перепаковывает leaf, освобождает пустые листья и опустевшие поддеревья, сокращает root с одним child. Непустые underfull узлы пока не сливаются: это выбранная простая корректная альтернатива; она может удерживать лишнее место после массового удаления. Это измеряемое ограничение; compaction/merge должны появиться при необходимости до конкурентного benchmark. Полное удаление оставляет одну пустую leaf, все остальные страницы повторно используются.

Free page kind=3: entry count/level=0, payload end=64; next=0 или валидная другая page ID. Allocator берет free head либо расширяет файл. Integrity check проверяет disjoint ownership дерева/free list, отсутствие циклов и потерянных страниц, bounds/levels/order/CRC. Bitmap ограничен 64 МиБ. Range results ограничены 100000 строк и 8 МиБ учитываемых байтов ключей/значений/descriptors; это отдельно от cache budget и RSS.

## Строки и индексные ключи

Row: version:u8=1, column count:u16, затем tagged values; максимум 256 columns и 8192 encoded bytes. Null=0; Int=1+i32 LE; BigInt=2+i64 LE; Bool=3+byte 0/1; Text=4+length:u32 LE+валидный UTF-8. Никаких implicit casts, invalid bool, неизвестных tags/version, trailing bytes или усечения UTF-8.

Первичный ключ BIGINT: big-endian u64 со смененным знаковым битом; byte order совпадает со signed integer order. Secondary key: tag/value encoding и PK suffix. Text бинарно сравнивается; нуль экранирован как 00ff, terminator 0000. Null сортируется раньше значений; SQL NULLS LAST — будущая executor policy. Одинаковые значения разных PK имеют разные ключи, включая NULL. Длинный encoded index key отклоняется явно. Атомарное обслуживание row+secondary indexes относится к этапам транзакций/каталога.

## Проверки

Случайный эталон — std BTreeMap с отдельным представлением данных; он не повторяет смещения и codec. Проверяются split разных уровней, variable keys, крайние BIGINT, максимальные строки, duplicates, update/range/delete, empty tree/refill, reopen, reuse и fail-closed sync. Реальный файл >4×cache читается после открытия с одной frame. Полная кампания задана как 100 seeds × 10000 действий с промежуточными checkpoints/reopens и полной сверкой.
