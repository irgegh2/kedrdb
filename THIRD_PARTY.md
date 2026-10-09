# Сторонние компоненты

Версии закреплены в Cargo.lock. Собственный код KedrDB — Apache-2.0; сторонние компоненты сохраняют свои лицензии.

| Компонент | Версия | Лицензия | Использование |
|---|---|---|---|
| sqlparser | 0.63.0 | Apache-2.0 | PostgreSQL tokenizer; grammar и executor Kedr собственные |
| log | 0.4.34 | MIT OR Apache-2.0 | Транзитивная зависимость sqlparser |

[sqlparser](https://github.com/apache/datafusion-sqlparser-rs), [log](https://github.com/rust-lang/log). Перед поставкой бинарных файлов на этапе 10 включаются необходимые license/notice материалы закрепленных версий.

PostgreSQL и Python/psycopg — внешние инструменты тестирования. Они не встроены в ядро KedrDB. Проверенная локальная версия psycopg 3.3.2 используется только harness; LGPL-3.0. В репозитории нет ее исходников или бинарных файлов. [Psycopg](https://www.psycopg.org/), [PostgreSQL](https://www.postgresql.org/).
