# Сторонние компоненты

Версии закреплены в Cargo.lock. Собственный код KedrDB — Apache-2.0; сторонние компоненты сохраняют свои лицензии.

| Компонент | Версия | Лицензия | Использование |
|---|---|---|---|
| sqlparser | 0.63.0 | Apache-2.0 | PostgreSQL tokenizer; grammar и executor Kedr собственные |
| log | 0.4.34 | MIT OR Apache-2.0 | Транзитивная зависимость sqlparser |
| eframe / egui | 0.35.0 | MIT OR Apache-2.0 | Нативный интерфейс Kedr Studio; glow, системные окна и встроенные шрифты |

[sqlparser](https://github.com/apache/datafusion-sqlparser-rs), [log](https://github.com/rust-lang/log). Перед поставкой бинарных файлов на этапе 10 включаются необходимые license/notice материалы закрепленных версий.

[eframe/egui](https://github.com/emilk/egui) добавляет транзитивные зависимости окон, графики, clipboard, доступности и шрифтов. Их конкретные версии закреплены в Cargo.lock; приведенная таблица прямых компонентов не является полным перечнем лицензий. Перед распространением бинарных сборок обязательны полный перечень компонентов фактической сборки и их license/notice материалы. WGPU не включен в выбранный renderer; возможные optional/целевые зависимости в lock не означают, что все они встроены в macOS binary. Функция screenshot предназначена для проверки UI, не стандартного запуска.

PostgreSQL и Python/psycopg — внешние инструменты тестирования. Они не встроены в ядро KedrDB. Проверенная локальная версия psycopg 3.3.2 используется только harness; LGPL-3.0. В репозитории нет ее исходников или бинарных файлов. [Psycopg](https://www.psycopg.org/), [PostgreSQL](https://www.postgresql.org/).
