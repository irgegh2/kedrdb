# Сторонние компоненты

Версии закреплены в Cargo.lock. Собственный код KedrDB — Apache-2.0; сторонние компоненты сохраняют свои лицензии.

| Компонент | Версия | Лицензия | Использование |
|---|---|---|---|
| sqlparser | 0.63.0 | Apache-2.0 | PostgreSQL tokenizer; grammar и executor Kedr собственные |
| log | 0.4.34 | MIT OR Apache-2.0 | Транзитивная зависимость sqlparser |
| eframe / egui | 0.35.0 | MIT OR Apache-2.0 | Нативный интерфейс Kedr Studio; glow, системные окна и встроенные шрифты |

| Tokio | 1.53.2 | MIT | Сетевой runtime и deadlines |
| tokio-postgres | 0.7.18 | MIT OR Apache-2.0 | Настоящий драйвер Studio и интеграционные тесты |
| tokio-postgres-rustls | 0.14.0 | MIT | TLS адаптер драйвера |
| tokio-rustls | 0.26.6 | MIT OR Apache-2.0 | TLS сервер |
| rustls / rustls-pemfile | 0.23.45 / 2.2.0 | Apache-2.0 OR ISC OR MIT | TLS и чтение PEM |
| rsasl | 2.3.1 | Apache-2.0 OR MIT | SCRAM-SHA-256, verifier и proof validation |
| sha2 | 0.10.9 | MIT OR Apache-2.0 | SHA-256 SCRAM и maintenance manifest/job |
| ring | 0.17.14 | Apache-2.0 AND ISC | TLS crypto provider и OS RNG; набор notice сохраняется |
| serde / serde_json | 1.0.229 / 1.0.151 | MIT OR Apache-2.0 | Строгие форматы verifier, backup/import manifests и diagnostics |
| stringprep | 0.1.5 | MIT OR Apache-2.0 | SASLprep пароля |
| zeroize | 1.9.1 | Apache-2.0 OR MIT | Очистка принадлежащих приложению секретных буферов |
| rpassword | 7.5.4 | Apache-2.0 | Скрытый ввод пароля в init-auth |
| futures-util | 0.3.34 | MIT OR Apache-2.0 | Дренирование потока строк драйвера |
| image | 0.25.10 | MIT OR Apache-2.0 | PNG настоящего окна, только screenshot feature |

| rfd | 0.17.2 | MIT | Системные async file dialogs Studio |
| pollster | 0.4.0 | MIT OR Apache-2.0 | Транзитивная зависимость rfd |

[sqlparser](https://github.com/apache/datafusion-sqlparser-rs), [log](https://github.com/rust-lang/log). Перед поставкой бинарных файлов на этапе 10 включаются необходимые license/notice материалы закрепленных версий.

[eframe/egui](https://github.com/emilk/egui) добавляет транзитивные зависимости окон, графики, clipboard, доступности и шрифтов. Их конкретные версии закреплены в Cargo.lock; приведенная таблица прямых компонентов не является полным перечнем лицензий. Перед распространением бинарных сборок обязательны полный перечень компонентов фактической сборки и их license/notice материалы. WGPU не включен в выбранный renderer; возможные optional/целевые зависимости в lock не означают, что все они встроены в macOS binary. Функция screenshot предназначена для проверки UI, не стандартного запуска.

PostgreSQL и Python/psycopg — внешние инструменты тестирования и отдельного PostgreSQL migration adapter. Они не встроены в ядро KedrDB. Проверенная локальная версия psycopg 3.3.2 используется harness и отдельным adapter; LGPL-3.0. В репозитории нет ее исходников или бинарных файлов. [Psycopg](https://www.psycopg.org/), [PostgreSQL](https://www.postgresql.org/).

Версии/лицензии прямых новых компонентов сверены с Cargo metadata закрепленной сборки. Транзитивная sha2 0.11.0 используется также драйвером; crypto и auth не заменяются самодельными алгоритмами. Полный состав фактического release binary и license/notice файлы проверяются перед поставкой этапа 10.
