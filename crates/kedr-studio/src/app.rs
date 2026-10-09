// SPDX-License-Identifier: Apache-2.0
use crate::backend::{self, Cell, Command, Payload, ResultSet, Table, Worker};
use eframe::egui::{self, Color32, FontId, RichText, Stroke, TextStyle};
use std::time::Duration;

const INK: Color32 = Color32::from_rgb(23, 39, 46);
const BACKGROUND: Color32 = Color32::from_rgb(245, 247, 249);
const BORDER: Color32 = Color32::from_rgb(213, 221, 227);
const PINE: Color32 = Color32::from_rgb(36, 107, 84);
const MUTED: Color32 = Color32::from_rgb(84, 104, 114);
const ERROR: Color32 = Color32::from_rgb(158, 43, 43);

pub struct Studio {
    worker: Result<Worker, String>,
    pending: Option<u64>,
    preview: bool,
    tables: Vec<Table>,
    sql: String,
    result: Option<ResultSet>,
    error: Option<String>,
    message: String,
    elapsed: Option<Duration>,
    connection_dialog: bool,
    show_messages: bool,
}

impl Studio {
    pub fn new(cc: &eframe::CreationContext<'_>, preview_results: bool) -> Self {
        configure_style(&cc.egui_ctx);
        Self {
            worker: Worker::new(backend::Preview),
            pending: None,
            preview: preview_results,
            tables: if preview_results {
                backend::preview_catalog()
            } else {
                Vec::new()
            },
            sql: backend::PRODUCT_SQL.to_owned(),
            result: preview_results.then(backend::product_rows),
            error: None,
            message: if preview_results {
                "Показан пример. Данные не получены из базы."
            } else {
                "Нет подключения. Откройте предпросмотр, чтобы познакомиться с интерфейсом."
            }
            .to_owned(),
            elapsed: None,
            connection_dialog: false,
            show_messages: false,
        }
    }

    fn submit(&mut self, command: Command) {
        self.error = None;
        self.elapsed = None;
        match &mut self.worker {
            Ok(worker) => match worker.submit(command) {
                Ok(id) => {
                    self.pending = Some(id);
                    self.message = "Выполняется пример…".to_owned();
                }
                Err(error) => self.error = Some(error),
            },
            Err(error) => self.error = Some(error.clone()),
        }
    }

    fn poll(&mut self, ctx: &egui::Context) {
        let reply = match &mut self.worker {
            Ok(worker) => worker.poll(),
            Err(_) => return,
        };
        match reply {
            Ok(Some(reply)) => {
                // A reply from a disconnected/invalidated operation cannot repopulate the UI.
                if self.pending != Some(reply.id) {
                    return;
                }
                self.pending = None;
                self.elapsed = Some(reply.elapsed);
                match reply.result {
                    Ok(Payload::Catalog(tables)) => {
                        self.tables = tables;
                        self.message =
                            "Предпросмотр открыт. Все объекты и строки — примеры.".to_owned();
                    }
                    Ok(Payload::Rows(result)) => {
                        self.message = format!(
                            "Показан пример: {} строк. Данные не получены из базы.",
                            result.rows.len()
                        );
                        self.result = Some(result);
                        self.show_messages = false;
                    }
                    Err(error) => {
                        self.result = None;
                        self.error = Some(error);
                        self.message = "Операция завершена без результата.".to_owned();
                        self.show_messages = true;
                    }
                }
            }
            Ok(None) => {}
            Err(error) => {
                self.pending = None;
                self.error = Some(error);
            }
        }
        if self.pending.is_some() {
            ctx.request_repaint_after(Duration::from_millis(16));
        }
    }

    fn header(&mut self, ui: &mut egui::Ui) {
        ui.horizontal(|ui| {
            ui.label(RichText::new("Kedr").size(22.0).strong().color(PINE));
            ui.label(RichText::new("Studio").size(22.0).color(INK));
            ui.add_space(18.0);
            if self.preview {
                ui.label(RichText::new("Предпросмотр · данные-примеры").color(MUTED));
            } else {
                ui.label(RichText::new("Нет подключения").color(MUTED));
            }
            ui.with_layout(egui::Layout::right_to_left(egui::Align::Center), |ui| {
                if ui.button("Подключение…").clicked() {
                    self.connection_dialog = true;
                }
                if self.preview {
                    if ui
                        .add_enabled(
                            self.pending.is_none(),
                            egui::Button::new("Закрыть предпросмотр"),
                        )
                        .clicked()
                    {
                        self.preview = false;
                        self.tables.clear();
                        self.result = None;
                        self.error = None;
                        self.message = "Нет подключения.".to_owned();
                        self.pending = None;
                    }
                } else if ui.button("Открыть предпросмотр").clicked() {
                    self.preview = true;
                    self.submit(Command::Catalog);
                }
            });
        });
    }

    fn objects(&mut self, ui: &mut egui::Ui) {
        ui.horizontal(|ui| {
            ui.label(RichText::new("Объекты").strong());
            if ui
                .add_enabled(
                    self.preview && self.pending.is_none(),
                    egui::Button::new("Обновить"),
                )
                .clicked()
            {
                self.submit(Command::Catalog);
            }
        });
        ui.separator();
        if !self.preview {
            ui.add_space(12.0);
            ui.label("Здесь появятся таблицы подключенной базы.");
            return;
        }
        ui.label(RichText::new("Пример: магазин").color(MUTED));
        ui.add_space(10.0);
        egui::ScrollArea::vertical()
            .id_salt("objects-scroll")
            .show(ui, |ui| {
                for table in &self.tables {
                    egui::CollapsingHeader::new(RichText::new(&table.name).strong())
                        .id_salt(&table.name)
                        .default_open(table.name == "products")
                        .show(ui, |ui| {
                            for column in &table.columns {
                                ui.horizontal(|ui| {
                                    ui.label(&column.name);
                                    ui.label(RichText::new(&column.kind).size(11.0).color(MUTED));
                                })
                                .response
                                .on_hover_text(&column.detail);
                            }
                            egui::CollapsingHeader::new("Индексы")
                                .id_salt((&table.name, "indices"))
                                .show(ui, |ui| {
                                    for index in &table.indices {
                                        ui.label(index);
                                    }
                                });
                            if ui.button("Запрос-пример").clicked() {
                                self.sql = if table.name == "products" {
                                    backend::PRODUCT_SQL
                                } else {
                                    backend::USER_SQL
                                }
                                .to_owned();
                            }
                        });
                }
            });
    }

    fn editor(&mut self, ui: &mut egui::Ui) {
        let execute = self.preview && self.pending.is_none();
        ui.horizontal(|ui| {
            ui.label(RichText::new("SQL").strong());
            ui.label(RichText::new("Запрос 1").color(MUTED));
            ui.with_layout(egui::Layout::right_to_left(egui::Align::Center), |ui| {
                if ui
                    .add_enabled(self.pending.is_some(), egui::Button::new("Отменить"))
                    .clicked()
                    && let Ok(worker) = &self.worker
                {
                    worker.cancel();
                    self.message = "Запрошена отмена…".to_owned();
                }
                if ui
                    .add_enabled(
                        execute,
                        egui::Button::new(RichText::new("Показать пример").color(Color32::WHITE))
                            .fill(PINE),
                    )
                    .clicked()
                {
                    self.submit(Command::Query(self.sql.clone()));
                }
            });
        });
        ui.add_space(10.0);
        egui::ScrollArea::both()
            .id_salt("sql-scroll")
            .show(ui, |ui| {
                let height = ui.available_height().max(100.0);
                ui.add_sized(
                    [ui.available_width(), height],
                    egui::TextEdit::multiline(&mut self.sql)
                        .font(FontId::monospace(15.0))
                        .char_limit(backend::MAX_SQL_BYTES / 4)
                        .desired_width(f32::INFINITY)
                        .hint_text("Введите SQL после подключения к серверу"),
                );
            });
    }

    fn results(&mut self, ui: &mut egui::Ui) {
        // A resizable Panel's frame follows its content size in egui 0.35.
        // Reserve the whole region even for an empty or short response.
        ui.set_min_height(ui.available_height());
        ui.horizontal(|ui| {
            ui.selectable_value(&mut self.show_messages, false, "Результат");
            ui.selectable_value(&mut self.show_messages, true, "Сообщения");
            if let Some(result) = &self.result {
                ui.label(
                    RichText::new(format!("{} строк-примеров", result.rows.len())).color(MUTED),
                );
            }
        });
        ui.separator();
        if let Some(error) = &self.error {
            ui.colored_label(ERROR, error);
            return;
        }
        if self.show_messages {
            ui.label(&self.message);
            return;
        }
        let Some(result) = &self.result else {
            ui.add_space(16.0);
            ui.label(RichText::new("Результат появится здесь").strong());
            ui.label(
                "В предпросмотре нажмите «Показать пример». Для работы с данными нужен сервер.",
            );
            return;
        };
        // Fixed row heights and horizontal scrolling keep large results responsive.
        egui::ScrollArea::horizontal()
            .id_salt("result-horizontal")
            .show(ui, |ui| {
                ui.set_min_width(680.0);
                ui.horizontal(|ui| {
                    ui.add_sized(
                        [36.0, 26.0],
                        egui::Label::new(RichText::new("#").color(MUTED)),
                    );
                    for (name, kind) in &result.columns {
                        ui.add_sized(
                            [cell_width(name), 26.0],
                            egui::Label::new(RichText::new(format!("{name}  {kind}")).strong())
                                .halign(egui::Align::Min),
                        );
                    }
                });
                ui.separator();
                egui::ScrollArea::vertical()
                    .id_salt("result-vertical")
                    .show_rows(ui, 28.0, result.rows.len(), |ui, range| {
                        for index in range {
                            ui.push_id(index, |ui| {
                                ui.horizontal(|ui| {
                                    ui.add_sized(
                                        [36.0, 28.0],
                                        egui::Label::new(
                                            RichText::new((index + 1).to_string()).color(MUTED),
                                        ),
                                    );
                                    for ((name, _), cell) in
                                        result.columns.iter().zip(&result.rows[index])
                                    {
                                        let text = cell.display();
                                        let rich = if matches!(cell, Cell::Null) {
                                            RichText::new(&text).italics().color(MUTED)
                                        } else {
                                            RichText::new(&text)
                                        };
                                        let response = ui.add_sized(
                                            [cell_width(name), 28.0],
                                            egui::Label::new(rich)
                                                .halign(egui::Align::Min)
                                                .truncate()
                                                .sense(egui::Sense::click()),
                                        );
                                        if response.clicked() {
                                            ui.ctx().copy_text(text.clone());
                                        }
                                        response.on_hover_text(format!(
                                            "{text}\nНажмите, чтобы скопировать"
                                        ));
                                    }
                                });
                            });
                        }
                    });
            });
    }
}

impl eframe::App for Studio {
    fn ui(&mut self, ui: &mut egui::Ui, _frame: &mut eframe::Frame) {
        let ctx = ui.ctx().clone();
        self.poll(&ctx);
        egui::Panel::top("header")
            .exact_size(60.0)
            .frame(panel_frame())
            .show(ui, |ui| self.header(ui));
        egui::Panel::bottom("status")
            .exact_size(36.0)
            .frame(panel_frame())
            .show(ui, |ui| {
                ui.horizontal(|ui| {
                    ui.label(
                        RichText::new(if self.preview {
                            "Предпросмотр"
                        } else {
                            "Не подключено"
                        })
                        .color(MUTED),
                    );
                    ui.separator();
                    if self.pending.is_some() {
                        ui.spinner();
                    }
                    ui.label(&self.message);
                    if let Some(elapsed) = self.elapsed {
                        ui.label(RichText::new(format!("{} мс", elapsed.as_millis())).color(MUTED));
                    }
                });
            });
        egui::Panel::left("objects")
            .default_size(245.0)
            .min_size(190.0)
            .max_size(380.0)
            .resizable(true)
            .frame(panel_frame())
            .show(ui, |ui| self.objects(ui));
        egui::Panel::bottom("results")
            .default_size(320.0)
            .min_size(160.0)
            .resizable(true)
            .frame(panel_frame())
            .show(ui, |ui| self.results(ui));
        egui::CentralPanel::default()
            .frame(panel_frame())
            .show(ui, |ui| self.editor(ui));
        egui::Window::new("Подключение к KedrDB").open(&mut self.connection_dialog)
            .collapsible(false).resizable(false).show(&ctx, |ui| {
                ui.set_max_width(420.0);
                ui.label("Сетевое подключение добавляется в этом этапе после интерфейса.");
                ui.add_space(8.0);
                ui.label("Сейчас можно открыть предпросмотр: в нем только примеры структуры и результатов. Запись данных недоступна.");
            });
        // Native screenshot tooling captures the third paint; ensure idle window repaints.
        #[cfg(feature = "screenshot")]
        if std::env::var_os("EFRAME_SCREENSHOT_TO").is_some() {
            ctx.request_repaint();
        }
    }
}

fn cell_width(name: &str) -> f32 {
    match name {
        "id" => 95.0,
        "name" | "email" => 245.0,
        _ => 130.0,
    }
}

fn panel_frame() -> egui::Frame {
    egui::Frame::new().fill(Color32::WHITE).inner_margin(12.0)
}

fn configure_style(ctx: &egui::Context) {
    let mut style = (*ctx.global_style()).clone();
    style.visuals = egui::Visuals::light();
    style.visuals.override_text_color = Some(INK);
    style.visuals.panel_fill = BACKGROUND;
    style.visuals.extreme_bg_color = Color32::WHITE;
    style.visuals.selection.bg_fill = Color32::from_rgb(220, 235, 246);
    style.visuals.selection.stroke = Stroke::new(1.0, Color32::from_rgb(61, 107, 189));
    style.visuals.widgets.noninteractive.bg_stroke = Stroke::new(1.0, BORDER);
    style.spacing.item_spacing = egui::vec2(8.0, 6.0);
    style.spacing.button_padding = egui::vec2(10.0, 6.0);
    style
        .text_styles
        .insert(TextStyle::Body, FontId::proportional(14.0));
    style
        .text_styles
        .insert(TextStyle::Button, FontId::proportional(14.0));
    style
        .text_styles
        .insert(TextStyle::Monospace, FontId::monospace(15.0));
    ctx.set_global_style(style);
}
