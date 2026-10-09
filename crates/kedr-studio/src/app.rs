// SPDX-License-Identifier: Apache-2.0
use crate::backend::{self, Cell, Command, Payload, Profile, ResultSet, Table, Worker};
use crate::network::{Network, identifier};
use eframe::egui::{self, Color32, FontFamily, FontId, RichText, Stroke, TextStyle};
use std::time::Duration;

const INK: Color32 = Color32::from_rgb(36, 39, 43);
const NAV: Color32 = Color32::from_rgb(242, 243, 245);
const BORDER: Color32 = Color32::from_rgb(216, 219, 223);
const ACCENT: Color32 = Color32::from_rgb(40, 92, 147);
const MUTED: Color32 = Color32::from_rgb(103, 109, 118);
const ERROR: Color32 = Color32::from_rgb(164, 41, 41);

pub struct Studio {
    worker: Result<Worker, String>,
    pending: Option<u64>,
    preview: bool,
    connected: bool,
    status: String,
    host: String,
    port: String,
    database: String,
    user: String,
    password: String,
    tls: bool,
    ca_file: String,
    selection: Option<std::ops::Range<usize>>,
    last_sql: String,
    tables: Vec<Table>,
    selected_table: Option<String>,
    structure: bool,
    sql: String,
    result: Option<ResultSet>,
    error: Option<String>,
    message: String,
    elapsed: Option<Duration>,
    connection_dialog: bool,
    #[cfg(feature = "screenshot")]
    snapshot_requested: bool,
    #[cfg(feature = "screenshot")]
    test_query: bool,
    #[cfg(feature = "screenshot")]
    test_mode: bool,
}

impl Studio {
    pub fn new(cc: &eframe::CreationContext<'_>, preview: bool) -> Self {
        configure_style(&cc.egui_ctx);
        Self {
            worker: if preview {
                Worker::new(backend::Preview)
            } else {
                Network::new().and_then(Worker::new)
            },
            pending: None,
            preview,
            connected: false,
            status: "Нет подключения".into(),
            host: "127.0.0.1".into(),
            port: "55432".into(),
            database: "kedrdb".into(),
            user: "admin".into(),
            password: String::new(),
            tls: false,
            ca_file: String::new(),
            selection: None,
            last_sql: String::new(),
            tables: if preview {
                backend::preview_catalog()
            } else {
                Vec::new()
            },
            selected_table: preview.then(|| "products".into()),
            structure: false,
            sql: if preview {
                backend::PRODUCT_SQL.into()
            } else {
                String::new()
            },
            result: preview.then(backend::product_rows),
            error: None,
            message: String::new(),
            elapsed: None,
            connection_dialog: false,
            #[cfg(feature = "screenshot")]
            snapshot_requested: false,
            #[cfg(feature = "screenshot")]
            test_query: false,
            #[cfg(feature = "screenshot")]
            test_mode: false,
        }
    }
    #[cfg(feature = "screenshot")]
    pub fn connect_test(&mut self, profile: Profile) {
        self.host = profile.host.clone();
        self.port = profile.port.to_string();
        self.database = profile.database.clone();
        self.user = profile.user.clone();
        self.test_query = true;
        self.test_mode = true;
        self.sql = backend::PRODUCT_SQL.into();
        self.submit(Command::Connect(profile));
    }
    fn run_editor(&mut self) {
        let sql = if let Some(range) = &self.selection {
            if range.is_empty() {
                self.sql.clone()
            } else {
                self.sql
                    .chars()
                    .skip(range.start)
                    .take(range.end - range.start)
                    .collect()
            }
        } else {
            self.sql.clone()
        };
        if !sql.trim().is_empty() {
            self.submit(Command::Query(sql));
        }
    }
    fn submit(&mut self, command: Command) {
        self.error = None;
        self.elapsed = None;
        if let Command::Query(sql) = &command {
            self.last_sql = sql.clone();
        }
        if matches!(command, Command::Connect(_)) {
            self.connected = false;
            self.tables.clear();
            self.result = None;
        }
        match &mut self.worker {
            Ok(worker) => match worker.submit(command) {
                Ok(id) => {
                    self.pending = Some(id);
                    self.message = "Выполняется…".into();
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
            Ok(Some(reply)) if self.pending == Some(reply.id) => {
                self.pending = None;
                self.elapsed = Some(reply.elapsed);
                self.status = reply.status;
                if self.status == "Нет подключения" {
                    self.connected = false;
                }
                match reply.result {
                    Ok(Payload::Connected(tables)) => {
                        self.connected = true;
                        self.tables = tables;
                        self.message.clear();
                        self.connection_dialog = false;
                        #[cfg(feature = "screenshot")]
                        if self.test_query {
                            self.test_query = false;
                            self.run_editor();
                        }
                    }
                    Ok(Payload::Catalog(tables)) => {
                        self.tables = tables;
                        self.message.clear();
                    }
                    Ok(Payload::Rows(result)) => {
                        self.message = if result.columns.is_empty() {
                            format!("Затронуто строк: {}", result.affected)
                        } else {
                            format!(
                                "{} строк{}",
                                result.rows.len(),
                                if result.truncated {
                                    " · результат усечен: лимит 1000 строк / 2 МиБ"
                                } else {
                                    ""
                                }
                            )
                        };
                        self.result = Some(result);
                        if self.connected
                            && self.last_sql.split_whitespace().next().is_some_and(|word| {
                                ["CREATE", "ALTER", "DROP"]
                                    .iter()
                                    .any(|kind| word.eq_ignore_ascii_case(kind))
                            })
                        {
                            self.submit(Command::Catalog);
                        }
                    }
                    Err(error) => {
                        self.result = None;
                        self.error = Some(error);
                        self.message.clear();
                    }
                }
            }
            Ok(_) => {}
            Err(error) => {
                self.pending = None;
                self.error = Some(error);
            }
        }
        if self.pending.is_some() {
            ctx.request_repaint_after(Duration::from_millis(16));
        }
        if self.connected {
            if self.worker.as_ref().ok().and_then(Worker::connection_alive) == Some(false) {
                self.connected = false;
                self.tables.clear();
                self.status = "Связь потеряна".into();
                self.error = Some("Соединение закрыто. Подключитесь снова; исход последней записи при разрыве может быть неизвестен. Автоматического повтора нет.".into());
            } else {
                ctx.request_repaint_after(Duration::from_secs(1));
            }
        }
    }
    fn header(&mut self, ui: &mut egui::Ui) {
        ui.horizontal(|ui| {
            if action(ui, "Подключиться", true).clicked() {
                self.connection_dialog = true;
            }
            ui.separator();
            ui.label(RichText::new("KedrDB").color(MUTED));
            if self.connected {
                ui.label(&self.database);
            }
            ui.with_layout(egui::Layout::right_to_left(egui::Align::Center), |ui| {
                ui.label(RichText::new("Kedr Studio").size(12.0).color(MUTED));
            });
        });
    }
    fn objects(&mut self, ui: &mut egui::Ui) {
        ui.horizontal(|ui| {
            ui.label(RichText::new("Таблицы").strong());
            ui.with_layout(egui::Layout::right_to_left(egui::Align::Center), |ui| {
                if action(
                    ui,
                    "Обновить",
                    (self.preview || self.connected) && self.pending.is_none(),
                )
                .clicked()
                {
                    self.submit(Command::Catalog);
                }
            });
        });
        ui.add_space(10.0);
        if self.tables.is_empty() {
            ui.label(
                RichText::new(if self.connected {
                    "Таблиц пока нет"
                } else {
                    "Нет подключения"
                })
                .color(MUTED),
            );
            return;
        }
        egui::ScrollArea::vertical()
            .id_salt("tables")
            .show(ui, |ui| {
                for table in &self.tables {
                    let selected = self.selected_table.as_deref() == Some(&table.name);
                    let (rect, response) = ui.allocate_exact_size(
                        egui::vec2(ui.available_width(), 28.0),
                        egui::Sense::click(),
                    );
                    response.widget_info(|| {
                        egui::WidgetInfo::selected(
                            egui::WidgetType::SelectableLabel,
                            true,
                            selected,
                            &table.name,
                        )
                    });
                    if selected || response.hovered() {
                        ui.painter().rect_filled(
                            rect,
                            0,
                            if selected {
                                Color32::from_rgb(222, 231, 241)
                            } else {
                                Color32::from_rgb(232, 234, 237)
                            },
                        );
                    }
                    ui.painter().text(
                        rect.left_center() + egui::vec2(9.0, 0.0),
                        egui::Align2::LEFT_CENTER,
                        &table.name,
                        FontId::proportional(13.0),
                        INK,
                    );
                    if response.clicked() {
                        self.selected_table = Some(table.name.clone());
                        self.structure = true;
                    }
                    if response.double_clicked() {
                        self.sql = if self.preview {
                            if table.name == "products" {
                                backend::PRODUCT_SQL.into()
                            } else {
                                backend::USER_SQL.into()
                            }
                        } else {
                            format!("SELECT * FROM {}\nLIMIT 100;", identifier(&table.name))
                        };
                        self.selection = None;
                        self.structure = false;
                    }
                    response.on_hover_text("Структура таблицы. Двойной щелчок — запрос.");
                }
            });
    }
    fn editor(&mut self, ui: &mut egui::Ui) {
        ui.horizontal(|ui| {
            if action(ui, "SQL", true).clicked() {
                self.structure = false;
            }
            if action(ui, "Структура", self.selected_table.is_some()).clicked() {
                self.structure = true;
            }
            ui.separator();
            if action(
                ui,
                "Выполнить",
                (self.preview || self.connected) && self.pending.is_none() && !self.structure,
            )
            .clicked()
            {
                self.run_editor();
            }
            if action(ui, "Остановить", self.pending.is_some()).clicked()
                && let Ok(worker) = &self.worker
            {
                worker.cancel();
            }
        });
        ui.separator();
        if self.structure {
            if let Some(table) = self
                .tables
                .iter()
                .find(|t| Some(t.name.as_str()) == self.selected_table.as_deref())
            {
                ui.add_space(8.0);
                ui.label(RichText::new(&table.name).strong());
                ui.add_space(8.0);
                egui::Grid::new("columns")
                    .spacing([24.0, 8.0])
                    .striped(true)
                    .show(ui, |ui| {
                        ui.strong("Столбец");
                        ui.strong("Тип");
                        ui.strong("Ограничение");
                        ui.end_row();
                        for column in &table.columns {
                            ui.label(&column.name);
                            ui.monospace(&column.kind);
                            ui.label(&column.detail);
                            ui.end_row();
                        }
                    });
                if !table.indices.is_empty() {
                    ui.add_space(18.0);
                    ui.strong("Индексы");
                    for index in &table.indices {
                        ui.label(index);
                    }
                }
            }
            return;
        }
        egui::ScrollArea::both().id_salt("editor").show(ui, |ui| {
            ui.horizontal_top(|ui| {
                let line_height = ui.text_style_height(&TextStyle::Monospace);
                ui.vertical(|ui| {
                    ui.set_min_width(24.0);
                    for number in 1..=self.sql.lines().count().max(1) {
                        ui.add_sized(
                            [24.0, line_height],
                            egui::Label::new(
                                RichText::new(number.to_string()).monospace().color(MUTED),
                            ),
                        );
                    }
                });
                ui.set_min_height(180.0);
                let output = egui::TextEdit::multiline(&mut self.sql)
                    .code_editor()
                    .font(FontId::monospace(14.0))
                    .char_limit(backend::MAX_SQL_BYTES / 4)
                    .frame(egui::Frame::NONE)
                    .desired_width(f32::INFINITY)
                    .desired_rows(12)
                    .hint_text("Напишите SQL-запрос")
                    .show(ui);
                if let Some(range) = output.cursor_range {
                    let range = range.as_sorted_char_range();
                    self.selection = Some(range.start.0..range.end.0);
                }
            });
        });
        if ui.input_mut(|input| input.consume_key(egui::Modifiers::COMMAND, egui::Key::Enter))
            && self.pending.is_none()
            && (self.connected || self.preview)
        {
            self.run_editor();
        }
    }
    fn results(&mut self, ui: &mut egui::Ui) {
        ui.set_min_height(ui.available_height());
        ui.horizontal(|ui| {
            ui.strong("Результат");
            if let Some(result) = &self.result {
                ui.label(RichText::new(format!("{} строк", result.rows.len())).color(MUTED));
            }
            if let Some(elapsed) = self.elapsed {
                ui.label(RichText::new(format!("{} мс", elapsed.as_millis())).color(MUTED));
            }
        });
        ui.separator();
        if let Some(error) = &self.error {
            ui.colored_label(ERROR, error);
            return;
        }
        let Some(result) = &self.result else {
            return;
        };
        egui::ScrollArea::both()
            .id_salt("result-scroll")
            .show(ui, |ui| {
                egui::Grid::new("result-grid")
                    .striped(true)
                    .spacing([22.0, 7.0])
                    .show(ui, |ui| {
                        ui.label(RichText::new("#").color(MUTED));
                        for (name, kind) in &result.columns {
                            ui.vertical(|ui| {
                                ui.set_min_width(width(name));
                                ui.strong(name);
                                ui.label(RichText::new(kind).size(10.0).color(MUTED));
                            });
                        }
                        ui.end_row();
                        for (index, row) in result.rows.iter().take(1000).enumerate() {
                            ui.label(RichText::new((index + 1).to_string()).color(MUTED));
                            for ((name, _), cell) in result.columns.iter().zip(row) {
                                ui.vertical(|ui| {
                                    ui.set_min_width(width(name));
                                    let text = cell.display();
                                    let rich = if matches!(cell, Cell::Null) {
                                        RichText::new(&text).color(MUTED).italics()
                                    } else {
                                        RichText::new(&text)
                                    };
                                    let response =
                                        ui.add(egui::Label::new(rich).sense(egui::Sense::click()));
                                    if response.clicked() {
                                        ui.ctx().copy_text(text.clone());
                                    }
                                    response.on_hover_text("Скопировать значение");
                                });
                            }
                            ui.end_row();
                        }
                    });
            });
    }
}
impl eframe::App for Studio {
    fn ui(&mut self, ui: &mut egui::Ui, _frame: &mut eframe::Frame) {
        let ctx = ui.ctx().clone();
        self.poll(&ctx);
        egui::Panel::top("connection-bar")
            .exact_size(38.0)
            .frame(frame(NAV, 6.0))
            .show(ui, |ui| self.header(ui));
        egui::Panel::bottom("status")
            .exact_size(25.0)
            .frame(frame(NAV, 4.0))
            .show(ui, |ui| {
                ui.horizontal(|ui| {
                    ui.label(
                        RichText::new(if self.preview {
                            "Тест интерфейса · сервер не подключён"
                        } else {
                            self.status.as_str()
                        })
                        .size(11.0)
                        .color(MUTED),
                    );
                    if self.pending.is_some() {
                        ui.spinner();
                    }
                    if !self.message.is_empty() {
                        ui.separator();
                        ui.label(RichText::new(&self.message).size(11.0));
                    }
                });
            });
        egui::Panel::left("tables")
            .default_size(205.0)
            .min_size(165.0)
            .max_size(350.0)
            .frame(frame(NAV, 10.0))
            .resizable(true)
            .show(ui, |ui| self.objects(ui));
        egui::Panel::bottom("results")
            .default_size(290.0)
            .min_size(120.0)
            .frame(frame(Color32::WHITE, 10.0))
            .resizable(true)
            .show(ui, |ui| self.results(ui));
        egui::CentralPanel::default()
            .frame(frame(Color32::WHITE, 10.0))
            .show(ui, |ui| self.editor(ui));
        let mut open = self.connection_dialog;
        let mut connect = false;
        egui::Window::new("Подключение")
            .open(&mut open)
            .collapsible(false)
            .resizable(false)
            .show(&ctx, |ui| {
                ui.set_min_width(370.0);
                egui::Grid::new("connection-fields")
                    .spacing([16.0, 9.0])
                    .show(ui, |ui| {
                        ui.label("Адрес");
                        ui.text_edit_singleline(&mut self.host);
                        ui.end_row();
                        ui.label("Порт");
                        ui.add(egui::TextEdit::singleline(&mut self.port).char_limit(5));
                        ui.end_row();
                        ui.label("База");
                        ui.text_edit_singleline(&mut self.database);
                        ui.end_row();
                        ui.label("Пользователь");
                        ui.text_edit_singleline(&mut self.user);
                        ui.end_row();
                        ui.label("Пароль");
                        ui.add(
                            egui::TextEdit::singleline(&mut self.password)
                                .password(true)
                                .char_limit(1024),
                        );
                        ui.end_row();
                        ui.label("Защита");
                        ui.checkbox(&mut self.tls, "TLS с проверкой сертификата");
                        ui.end_row();
                        if self.tls {
                            ui.label("Файл CA");
                            ui.text_edit_singleline(&mut self.ca_file);
                            ui.end_row();
                        }
                    });
                ui.add_space(12.0);
                connect = ui
                    .add_enabled(self.pending.is_none(), egui::Button::new("Подключиться"))
                    .clicked();
                if let Some(error) = &self.error {
                    ui.colored_label(ERROR, error);
                }
            });
        self.connection_dialog = open;
        if connect {
            match self.port.parse::<u16>().ok().filter(|port| *port > 0) {
                Some(port) => {
                    let password = zeroize::Zeroizing::new(std::mem::take(&mut self.password));
                    self.submit(Command::Connect(Profile {
                        host: self.host.trim().into(),
                        port,
                        database: self.database.trim().into(),
                        user: self.user.trim().into(),
                        password,
                        ca_file: self.tls.then(|| self.ca_file.clone()),
                    }));
                }
                None => self.error = Some("Порт должен быть числом от 1 до 65535.".into()),
            }
        }
        if !open {
            zeroize::Zeroize::zeroize(&mut self.password);
        }
        #[cfg(feature = "screenshot")]
        self.snapshot(&ctx);
    }
}
#[cfg(feature = "screenshot")]
impl Studio {
    fn snapshot(&mut self, ctx: &egui::Context) {
        let Some(path) = std::env::var_os("KEDR_STUDIO_SCREENSHOT") else {
            return;
        };
        let screenshot = ctx.input(|input| {
            input.events.iter().find_map(|event| match event {
                egui::Event::Screenshot { image, .. } => Some(image.clone()),
                _ => None,
            })
        });
        if let Some(image) = screenshot {
            if self.test_mode {
                println!(
                    "KEDR_STUDIO_TEST_RESULT connected={} rows={} tables={} error={}",
                    self.connected,
                    self.result.as_ref().map_or(0, |result| result.rows.len()),
                    self.tables.len(),
                    self.error.is_some()
                );
            }
            let bytes = image
                .pixels
                .iter()
                .flat_map(|pixel| pixel.to_array())
                .collect::<Vec<_>>();
            if let Err(error) = image::save_buffer(
                std::path::Path::new(&path),
                &bytes,
                image.size[0] as u32,
                image.size[1] as u32,
                image::ColorType::Rgba8,
            ) {
                eprintln!("screenshot failed: {error}");
            }
            ctx.send_viewport_cmd(egui::ViewportCommand::Close);
        } else {
            if !self.snapshot_requested
                && ctx.cumulative_frame_nr() >= 10
                && self.pending.is_none()
                && !self.test_query
            {
                ctx.send_viewport_cmd(egui::ViewportCommand::Screenshot(Default::default()));
                self.snapshot_requested = true;
            }
            ctx.request_repaint_after(Duration::from_millis(30));
        }
    }
}
fn width(name: &str) -> f32 {
    match name {
        "id" => 65.0,
        "name" | "email" => 220.0,
        _ => 95.0,
    }
}
fn action(ui: &mut egui::Ui, label: &str, enabled: bool) -> egui::Response {
    ui.add_enabled(
        enabled,
        egui::Button::new(label).frame(false).corner_radius(2),
    )
}
fn frame(fill: Color32, margin: f32) -> egui::Frame {
    egui::Frame::new().fill(fill).inner_margin(margin)
}
fn configure_style(ctx: &egui::Context) {
    let mut fonts = egui::FontDefinitions::default();
    for (family, name, paths) in [
        (
            FontFamily::Proportional,
            "system-sans",
            vec![
                "/System/Library/Fonts/SFNS.ttf",
                "/usr/share/fonts/truetype/dejavu/DejaVuSans.ttf",
            ],
        ),
        (
            FontFamily::Monospace,
            "system-mono",
            vec![
                "/System/Library/Fonts/Menlo.ttc",
                "/usr/share/fonts/truetype/dejavu/DejaVuSansMono.ttf",
            ],
        ),
    ] {
        for path in paths {
            if std::fs::metadata(path).is_ok_and(|meta| meta.len() < 16 * 1024 * 1024)
                && let Ok(bytes) = std::fs::read(path)
            {
                fonts
                    .font_data
                    .insert(name.into(), egui::FontData::from_owned(bytes).into());
                fonts
                    .families
                    .entry(family.clone())
                    .or_default()
                    .insert(0, name.into());
                break;
            }
        }
    }
    ctx.set_fonts(fonts);
    let mut style = (*ctx.global_style()).clone();
    style.visuals = egui::Visuals::light();
    style.visuals.override_text_color = Some(INK);
    style.visuals.panel_fill = NAV;
    style.visuals.extreme_bg_color = Color32::WHITE;
    style.visuals.selection.bg_fill = Color32::from_rgb(222, 231, 241);
    style.visuals.selection.stroke = Stroke::new(1.0, ACCENT);
    style.visuals.widgets.noninteractive.bg_stroke = Stroke::new(1.0, BORDER);
    style.visuals.widgets.noninteractive.corner_radius = 2.into();
    style.spacing.item_spacing = egui::vec2(6.0, 3.0);
    style.spacing.button_padding = egui::vec2(7.0, 3.0);
    style
        .text_styles
        .insert(TextStyle::Body, FontId::proportional(13.0));
    style
        .text_styles
        .insert(TextStyle::Button, FontId::proportional(13.0));
    style
        .text_styles
        .insert(TextStyle::Monospace, FontId::monospace(14.0));
    ctx.set_global_style(style);
}
