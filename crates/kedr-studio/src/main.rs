// SPDX-License-Identifier: Apache-2.0
mod app;
mod backend;

fn main() -> eframe::Result {
    let preview_results = std::env::args().any(|arg| arg == "--preview-results");
    eframe::run_native(
        "Kedr Studio",
        eframe::NativeOptions {
            viewport: eframe::egui::ViewportBuilder::default()
                .with_inner_size([1180.0, 780.0])
                .with_min_inner_size([820.0, 600.0]),
            ..Default::default()
        },
        Box::new(move |cc| Ok(Box::new(app::Studio::new(cc, preview_results)))),
    )
}
