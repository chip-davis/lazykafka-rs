use std::time::Duration;

use ratatui::{
    prelude::*,
    widgets::{Block, Paragraph},
};

use crate::{theme, tui::Tui};

pub const DURATION: Duration = Duration::from_millis(2500);

const LOGO: [&str; 6] = [
    "██╗      █████╗ ███████╗██╗   ██╗██╗  ██╗ █████╗ ███████╗██╗  ██╗ █████╗ ",
    "██║     ██╔══██╗╚══███╔╝╚██╗ ██╔╝██║ ██╔╝██╔══██╗██╔════╝██║ ██╔╝██╔══██╗",
    "██║     ███████║  ███╔╝  ╚████╔╝ █████╔╝ ███████║█████╗  █████╔╝ ███████║",
    "██║     ██╔══██║ ███╔╝    ╚██╔╝  ██╔═██╗ ██╔══██║██╔══╝  ██╔═██╗ ██╔══██║",
    "███████╗██║  ██║███████╗   ██║   ██║  ██╗██║  ██║██║     ██║  ██╗██║  ██║",
    "╚══════╝╚═╝  ╚═╝╚══════╝   ╚═╝   ╚═╝  ╚═╝╚═╝  ╚═╝╚═╝     ╚═╝  ╚═╝╚═╝  ╚═╝",
];

pub fn draw(tui: &mut Tui) -> color_eyre::Result<()> {
    tui.draw(|frame| {
        let area = frame.area();
        frame.render_widget(Block::new().style(Style::new().bg(theme::BG)), area);

        let mut lines: Vec<Line> = LOGO
            .iter()
            .map(|row| Line::styled(*row, theme::accent().bg(theme::BG)))
            .collect();
        lines.push(Line::default());
        lines.push(Line::styled(
            format!("a tui for kafka · v{}", env!("CARGO_PKG_VERSION")),
            theme::dim(),
        ));

        let [_, mid, _] = Layout::vertical([
            Constraint::Fill(1),
            Constraint::Length(lines.len() as u16),
            Constraint::Fill(1),
        ])
        .areas(area);
        frame.render_widget(Paragraph::new(lines).alignment(Alignment::Center), mid);
    })?;
    Ok(())
}
