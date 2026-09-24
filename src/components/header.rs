use ratatui::{prelude::*, widgets::Paragraph};

use crate::components::Component;

pub struct Header;

impl Component for Header {
    fn draw(
        &mut self,
        frame: &mut ratatui::prelude::Frame,
        area: ratatui::prelude::Rect,
    ) -> color_eyre::Result<()> {
        let dim = Style::new().fg(Color::DarkGray);

        let info_lines = vec![
            Line::from(vec![
                Span::styled("cluster  ", dim),
                Span::raw("prod-eu-west-1-hardcoded"),
            ]),
            Line::from(vec![
                Span::styled("brokers  ", dim),
                Span::raw("6 — controller b-1-hardcoded"),
            ]),
            Line::from(vec![
                Span::styled("auth     ", dim),
                Span::raw("SCRAM-SHA-512-hardcoded"),
            ]),
            Line::from(vec![Span::styled("resource ", dim), Span::raw("topics")]),
        ];
        let brand_lines = vec![Line::from("LAZYKAFKA"), Line::styled("v0.1.0", dim)];

        let info_width = info_lines.iter().map(Line::width).max().unwrap_or(0) as u16;
        let brand_width = brand_lines.iter().map(Line::width).max().unwrap_or(0) as u16;

        let [info, keymap, brand] = Layout::horizontal([
            Constraint::Length(info_width),
            Constraint::Fill(1),
            Constraint::Length(brand_width),
        ])
        .areas(area);

        frame.render_widget(Paragraph::new(info_lines), info);

        let accent = Style::new().fg(Color::Rgb(0xec, 0x30, 0x13));
        let key = |k: &str, label: &str| {
            Line::from(vec![
                Span::styled(format!("<{k}> "), accent),
                Span::raw(label.to_string()),
            ])
        };

        let groups = [
            vec![
                key("h/l", "column"),
                key("j/k", "select"),
                key("PgUp/PgDn", "jump"),
                key("Home/End", "page ends"),
            ],
            vec![
                key("n", "next 500"),
                key("p", "prev 500"),
                key("g", "first page"),
                key("G", "last page"),
            ],
            vec![
                key("o", "go to offset"),
                key("t", "tail (live)"),
                key("space", "pause tail"),
                key("q", "quit"),
            ],
        ];

        let cols: [Rect; 3] = Layout::horizontal([Constraint::Ratio(1, 3); 3]).areas(keymap);
        for (col_area, lines) in cols.iter().zip(groups) {
            frame.render_widget(Paragraph::new(lines), *col_area);
        }
        frame.render_widget(Paragraph::new(brand_lines).right_aligned(), brand);

        Ok(())
    }
}
