use crossterm::event::KeyCode;
use ratatui::{prelude::*, widgets::*};

use crate::{action::Action, components::Component, config::Config, kafka};

#[derive(Default)]
pub struct RecordDetail {
    config: Config,
    record: Option<kafka::Record>,
    lines: Vec<String>,
    lines_for: Option<(i32, i64)>,
    scroll: usize,
    visible: usize,
    focused: bool,
}

impl RecordDetail {
    pub fn new() -> Self {
        Self::default()
    }

    fn max_scroll(&self) -> usize {
        self.lines.len().saturating_sub(self.visible.max(1))
    }
}

const MAX_LINES: usize = 20_000;

fn format_payload(payload: &str) -> Vec<String> {
    let pretty = serde_json::from_str::<serde_json::Value>(payload)
        .ok()
        .and_then(|value| serde_json::to_string_pretty(&value).ok());
    let text = pretty.as_deref().unwrap_or(payload);
    let mut lines: Vec<String> = text.lines().take(MAX_LINES).map(str::to_string).collect();
    if text.lines().nth(MAX_LINES).is_some() {
        lines.push(format!("… truncated at {MAX_LINES} lines"));
    }
    lines
}

impl Component for RecordDetail {
    fn register_config_handler(&mut self, config: Config) -> color_eyre::Result<()> {
        self.config = config;
        Ok(())
    }

    fn set_focused(&mut self, focused: bool) {
        self.focused = focused;
    }

    fn handle_key_event(
        &mut self,
        key: crossterm::event::KeyEvent,
    ) -> color_eyre::Result<Option<Action>> {
        let jump = self.visible.max(1);
        let max = self.max_scroll();
        match key.code {
            KeyCode::Down | KeyCode::Char('j') => self.scroll = (self.scroll + 1).min(max),
            KeyCode::Up | KeyCode::Char('k') => self.scroll = self.scroll.saturating_sub(1),
            KeyCode::PageDown => self.scroll = (self.scroll + jump).min(max),
            KeyCode::PageUp => self.scroll = self.scroll.saturating_sub(jump),
            KeyCode::Char('d') => self.scroll = (self.scroll + jump / 2).min(max),
            KeyCode::Char('u') => self.scroll = self.scroll.saturating_sub(jump / 2),
            KeyCode::Home | KeyCode::Char('g') => self.scroll = 0,
            KeyCode::End | KeyCode::Char('G') => self.scroll = max,
            _ => {}
        }
        Ok(None)
    }

    fn update(&mut self, action: Action) -> color_eyre::Result<Option<Action>> {
        if let Action::RecordSelected(record) = action {
            self.record = record;
        }
        Ok(None)
    }

    fn draw(&mut self, frame: &mut Frame, area: Rect) -> color_eyre::Result<()> {
        let muted = Style::default().fg(Color::DarkGray);
        let [title, meta, body, footer] = Layout::vertical([
            Constraint::Length(1),
            Constraint::Length(3),
            Constraint::Fill(1),
            Constraint::Length(1),
        ])
        .areas(area);

        let heading = if self.focused {
            Style::default().fg(Color::Rgb(0xe8, 0xe4, 0xe2))
        } else {
            muted
        };
        frame.render_widget(Paragraph::new("MESSAGE DETAILS").style(heading), title);

        let Some(id) = self.record.as_ref().map(|r| (r.partition, r.offset)) else {
            self.lines.clear();
            self.lines_for = None;
            frame.render_widget(
                Paragraph::new("Select a message to view its details").style(muted),
                body,
            );
            return Ok(());
        };

        if self.lines_for != Some(id) {
            if let Some(record) = self.record.take() {
                self.lines = format_payload(&record.payload);
                self.record = Some(record);
            }
            self.lines_for = Some(id);
            self.scroll = 0;
        }

        if let Some(record) = &self.record {
            let key = record.key.as_deref().unwrap_or("<none>");
            frame.render_widget(
                Paragraph::new(vec![
                    Line::from(vec![
                        Span::styled("Partition: ", muted),
                        Span::raw(id.0.to_string()),
                        Span::styled("  Offset: ", muted),
                        Span::raw(id.1.to_string()),
                    ]),
                    Line::from(vec![Span::styled("Key: ", muted), Span::raw(key)]),
                ]),
                meta,
            );
        }

        self.visible = (body.height as usize).max(1);
        self.scroll = self.scroll.min(self.max_scroll());

        let window: Vec<Line> = self
            .lines
            .iter()
            .skip(self.scroll)
            .take(self.visible)
            .map(|line| Line::raw(line.as_str()))
            .collect();
        frame.render_widget(Paragraph::new(window), body);

        let total = self.lines.len();
        let from = if total == 0 { 0 } else { self.scroll + 1 };
        let to = (self.scroll + self.visible).min(total);
        frame.render_widget(
            Paragraph::new(format!("{from}-{to}/{total} lines")).style(muted),
            footer,
        );

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn json_is_pretty_printed_and_plain_text_is_not() {
        assert_eq!(
            format_payload(r#"{"a":1,"b":[2,3]}"#),
            vec!["{", "  \"a\": 1,", "  \"b\": [", "    2,", "    3", "  ]", "}"]
        );
        assert_eq!(format_payload("not json\nsecond"), vec!["not json", "second"]);
    }

    #[test]
    fn oversized_payload_is_capped() {
        let payload = "x\n".repeat(MAX_LINES + 50);
        let lines = format_payload(&payload);
        assert_eq!(lines.len(), MAX_LINES + 1);
        assert!(lines.last().unwrap().starts_with("… truncated"));
    }

    #[test]
    fn scroll_clamps_to_last_screen() {
        let mut d = RecordDetail::new();
        d.lines = (0..100).map(|i| i.to_string()).collect();
        d.visible = 10;
        assert_eq!(d.max_scroll(), 90);

        let end = crossterm::event::KeyEvent::from(KeyCode::End);
        d.handle_key_event(end).unwrap();
        assert_eq!(d.scroll, 90);

        let down = crossterm::event::KeyEvent::from(KeyCode::Down);
        d.handle_key_event(down).unwrap();
        assert_eq!(d.scroll, 90);

        let up = crossterm::event::KeyEvent::from(KeyCode::PageUp);
        d.handle_key_event(up).unwrap();
        assert_eq!(d.scroll, 80);
    }
}
