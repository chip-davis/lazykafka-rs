use crossterm::event::KeyCode;
use ratatui::{prelude::*, widgets::*};

use crate::{action::Action, components::Component, config::Config, highlight, kafka, theme};

#[derive(Default)]
pub struct RecordDetail {
    config: Config,
    record: Option<kafka::Record>,
    lines: Vec<Line<'static>>,
    lines_for: Option<(i32, i64, u16)>,
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

fn wrap_line(line: &str, width: usize) -> Vec<String> {
    if width == 0 || line.chars().count() <= width {
        return vec![line.to_string()];
    }
    let indent: String = line.chars().take_while(|c| c.is_whitespace()).collect();
    let indent_len = indent.chars().count();
    let avail = width.saturating_sub(indent_len).max(1);
    line.chars()
        .skip(indent_len)
        .collect::<Vec<_>>()
        .chunks(avail)
        .map(|chunk| format!("{indent}{}", chunk.iter().collect::<String>()))
        .collect()
}

fn wrap_spans(spans: &[(String, Style)], width: usize) -> Vec<Vec<(String, Style)>> {
    let len: usize = spans.iter().map(|(t, _)| t.chars().count()).sum();
    if width == 0 || len <= width {
        return vec![spans.to_vec()];
    }
    let chars: Vec<(char, Style)> = spans
        .iter()
        .flat_map(|(t, s)| t.chars().map(move |c| (c, *s)))
        .collect();
    let indent_len = chars.iter().take_while(|(c, _)| c.is_whitespace()).count();
    let indent_style = chars.first().map(|(_, s)| *s).unwrap_or_default();
    let avail = width.saturating_sub(indent_len).max(1);

    chars[indent_len..]
        .chunks(avail)
        .map(|chunk| {
            let mut line = vec![(" ".repeat(indent_len), indent_style)];
            for &(c, style) in chunk {
                match line.last_mut() {
                    Some((text, last_style)) if *last_style == style => text.push(c),
                    _ => line.push((c.to_string(), style)),
                }
            }
            line
        })
        .collect()
}

fn format_payload(payload: &str, theme: &str, width: usize) -> Vec<Line<'static>> {
    let pretty = serde_json::from_str::<serde_json::Value>(payload)
        .ok()
        .and_then(|value| serde_json::to_string_pretty(&value).ok());
    let is_json = pretty.is_some();
    let text = pretty.as_deref().unwrap_or(payload);
    let truncated = text.lines().nth(MAX_LINES).is_some();
    let capped_lines: Vec<&str> = text.lines().take(MAX_LINES).collect();

    let mut lines: Vec<Line<'static>> = if is_json {
        highlight::highlight_json(&capped_lines.join("\n"), theme)
            .into_iter()
            .flat_map(|spans| wrap_spans(&spans, width))
            .map(|spans| {
                Line::from(
                    spans
                        .into_iter()
                        .map(|(text, style)| Span::styled(text, style))
                        .collect::<Vec<_>>(),
                )
            })
            .collect()
    } else {
        capped_lines
            .into_iter()
            .flat_map(|l| wrap_line(l, width))
            .map(Line::raw)
            .collect()
    };
    if truncated {
        lines.push(Line::raw(format!("… truncated at {MAX_LINES} lines")));
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
        let muted = theme::dim();
        let value = Style::new().bg(theme::BG).fg(theme::FG);
        let [title, meta, body, footer] = Layout::vertical([
            Constraint::Length(1),
            Constraint::Length(3),
            Constraint::Fill(1),
            Constraint::Length(1),
        ])
        .areas(area);

        frame.render_widget(
            Paragraph::new("PREVIEW").style(theme::heading(self.focused)),
            title,
        );

        let Some(id) = self.record.as_ref().map(|r| (r.partition, r.offset)) else {
            self.lines.clear();
            self.lines_for = None;
            frame.render_widget(
                Paragraph::new("Select a message to view its details").style(muted),
                body,
            );
            return Ok(());
        };
        let cache_key = (id.0, id.1, body.width);

        if self.lines_for != Some(cache_key) {
            if let Some(record) = self.record.take() {
                self.lines =
                    format_payload(&record.payload, &self.config.theme, body.width as usize);
                self.record = Some(record);
            }
            self.lines_for = Some(cache_key);
            self.scroll = 0;
        }

        if let Some(record) = &self.record {
            let key = record.key.as_deref().unwrap_or("<none>");
            frame.render_widget(
                Paragraph::new(vec![
                    Line::from(vec![
                        Span::styled("Partition: ", muted),
                        Span::styled(id.0.to_string(), value),
                        Span::styled("  Offset: ", muted),
                        Span::styled(id.1.to_string(), value),
                    ]),
                    Line::from(vec![
                        Span::styled("Key: ", muted),
                        Span::styled(key, value),
                    ]),
                ])
                .style(muted),
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
            .cloned()
            .collect();
        frame.render_widget(Paragraph::new(window).style(muted), body);

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

    fn line_text(line: &Line) -> String {
        line.spans.iter().map(|s| s.content.as_ref()).collect()
    }

    #[test]
    fn json_is_pretty_printed_and_plain_text_is_not() {
        let lines = format_payload(r#"{"a":1,"b":[2,3]}"#, "base16-ocean.dark", 80);
        let text: Vec<String> = lines.iter().map(line_text).collect();
        assert_eq!(
            text,
            vec![
                "{",
                "  \"a\": 1,",
                "  \"b\": [",
                "    2,",
                "    3",
                "  ]",
                "}"
            ]
        );
        assert!(
            lines[1].spans.len() > 1,
            "JSON lines should carry multiple highlighted spans"
        );

        let plain = format_payload("not json\nsecond", "base16-ocean.dark", 80);
        let text: Vec<String> = plain.iter().map(line_text).collect();
        assert_eq!(text, vec!["not json", "second"]);
        assert_eq!(
            plain[0].spans.len(),
            1,
            "non-JSON stays a single plain span"
        );
    }

    #[test]
    fn long_json_value_wraps_at_width() {
        let bio = "b".repeat(500);
        let payload = format!(r#"{{"bio":"{bio}"}}"#);
        let lines = format_payload(&payload, "base16-ocean.dark", 40);
        for line in &lines {
            assert!(
                line_text(line).chars().count() <= 40,
                "line exceeded wrap width: {:?}",
                line_text(line)
            );
        }
        assert!(lines.len() > 3, "long value should span multiple wrapped lines");
    }

    #[test]
    fn wrap_line_is_noop_under_width_and_splits_over() {
        assert_eq!(wrap_line("short", 40), vec!["short"]);
        assert_eq!(wrap_line("abcdef", 2), vec!["ab", "cd", "ef"]);
        assert_eq!(wrap_line("abcde", 0), vec!["abcde"]);
    }

    #[test]
    fn oversized_payload_is_capped() {
        let payload = "x\n".repeat(MAX_LINES + 50);
        let lines = format_payload(&payload, "base16-ocean.dark", 80);
        assert_eq!(lines.len(), MAX_LINES + 1);
        assert!(line_text(&lines[MAX_LINES]).starts_with("… truncated"));
    }

    #[test]
    fn scroll_clamps_to_last_screen() {
        let mut d = RecordDetail::new();
        d.lines = (0..100).map(|i| Line::raw(i.to_string())).collect();
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

    #[test]
    fn wrapped_string_stays_highlighted_and_indented() {
        let bio = "a".repeat(90);
        let payload = format!(r#"{{"bio":"{bio}"}}"#);
        let lines = format_payload(&payload, "base16-ocean.dark", 30);
        for (i, line) in lines.iter().enumerate() {
            let text = line_text(line);
            if i > 1 && i < lines.len() - 1 && text.starts_with("  a") {
                assert!(text.starts_with("  "));
                for span in &line.spans {
                    assert_ne!(span.style.fg, Some(Color::Rgb(43, 48, 59)));
                }
            }
        }
    }
}

