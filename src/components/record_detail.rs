use crate::{action::Action, components::Component, config::Config, kafka};
use ratatui::{prelude::*, widgets::*};

#[derive(Default)]
pub struct RecordDetail {
    config: Config,
    record: Option<kafka::Record>,
}

impl RecordDetail {
    pub fn new() -> Self {
        Self::default()
    }
}

impl Component for RecordDetail {
    fn register_config_handler(&mut self, config: Config) -> color_eyre::Result<()> {
        self.config = config;
        Ok(())
    }

    fn update(&mut self, action: Action) -> color_eyre::Result<Option<Action>> {
        if let Action::RecordSelected(record) = action {
            self.record = record;
        }
        Ok(None)
    }

    fn draw(&mut self, frame: &mut Frame, area: Rect) -> color_eyre::Result<()> {
        let muted = Style::default().fg(Color::DarkGray);
        let [title, meta, body] = Layout::vertical([
            Constraint::Length(1),
            Constraint::Length(3),
            Constraint::Fill(1),
        ])
        .areas(area);

        frame.render_widget(Paragraph::new("MESSAGE DETAILS").style(muted), title);

        let Some(record) = &self.record else {
            frame.render_widget(
                Paragraph::new("Select a message to view its details").style(muted),
                body,
            );
            return Ok(());
        };

        let key = record.key.as_deref().unwrap_or("<none>");
        frame.render_widget(
            Paragraph::new(vec![
                Line::from(vec![
                    Span::styled("Partition: ", muted),
                    Span::raw(record.partition.to_string()),
                    Span::styled("  Offset: ", muted),
                    Span::raw(record.offset.to_string()),
                ]),
                Line::from(vec![Span::styled("Key: ", muted), Span::raw(key)]),
            ]),
            meta,
        );

        let payload = match serde_json::from_str::<serde_json::Value>(&record.payload) {
            Ok(value) => {
                let pretty =
                    serde_json::to_string_pretty(&value).unwrap_or_else(|_| record.payload.clone());
                json_lines(&pretty)
            }
            Err(_) => vec![Line::from(record.payload.as_str())],
        };
        frame.render_widget(
            Paragraph::new(payload)
                .scroll((0, 0))
                .wrap(Wrap { trim: false }),
            body,
        );
        Ok(())
    }
}

fn json_lines(json: &str) -> Vec<Line<'static>> {
    json.lines()
        .map(|line| {
            let mut spans = Vec::new();
            let mut token = String::new();
            let mut in_string = false;
            let mut escaped = false;

            for ch in line.chars() {
                if in_string {
                    token.push(ch);
                    if escaped {
                        escaped = false;
                    } else if ch == '\\' {
                        escaped = true;
                    } else if ch == '"' {
                        in_string = false;
                        spans.push(Span::styled(
                            token.clone(),
                            Style::default().fg(Color::Green),
                        ));
                        token.clear();
                    }
                } else if ch == '"' {
                    if !token.is_empty() {
                        spans.push(Span::styled(
                            std::mem::take(&mut token),
                            Style::default().fg(Color::Gray),
                        ));
                    }
                    in_string = true;
                    token.push(ch);
                } else if ch.is_ascii_digit() || ch == '-' {
                    if !token.is_empty() {
                        spans.push(Span::styled(
                            std::mem::take(&mut token),
                            Style::default().fg(Color::Gray),
                        ));
                    }
                    token.push(ch);
                } else if ch.is_ascii_alphanumeric() {
                    token.push(ch);
                } else {
                    if !token.is_empty() {
                        let style = match token.as_str() {
                            "true" | "false" | "null" => Style::default().fg(Color::LightMagenta),
                            _ => Style::default().fg(Color::Gray),
                        };
                        spans.push(Span::styled(std::mem::take(&mut token), style));
                    }
                    let style = match ch {
                        '{' | '}' | '[' | ']' => Style::default().fg(Color::Yellow),
                        ':' | ',' => Style::default().fg(Color::DarkGray),
                        _ => Style::default().fg(Color::Gray),
                    };
                    spans.push(Span::styled(ch.to_string(), style));
                }
            }
            if !token.is_empty() {
                spans.push(Span::styled(token, Style::default().fg(Color::Gray)));
            }
            Line::from(spans)
        })
        .collect()
}
