use std::collections::VecDeque;

use crate::{action::Action, components::Component, config::Config, kafka};
use crossterm::event::KeyCode;
use ratatui::{prelude::*, widgets::*};
use tokio::sync::mpsc::UnboundedSender;
const CAP: usize = 500;

#[derive(Default)]
pub struct Records {
    command_tx: Option<UnboundedSender<Action>>,
    config: Config,
    focused: bool,
    current_topic: Option<String>,
    records: VecDeque<kafka::Record>,
    paused: bool,
    state: ListState,
}

impl Records {
    pub fn new() -> Self {
        Self::default()
    }
}

impl Component for Records {
    fn handle_key_event(
        &mut self,
        key: crossterm::event::KeyEvent,
    ) -> color_eyre::Result<Option<crate::action::Action>> {
        match key.code {
            KeyCode::Char(' ') => self.paused = !self.paused,
            KeyCode::Down | KeyCode::Char('j') => self.state.select_next(),
            KeyCode::Up | KeyCode::Char('k') => self.state.select_previous(),
            _ => {}
        }
        Ok(None)
    }

    fn set_focused(&mut self, focused: bool) {
        self.focused = focused;
    }

    fn update(&mut self, action: Action) -> color_eyre::Result<Option<Action>> {
        match action {
            Action::PartitionSelected(topic, _ids) => {
                self.current_topic = Some(topic);
                self.records.clear();
                self.state = ListState::default();
            }
            Action::RecordReceived(topic, rec) => {
                if self.paused || Some(&topic) != self.current_topic.as_ref() {
                    return Ok(None);
                }
                self.records.push_back(rec);
                if self.records.len() > CAP {
                    self.records.pop_front();
                }
                self.state.select_last();
            }
            _ => {}
        }
        Ok(None)
    }

    fn draw(&mut self, frame: &mut Frame, area: Rect) -> color_eyre::Result<()> {
        let dim_bg = Style::new()
            .bg(Color::Rgb(0x24, 0x1c, 0x1a))
            .fg(Color::Rgb(0x8a, 0x80, 0x7d));
        let sel_style = if self.focused {
            Style::new()
                .bg(Color::Rgb(0x3a, 0x2a, 0x26))
                .fg(Color::Rgb(0xe8, 0xe4, 0xe2))
                .add_modifier(Modifier::BOLD)
        } else {
            dim_bg
        };

        let [title, list_area, footer] = Layout::vertical([
            Constraint::Length(1),
            Constraint::Fill(1),
            Constraint::Length(1),
        ])
        .areas(area);

        let paused_tag = if self.paused { " [PAUSED]" } else { "" };
        frame.render_widget(
            Paragraph::new(format!("RECORDS {}{}", self.records.len(), paused_tag)).style(dim_bg),
            title,
        );

        let items = self
            .records
            .iter()
            .map(|r| format!("p{} @{} {}", r.partition, r.offset, r.payload));
        frame.render_stateful_widget(
            List::new(items).highlight_style(sel_style),
            list_area,
            &mut self.state,
        );

        let pos = self.state.selected().map_or(0, |i| i + 1);
        frame.render_widget(
            Paragraph::new(format!("{pos}/{}", self.records.len())),
            footer,
        );

        Ok(())
    }
}
