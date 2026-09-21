use crossterm::event::KeyCode;
use ratatui::{prelude::*, widgets::*};
use tokio::sync::mpsc::UnboundedSender;

use super::Component;
use crate::{action::Action, config::Config, kafka};

#[derive(Default)]
pub struct Home {
    command_tx: Option<UnboundedSender<Action>>,
    config: Config,
    topics: Vec<kafka::Topic>,
    state: ListState,
    focused: bool,
}

impl Home {
    pub fn new() -> Self {
        Self::default()
    }

    fn emit_selection(&self) -> Option<Action> {
        let topic = self.topics.get(self.state.selected()?)?;
        Some(Action::TopicSelected(topic.name.clone()))
    }
}

impl Component for Home {
    fn handle_key_event(
        &mut self,
        key: crossterm::event::KeyEvent,
    ) -> color_eyre::Result<Option<Action>> {
        match key.code {
            KeyCode::Down | KeyCode::Char('j') => self.state.select_next(),
            KeyCode::Up | KeyCode::Char('k') => self.state.select_previous(),
            _ => return Ok(None),
        }
        Ok(self.emit_selection())
    }

    fn set_focused(&mut self, focused: bool) {
        self.focused = focused;
    }

    fn register_action_handler(&mut self, tx: UnboundedSender<Action>) -> color_eyre::Result<()> {
        self.command_tx = Some(tx);
        Ok(())
    }

    fn register_config_handler(&mut self, config: Config) -> color_eyre::Result<()> {
        self.config = config;
        Ok(())
    }

    fn update(&mut self, action: Action) -> color_eyre::Result<Option<Action>> {
        match action {
            Action::Tick => {
                // add any logic here that should run on every tick
            }
            Action::Render => {
                // add any logic here that should run on every render
            }
            Action::TopicsLoaded(topics) => {
                self.topics = topics;
                if self.state.selected().is_none() && !self.topics.is_empty() {
                    self.state.select_first();
                }
                return Ok(self.emit_selection());
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

        let width = self.topics.iter().map(|t| t.name.len()).max().unwrap_or(10) as u16 + 8; // +8 for " NNp" suffix/padding
        let [col, _rest] =
            Layout::horizontal([Constraint::Length(width), Constraint::Fill(1)]).areas(area);

        let [title, list_area, footer] = Layout::vertical([
            Constraint::Length(1),
            Constraint::Fill(1),
            Constraint::Length(1),
        ])
        .areas(col);

        frame.render_widget(
            Paragraph::new(format!("TOPICS {}", self.topics.len())).style(dim_bg),
            title,
        );

        let items = self
            .topics
            .iter()
            .map(|t| format!("{} {}p", t.name, t.partition_count));
        frame.render_stateful_widget(
            List::new(items).highlight_style(sel_style),
            list_area,
            &mut self.state,
        );

        let pos = self.state.selected().map_or(0, |i| i + 1);
        frame.render_widget(
            Paragraph::new(format!("{pos}/{}", self.topics.len())),
            footer,
        );

        Ok(())
    }
}
