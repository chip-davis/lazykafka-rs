use crossterm::event::KeyCode;
use ratatui::{prelude::*, widgets::*};
use tokio::sync::mpsc::UnboundedSender;

use super::Component;
use crate::{action::Action, config::Config, kafka};

#[derive(Default)]
pub struct Partitions {
    command_tx: Option<UnboundedSender<Action>>,
    config: Config,
    current_topic: Option<String>,
    partitions: Vec<kafka::Partition>,
    state: ListState,
    focused: bool,
}

impl Partitions {
    pub fn new() -> Self {
        Self::default()
    }
    fn emit_selection(&self) -> Option<Action> {
        let topic = self.current_topic.clone()?;
        let ids = match self.state.selected()? {
            0 => self.partitions.iter().map(|p| p.id).collect(),
            i => vec![self.partitions.get(i - 1)?.id],
        };
        Some(Action::PartitionSelected(topic, ids))
    }
}

impl Component for Partitions {
    fn handle_key_event(
        &mut self,
        key: crossterm::event::KeyEvent,
    ) -> color_eyre::Result<Option<Action>> {
        match key.code {
            KeyCode::Down | KeyCode::Char('j') => self.state.select_next(),
            KeyCode::Up | KeyCode::Char('k') => self.state.select_previous(),
            _ => {}
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
            Action::TopicSelected(topic) => {
                self.current_topic = Some(topic);
                self.partitions.clear();
                self.state = ListState::default();
            }
            Action::PartitionsLoaded(topic, parts) => {
                if Some(&topic) == self.current_topic.as_ref() {
                    self.partitions = parts;
                    if !self.partitions.is_empty() {
                        self.state.select_first();
                        return Ok(self.emit_selection());
                    }
                }
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

        let total = self.partitions.len() + 1; // +1 for the ALL row

        frame.render_widget(
            Paragraph::new(format!("PARTITIONS {total}")).style(dim_bg),
            title,
        );

        let items = std::iter::once("ALL".to_string()).chain(
            self.partitions
                .iter()
                .map(|p| format!("p{} {}..{}", p.id, p.low, p.high)),
        );
        frame.render_stateful_widget(
            List::new(items).highlight_style(sel_style),
            list_area,
            &mut self.state,
        );

        let pos = self.state.selected().map_or(0, |i| i + 1);
        frame.render_widget(Paragraph::new(format!("{pos}/{total}")), footer);

        Ok(())
    }
}
