use crossterm::event::KeyCode;
use ratatui::{prelude::*, widgets::*};
use tokio::sync::mpsc::UnboundedSender;

use super::Component;
use crate::{action::Action, config::Config, kafka, theme};

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

    fn under_replicated(&self) -> usize {
        self.partitions
            .iter()
            .filter(|p| p.isr < p.replicas)
            .count()
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
        let dim_bg = theme::dim();
        let sel_style = theme::list_selection(self.focused);

        let [title, list_area, footer] = Layout::vertical([
            Constraint::Length(1),
            Constraint::Fill(1),
            Constraint::Length(1),
        ])
        .areas(area);

        let total = self.partitions.len() + 1; // +1 for the ALL row

        frame.render_widget(
            Paragraph::new(format!("PARTITIONS {total}")).style(theme::heading(self.focused)),
            title,
        );

        let items = std::iter::once(ListItem::new("ALL").style(dim_bg)).chain(
            self.partitions.iter().map(|p| {
                let warn = if p.isr < p.replicas { " !" } else { "" };
                let line = Line::from(vec![
                    Span::styled(format!("p{} {}..{}", p.id, p.low, p.high), dim_bg),
                    Span::styled(warn, theme::accent()),
                ]);
                ListItem::new(line).style(dim_bg)
            }),
        );
        frame.render_stateful_widget(
            List::new(items).style(dim_bg).highlight_style(sel_style),
            list_area,
            &mut self.state,
        );

        let pos = self.state.selected().map_or(0, |i| i + 1);
        let under = self.under_replicated();
        let isr_summary = self
            .partitions
            .iter()
            .map(|p| (p.isr, p.replicas))
            .min_by_key(|&(isr, _)| isr)
            .map(|(isr, replicas)| format!("  isr {isr}/{replicas} — {under}/{total}"))
            .unwrap_or_default();
        frame.render_widget(
            Paragraph::new(format!("{pos}/{total}{isr_summary}")).style(dim_bg),
            footer,
        );

        Ok(())
    }
}
