use ratatui::{prelude::*, widgets::Paragraph};

use crate::{action::Action, components::Component, theme};

#[derive(Default)]
pub struct Breadcrumb {
    topic: Option<String>,
    partitions: Vec<i32>,
    offset: Option<i64>,
}

impl Breadcrumb {
    pub fn new() -> Self {
        Self::default()
    }

    fn partition_label(&self) -> Option<String> {
        match self.partitions.as_slice() {
            [] => None,
            [id] => Some(format!("p{id}")),
            _ => Some("p*".to_string()),
        }
    }
}

impl Component for Breadcrumb {
    fn update(&mut self, action: Action) -> color_eyre::Result<Option<Action>> {
        match action {
            Action::TopicSelected(topic) => {
                self.topic = Some(topic);
                self.partitions.clear();
                self.offset = None;
            }
            Action::PartitionSelected(_, ids) => {
                self.partitions = ids;
                self.offset = None;
            }
            Action::RecordSelected(record) => {
                self.offset = record.map(|r| r.offset);
            }
            _ => {}
        }
        Ok(None)
    }

    fn draw(&mut self, frame: &mut Frame, area: Rect) -> color_eyre::Result<()> {
        let dim = theme::dim();
        let mut spans = vec![Span::styled(format!("/{}", theme::CLUSTER), dim)];
        if let Some(topic) = &self.topic {
            spans.push(Span::styled(format!("/{topic}"), dim));
        }
        if let Some(p) = self.partition_label() {
            spans.push(Span::styled(format!("/{p}"), dim));
        }
        if let Some(offset) = self.offset {
            spans.push(Span::styled("/", dim));
            spans.push(Span::styled(format!(" {offset} "), theme::accent_selection(true)));
        }
        frame.render_widget(Paragraph::new(Line::from(spans)).style(dim), area);
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::kafka;

    #[test]
    fn tracks_selection_path() {
        let mut b = Breadcrumb::new();
        assert_eq!(b.partition_label(), None);

        b.update(Action::TopicSelected("orders.v2".into())).unwrap();
        b.update(Action::PartitionSelected("orders.v2".into(), vec![0]))
            .unwrap();
        assert_eq!(b.partition_label(), Some("p0".to_string()));

        b.update(Action::PartitionSelected("orders.v2".into(), vec![0, 1, 2]))
            .unwrap();
        assert_eq!(b.partition_label(), Some("p*".to_string()));

        b.update(Action::RecordSelected(Some(kafka::Record {
            partition: 0,
            offset: 4_017_998,
            key: None,
            payload: String::new(),
        })))
        .unwrap();
        assert_eq!(b.offset, Some(4_017_998));

        b.update(Action::PartitionSelected("orders.v2".into(), vec![0]))
            .unwrap();
        assert_eq!(b.offset, None, "changing partition clears the stale offset");
    }
}
