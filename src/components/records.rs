use std::collections::{HashMap, VecDeque};

use crate::{action::Action, components::Component, config::Config, kafka};
use crossterm::event::KeyCode;
use ratatui::{prelude::*, widgets::*};
use tokio::sync::mpsc::UnboundedSender;

pub const PAGE: usize = 500;
const LIVE_CAP: usize = 5_000;

type Key = (String, Vec<i32>);
type Starts = Vec<(i32, i64)>;

fn scroll_offset(sel: usize, off: usize, height: usize, len: usize) -> usize {
    let off = if sel < off {
        sel
    } else if sel >= off + height {
        sel + 1 - height
    } else {
        off
    };
    off.min(len.saturating_sub(1))
}

fn next_starts(starts: &Starts, records: &VecDeque<kafka::Record>) -> Starts {
    let mut next = starts.clone();
    for record in records {
        if let Some(start) = next.iter_mut().find(|(id, _)| *id == record.partition) {
            start.1 = start.1.max(record.offset + 1);
        }
    }
    next
}

#[derive(Default)]
struct Page {
    records: VecDeque<kafka::Record>,
    starts: Starts,
    back: Vec<Starts>,
    has_more: bool,
    loading: bool,
}

#[derive(Default)]
pub struct Records {
    command_tx: Option<UnboundedSender<Action>>,
    config: Config,
    focused: bool,
    key: Option<Key>,
    pages: HashMap<Key, Page>,
    watermarks: HashMap<String, Vec<kafka::Partition>>,
    seek: Option<String>,
    live: bool,
    paused: bool,
    sel: usize,
    off: usize,
    visible: usize,
}

impl Records {
    pub fn new() -> Self {
        Self::default()
    }

    fn page(&self) -> Option<&Page> {
        self.pages.get(self.key.as_ref()?)
    }

    fn len(&self) -> usize {
        self.page().map_or(0, |p| p.records.len())
    }

    fn selected_record(&self) -> Option<&kafka::Record> {
        self.page()?.records.get(self.sel)
    }

    fn selection(&self) -> Option<Action> {
        Some(Action::RecordSelected(self.selected_record().cloned()))
    }

    fn ids(&self) -> Vec<i32> {
        self.key
            .as_ref()
            .map(|(_, ids)| ids.clone())
            .unwrap_or_default()
    }

    fn beginning(&self) -> Starts {
        self.ids().into_iter().map(|id| (id, -1)).collect()
    }

    fn at_offset(&self, offset: i64) -> Starts {
        self.ids().into_iter().map(|id| (id, offset)).collect()
    }

    fn last_page(&self) -> Starts {
        let ids = self.ids();
        let share = (PAGE / ids.len().max(1)) as i64;
        let parts = self.key.as_ref().and_then(|(t, _)| self.watermarks.get(t));
        ids.into_iter()
            .map(|id| {
                let start = parts
                    .and_then(|ps| ps.iter().find(|p| p.id == id))
                    .map_or(-1, |p| (p.high - share).max(p.low));
                (id, start)
            })
            .collect()
    }

    fn position(&self) -> String {
        let Some(page) = self.page() else {
            return String::new();
        };
        let (Some(first), Some(last)) = (page.records.front(), page.records.back()) else {
            return "empty".into();
        };
        let end = self
            .key
            .as_ref()
            .and_then(|(t, _)| self.watermarks.get(t))
            .map(|ps| {
                ps.iter()
                    .filter(|p| self.ids().contains(&p.id))
                    .map(|p| p.high)
                    .max()
                    .unwrap_or(0)
            });
        match end {
            Some(end) => format!("@{}-{} of {}", first.offset, last.offset, end - 1),
            None => format!("@{}-{}", first.offset, last.offset),
        }
    }

    fn request(&mut self, starts: Starts, keep_back: bool) -> Option<Action> {
        let (topic, ids) = self.key.clone()?;
        let page = self.pages.get_mut(&(topic.clone(), ids.clone()))?;
        if page.loading {
            return None;
        }
        page.loading = true;
        if !keep_back {
            page.back.clear();
        }
        page.starts = starts.clone();
        Some(Action::LoadNextRecords(topic, ids, starts))
    }

    fn forward(&mut self) -> Option<Action> {
        let page = self.pages.get(self.key.as_ref()?)?;
        if page.loading || !page.has_more {
            return None;
        }
        let here = page.starts.clone();
        let starts = next_starts(&here, &page.records);
        let action = self.request(starts, true)?;
        self.pages.get_mut(self.key.as_ref()?)?.back.push(here);
        Some(action)
    }

    fn backward(&mut self) -> Option<Action> {
        let page = self.pages.get_mut(self.key.as_ref()?)?;
        if page.loading {
            return None;
        }
        let starts = page.back.pop()?;
        self.request(starts, true)
    }
}

impl Component for Records {
    fn register_action_handler(&mut self, tx: UnboundedSender<Action>) -> color_eyre::Result<()> {
        self.command_tx = Some(tx);
        Ok(())
    }

    fn register_config_handler(&mut self, config: Config) -> color_eyre::Result<()> {
        self.config = config;
        Ok(())
    }

    fn handle_key_event(
        &mut self,
        key: crossterm::event::KeyEvent,
    ) -> color_eyre::Result<Option<crate::action::Action>> {
        if let Some(buf) = self.seek.as_mut() {
            match key.code {
                KeyCode::Char(c) if c.is_ascii_digit() => buf.push(c),
                KeyCode::Backspace => {
                    buf.pop();
                }
                KeyCode::Esc => self.seek = None,
                KeyCode::Enter => {
                    let offset = buf.parse::<i64>().ok();
                    self.seek = None;
                    if let Some(offset) = offset {
                        let starts = self.at_offset(offset);
                        return Ok(self.request(starts, false));
                    }
                }
                _ => {}
            }
            return Ok(None);
        }

        let jump = self.visible.max(1);
        match key.code {
            KeyCode::Char(' ') => self.paused = !self.paused,
            KeyCode::Char('n') => return Ok(self.forward()),
            KeyCode::Char('p') => return Ok(self.backward()),
            KeyCode::Char('g') => {
                let starts = self.beginning();
                return Ok(self.request(starts, false));
            }
            KeyCode::Char('G') => {
                let starts = self.last_page();
                return Ok(self.request(starts, false));
            }
            KeyCode::Char('o') => {
                self.seek = Some(String::new());
                return Ok(None);
            }
            KeyCode::Char('t') => {
                let Some((topic, ids)) = self.key.clone() else {
                    return Ok(None);
                };
                self.live = !self.live;
                return Ok(Some(if self.live {
                    Action::TailSelected(topic, ids)
                } else {
                    Action::TailStopped
                }));
            }
            KeyCode::Home => self.sel = 0,
            KeyCode::End => self.sel = self.len().saturating_sub(1),
            KeyCode::PageDown => self.sel = (self.sel + jump).min(self.len().saturating_sub(1)),
            KeyCode::PageUp => self.sel = self.sel.saturating_sub(jump),
            KeyCode::Down | KeyCode::Char('j') => {
                self.sel = (self.sel + 1).min(self.len().saturating_sub(1))
            }
            KeyCode::Up | KeyCode::Char('k') => self.sel = self.sel.saturating_sub(1),
            _ => {}
        }
        Ok(self.selection())
    }

    fn set_focused(&mut self, focused: bool) {
        self.focused = focused;
    }

    fn update(&mut self, action: Action) -> color_eyre::Result<Option<Action>> {
        match action {
            Action::PartitionsLoaded(topic, parts) => {
                self.watermarks.insert(topic, parts);
            }
            Action::PageFailed(topic, ids) => {
                if let Some(page) = self.pages.get_mut(&(topic, ids)) {
                    page.loading = false;
                }
            }
            Action::PartitionSelected(topic, ids) => {
                self.key = Some((topic, ids));
                self.live = false;
                self.seek = None;
                self.sel = 0;
                self.off = 0;
                let key = self.key.clone().unwrap();
                if !self.pages.contains_key(&key) {
                    self.pages.insert(key, Page::default());
                    let starts = self.beginning();
                    return Ok(self.request(starts, false));
                }
                return Ok(self.selection());
            }
            Action::HistoricalRecordsLoaded(topic, ids, records, has_more) => {
                let key = (topic, ids);
                if self.key.as_ref() != Some(&key) {
                    return Ok(None);
                }
                let page = self.pages.entry(key).or_default();
                page.records = records.into();
                page.has_more = has_more;
                page.loading = false;
                self.sel = 0;
                self.off = 0;
                return Ok(self.selection());
            }
            Action::RecordReceived(topic, rec) => {
                if !self.live || self.paused {
                    return Ok(None);
                }
                let Some(key) = self.key.clone().filter(|(t, _)| *t == topic) else {
                    return Ok(None);
                };
                let page = self.pages.entry(key).or_default();
                page.records.push_back(rec);
                while page.records.len() > LIVE_CAP {
                    page.records.pop_front();
                }
                self.sel = page.records.len() - 1;
                return Ok(self.selection());
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

        let (has_more, loading) = self.page().map_or((false, false), |p| (p.has_more, p.loading));
        let tag = match (self.live, self.paused, loading) {
            (_, _, true) => " [LOADING]",
            (true, true, _) => " [LIVE PAUSED]",
            (true, false, _) => " [LIVE]",
            _ => "",
        };
        let len = self.len();
        let pos = if len == 0 { 0 } else { self.sel + 1 };
        frame.render_widget(
            Paragraph::new(format!("RECORDS {pos}/{len}{tag}")).style(dim_bg),
            title,
        );

        let height = (list_area.height as usize).max(1);
        self.visible = height;
        self.sel = self.sel.min(len.saturating_sub(1));
        self.off = scroll_offset(self.sel, self.off, height, len);

        let window = self.page().into_iter().flat_map(|p| {
            p.records
                .iter()
                .skip(self.off)
                .take(height)
                .map(|r| {
                    let mut line = format!("p{} @{} ", r.partition, r.offset);
                    line.extend(
                        r.payload
                            .chars()
                            .take(200)
                            .map(|c| if c.is_control() { ' ' } else { c }),
                    );
                    line
                })
        });
        let mut window_state = ListState::default();
        if len > 0 {
            window_state.select(Some(self.sel - self.off));
        }
        frame.render_stateful_widget(
            List::new(window.collect::<Vec<_>>()).highlight_style(sel_style),
            list_area,
            &mut window_state,
        );

        let status = match &self.seek {
            Some(buf) => format!("offset> {buf}_"),
            None => self.position(),
        };
        frame.render_widget(Paragraph::new(status), footer);

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    pub(super) fn rec(partition: i32, offset: i64) -> kafka::Record {
        kafka::Record {
            partition,
            offset,
            key: None,
            payload: String::new(),
        }
    }

    fn loaded(r: &mut Records, records: Vec<kafka::Record>, has_more: bool) {
        let (topic, ids) = r.key.clone().unwrap();
        r.update(Action::HistoricalRecordsLoaded(
            topic, ids, records, has_more,
        ))
        .unwrap();
    }

    fn setup() -> Records {
        let mut r = Records::new();
        r.update(Action::PartitionSelected("t".into(), vec![0, 1]))
            .unwrap();
        r
    }

    #[test]
    fn paging_replaces_and_walks_back() {
        let mut r = setup();
        assert_eq!(r.page().unwrap().starts, vec![(0, -1), (1, -1)]);

        loaded(&mut r, vec![rec(0, 0), rec(1, 5), rec(0, 2)], true);
        assert_eq!(r.len(), 3);

        assert_eq!(
            r.forward(),
            Some(Action::LoadNextRecords(
                "t".into(),
                vec![0, 1],
                vec![(0, 3), (1, 6)]
            ))
        );
        loaded(&mut r, vec![rec(0, 3), rec(1, 6)], false);
        assert_eq!(r.len(), 2);

        assert_eq!(r.forward(), None);

        assert_eq!(
            r.backward(),
            Some(Action::LoadNextRecords(
                "t".into(),
                vec![0, 1],
                vec![(0, -1), (1, -1)]
            ))
        );
        loaded(&mut r, vec![rec(0, 0), rec(1, 5), rec(0, 2)], true);
        assert_eq!(r.backward(), None);
    }

    #[test]
    fn jumps_use_watermarks_and_clear_history() {
        let mut r = setup();
        r.watermarks.insert(
            "t".into(),
            vec![
                kafka::Partition {
                    id: 0,
                    leader: 0,
                    low: 10,
                    high: 9_000,
                },
                kafka::Partition {
                    id: 1,
                    leader: 0,
                    low: 0,
                    high: 100,
                },
            ],
        );
        loaded(&mut r, vec![rec(0, 10)], true);
        r.forward();
        loaded(&mut r, vec![rec(0, 11)], true);
        assert_eq!(r.page().unwrap().back.len(), 1);

        let starts = r.last_page();
        assert_eq!(starts, vec![(0, 8_750), (1, 0)]);
        r.request(starts, false);
        assert!(r.page().unwrap().back.is_empty());

        loaded(&mut r, vec![], false);
        assert_eq!(r.at_offset(4_000), vec![(0, 4_000), (1, 4_000)]);
    }

    #[test]
    fn failed_page_clears_loading() {
        let mut r = setup();
        assert!(r.page().unwrap().loading);
        r.update(Action::PageFailed("t".into(), vec![0, 1])).unwrap();
        assert!(!r.page().unwrap().loading);
        assert!(r.request(r.beginning(), false).is_some());
    }

    #[test]
    fn position_reports_offset_span_against_watermark() {
        let mut r = setup();
        r.watermarks.insert(
            "t".into(),
            vec![
                kafka::Partition { id: 0, leader: 0, low: 0, high: 62_326 },
                kafka::Partition { id: 1, leader: 0, low: 0, high: 100 },
            ],
        );
        assert_eq!(r.position(), "empty");
        loaded(&mut r, vec![rec(0, 61_826), rec(0, 62_325)], false);
        assert_eq!(r.position(), "@61826-62325 of 62325");
    }

    #[test]
    fn scroll_offset_follows_selection() {
        assert_eq!(scroll_offset(0, 0, 10, 0), 0);
        assert_eq!(scroll_offset(5, 0, 10, 8500), 0);
        assert_eq!(scroll_offset(10, 0, 10, 8500), 1);
        assert_eq!(scroll_offset(8499, 0, 10, 8500), 8490);
        assert_eq!(scroll_offset(3, 100, 10, 8500), 3);
        assert_eq!(scroll_offset(0, 50, 10, 1), 0);
    }
}

