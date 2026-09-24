use crossterm::event::{KeyCode, KeyEvent};
use ratatui::prelude::*;
use serde::{Deserialize, Serialize};
use tokio::{sync::mpsc, task::JoinHandle};
use tracing::{debug, info};

use crate::{
    action::Action,
    components::{
        Component, breadcrumb::Breadcrumb, fps::FpsCounter, header::Header, home::Home,
        partitions::Partitions, record_detail::RecordDetail, records, records::Records,
    },
    config::Config,
    kafka,
    tui::{Event, Tui},
};

const NAV_COLUMNS: usize = 4;

pub struct App {
    config: Config,
    header: Header,
    breadcrumb: Breadcrumb,
    tick_rate: f64,
    frame_rate: f64,
    components: Vec<Box<dyn Component>>,
    focus: usize,
    should_quit: bool,
    should_suspend: bool,
    mode: Mode,
    last_tick_key_events: Vec<KeyEvent>,
    action_tx: mpsc::UnboundedSender<Action>,
    action_rx: mpsc::UnboundedReceiver<Action>,
    tail_handle: Option<JoinHandle<()>>,
    dirty: bool,
}

#[derive(Default, Debug, Copy, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum Mode {
    #[default]
    Home,
}

impl App {
    pub fn new(tick_rate: f64, frame_rate: f64) -> color_eyre::Result<Self> {
        let (action_tx, action_rx) = mpsc::unbounded_channel();
        Ok(Self {
            tick_rate,
            frame_rate,
            header: Header,
            breadcrumb: Breadcrumb::new(),
            components: vec![
                Box::new(Home::new()),
                Box::new(Partitions::new()),
                Box::new(Records::new()),
                Box::new(RecordDetail::new()),
                Box::new(FpsCounter::default()),
            ],
            focus: 0,
            should_quit: false,
            should_suspend: false,
            config: Config::new()?,
            mode: Mode::Home,
            last_tick_key_events: Vec::new(),
            action_tx,
            action_rx,
            tail_handle: None,
            dirty: true,
        })
    }

    pub async fn run(&mut self) -> color_eyre::Result<()> {
        let mut tui = Tui::new()?
            // .mouse(true) // uncomment this line to enable mouse support
            .tick_rate(self.tick_rate)
            .frame_rate(self.frame_rate);
        tui.enter()?;

        for component in self.components.iter_mut() {
            component.register_action_handler(self.action_tx.clone())?;
            self.header
                .register_action_handler(self.action_tx.clone())?;
        }
        for component in self.components.iter_mut() {
            component.register_config_handler(self.config.clone())?;
            self.header
                .register_action_handler(self.action_tx.clone())?;
        }
        for component in self.components.iter_mut() {
            component.init(tui.size()?)?;
            self.header
                .register_action_handler(self.action_tx.clone())?;
        }
        self.components[self.focus].set_focused(true);

        let action_tx = self.action_tx.clone();
        let kafka_action_tx = action_tx.clone();
        tokio::spawn(async move {
            let cfg = kafka::admin_client_config();
            let result = async {
                let consumer = kafka::build_consumer(&cfg)?;
                kafka::list_topics(&consumer).await
            }
            .await;
            match result {
                Ok(topics) => kafka_action_tx.send(Action::TopicsLoaded(topics)),
                Err(err) => kafka_action_tx.send(Action::Error(err.to_string())),
            }
        });
        loop {
            self.handle_events(&mut tui).await?;
            self.handle_actions(&mut tui)?;
            if self.should_suspend {
                tui.suspend()?;
                action_tx.send(Action::Resume)?;
                action_tx.send(Action::ClearScreen)?;
                // tui.mouse(true);
                tui.enter()?;
            } else if self.should_quit {
                tui.stop()?;
                break;
            }
        }
        tui.exit()?;
        Ok(())
    }

    async fn handle_events(&mut self, tui: &mut Tui) -> color_eyre::Result<()> {
        let Some(event) = tui.next_event().await else {
            return Ok(());
        };
        let action_tx = self.action_tx.clone();
        if !matches!(event, Event::Tick | Event::Render) {
            self.dirty = true;
        }
        match event {
            Event::Quit => action_tx.send(Action::Quit)?,
            Event::Tick => action_tx.send(Action::Tick)?,
            Event::Render => action_tx.send(Action::Render)?,
            Event::Resize(x, y) => action_tx.send(Action::Resize(x, y))?,
            Event::Key(key) => self.handle_key_event(key)?,
            _ => {}
        }

        if let Event::Key(key) = event {
            match key.code {
                KeyCode::Right | KeyCode::Char('l') => {
                    self.components[self.focus].set_focused(false);
                    self.focus = (self.focus + 1).min(NAV_COLUMNS - 1);
                    self.components[self.focus].set_focused(true);
                }
                KeyCode::Left | KeyCode::Char('h') => {
                    self.components[self.focus].set_focused(false);
                    self.focus = self.focus.saturating_sub(1);
                    self.components[self.focus].set_focused(true);
                }
                // only the focused column gets raw key events (j/k list-nav etc.) —
                // otherwise every column would scroll together on every keypress
                _ => {
                    if let Some(component) = self.components.get_mut(self.focus)
                        && let Some(action) = component.handle_events(Some(event))?
                    {
                        action_tx.send(action)?;
                    }
                }
            }
        } else {
            for component in self.components.iter_mut() {
                if let Some(action) = component.handle_events(Some(event.clone()))? {
                    action_tx.send(action)?;
                }
            }
        }
        Ok(())
    }

    fn handle_key_event(&mut self, key: KeyEvent) -> color_eyre::Result<()> {
        let action_tx = self.action_tx.clone();
        let Some(keymap) = self.config.keybindings.0.get(&self.mode) else {
            return Ok(());
        };
        match keymap.get(&vec![key]) {
            Some(action) => {
                info!("Got action: {action:?}");
                action_tx.send(action.clone())?;
            }
            _ => {
                // If the key was not handled as a single key action,
                // then consider it for multi-key combinations.
                self.last_tick_key_events.push(key);

                // Check for multi-key combinations
                if let Some(action) = keymap.get(&self.last_tick_key_events) {
                    info!("Got action: {action:?}");
                    action_tx.send(action.clone())?;
                }
            }
        }
        Ok(())
    }

    fn handle_actions(&mut self, tui: &mut Tui) -> color_eyre::Result<()> {
        while let Ok(action) = self.action_rx.try_recv() {
            if action != Action::Tick && action != Action::Render {
                debug!("{action:?}");
            }
            if !matches!(action, Action::Tick | Action::Render) {
                self.dirty = true;
            }
            match action {
                Action::Tick => {
                    self.last_tick_key_events.drain(..);
                }
                Action::Quit => self.should_quit = true,
                Action::Suspend => self.should_suspend = true,
                Action::Resume => self.should_suspend = false,
                Action::ClearScreen => tui.terminal.clear()?,
                Action::Resize(w, h) => self.handle_resize(tui, w, h)?,
                Action::Render => {
                    if self.dirty {
                        self.render(tui)?;
                        self.dirty = false;
                    }
                }
                Action::Error(ref err) => {
                    tracing::error!(?err)
                }
                Action::TopicSelected(ref topic) => {
                    let tx = self.action_tx.clone();
                    let topic = topic.clone();
                    tokio::spawn(async move {
                        let cfg = kafka::admin_client_config();
                        let result = async {
                            let consumer = kafka::build_consumer(&cfg)?;
                            kafka::list_partitions(&consumer, &topic).await
                        }
                        .await;
                        match result {
                            Ok(partitions) => tx.send(Action::PartitionsLoaded(topic, partitions)),
                            Err(err) => tx.send(Action::Error(err.to_string())),
                        }
                    });
                }
                Action::PartitionSelected(..) | Action::TailStopped => {
                    if let Some(h) = self.tail_handle.take() {
                        h.abort();
                    }
                }
                Action::LoadNextRecords(ref topic, ref ids, ref starts) => {
                    let tx = self.action_tx.clone();
                    let topic = topic.clone();
                    let ids = ids.clone();
                    let starts = starts.clone();
                    tokio::task::spawn_blocking(move || {
                        let cfg = kafka::admin_client_config();
                        match kafka::fetch_records(&cfg, &topic, &starts, records::PAGE) {
                            Ok(records) => {
                                let has_more = records.len() == records::PAGE;
                                tx.send(Action::HistoricalRecordsLoaded(
                                    topic, ids, records, has_more,
                                ))
                            }
                            Err(err) => {
                                let _ = tx.send(Action::PageFailed(topic, ids));
                                tx.send(Action::Error(err.to_string()))
                            }
                        }
                    });
                }
                Action::TailSelected(ref topic, ref ids) => {
                    if let Some(h) = self.tail_handle.take() {
                        h.abort();
                    }
                    let tx = self.action_tx.clone();
                    let topic = topic.clone();
                    let ids = ids.clone();
                    self.tail_handle = Some(tokio::spawn(async move {
                        let cfg = kafka::admin_client_config();
                        let result = async {
                            let consumer = kafka::build_stream_consumer(&cfg)?;
                            kafka::assign_partitions(&consumer, &topic, &ids)?;
                            kafka::tail(consumer, tx.clone(), topic).await;
                            Ok::<_, color_eyre::Report>(())
                        }
                        .await;
                        if let Err(err) = result {
                            let _ = tx.send(Action::Error(err.to_string()));
                        }
                    }))
                }
                _ => {}
            }
            if let Some(action) = self.breadcrumb.update(action.clone())? {
                self.action_tx.send(action)?;
            }
            for component in self.components.iter_mut() {
                if let Some(action) = component.update(action.clone())? {
                    self.action_tx.send(action)?
                };
            }
        }
        Ok(())
    }

    fn handle_resize(&mut self, tui: &mut Tui, w: u16, h: u16) -> color_eyre::Result<()> {
        tui.resize(Rect::new(0, 0, w, h))?;
        self.render(tui)?;
        Ok(())
    }

    fn render(&mut self, tui: &mut Tui) -> color_eyre::Result<()> {
        tui.draw(|frame| {
            let [header_area, breadcrumb_area, body_area] = Layout::vertical([
                Constraint::Length(9), // header row count from the mockup — tune to taste
                Constraint::Length(1),
                Constraint::Min(0),
            ])
            .areas(frame.area());

            let _ = self.header.draw(frame, header_area);
            let _ = self.breadcrumb.draw(frame, breadcrumb_area);

            let [topics_area, partitions_area, records_area, detail_area] = Layout::horizontal([
                Constraint::Length(24),
                Constraint::Length(24),
                Constraint::Length(36),
                Constraint::Fill(1),
            ])
            .areas(body_area);
            // FpsCounter is an overlay and wants the full body area.
            let areas = [
                topics_area,
                partitions_area,
                records_area,
                detail_area,
                body_area,
            ];

            for (component, area) in self.components.iter_mut().zip(areas) {
                if let Err(err) = component.draw(frame, area) {
                    let _ = self
                        .action_tx
                        .send(Action::Error(format!("Failed to draw: {:?}", err)));
                }
            }
        })?;
        Ok(())
    }
}


