use ratatui::{
    prelude::*,
    widgets::{Block, Clear, Paragraph},
};

use crate::components::Component;

#[derive(Debug)]
pub struct Download {
    pub show_popup: bool,
}

impl Default for Download {
    fn default() -> Self {
        Self::new()
    }
}

impl Download {
    pub fn new() -> Self {
        Self { show_popup: false }
    }
}

impl Component for Download {
    fn draw(&mut self, frame: &mut Frame, area: Rect) -> color_eyre::Result<()> {
        if self.show_popup {
            let popup_block = Block::bordered().title("Download");
            let popup_area = area.centered(Constraint::Percentage(60), Constraint::Percentage(20));

            frame.render_widget(Clear, popup_area);
            let paragraph_placeholder = Paragraph::new("Placeholder");
            frame.render_widget(paragraph_placeholder, popup_area);
        }
        Ok(())
    }
}
