use ratatui::style::{Color, Modifier, Style};

pub const CLUSTER: &str = "prod-eu-west-1";

pub const BG: Color = Color::Rgb(0x0d, 0x0c, 0x0b);
pub const FG_DIM: Color = Color::Rgb(0x6e, 0x68, 0x64);
pub const FG: Color = Color::Rgb(0xe8, 0xe4, 0xe2);
pub const ACCENT: Color = Color::Rgb(0xec, 0x30, 0x13);

pub fn dim() -> Style {
    Style::new().bg(BG).fg(FG_DIM)
}

pub const HEADING_DIM: Color = Color::Rgb(0x9a, 0x92, 0x8c);

pub fn heading(focused: bool) -> Style {
    let fg = if focused { FG } else { HEADING_DIM };
    Style::new().bg(BG).fg(fg).add_modifier(Modifier::BOLD)
}

pub fn list_selection(focused: bool) -> Style {
    if focused {
        Style::new()
            .bg(Color::Rgb(0x26, 0x24, 0x22))
            .fg(FG)
            .add_modifier(Modifier::BOLD)
    } else {
        dim()
    }
}

pub fn accent_selection(focused: bool) -> Style {
    if focused {
        Style::new().bg(ACCENT).fg(BG).add_modifier(Modifier::BOLD)
    } else {
        dim()
    }
}

pub fn accent() -> Style {
    Style::new().fg(ACCENT)
}
