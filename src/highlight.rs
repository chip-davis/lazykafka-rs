use std::sync::LazyLock;

use ratatui::style::{Color, Modifier, Style};
use syntect::{
    easy::HighlightLines,
    highlighting::{Theme, ThemeSet},
    parsing::{SyntaxReference, SyntaxSet},
    util::LinesWithEndings,
};

static SYNTAX_SET: LazyLock<SyntaxSet> = LazyLock::new(SyntaxSet::load_defaults_newlines);
static THEME_SET: LazyLock<ThemeSet> = LazyLock::new(ThemeSet::load_defaults);

pub fn json_syntax() -> &'static SyntaxReference {
    SYNTAX_SET
        .find_syntax_by_extension("json")
        .unwrap_or_else(|| SYNTAX_SET.find_syntax_plain_text())
}

pub fn theme(name: &str) -> &'static Theme {
    THEME_SET
        .themes
        .get(name)
        .unwrap_or_else(|| &THEME_SET.themes["base16-ocean.dark"])
}

fn to_style(s: syntect::highlighting::Style) -> Style {
    let mut style = Style::default().fg(Color::Rgb(s.foreground.r, s.foreground.g, s.foreground.b));
    if s.font_style
        .contains(syntect::highlighting::FontStyle::BOLD)
    {
        style = style.add_modifier(Modifier::BOLD);
    }
    if s.font_style
        .contains(syntect::highlighting::FontStyle::ITALIC)
    {
        style = style.add_modifier(Modifier::ITALIC);
    }
    if s.font_style
        .contains(syntect::highlighting::FontStyle::UNDERLINE)
    {
        style = style.add_modifier(Modifier::UNDERLINED);
    }
    style
}

pub fn highlight_json(text: &str, theme_name: &str) -> Vec<Vec<(String, Style)>> {
    let mut highlighter = HighlightLines::new(json_syntax(), theme(theme_name));
    LinesWithEndings::from(text)
        .map(|line| {
            let ranges = highlighter
                .highlight_line(line, &SYNTAX_SET)
                .unwrap_or_default();
            let mut spans: Vec<(String, Style)> = ranges
                .into_iter()
                .map(|(style, piece)| (piece.to_string(), to_style(style)))
                .collect();
            if let Some((last, _)) = spans.last_mut() {
                *last = last.trim_end_matches(['\n', '\r']).to_string();
            }
            spans
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn highlights_json_into_multiple_styled_spans() {
        let out = highlight_json("{\"a\": 1}", "base16-ocean.dark");
        assert_eq!(out.len(), 1);
        assert!(
            out[0].len() > 1,
            "expected more than one span for punctuation + key + value"
        );
    }

    #[test]
    fn unknown_theme_falls_back_instead_of_panicking() {
        let out = highlight_json("{}", "not-a-real-theme");
        assert_eq!(out.len(), 1);
    }
}

