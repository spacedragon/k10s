//! Lightweight, zero-dependency YAML syntax highlighting for egui.
//!
//! Designed for Kubernetes manifests and configuration files:
//! - Pure safe Rust with zero external dependencies (no regex, no C libraries,
//!   instant compilation, tiny WASM and native binary footprint).
//! - Streaming, fault-tolerant tokenization (never fails or panics on incomplete
//!   or invalid YAML during live text editing).
//! - Direct generation of [`egui::text::LayoutJob`] for [`egui::Label`] and
//!   [`egui::TextEdit::layouter`].

use egui::text::LayoutJob;
use egui::{Color32, Style, TextFormat, TextStyle};

/// Classification of YAML tokens for syntax coloring.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum YamlTokenKind {
    /// Whitespace (indentation, spaces, newlines).
    Whitespace,
    /// Comments starting with `#`.
    Comment,
    /// Document directives (`---`, `...`, `%YAML`).
    Directive,
    /// List item bullet `-`.
    ListBullet,
    /// Mapping key (e.g. `apiVersion`, `name`).
    Key,
    /// Mapping colon separator `:`.
    Colon,
    /// Quoted string literal (`"..."` or `'...'`).
    StringLiteral,
    /// Numeric scalar (integer, float, hex).
    Number,
    /// Boolean scalar (`true`, `false`, `yes`, `no`).
    Boolean,
    /// Null scalar (`null`, `~`).
    Null,
    /// Block scalar indicator (`|`, `>`, `|-`, etc.).
    BlockScalar,
    /// Anchor or alias (`&anchor`, `*alias`).
    AnchorAlias,
    /// Flow collection delimiters and punctuation (`[`, `]`, `{`, `}`, `,`).
    Punctuation,
    /// Unquoted plain scalar value.
    PlainScalar,
}

/// Token foreground color based on dark console theme with WCAG AA compliance.
#[must_use]
pub fn token_color(kind: YamlTokenKind) -> Color32 {
    match kind {
        YamlTokenKind::Whitespace => Color32::TRANSPARENT,
        // Secondary/dim comment color:
        YamlTokenKind::Comment => Color32::from_rgb(134, 141, 149),
        // Warning/directive amber:
        YamlTokenKind::Directive => Color32::from_rgb(246, 200, 95),
        // List item bullet amber:
        YamlTokenKind::ListBullet => Color32::from_rgb(246, 200, 95),
        // Accent cyan for keys:
        YamlTokenKind::Key => Color32::from_rgb(102, 194, 255),
        // Muted gray for colons:
        YamlTokenKind::Colon => Color32::from_rgb(171, 178, 186),
        // Healthy green for strings:
        YamlTokenKind::StringLiteral => Color32::from_rgb(91, 214, 156),
        // Warm orange for numbers:
        YamlTokenKind::Number => Color32::from_rgb(240, 160, 90),
        // Soft purple for booleans:
        YamlTokenKind::Boolean => Color32::from_rgb(220, 150, 230),
        // Soft red for nulls:
        YamlTokenKind::Null => Color32::from_rgb(255, 120, 130),
        // Amber for block scalars:
        YamlTokenKind::BlockScalar => Color32::from_rgb(246, 200, 95),
        // Violet for anchors and aliases:
        YamlTokenKind::AnchorAlias => Color32::from_rgb(200, 140, 240),
        // Muted text for structural brackets/commas:
        YamlTokenKind::Punctuation => Color32::from_rgb(171, 178, 186),
        // Primary text for unquoted values:
        YamlTokenKind::PlainScalar => Color32::from_rgb(229, 232, 235),
    }
}

/// Highlight YAML text into an [`egui::text::LayoutJob`].
#[must_use]
pub fn highlight_yaml(style: &Style, text: &str) -> LayoutJob {
    let font_id = TextStyle::Monospace.resolve(style);
    let mut job = LayoutJob::default();

    let tokens = tokenize_yaml(text);
    for (slice, kind) in tokens {
        let color = token_color(kind);
        job.append(
            slice,
            0.0,
            TextFormat {
                font_id: font_id.clone(),
                color,
                ..Default::default()
            },
        );
    }

    job
}

/// Tokenize full YAML text into contiguous string slices and token kinds.
///
/// Concatenating every token slice reconstructs the exact original text.
#[must_use]
pub fn tokenize_yaml(text: &str) -> Vec<(&str, YamlTokenKind)> {
    let mut tokens = Vec::new();

    for line in text.split_inclusive('\n') {
        let (content, newline) = split_newline(line);
        tokenize_line(content, &mut tokens);
        if !newline.is_empty() {
            tokens.push((newline, YamlTokenKind::Whitespace));
        }
    }

    tokens
}

fn split_newline(line: &str) -> (&str, &str) {
    if let Some(stripped) = line.strip_suffix("\r\n") {
        (stripped, &line[line.len() - 2..])
    } else if let Some(stripped) = line.strip_suffix('\n') {
        (stripped, &line[line.len() - 1..])
    } else {
        (line, "")
    }
}

fn tokenize_line<'a>(line: &'a str, tokens: &mut Vec<(&'a str, YamlTokenKind)>) {
    if line.is_empty() {
        return;
    }

    // 1. Extract leading indentation.
    let ws_len = line
        .find(|c: char| !c.is_whitespace())
        .unwrap_or(line.len());
    if ws_len > 0 {
        tokens.push((&line[..ws_len], YamlTokenKind::Whitespace));
    }
    let mut rest = &line[ws_len..];
    if rest.is_empty() {
        return;
    }

    // 2. Full-line comment.
    if rest.starts_with('#') {
        tokens.push((rest, YamlTokenKind::Comment));
        return;
    }

    // 3. Document directives and boundaries.
    if rest.starts_with("---") || rest.starts_with("...") {
        let prefix_len = 3;
        let suffix = &rest[prefix_len..];
        if suffix.is_empty() || suffix.starts_with(' ') || suffix.starts_with('\t') {
            tokens.push((&rest[..prefix_len], YamlTokenKind::Directive));
            tokenize_values(suffix, tokens);
            return;
        }
    }
    if rest.starts_with('%') {
        tokens.push((rest, YamlTokenKind::Directive));
        return;
    }

    // 4. List item bullet (`-`).
    if rest.starts_with('-')
        && (rest.len() == 1 || rest[1..].starts_with(' ') || rest[1..].starts_with('\t'))
    {
        tokens.push((&rest[..1], YamlTokenKind::ListBullet));
        rest = &rest[1..];
        let ws_after = rest
            .find(|c: char| !c.is_whitespace())
            .unwrap_or(rest.len());
        if ws_after > 0 {
            tokens.push((&rest[..ws_after], YamlTokenKind::Whitespace));
            rest = &rest[ws_after..];
        }
        if rest.is_empty() {
            return;
        }
    }

    // 5. Mapping key: check for key followed by `:`.
    if let Some((key, colon_idx)) = find_mapping_key(rest) {
        let key_trimmed = key.trim_end();
        tokens.push((key_trimmed, YamlTokenKind::Key));
        if key_trimmed.len() < key.len() {
            tokens.push((&key[key_trimmed.len()..], YamlTokenKind::Whitespace));
        }
        tokens.push((&rest[colon_idx..colon_idx + 1], YamlTokenKind::Colon));
        let after_colon = &rest[colon_idx + 1..];
        tokenize_values(after_colon, tokens);
    } else {
        // Scalar line or flow collection item.
        tokenize_values(rest, tokens);
    }
}

/// Find a mapping key in `rest`. Returns `(key_slice, colon_byte_offset)` if found.
fn find_mapping_key(rest: &str) -> Option<(&str, usize)> {
    if rest.starts_with('"') {
        let end = find_closing_quote(rest, '"')?;
        let after = &rest[end..];
        if after.starts_with(':') && is_mapping_colon_after(&after[1..]) {
            return Some((&rest[..end], end));
        }
    } else if rest.starts_with('\'') {
        let end = find_closing_single_quote(rest)?;
        let after = &rest[end..];
        if after.starts_with(':') && is_mapping_colon_after(&after[1..]) {
            return Some((&rest[..end], end));
        }
    }

    // Scan for unquoted key before `:`.
    // Must not be inside brackets/braces, and must be followed by whitespace or EOL.
    let bytes = rest.as_bytes();
    let mut bracket_depth: usize = 0;
    let mut brace_depth: usize = 0;

    for (i, &b) in bytes.iter().enumerate() {
        match b {
            b'[' => bracket_depth += 1,
            b']' => bracket_depth = bracket_depth.saturating_sub(1),
            b'{' => brace_depth += 1,
            b'}' => brace_depth = brace_depth.saturating_sub(1),
            b'#' if bracket_depth == 0 && brace_depth == 0 => {
                // Comment starts before any colon: no key here.
                return None;
            }
            b':' if bracket_depth == 0 && brace_depth == 0 => {
                let after = &rest[i + 1..];
                if is_mapping_colon_after(after) {
                    let key = &rest[..i];
                    if !key.trim().is_empty() {
                        return Some((key, i));
                    }
                }
            }
            _ => {}
        }
    }
    None
}

fn is_mapping_colon_after(after: &str) -> bool {
    after.is_empty()
        || after.starts_with(' ')
        || after.starts_with('\t')
        || after.starts_with('\r')
        || after.starts_with('\n')
}

fn find_closing_quote(s: &str, quote: char) -> Option<usize> {
    let mut chars = s.char_indices();
    chars.next(); // Skip opening quote
    let mut escaped = false;
    for (i, c) in chars {
        if escaped {
            escaped = false;
        } else if c == '\\' {
            escaped = true;
        } else if c == quote {
            return Some(i + quote.len_utf8());
        }
    }
    None
}

fn find_closing_single_quote(s: &str) -> Option<usize> {
    let mut chars = s.char_indices();
    chars.next(); // Skip opening quote
    let mut iter = chars.peekable();
    while let Some((i, c)) = iter.next() {
        if c == '\'' {
            if iter.peek().is_some_and(|(_, next)| *next == '\'') {
                iter.next(); // Skip escaped ''
            } else {
                return Some(i + 1);
            }
        }
    }
    None
}

/// Tokenize the value portion of a YAML line.
fn tokenize_values<'a>(mut rest: &'a str, tokens: &mut Vec<(&'a str, YamlTokenKind)>) {
    while !rest.is_empty() {
        // Whitespace
        let ws_len = rest
            .find(|c: char| !c.is_whitespace())
            .unwrap_or(rest.len());
        if ws_len > 0 {
            tokens.push((&rest[..ws_len], YamlTokenKind::Whitespace));
            rest = &rest[ws_len..];
            if rest.is_empty() {
                break;
            }
        }

        // Inline comment
        if rest.starts_with('#') {
            tokens.push((rest, YamlTokenKind::Comment));
            break;
        }

        // Double quoted string
        if rest.starts_with('"') {
            let end = find_closing_quote(rest, '"').unwrap_or(rest.len());
            tokens.push((&rest[..end], YamlTokenKind::StringLiteral));
            rest = &rest[end..];
            continue;
        }

        // Single quoted string
        if rest.starts_with('\'') {
            let end = find_closing_single_quote(rest).unwrap_or(rest.len());
            tokens.push((&rest[..end], YamlTokenKind::StringLiteral));
            rest = &rest[end..];
            continue;
        }

        // Anchors & aliases (&foo, *foo)
        if rest.starts_with('&') || rest.starts_with('*') {
            let end = rest
                .find(|c: char| c.is_whitespace() || matches!(c, ',' | ']' | '}' | '#' | '[' | '{'))
                .unwrap_or(rest.len());
            tokens.push((&rest[..end], YamlTokenKind::AnchorAlias));
            rest = &rest[end..];
            continue;
        }

        // Block scalars (`|`, `>`, `|-`, `|+`, `>-`, `>+`)
        if let Some(block_marker) = match_block_scalar_indicator(rest) {
            tokens.push((block_marker, YamlTokenKind::BlockScalar));
            rest = &rest[block_marker.len()..];
            continue;
        }

        // Structural punctuation in flow collections
        if rest.starts_with('[')
            || rest.starts_with(']')
            || rest.starts_with('{')
            || rest.starts_with('}')
            || rest.starts_with(',')
        {
            tokens.push((&rest[..1], YamlTokenKind::Punctuation));
            rest = &rest[1..];
            continue;
        }

        // Check if there's an embedded mapping key in flow mappings, e.g. `{name: foo}`
        if let Some((key, colon_idx)) = find_flow_key(rest) {
            let key_trimmed = key.trim_end();
            tokens.push((key_trimmed, YamlTokenKind::Key));
            if key_trimmed.len() < key.len() {
                tokens.push((&key[key_trimmed.len()..], YamlTokenKind::Whitespace));
            }
            tokens.push((&rest[colon_idx..colon_idx + 1], YamlTokenKind::Colon));
            rest = &rest[colon_idx + 1..];
            continue;
        }

        // General scalar token: extract until next delimiter
        let end = rest
            .find(|c: char| c.is_whitespace() || matches!(c, ',' | ']' | '}' | '[' | '{'))
            .unwrap_or(rest.len());
        let token = &rest[..end];

        let kind = classify_scalar(token);
        tokens.push((token, kind));
        rest = &rest[end..];
    }
}

fn match_block_scalar_indicator(s: &str) -> Option<&str> {
    for prefix in ["|-", "|+", ">-", ">+", "|", ">"] {
        if let Some(after) = s.strip_prefix(prefix)
            && (after.is_empty()
                || after.starts_with(' ')
                || after.starts_with('\t')
                || after.starts_with('#'))
        {
            return Some(&s[..prefix.len()]);
        }
    }
    None
}

fn find_flow_key(rest: &str) -> Option<(&str, usize)> {
    let end = rest.find([':', ',', ']', '}', '[', '{'])?;
    if rest.as_bytes()[end] == b':' {
        let after = &rest[end + 1..];
        if is_mapping_colon_after(after) {
            let key = &rest[..end];
            if !key.trim().is_empty() {
                return Some((key, end));
            }
        }
    }
    None
}

fn classify_scalar(token: &str) -> YamlTokenKind {
    match token {
        "true" | "false" | "True" | "False" | "TRUE" | "FALSE" | "yes" | "no" | "on" | "off" => {
            YamlTokenKind::Boolean
        }
        "null" | "Null" | "NULL" | "~" => YamlTokenKind::Null,
        _ if is_yaml_number(token) => YamlTokenKind::Number,
        _ => YamlTokenKind::PlainScalar,
    }
}

fn is_yaml_number(token: &str) -> bool {
    if token.is_empty() {
        return false;
    }
    // Hexadecimal
    if token.starts_with("0x") || token.starts_with("0X") {
        return token.len() > 2 && token[2..].chars().all(|c| c.is_ascii_hexdigit());
    }
    // Optional leading sign
    let s = token
        .strip_prefix('+')
        .or_else(|| token.strip_prefix('-'))
        .unwrap_or(token);
    if s.is_empty() {
        return false;
    }
    // Check integer or float
    let mut has_digits = false;
    let mut has_dot = false;
    let mut chars = s.chars().peekable();

    while let Some(&c) = chars.peek() {
        if c.is_ascii_digit() {
            has_digits = true;
            chars.next();
        } else if c == '.' && !has_dot {
            has_dot = true;
            chars.next();
        } else if (c == 'e' || c == 'E') && has_digits {
            chars.next();
            if chars.peek().is_some_and(|&sign| sign == '+' || sign == '-') {
                chars.next();
            }
            let mut exp_digits = false;
            for ec in chars {
                if ec.is_ascii_digit() {
                    exp_digits = true;
                } else {
                    return false;
                }
            }
            return exp_digits;
        } else {
            return false;
        }
    }

    has_digits
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn exact_text_reconstruction() {
        let manifest = r#"apiVersion: apps/v1
kind: Deployment
metadata:
  name: web-frontend # service name
  labels:
    app: "frontend"
    tier: 'web'
spec:
  replicas: 3
  paused: false
  template:
    spec:
      containers:
      - name: nginx
        image: nginx:1.25.3
        command: ["/bin/sh", "-c"]
        ports:
        - containerPort: 80
        env:
        - name: CONFIG
          value: |
            multiline
            content
---
# Next document
"#;
        let tokens = tokenize_yaml(manifest);
        let reconstructed: String = tokens.iter().map(|(s, _)| *s).collect();
        assert_eq!(reconstructed, manifest);
    }

    #[test]
    fn tokenizes_keys_and_values() {
        let input = "apiVersion: apps/v1\n";
        let tokens = tokenize_yaml(input);
        assert_eq!(
            tokens,
            vec![
                ("apiVersion", YamlTokenKind::Key),
                (":", YamlTokenKind::Colon),
                (" ", YamlTokenKind::Whitespace),
                ("apps/v1", YamlTokenKind::PlainScalar),
                ("\n", YamlTokenKind::Whitespace),
            ]
        );
    }

    #[test]
    fn tokenizes_numbers_booleans_and_comments() {
        let input = "  replicas: 3 # total pods\n  enabled: true\n";
        let tokens = tokenize_yaml(input);
        assert_eq!(
            tokens,
            vec![
                ("  ", YamlTokenKind::Whitespace),
                ("replicas", YamlTokenKind::Key),
                (":", YamlTokenKind::Colon),
                (" ", YamlTokenKind::Whitespace),
                ("3", YamlTokenKind::Number),
                (" ", YamlTokenKind::Whitespace),
                ("# total pods", YamlTokenKind::Comment),
                ("\n", YamlTokenKind::Whitespace),
                ("  ", YamlTokenKind::Whitespace),
                ("enabled", YamlTokenKind::Key),
                (":", YamlTokenKind::Colon),
                (" ", YamlTokenKind::Whitespace),
                ("true", YamlTokenKind::Boolean),
                ("\n", YamlTokenKind::Whitespace),
            ]
        );
    }

    #[test]
    fn tokenizes_url_scalar_without_false_key() {
        let input = "  image: quay.io/coreos/etcd:v3.5.9\n";
        let tokens = tokenize_yaml(input);
        assert_eq!(
            tokens,
            vec![
                ("  ", YamlTokenKind::Whitespace),
                ("image", YamlTokenKind::Key),
                (":", YamlTokenKind::Colon),
                (" ", YamlTokenKind::Whitespace),
                ("quay.io/coreos/etcd:v3.5.9", YamlTokenKind::PlainScalar),
                ("\n", YamlTokenKind::Whitespace),
            ]
        );
    }

    #[test]
    fn tokenizes_flow_lists() {
        let input = "  ports: [80, 443]\n";
        let tokens = tokenize_yaml(input);
        assert_eq!(
            tokens,
            vec![
                ("  ", YamlTokenKind::Whitespace),
                ("ports", YamlTokenKind::Key),
                (":", YamlTokenKind::Colon),
                (" ", YamlTokenKind::Whitespace),
                ("[", YamlTokenKind::Punctuation),
                ("80", YamlTokenKind::Number),
                (",", YamlTokenKind::Punctuation),
                (" ", YamlTokenKind::Whitespace),
                ("443", YamlTokenKind::Number),
                ("]", YamlTokenKind::Punctuation),
                ("\n", YamlTokenKind::Whitespace),
            ]
        );
    }

    #[test]
    fn tokenizes_list_item_with_key() {
        let input = "- name: http\n";
        let tokens = tokenize_yaml(input);
        assert_eq!(
            tokens,
            vec![
                ("-", YamlTokenKind::ListBullet),
                (" ", YamlTokenKind::Whitespace),
                ("name", YamlTokenKind::Key),
                (":", YamlTokenKind::Colon),
                (" ", YamlTokenKind::Whitespace),
                ("http", YamlTokenKind::PlainScalar),
                ("\n", YamlTokenKind::Whitespace),
            ]
        );
    }
}
