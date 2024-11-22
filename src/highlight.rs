use avro_rs::types::Value as AvroValue;
use chrono::{DateTime, Datelike, NaiveDate};
use clap::builder::styling::{AnsiColor, Color, Style};
use lazy_static::lazy_static;
use serde_json::Value as JsonValue;
use std::fmt::Display;

const PARTITION_COLOR_CODES: [u8; 12] = [124, 19, 29, 136, 63, 88, 27, 36, 214, 160, 69, 66];

impl Highlighting {
    /// No highlighting
    pub fn plain() -> Self {
        Self {
            plain: Style::new(),
            bold: Style::new(),
            dimmed: Style::new(),
            success: Style::new(),
            error: Style::new(),
            partition_styles: vec![Style::new()],
            key: Style::new(),
            string: Style::new(),
            number: Style::new(),
            keyword: Style::new(),
        }
    }

    /// Color highlighting
    pub fn colors() -> Self {
        Self {
            plain: Style::new(),
            bold: Style::new().bold(),
            dimmed: Style::new().dimmed(),
            success: Style::new().fg_color(Some(Color::from(22))).bold(),
            error: Style::new().fg_color(Some(Color::Ansi(AnsiColor::Red))).bold(),
            partition_styles: PARTITION_COLOR_CODES.iter().map(|color| Style::new().fg_color(Some(Color::from(*color)))).collect(),
            // These are close to the colors of the IntelliJ IDEA JSON viewer
            key: Style::new().fg_color(Some(Color::from(54))),
            string: Style::new().fg_color(Some(Color::from(22))),
            number: Style::new().fg_color(Some(Color::from(19))),
            keyword: Style::new().fg_color(Some(Color::from(18))),
        }
    }

    pub fn partition(&self, index: i32) -> &Style {
        &self.partition_styles[index as usize % self.partition_styles.len()]
    }
}

pub fn format_null(highlighting: &Highlighting) -> String {
    format!("{style}null{style:#}", style = highlighting.keyword)
}

fn push_key(result: &mut String, key: &str, highlighting: &Highlighting) {
    result.push_str(&format!("{style}\"{key}\"{style:#}", style = highlighting.key));
}

fn push_string_value<S: Into<String>>(result: &mut String, value: S, highlighting: &Highlighting) {
    result.push_str(&format!("{style}\"{value}\"{style:#}", value = value.into(), style = highlighting.string));
}

fn push_number<V: Display>(result: &mut String, value: V, highlighting: &Highlighting) {
    result.push_str(&format!("{style}{value}{style:#}", style = highlighting.number));
}

fn push_keyword<V: ToString>(result: &mut String, value: V, highlighting: &Highlighting) {
    result.push_str(&format!("{style}{value}{style:#}", value = value.to_string(), style = highlighting.keyword));
}

fn push_null(result: &mut String, highlighting: &Highlighting) {
    result.push_str(&format_null(highlighting));
}

fn push_entries<'a, V: 'a, I, F>(result: &mut String, entries: I, prefix: &str, push_entry: F, suffix: &str, highlighting: &Highlighting)
where
    I: Iterator<Item=V>,
    F: Fn(&mut String, &V, &Highlighting),
{
    result.push_str(prefix);
    let mut first = true;
    for entry in entries {
        if first {
            first = false;
        } else {
            result.push_str(",");
        }
        push_entry(result, &entry, highlighting);
    }
    result.push_str(suffix);
}

fn push_array<'a, V: 'a, I, F>(result: &mut String, values: I, push_value: F, highlighting: &Highlighting)
where
    I: Iterator<Item=&'a V>,
    F: Fn(&mut String, &V, &Highlighting),
{
    push_entries(result, values, "[", |result, value, _| push_value(result, *value, highlighting), "]", highlighting);
}

fn push_map<'a, V: 'a, I, F>(result: &mut String, entries: I, push_value: F, highlighting: &Highlighting)
where
    I: Iterator<Item=(&'a String, &'a V)>,
    F: Fn(&mut String, &V, &Highlighting),
{
    push_entries(result, entries, "{", |result, (key, value), _| {
        push_key(result, key, highlighting);
        result.push_str(":");
        push_value(result, value, highlighting);
    }, "}", highlighting);
}

///
/// Formats a JSON value to a string with syntax highlighting.
///
pub fn format_json_value(value: &JsonValue, highlighting: &Highlighting) -> String {
    let mut result = String::with_capacity(1024);
    push_json_value(&mut result, value, highlighting);
    result
}

fn push_json_value(result: &mut String, value: &JsonValue, highlighting: &Highlighting) {
    match value {
        JsonValue::Null => push_null(result, highlighting),
        JsonValue::Bool(boolean) => push_keyword(result, *boolean, highlighting),
        JsonValue::Number(number) => push_number(result, number, highlighting),
        JsonValue::String(string) => push_string_value(result, string, highlighting),
        JsonValue::Array(values) => push_array(result, values.iter(), push_json_value, highlighting),
        JsonValue::Object(entries) => push_map(result, entries.iter(), push_json_value, highlighting),
    }
}

///
/// Formats an AVRO value to a string with optional syntax highlighting.
///
pub fn format_avro_value(value: &AvroValue, highlighting: &Highlighting) -> String {
    let mut result = String::with_capacity(1024);
    push_avro_value(&mut result, value, highlighting);
    result
}

lazy_static! {
    /// Number of days between the proleptic Gregorian calendar and the Unix epoch
    static ref EPOCH_DAYS_OFFSET: i32 = NaiveDate::from_ymd_opt(1970, 1, 1).unwrap().num_days_from_ce();
}

fn push_avro_value(result: &mut String, value: &AvroValue, highlighting: &Highlighting) {
    match value {
        AvroValue::Record(record) => push_map(result, record.iter().map(|(k, v)| (k, v)), push_avro_value, highlighting),
        AvroValue::Map(map) => push_map(result, map.iter(), push_avro_value, highlighting),
        AvroValue::Enum(_, symbol) => push_string_value(result, symbol, highlighting),
        AvroValue::Union(value) => push_avro_value(result, value, highlighting),
        AvroValue::Array(array) => push_array(result, array.iter(), push_avro_value, highlighting),
        AvroValue::Fixed(_, bytes) => push_string_value(result, format!("{:?}", bytes), highlighting),
        AvroValue::String(string) => push_string_value(result, string, highlighting),
        AvroValue::Bytes(bytes) => push_string_value(result, format!("{:?}", bytes), highlighting),
        AvroValue::Int(int) => push_number(result, int, highlighting),
        AvroValue::Long(long) => push_number(result, long, highlighting),
        AvroValue::Float(float) => push_number(result, float, highlighting),
        AvroValue::Double(double) => push_number(result, double, highlighting),
        AvroValue::Boolean(boolean) => push_keyword(result, boolean, highlighting),
        AvroValue::Null => push_keyword(result, "null", highlighting),
        AvroValue::Date(date) => push_string_value(result, NaiveDate::from_num_days_from_ce_opt(*EPOCH_DAYS_OFFSET + date).unwrap().to_string(), highlighting),
        AvroValue::Decimal(decimal) => push_string_value(result, format!("{:?}", decimal), highlighting),
        AvroValue::TimeMillis(ms) => push_string_value(result, format!("{ms}ms"), highlighting),
        AvroValue::TimeMicros(micros) => push_string_value(result, format!("{micros}µs"), highlighting),
        AvroValue::TimestampMillis(ts) => push_string_value(result, DateTime::from_timestamp_millis(*ts).unwrap().to_string(), highlighting),
        AvroValue::TimestampMicros(ts) => push_string_value(result, DateTime::from_timestamp_micros(*ts).unwrap().to_string(), highlighting),
        AvroValue::Duration(duration) => push_string_value(result, format!("{:?}m{:?}d{:?}ms", duration.months(), duration.days(), duration.millis()), highlighting),
        AvroValue::Uuid(uuid) => push_string_value(result, uuid.to_string(), highlighting),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_format_json_value() {
        let value = JsonValue::Object(serde_json::Map::from_iter(vec![
            ("name".to_string(), JsonValue::String("Jane Doe".to_string())),
            ("age".to_string(), JsonValue::Number(25.into())),
            ("city".to_string(), JsonValue::String("San Francisco".to_string())),
        ]
        ));
        let formatted = format_json_value(&value, &Highlighting::plain());
        assert_eq!(formatted, "{\"age\":25,\"city\":\"San Francisco\",\"name\":\"Jane Doe\"}");

        // make sure the last ANSI code is a reset code
        let formatted = format_json_value(&value, &Highlighting::colors());
        let last_reset_index = formatted.rfind("\u{001B}[0m").unwrap();
        let last_ansi_index = formatted.rfind("\u{001B}").unwrap();
        assert_eq!(last_reset_index, last_ansi_index);
    }

    #[test]
    fn test_format_avro_value() {
        let value = AvroValue::Record(vec![
            ("name".to_string(), AvroValue::String("Jane Doe".to_string())),
            ("age".to_string(), AvroValue::Int(25)),
            ("city".to_string(), AvroValue::String("San Francisco".to_string())),
        ]);

        let formatted = format_avro_value(&value, &Highlighting::plain());
        assert_eq!(formatted, "{\"name\":\"Jane Doe\",\"age\":25,\"city\":\"San Francisco\"}");

        // make sure the last ANSI code is a reset code
        let formatted = format_avro_value(&value, &Highlighting::colors());
        let last_reset_index = formatted.rfind("\u{001B}[0m").unwrap();
        let last_ansi_index = formatted.rfind("\u{001B}").unwrap();
        assert_eq!(last_reset_index, last_ansi_index);
    }
}


#[derive(Clone, Debug)]
#[allow(missing_copy_implementations)]
pub struct Highlighting {
    pub plain: Style,
    pub bold: Style,
    pub dimmed: Style,
    pub success: Style,
    pub error: Style,
    pub partition_styles: Vec<Style>,
    pub key: Style,
    pub string: Style,
    pub number: Style,
    pub keyword: Style,
}
