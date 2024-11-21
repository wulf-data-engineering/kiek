use serde_json::{Value as JsonValue};
use avro_rs::types::{Value as AvroValue};
use std::fmt::Display;
use chrono::{DateTime, Datelike, NaiveDate};
use lazy_static::lazy_static;

pub const RESET: &str = "\u{001B}[0m";

pub const BOLD: &str = "\u{001B}[1m";

pub const SEPARATOR: &str = "\u{001B}[38;5;248m";

pub const GREY: &str = "\u{001B}[38;5;244m";

const PARTITION_COLOR_CODES: [i32; 12] = [124, 19, 29, 136, 63, 88, 27, 36, 214, 160, 69, 66];

lazy_static! {

    static ref PARTITION_COLORS: Vec<String> =
        PARTITION_COLOR_CODES.iter().map(|color| format!("\u{001B}[38;5;{}m", color)).collect();

    static ref PARTITION_COLORS_BOLD: Vec<String> =
        PARTITION_COLOR_CODES.iter().map(|color| format!("\u{001B}[38;5;{};1m", color)).collect();
}

pub fn partition_color(index: i32) -> &'static str {
    &PARTITION_COLORS[index as usize % PARTITION_COLORS.len()]
}

pub fn partition_color_bold(index: i32) -> &'static str {
    &PARTITION_COLORS_BOLD[index as usize % PARTITION_COLORS_BOLD.len()]
}

pub fn format<S: Into<String>>(value: S, code: &str, highlight: bool) -> String {
    if highlight {
        format!("{code}{value}{RESET}", value = value.into())
    } else {
        value.into()
    }
}

pub fn format_null(highlight: bool) -> String {
    if highlight {
        NULL.to_string()
    } else {
        "null".to_string()
    }
}

// These are close to the colors of the IntelliJ IDEA JSON viewer
const KEY: &str = "\u{001B}[38;5;54m"; // 90
const STRING: &str = "\u{001B}[38;5;22m"; // 28
const NUMBER: &str = "\u{001B}[38;5;19m"; // 20
const KEYWORD: &str = "\u{001B}[38;5;18m"; // 19

const NULL: &str = "\u{001B}[38;5;18mnull\u{001B}[0m"; // KEYWORD

fn push(result: &mut String, value: &str, code: &str, highlight: bool) {
    result.push_str(&format(value, code, highlight));
}

fn push_key(result: &mut String, key: &str, highlight: bool) {
    push(result, &format!("\"{key}\""), KEY, highlight);
}

fn push_string_value<S: Into<String>>(result: &mut String, value: S, highlight: bool) {
    push(result, &format!("\"{}\"", value.into()), STRING, highlight);
}

fn push_number<V: Display>(result: &mut String, value: V, highlight: bool) {
    push(result, &value.to_string(), NUMBER, highlight);
}

fn push_keyword<V: ToString>(result: &mut String, value: V, highlight: bool) {
    push(result, &value.to_string(), KEYWORD, highlight);
}

fn push_null(result: &mut String, highlight: bool) {
    if highlight {
        result.push_str(NULL);
    } else {
        result.push_str("null");
    }
}

fn push_entries<'a, V: 'a, I, F>(result: &mut String, entries: I, prefix: &str, push_entry: F, suffix: &str, highlight: bool)
where
    I: Iterator<Item=V>,
    F: Fn(&mut String, &V, bool),
{
    result.push_str(prefix);
    let mut first = true;
    for entry in entries {
        if first {
            first = false;
        } else {
            result.push_str(",");
        }
        push_entry(result, &entry, highlight);
    }
    result.push_str(suffix);
}

fn push_array<'a, V: 'a, I, F>(result: &mut String, values: I, push_value: F, highlight: bool)
where
    I: Iterator<Item=&'a V>,
    F: Fn(&mut String, &V, bool),
{
    push_entries(result, values, "[", |result, value, _| push_value(result, *value, highlight), "]", highlight);
}

fn push_map<'a, V: 'a, I, F>(result: &mut String, entries: I, push_value: F, highlight: bool)
where
    I: Iterator<Item=(&'a String, &'a V)>,
    F: Fn(&mut String, &V, bool),
{
    push_entries(result, entries, "{", |result, (key, value), _| {
        push_key(result, key, highlight);
        result.push_str(":");
        push_value(result, value, highlight);
    }, "}", highlight);
}

///
/// Formats a JSON value to a string with syntax highlighting.
///
pub fn format_json_value(value: &JsonValue, highlight: bool) -> String {
    let mut result = String::with_capacity(1024);
    push_json_value(&mut result, value, highlight);
    result
}

fn push_json_value(result: &mut String, value: &JsonValue, highlight: bool) {
    match value {
        JsonValue::Null => push_null(result, highlight),
        JsonValue::Bool(boolean) => push_keyword(result, *boolean, highlight),
        JsonValue::Number(number) => push_number(result, number, highlight),
        JsonValue::String(string) => push_string_value(result, string, highlight),
        JsonValue::Array(values) => push_array(result, values.iter(), push_json_value, highlight),
        JsonValue::Object(entries) => push_map(result, entries.iter(), push_json_value, highlight),
    }
}

///
/// Formats an AVRO value to a string with optional syntax highlighting.
///
pub fn format_avro_value(value: &AvroValue, highlight: bool) -> String {
    let mut result = String::with_capacity(1024);
    push_avro_value(&mut result, value, highlight);
    result
}

lazy_static! {
    /// Number of days between the proleptic Gregorian calendar and the Unix epoch
    static ref EPOCH_DAYS_OFFSET: i32 = NaiveDate::from_ymd_opt(1970, 1, 1).unwrap().num_days_from_ce();
}

fn push_avro_value(result: &mut String, value: &AvroValue, highlight: bool) {
    match value {
        AvroValue::Record(record) => push_map(result, record.iter().map(|(k, v)| (k, v)), push_avro_value, highlight),
        AvroValue::Map(map) => push_map(result, map.iter(), push_avro_value, highlight),
        AvroValue::Enum(_, symbol) => push_string_value(result, symbol, highlight),
        AvroValue::Union(value) => push_avro_value(result, value, highlight),
        AvroValue::Array(array) => push_array(result, array.iter(), push_avro_value, highlight),
        AvroValue::Fixed(_, bytes) => push_string_value(result, format!("{:?}", bytes), highlight),
        AvroValue::String(string) => push_string_value(result, string, highlight),
        AvroValue::Bytes(bytes) => push_string_value(result, format!("{:?}", bytes), highlight),
        AvroValue::Int(int) => push_number(result, int, highlight),
        AvroValue::Long(long) => push_number(result, long, highlight),
        AvroValue::Float(float) => push_number(result, float, highlight),
        AvroValue::Double(double) => push_number(result, double, highlight),
        AvroValue::Boolean(boolean) => push_keyword(result, boolean, highlight),
        AvroValue::Null => push_keyword(result, "null", highlight),
        AvroValue::Date(date) => push_string_value(result, NaiveDate::from_num_days_from_ce_opt(*EPOCH_DAYS_OFFSET + date).unwrap().to_string(), highlight),
        AvroValue::Decimal(decimal) => push_string_value(result, format!("{:?}", decimal), highlight),
        AvroValue::TimeMillis(ms) => push_string_value(result, format!("{ms}ms"), highlight),
        AvroValue::TimeMicros(micros) => push_string_value(result, format!("{micros}µs"), highlight),
        AvroValue::TimestampMillis(ts) => push_string_value(result, DateTime::from_timestamp_millis(*ts).unwrap().to_string(), highlight),
        AvroValue::TimestampMicros(ts) => push_string_value(result, DateTime::from_timestamp_micros(*ts).unwrap().to_string(), highlight),
        AvroValue::Duration(duration) => push_string_value(result, format!("{:?}m{:?}d{:?}ms", duration.months(), duration.days(), duration.millis()), highlight),
        AvroValue::Uuid(uuid) => push_string_value(result, uuid.to_string(), highlight),
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
        let formatted = format_json_value(&value, false);
        assert_eq!(formatted, "{\"age\":25,\"city\":\"San Francisco\",\"name\":\"Jane Doe\"}");

        // make sure the last ANSI code is a reset code
        let formatted = format_json_value(&value, true);
        let last_reset_index = formatted.rfind(RESET).unwrap();
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

        let formatted = format_avro_value(&value, false);
        assert_eq!(formatted, "{\"name\":\"Jane Doe\",\"age\":25,\"city\":\"San Francisco\"}");

        // make sure the last ANSI code is a reset code
        let formatted = format_avro_value(&value, true);
        let last_reset_index = formatted.rfind(RESET).unwrap();
        let last_ansi_index = formatted.rfind("\u{001B}").unwrap();
        assert_eq!(last_reset_index, last_ansi_index);
    }
}

