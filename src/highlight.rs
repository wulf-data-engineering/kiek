use apache_avro::types::Value as AvroValue;
use chrono::{DateTime, Datelike, NaiveDate};
use clap::builder::styling::{AnsiColor, Color, Style};
use dialoguer::theme::{ColorfulTheme, SimpleTheme, Theme};
use lazy_static::lazy_static;
use serde_json::Value as JsonValue;
use std::fmt::{Display, Formatter};

const PARTITION_COLOR_CODES: [u8; 12] = [124, 19, 29, 136, 63, 88, 27, 36, 214, 160, 69, 66];

#[derive(Clone, Debug)]
#[allow(missing_copy_implementations)]
pub struct Highlighting {
    pub colors: bool,
    pub plain: Style,
    pub bold: Style,
    pub dimmed: Style,
    pub success: Style,
    pub warning: Style,
    pub error: Style,
    pub partition_styles: Vec<Style>,
    pub key: Style,
    pub string: Style,
    pub number: Style,
    pub keyword: Style,
}

impl Highlighting {
    /// No highlighting
    pub fn plain() -> Self {
        Self {
            colors: false,
            plain: Style::new(),
            bold: Style::new(),
            dimmed: Style::new(),
            success: Style::new(),
            warning: Style::new(),
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
            colors: true,
            plain: Style::new(),
            bold: Style::new().bold(),
            dimmed: Style::new().dimmed(),
            success: Style::new()
                .fg_color(Some(Color::Ansi(AnsiColor::Green)))
                .bold(),
            warning: Style::new()
                .fg_color(Some(Color::Ansi(AnsiColor::Yellow)))
                .bold(),
            error: Style::new()
                .fg_color(Some(Color::Ansi(AnsiColor::Red)))
                .bold(),
            partition_styles: PARTITION_COLOR_CODES
                .iter()
                .map(|color| Style::new().fg_color(Some(Color::from(*color))))
                .collect(),
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

    pub fn dialoguer_theme(&self) -> Box<dyn Theme> {
        if self.colors {
            Box::new(ColorfulTheme::default())
        } else {
            Box::new(SimpleTheme)
        }
    }
}

///
/// Formats a JSON value to a string with syntax highlighting.
///
pub(crate) fn write_json_value(
    f: &mut Formatter<'_>,
    value: &JsonValue,
    highlighting: &Highlighting,
) -> std::fmt::Result {
    match value {
        JsonValue::Null => write_null(f, highlighting),
        JsonValue::Bool(boolean) => write_keyword(f, *boolean, highlighting),
        JsonValue::Number(number) => write_number(f, number, highlighting),
        JsonValue::String(string) => write_string_value(f, string, highlighting),
        JsonValue::Array(values) => write_array(f, values.iter(), write_json_value, highlighting),
        JsonValue::Object(entries) => write_map(f, entries.iter(), write_json_value, highlighting),
    }
}

///
/// Formats an AVRO value to a string with syntax highlighting.
///
pub(crate) fn write_avro_value(
    f: &mut Formatter<'_>,
    value: &AvroValue,
    highlighting: &Highlighting,
) -> std::fmt::Result {
    match value {
        AvroValue::Record(record) => write_map(
            f,
            record.iter().map(|(k, v)| (k, v)),
            write_avro_value,
            highlighting,
        ),
        AvroValue::Map(map) => write_map(f, map.iter(), write_avro_value, highlighting),
        AvroValue::Enum(_, symbol) => write_string_value(f, symbol, highlighting),
        AvroValue::Union(_, value) => write_avro_value(f, value, highlighting),
        AvroValue::Array(array) => write_array(f, array.iter(), write_avro_value, highlighting),
        AvroValue::Fixed(_, bytes) => write_string_value(f, format!("{:?}", bytes), highlighting),
        AvroValue::String(string) => write_string_value(f, string, highlighting),
        AvroValue::Bytes(bytes) => write_string_value(f, format!("{:?}", bytes), highlighting),
        AvroValue::Int(int) => write_number(f, int, highlighting),
        AvroValue::Long(long) => write_number(f, long, highlighting),
        AvroValue::Float(float) => write_number(f, float, highlighting),
        AvroValue::Double(double) => write_number(f, double, highlighting),
        AvroValue::BigDecimal(big_decimal) => write_number(f, big_decimal, highlighting),
        AvroValue::Boolean(boolean) => write_keyword(f, boolean, highlighting),
        AvroValue::Null => write_keyword(f, "null", highlighting),
        AvroValue::Date(date) => write_string_value(
            f,
            NaiveDate::from_num_days_from_ce_opt(*EPOCH_DAYS_OFFSET + date)
                .unwrap()
                .to_string(),
            highlighting,
        ),
        AvroValue::Decimal(decimal) => {
            write_string_value(f, format!("{:?}", decimal), highlighting)
        }
        AvroValue::TimeMillis(ms) => write_string_value(f, format!("{ms}ms"), highlighting),
        AvroValue::TimeMicros(micros) => write_string_value(f, format!("{micros}µs"), highlighting),
        AvroValue::TimestampMillis(ts) => write_string_value(
            f,
            DateTime::from_timestamp_millis(*ts).unwrap().to_string(),
            highlighting,
        ),
        AvroValue::TimestampMicros(ts) => write_string_value(
            f,
            DateTime::from_timestamp_micros(*ts).unwrap().to_string(),
            highlighting,
        ),
        AvroValue::TimestampNanos(ts) => write_string_value(
            f,
            DateTime::from_timestamp_nanos(*ts).to_string(),
            highlighting,
        ),
        AvroValue::LocalTimestampMillis(ts) => write_string_value(
            f,
            DateTime::from_timestamp_millis(*ts).unwrap().to_string(),
            highlighting,
        ),
        AvroValue::LocalTimestampMicros(ts) => write_string_value(
            f,
            DateTime::from_timestamp_micros(*ts).unwrap().to_string(),
            highlighting,
        ),
        AvroValue::LocalTimestampNanos(ts) => write_string_value(
            f,
            DateTime::from_timestamp_nanos(*ts).to_string(),
            highlighting,
        ),
        AvroValue::Duration(duration) => {
            write_string_value(f, format!("{duration:?}"), highlighting)
        }
        AvroValue::Uuid(uuid) => write_string_value(f, uuid.to_string(), highlighting),
    }
}

fn write_key(f: &mut Formatter<'_>, key: &str, highlighting: &Highlighting) -> std::fmt::Result {
    write!(f, "{style}\"{key}\"{style:#}", style = highlighting.key)
}

fn write_string_value<S: Into<String>>(
    f: &mut Formatter<'_>,
    value: S,
    highlighting: &Highlighting,
) -> std::fmt::Result {
    write!(
        f,
        "{style}\"{value}\"{style:#}",
        value = value.into(),
        style = highlighting.string
    )
}

fn write_number<V: Display>(
    f: &mut Formatter<'_>,
    value: V,
    highlighting: &Highlighting,
) -> std::fmt::Result {
    write!(f, "{style}{value}{style:#}", style = highlighting.number)
}

fn write_keyword<V: ToString>(
    f: &mut Formatter<'_>,
    value: V,
    highlighting: &Highlighting,
) -> std::fmt::Result {
    write!(
        f,
        "{style}{value}{style:#}",
        value = value.to_string(),
        style = highlighting.keyword
    )
}

pub(crate) fn write_null(f: &mut Formatter<'_>, highlighting: &Highlighting) -> std::fmt::Result {
    write!(f, "{style}null{style:#}", style = highlighting.keyword)
}

fn write_entries<'a, V: 'a, I, F>(
    f: &mut Formatter<'_>,
    entries: I,
    prefix: &str,
    mut write_entry: F,
    suffix: &str,
    highlighting: &Highlighting,
) -> std::fmt::Result
where
    I: Iterator<Item = V>,
    F: FnMut(&mut Formatter<'_>, &V, &Highlighting) -> std::fmt::Result,
{
    f.write_str(prefix)?;
    let mut first = true;
    for entry in entries {
        if first {
            first = false;
        } else {
            f.write_str(",")?
        }
        write_entry(f, &entry, highlighting)?
    }
    f.write_str(suffix)
}

fn write_array<'a, V: 'a, I, F>(
    f: &mut Formatter<'_>,
    values: I,
    mut write_value: F,
    highlighting: &Highlighting,
) -> std::fmt::Result
where
    I: Iterator<Item = &'a V>,
    F: FnMut(&mut Formatter<'_>, &V, &Highlighting) -> std::fmt::Result,
{
    write_entries(
        f,
        values,
        "[",
        |f, value, _| write_value(f, *value, highlighting),
        "]",
        highlighting,
    )
}

fn write_map<'a, V: 'a, I, F>(
    f: &mut Formatter<'_>,
    entries: I,
    mut write_value: F,
    highlighting: &Highlighting,
) -> std::fmt::Result
where
    I: Iterator<Item = (&'a String, &'a V)>,
    F: FnMut(&mut Formatter<'_>, &V, &Highlighting) -> std::fmt::Result,
{
    write_entries(
        f,
        entries,
        "{",
        |f, (key, value), _| {
            write_key(f, key, highlighting)?;
            f.write_str(":")?;
            write_value(f, value, highlighting)
        },
        "}",
        highlighting,
    )
}

lazy_static! {
    /// Number of days between the proleptic Gregorian calendar and the Unix epoch
    static ref EPOCH_DAYS_OFFSET: i32 = NaiveDate::from_ymd_opt(1970, 1, 1).unwrap().num_days_from_ce();
}

#[cfg(test)]
mod tests {
    use super::*;
    use apache_avro::{Days, Duration, Millis, Months};
    use std::str::FromStr;
    struct JsonFormatting {
        value: JsonValue,
        highlighting: Highlighting,
    }

    impl Display for JsonFormatting {
        fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
            write_json_value(f, &self.value, &self.highlighting)
        }
    }

    struct AvroFormatting {
        value: AvroValue,
        highlighting: Highlighting,
    }

    impl Display for AvroFormatting {
        fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
            write_avro_value(f, &self.value, &self.highlighting)
        }
    }

    #[test]
    fn test_format_json_value() {
        let value = JsonValue::Object(serde_json::Map::from_iter(vec![
            (
                "name".to_string(),
                JsonValue::String("Jane Doe".to_string()),
            ),
            ("age".to_string(), JsonValue::Number(25.into())),
            (
                "city".to_string(),
                JsonValue::String("San Francisco".to_string()),
            ),
        ]));

        let formatted = format!(
            "{}",
            JsonFormatting {
                value: value.clone(),
                highlighting: Highlighting::plain()
            }
        );
        assert_eq!(
            formatted,
            "{\"age\":25,\"city\":\"San Francisco\",\"name\":\"Jane Doe\"}"
        );

        // make sure the last ANSI code is a reset code
        let formatted = format!(
            "{}",
            JsonFormatting {
                value: value.clone(),
                highlighting: Highlighting::colors()
            }
        );
        let last_reset_index = formatted.rfind("\u{001B}[0m").unwrap();
        let last_ansi_index = formatted.rfind("\u{001B}").unwrap();
        assert_eq!(last_reset_index, last_ansi_index);
    }

    #[test]
    fn test_format_avro_value() {
        let value = AvroValue::Record(vec![
            (
                "name".to_string(),
                AvroValue::String("Jane Doe".to_string()),
            ),
            ("age".to_string(), AvroValue::Int(25)),
            (
                "city".to_string(),
                AvroValue::String("San Francisco".to_string()),
            ),
        ]);

        let formatted = format!(
            "{}",
            AvroFormatting {
                value: value.clone(),
                highlighting: Highlighting::plain()
            }
        );
        assert_eq!(
            formatted,
            "{\"name\":\"Jane Doe\",\"age\":25,\"city\":\"San Francisco\"}"
        );

        // make sure the last ANSI code is a reset code
        let formatted = format!(
            "{}",
            AvroFormatting {
                value: value.clone(),
                highlighting: Highlighting::colors()
            }
        );
        let last_reset_index = formatted.rfind("\u{001B}[0m").unwrap();
        let last_ansi_index = formatted.rfind("\u{001B}").unwrap();
        assert_eq!(last_reset_index, last_ansi_index);

        let value =
            AvroValue::BigDecimal(bigdecimal::BigDecimal::from_str("1.23000000004").unwrap());
        let formatted = format!(
            "{}",
            AvroFormatting {
                value: value.clone(),
                highlighting: Highlighting::plain()
            }
        );
        assert_eq!(formatted, "1.23000000004");

        let value = AvroValue::Date(4);
        let formatted = format!(
            "{}",
            AvroFormatting {
                value: value.clone(),
                highlighting: Highlighting::plain()
            }
        );
        assert_eq!(formatted, "\"1970-01-05\"");

        let value = AvroValue::TimeMillis(1234567890);
        let formatted = format!(
            "{}",
            AvroFormatting {
                value: value.clone(),
                highlighting: Highlighting::plain()
            }
        );
        assert_eq!(formatted, "\"1234567890ms\"");

        let value = AvroValue::TimeMicros(1234567890);
        let formatted = format!(
            "{}",
            AvroFormatting {
                value: value.clone(),
                highlighting: Highlighting::plain()
            }
        );
        assert_eq!(formatted, "\"1234567890µs\"");

        let value = AvroValue::TimestampMillis(1234567890);
        let formatted = format!(
            "{}",
            AvroFormatting {
                value: value.clone(),
                highlighting: Highlighting::plain()
            }
        );
        assert_eq!(formatted, "\"1970-01-15 06:56:07.890 UTC\"");

        let value = AvroValue::TimestampMicros(1234567890000);
        let formatted = format!(
            "{}",
            AvroFormatting {
                value: value.clone(),
                highlighting: Highlighting::plain()
            }
        );
        assert_eq!(formatted, "\"1970-01-15 06:56:07.890 UTC\"");

        let value = AvroValue::TimestampMicros(1234567890001);
        let formatted = format!(
            "{}",
            AvroFormatting {
                value: value.clone(),
                highlighting: Highlighting::plain()
            }
        );
        assert_eq!(formatted, "\"1970-01-15 06:56:07.890001 UTC\"");

        let value =
            AvroValue::Duration(Duration::new(Months::new(1), Days::new(2), Millis::new(3)));
        let formatted = format!(
            "{}",
            AvroFormatting {
                value: value.clone(),
                highlighting: Highlighting::plain()
            }
        );
        assert_eq!(
            formatted,
            "\"Duration { months: Months(1), days: Days(2), millis: Millis(3) }\""
        );
    }
}
