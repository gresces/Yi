//! Zero-dependency Rust client for the Yi event service.
//!
//! Everything lives in this single file: a small JSON value/parser/serializer,
//! an HTTP/1.1 client over `std::net::TcpStream`, the event models, the
//! producer and the pull-based consumer.
//!
//! The Yi service speaks plain HTTP. This client intentionally supports only
//! `http://` URLs; terminate TLS in a proxy if you need it.
//!
//! ```no_run
//! use yi::{Client, Consumer};
//!
//! # fn main() -> Result<(), yi::Error> {
//! let client = Client::new("http://127.0.0.1:8081")?;
//! client.capture("随手记一句")?;
//!
//! let consumer = Consumer::new(&client, "classifier", ["raw.captured"])?;
//! while let Some(message) = consumer.next()? {
//!     message.ack()?;
//! }
//! # Ok(())
//! # }
//! ```

#![forbid(unsafe_code)]

use std::cell::Cell;
use std::collections::BTreeMap;
use std::fmt;
use std::io::{self, Read, Write};
use std::marker::PhantomData;
use std::net::{TcpStream, ToSocketAddrs};
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

pub const VERSION: &str = "0.1.1";
pub const DEFAULT_BASE_URL: &str = "http://127.0.0.1:8081";
pub const DEFAULT_TIMEOUT: Duration = Duration::from_secs(10);
pub const DEFAULT_POLL_TIMEOUT: Duration = Duration::from_secs(25);

// ---- errors ---------------------------------------------------------------

#[derive(Debug)]
pub enum Error {
    Io(io::Error),
    InvalidUrl(String),
    Protocol(String),
    Json(String),
    Http(HttpError),
    Conflict(HttpError),
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct HttpError {
    pub status: u16,
    pub body: String,
    pub message: String,
}

impl Error {
    pub fn status(&self) -> Option<u16> {
        match self {
            Error::Http(error) | Error::Conflict(error) => Some(error.status),
            _ => None,
        }
    }

    pub fn is_conflict(&self) -> bool {
        matches!(self, Error::Conflict(_))
    }

    pub fn http_error(&self) -> Option<&HttpError> {
        match self {
            Error::Http(error) | Error::Conflict(error) => Some(error),
            _ => None,
        }
    }
}

impl fmt::Display for Error {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Error::Io(error) => write!(formatter, "io error: {error}"),
            Error::InvalidUrl(message) => write!(formatter, "invalid URL: {message}"),
            Error::Protocol(message) => write!(formatter, "protocol error: {message}"),
            Error::Json(message) => write!(formatter, "JSON error: {message}"),
            Error::Http(error) | Error::Conflict(error) => write!(formatter, "{error}"),
        }
    }
}

impl std::error::Error for Error {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Error::Io(error) => Some(error),
            _ => None,
        }
    }
}

impl From<io::Error> for Error {
    fn from(error: io::Error) -> Self {
        Error::Io(error)
    }
}

impl fmt::Display for HttpError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(&self.message)
    }
}

// ---- JSON -----------------------------------------------------------------

#[derive(Clone, Copy, Debug, PartialEq)]
pub enum JsonNumber {
    Int(i64),
    UInt(u64),
    Float(f64),
}

#[derive(Clone, Debug, PartialEq)]
pub enum Json {
    Null,
    Bool(bool),
    Number(JsonNumber),
    String(String),
    Array(Vec<Json>),
    Object(BTreeMap<String, Json>),
}

impl Json {
    pub fn parse(input: &str) -> Result<Self, Error> {
        Parser::new(input).parse_document()
    }

    pub fn object() -> Self {
        Json::Object(BTreeMap::new())
    }

    pub fn array() -> Self {
        Json::Array(Vec::new())
    }

    pub fn from_pairs<I, K, V>(pairs: I) -> Self
    where
        I: IntoIterator<Item = (K, V)>,
        K: Into<String>,
        V: Into<Json>,
    {
        let mut object = BTreeMap::new();
        for (key, value) in pairs {
            object.insert(key.into(), value.into());
        }
        Json::Object(object)
    }

    pub fn get(&self, key: &str) -> Option<&Json> {
        match self {
            Json::Object(object) => object.get(key),
            _ => None,
        }
    }

    pub fn insert<K, V>(&mut self, key: K, value: V) -> Option<Json>
    where
        K: Into<String>,
        V: Into<Json>,
    {
        match self {
            Json::Object(object) => object.insert(key.into(), value.into()),
            _ => None,
        }
    }

    pub fn push<V: Into<Json>>(&mut self, value: V) -> bool {
        match self {
            Json::Array(items) => {
                items.push(value.into());
                true
            }
            _ => false,
        }
    }

    pub fn as_str(&self) -> Option<&str> {
        match self {
            Json::String(value) => Some(value),
            _ => None,
        }
    }

    pub fn as_bool(&self) -> Option<bool> {
        match self {
            Json::Bool(value) => Some(*value),
            _ => None,
        }
    }

    pub fn as_i64(&self) -> Option<i64> {
        match self {
            Json::Number(JsonNumber::Int(value)) => Some(*value),
            Json::Number(JsonNumber::UInt(value)) => i64::try_from(*value).ok(),
            _ => None,
        }
    }

    pub fn as_u64(&self) -> Option<u64> {
        match self {
            Json::Number(JsonNumber::UInt(value)) => Some(*value),
            Json::Number(JsonNumber::Int(value)) => u64::try_from(*value).ok(),
            _ => None,
        }
    }

    pub fn as_f64(&self) -> Option<f64> {
        match self {
            Json::Number(JsonNumber::Int(value)) => Some(*value as f64),
            Json::Number(JsonNumber::UInt(value)) => Some(*value as f64),
            Json::Number(JsonNumber::Float(value)) => Some(*value),
            _ => None,
        }
    }

    pub fn as_array(&self) -> Option<&[Json]> {
        match self {
            Json::Array(items) => Some(items),
            _ => None,
        }
    }

    pub fn as_object(&self) -> Option<&BTreeMap<String, Json>> {
        match self {
            Json::Object(object) => Some(object),
            _ => None,
        }
    }

    pub fn is_null(&self) -> bool {
        matches!(self, Json::Null)
    }

    pub fn is_array(&self) -> bool {
        matches!(self, Json::Array(_))
    }

    pub fn is_object(&self) -> bool {
        matches!(self, Json::Object(_))
    }

    pub fn dump(&self) -> String {
        let mut output = String::new();
        write_json(self, &mut output);
        output
    }
}

impl fmt::Display for Json {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(&self.dump())
    }
}

impl From<&str> for Json {
    fn from(value: &str) -> Self {
        Json::String(value.to_string())
    }
}

impl From<String> for Json {
    fn from(value: String) -> Self {
        Json::String(value)
    }
}

impl From<&String> for Json {
    fn from(value: &String) -> Self {
        Json::String(value.clone())
    }
}

impl From<bool> for Json {
    fn from(value: bool) -> Self {
        Json::Bool(value)
    }
}

impl From<i64> for Json {
    fn from(value: i64) -> Self {
        Json::Number(JsonNumber::Int(value))
    }
}

impl From<i32> for Json {
    fn from(value: i32) -> Self {
        Json::Number(JsonNumber::Int(i64::from(value)))
    }
}

impl From<u64> for Json {
    fn from(value: u64) -> Self {
        match i64::try_from(value) {
            Ok(value) => Json::Number(JsonNumber::Int(value)),
            Err(_) => Json::Number(JsonNumber::UInt(value)),
        }
    }
}

impl From<u32> for Json {
    fn from(value: u32) -> Self {
        Json::Number(JsonNumber::Int(i64::from(value)))
    }
}

impl From<usize> for Json {
    fn from(value: usize) -> Self {
        Json::from(value as u64)
    }
}

impl From<f64> for Json {
    fn from(value: f64) -> Self {
        Json::Number(JsonNumber::Float(value))
    }
}

impl From<()> for Json {
    fn from(_: ()) -> Self {
        Json::Null
    }
}

impl From<Vec<Json>> for Json {
    fn from(value: Vec<Json>) -> Self {
        Json::Array(value)
    }
}

impl From<BTreeMap<String, Json>> for Json {
    fn from(value: BTreeMap<String, Json>) -> Self {
        Json::Object(value)
    }
}

impl<T: Into<Json>> From<Option<T>> for Json {
    fn from(value: Option<T>) -> Self {
        match value {
            Some(value) => value.into(),
            None => Json::Null,
        }
    }
}

fn write_json(value: &Json, output: &mut String) {
    match value {
        Json::Null => output.push_str("null"),
        Json::Bool(value) => output.push_str(if *value { "true" } else { "false" }),
        Json::Number(JsonNumber::Int(value)) => output.push_str(&value.to_string()),
        Json::Number(JsonNumber::UInt(value)) => output.push_str(&value.to_string()),
        Json::Number(JsonNumber::Float(value)) => {
            if value.is_finite() {
                if value.fract() == 0.0 {
                    output.push_str(&format!("{value:.1}"));
                } else {
                    output.push_str(&value.to_string());
                }
            } else {
                output.push_str("null");
            }
        }
        Json::String(value) => write_json_string(value, output),
        Json::Array(items) => {
            output.push('[');
            for (index, item) in items.iter().enumerate() {
                if index > 0 {
                    output.push(',');
                }
                write_json(item, output);
            }
            output.push(']');
        }
        Json::Object(object) => {
            output.push('{');
            for (index, (key, value)) in object.iter().enumerate() {
                if index > 0 {
                    output.push(',');
                }
                write_json_string(key, output);
                output.push(':');
                write_json(value, output);
            }
            output.push('}');
        }
    }
}

fn write_json_string(value: &str, output: &mut String) {
    output.push('"');
    for character in value.chars() {
        match character {
            '"' => output.push_str("\\\""),
            '\\' => output.push_str("\\\\"),
            '\n' => output.push_str("\\n"),
            '\r' => output.push_str("\\r"),
            '\t' => output.push_str("\\t"),
            '\u{08}' => output.push_str("\\b"),
            '\u{0c}' => output.push_str("\\f"),
            character if (character as u32) < 0x20 => {
                output.push_str(&format!("\\u{:04x}", character as u32));
            }
            character => output.push(character),
        }
    }
    output.push('"');
}

struct Parser<'a> {
    input: &'a [u8],
    position: usize,
}

impl<'a> Parser<'a> {
    fn new(input: &'a str) -> Self {
        Self {
            input: input.as_bytes(),
            position: 0,
        }
    }

    fn parse_document(mut self) -> Result<Json, Error> {
        self.skip_whitespace();
        let value = self.parse_value()?;
        self.skip_whitespace();
        if self.position != self.input.len() {
            return Err(self.error("trailing characters"));
        }
        Ok(value)
    }

    fn parse_value(&mut self) -> Result<Json, Error> {
        self.skip_whitespace();
        match self.peek() {
            None => Err(self.error("unexpected end of input")),
            Some(b'n') => {
                self.expect_literal(b"null")?;
                Ok(Json::Null)
            }
            Some(b't') => {
                self.expect_literal(b"true")?;
                Ok(Json::Bool(true))
            }
            Some(b'f') => {
                self.expect_literal(b"false")?;
                Ok(Json::Bool(false))
            }
            Some(b'"') => Ok(Json::String(self.parse_string()?)),
            Some(b'[') => self.parse_array(),
            Some(b'{') => self.parse_object(),
            Some(b'-') | Some(b'0'..=b'9') => self.parse_number(),
            Some(_) => Err(self.error("unexpected character")),
        }
    }

    fn parse_array(&mut self) -> Result<Json, Error> {
        self.expect_byte(b'[')?;
        self.skip_whitespace();
        let mut items = Vec::new();
        if self.peek() == Some(b']') {
            self.position += 1;
            return Ok(Json::Array(items));
        }
        loop {
            items.push(self.parse_value()?);
            self.skip_whitespace();
            match self.next() {
                Some(b',') => {
                    self.skip_whitespace();
                }
                Some(b']') => break,
                _ => return Err(self.error("expected ',' or ']'")),
            }
        }
        Ok(Json::Array(items))
    }

    fn parse_object(&mut self) -> Result<Json, Error> {
        self.expect_byte(b'{')?;
        self.skip_whitespace();
        let mut object = BTreeMap::new();
        if self.peek() == Some(b'}') {
            self.position += 1;
            return Ok(Json::Object(object));
        }
        loop {
            self.skip_whitespace();
            let key = self.parse_string()?;
            self.skip_whitespace();
            self.expect_byte(b':')?;
            let value = self.parse_value()?;
            object.insert(key, value);
            self.skip_whitespace();
            match self.next() {
                Some(b',') => {
                    self.skip_whitespace();
                }
                Some(b'}') => break,
                _ => return Err(self.error("expected ',' or '}'")),
            }
        }
        Ok(Json::Object(object))
    }

    fn parse_string(&mut self) -> Result<String, Error> {
        self.expect_byte(b'"')?;
        let mut output = Vec::new();
        loop {
            let byte = match self.next() {
                Some(byte) => byte,
                None => return Err(self.error("unterminated string")),
            };
            match byte {
                b'"' => break,
                b'\\' => {
                    let escape = match self.next() {
                        Some(escape) => escape,
                        None => return Err(self.error("unterminated escape")),
                    };
                    match escape {
                        b'"' => output.push(b'"'),
                        b'\\' => output.push(b'\\'),
                        b'/' => output.push(b'/'),
                        b'b' => output.push(0x08),
                        b'f' => output.push(0x0c),
                        b'n' => output.push(b'\n'),
                        b'r' => output.push(b'\r'),
                        b't' => output.push(b'\t'),
                        b'u' => {
                            let first = self.parse_hex4()?;
                            let code = if (0xd800..=0xdbff).contains(&first) {
                                if self.next() != Some(b'\\') || self.next() != Some(b'u') {
                                    return Err(self.error("invalid surrogate pair"));
                                }
                                let second = self.parse_hex4()?;
                                if !(0xdc00..=0xdfff).contains(&second) {
                                    return Err(self.error("invalid low surrogate"));
                                }
                                0x10000 + ((first - 0xd800) << 10) + (second - 0xdc00)
                            } else if (0xdc00..=0xdfff).contains(&first) {
                                return Err(self.error("unexpected low surrogate"));
                            } else {
                                first
                            };
                            let character = match char::from_u32(code) {
                                Some(character) => character,
                                None => return Err(self.error("invalid unicode escape")),
                            };
                            let mut buffer = [0u8; 4];
                            output.extend_from_slice(character.encode_utf8(&mut buffer).as_bytes());
                        }
                        _ => return Err(self.error("invalid escape")),
                    }
                }
                0x00..=0x1f => return Err(self.error("control character in string")),
                byte => output.push(byte),
            }
        }
        String::from_utf8(output).map_err(|_| self.error("invalid UTF-8 in string"))
    }

    fn parse_hex4(&mut self) -> Result<u32, Error> {
        let mut value = 0u32;
        for _ in 0..4 {
            let byte = match self.next() {
                Some(byte) => byte,
                None => return Err(self.error("truncated unicode escape")),
            };
            let digit = match byte {
                b'0'..=b'9' => u32::from(byte - b'0'),
                b'a'..=b'f' => u32::from(byte - b'a' + 10),
                b'A'..=b'F' => u32::from(byte - b'A' + 10),
                _ => return Err(self.error("invalid unicode escape")),
            };
            value = value * 16 + digit;
        }
        Ok(value)
    }

    fn parse_number(&mut self) -> Result<Json, Error> {
        let start = self.position;
        if self.peek() == Some(b'-') {
            self.position += 1;
        }

        match self.peek() {
            Some(b'0') => {
                self.position += 1;
                if matches!(self.peek(), Some(b'0'..=b'9')) {
                    return Err(self.error("leading zero in number"));
                }
            }
            Some(b'1'..=b'9') => {
                while matches!(self.peek(), Some(b'0'..=b'9')) {
                    self.position += 1;
                }
            }
            _ => return Err(self.error("invalid number")),
        }

        let mut float = false;
        if self.peek() == Some(b'.') {
            float = true;
            self.position += 1;
            if !matches!(self.peek(), Some(b'0'..=b'9')) {
                return Err(self.error("invalid fraction"));
            }
            while matches!(self.peek(), Some(b'0'..=b'9')) {
                self.position += 1;
            }
        }
        if matches!(self.peek(), Some(b'e') | Some(b'E')) {
            float = true;
            self.position += 1;
            if matches!(self.peek(), Some(b'+') | Some(b'-')) {
                self.position += 1;
            }
            if !matches!(self.peek(), Some(b'0'..=b'9')) {
                return Err(self.error("invalid exponent"));
            }
            while matches!(self.peek(), Some(b'0'..=b'9')) {
                self.position += 1;
            }
        }

        let text = match std::str::from_utf8(&self.input[start..self.position]) {
            Ok(text) => text,
            Err(_) => return Err(self.error("invalid number")),
        };
        if !float {
            if text.starts_with('-') {
                if let Ok(value) = text.parse::<i64>() {
                    return Ok(Json::Number(JsonNumber::Int(value)));
                }
            } else if let Ok(value) = text.parse::<i64>() {
                return Ok(Json::Number(JsonNumber::Int(value)));
            } else if let Ok(value) = text.parse::<u64>() {
                return Ok(Json::Number(JsonNumber::UInt(value)));
            }
        }
        let value = match text.parse::<f64>() {
            Ok(value) => value,
            Err(_) => return Err(self.error("invalid number")),
        };
        if !value.is_finite() {
            return Err(self.error("number is not finite"));
        }
        Ok(Json::Number(JsonNumber::Float(value)))
    }

    fn expect_literal(&mut self, literal: &[u8]) -> Result<(), Error> {
        if self.input[self.position..].starts_with(literal) {
            self.position += literal.len();
            Ok(())
        } else {
            Err(self.error("invalid literal"))
        }
    }

    fn expect_byte(&mut self, expected: u8) -> Result<(), Error> {
        if self.next() == Some(expected) {
            Ok(())
        } else {
            Err(self.error("unexpected character"))
        }
    }

    fn skip_whitespace(&mut self) {
        while let Some(byte) = self.peek() {
            if matches!(byte, b' ' | b'\n' | b'\r' | b'\t') {
                self.position += 1;
            } else {
                break;
            }
        }
    }

    fn peek(&self) -> Option<u8> {
        self.input.get(self.position).copied()
    }

    fn next(&mut self) -> Option<u8> {
        let byte = self.peek();
        if byte.is_some() {
            self.position += 1;
        }
        byte
    }

    fn error(&self, message: &str) -> Error {
        Error::Json(format!("{message} at byte {}", self.position))
    }
}

// ---- models ---------------------------------------------------------------

#[derive(Clone, Debug, PartialEq)]
pub struct Event {
    pub id: String,
    pub source: String,
    pub types: Vec<String>,
    pub time: String,
    pub data: Json,
}

impl Event {
    /// Build a new event with a random id and the current UTC time.
    ///
    /// `types` are trimmed, deduplicated and sorted. `data` must be a JSON
    /// object. At least one non-empty type is required.
    pub fn new<S, T, I>(source: S, types: I, data: Json) -> Result<Self, Error>
    where
        S: Into<String>,
        T: Into<String>,
        I: IntoIterator<Item = T>,
    {
        let source = source.into();
        if source.is_empty() {
            return Err(Error::Protocol("event source is required".into()));
        }
        let types = normalize_types(types);
        if types.is_empty() {
            return Err(Error::Protocol("event needs at least one type".into()));
        }
        if !data.is_object() {
            return Err(Error::Protocol("event data must be a JSON object".into()));
        }
        Ok(Event {
            id: new_id(),
            source,
            types,
            time: now_rfc3339(),
            data,
        })
    }

    pub fn from_json(value: &Json) -> Result<Self, Error> {
        if !value.is_object() {
            return Err(Error::Protocol("event must be a JSON object".into()));
        }

        let legacy_type = value.get("type").and_then(Json::as_str).map(str::to_string);
        let types = match value.get("types") {
            Some(data) if data.is_array() => data
                .as_array()
                .unwrap()
                .iter()
                .filter_map(|item| item.as_str().map(str::to_string))
                .collect::<Vec<_>>(),
            Some(data) if data.is_null() => legacy_type.clone().into_iter().collect::<Vec<_>>(),
            None => legacy_type.into_iter().collect::<Vec<_>>(),
            Some(_) => {
                return Err(Error::Protocol("event types must be a JSON array".into()));
            }
        };

        let data = match value.get("data") {
            Some(data) if data.is_object() => data.clone(),
            Some(data) if data.is_null() => Json::object(),
            None => Json::object(),
            Some(_) => {
                return Err(Error::Protocol("event data must be a JSON object".into()));
            }
        };

        Ok(Event {
            id: string_field(value, "id"),
            source: string_field(value, "source"),
            types: normalize_types(types),
            time: string_field(value, "time"),
            data,
        })
    }

    pub fn to_json(&self) -> Json {
        Json::from_pairs([
            ("id", Json::from(&self.id)),
            ("source", Json::from(&self.source)),
            (
                "types",
                Json::Array(
                    self.types
                        .iter()
                        .cloned()
                        .map(Json::String)
                        .collect::<Vec<_>>(),
                ),
            ),
            ("time", Json::from(&self.time)),
            ("data", self.data.clone()),
        ])
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct PublishResult {
    pub duplicate: bool,
    pub matched: u64,
}

impl PublishResult {
    fn from_json(value: &Json) -> Result<Self, Error> {
        if !value.is_object() {
            return Err(Error::Protocol(
                "publish result must be a JSON object".into(),
            ));
        }
        Ok(PublishResult {
            duplicate: bool_field(value, "duplicate"),
            matched: uint_field(value, "matched"),
        })
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ConsumerStats {
    pub name: String,
    pub types: Vec<String>,
    pub cursor: i64,
    pub pending: i64,
    pub delivered: i64,
    pub failed: i64,
    pub last_error: Option<String>,
    pub last_event: Option<String>,
    pub last_at: Option<String>,
}

impl ConsumerStats {
    fn from_json(value: &Json) -> Result<Self, Error> {
        if !value.is_object() {
            return Err(Error::Protocol(
                "consumer stats must be a JSON object".into(),
            ));
        }
        Ok(ConsumerStats {
            name: string_field(value, "name"),
            types: string_array(value, "types", "consumer stats types")?,
            cursor: int_field(value, "cursor"),
            pending: int_field(value, "pending"),
            delivered: int_field(value, "delivered"),
            failed: int_field(value, "failed"),
            last_error: optional_string_field(value, "last_error"),
            last_event: optional_string_field(value, "last_event"),
            last_at: optional_string_field(value, "last_at"),
        })
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Failure {
    pub consumer: String,
    pub event_id: String,
    pub source: String,
    pub error: String,
    pub at: String,
}

impl Failure {
    fn from_json(value: &Json) -> Result<Self, Error> {
        if !value.is_object() {
            return Err(Error::Protocol("failure must be a JSON object".into()));
        }
        Ok(Failure {
            consumer: string_field(value, "consumer"),
            event_id: string_field(value, "event_id"),
            source: string_field(value, "source"),
            error: string_field(value, "error"),
            at: string_field(value, "at"),
        })
    }
}

fn string_field(value: &Json, key: &str) -> String {
    value
        .get(key)
        .and_then(Json::as_str)
        .unwrap_or("")
        .to_string()
}

fn optional_string_field(value: &Json, key: &str) -> Option<String> {
    value.get(key).and_then(Json::as_str).map(str::to_string)
}

fn int_field(value: &Json, key: &str) -> i64 {
    value.get(key).and_then(Json::as_i64).unwrap_or(0)
}

fn uint_field(value: &Json, key: &str) -> u64 {
    value.get(key).and_then(Json::as_u64).unwrap_or(0)
}

fn bool_field(value: &Json, key: &str) -> bool {
    value.get(key).and_then(Json::as_bool).unwrap_or(false)
}

fn string_array(value: &Json, key: &str, what: &str) -> Result<Vec<String>, Error> {
    match value.get(key) {
        Some(data) if data.is_array() => Ok(data
            .as_array()
            .unwrap()
            .iter()
            .filter_map(|item| item.as_str().map(str::to_string))
            .collect()),
        Some(data) if data.is_null() => Ok(Vec::new()),
        None => Ok(Vec::new()),
        Some(_) => Err(Error::Protocol(format!("{what} must be a JSON array"))),
    }
}

/// Trim, drop empty entries, deduplicate and sort event types.
pub fn normalize_types<T, I>(types: I) -> Vec<String>
where
    T: Into<String>,
    I: IntoIterator<Item = T>,
{
    let mut types = types
        .into_iter()
        .map(|value| value.into().trim().to_string())
        .filter(|value| !value.is_empty())
        .collect::<Vec<_>>();
    types.sort();
    types.dedup();
    types
}

/// Options for [`Client::capture_with`].
#[derive(Clone, Debug, PartialEq)]
pub struct CaptureOptions {
    pub source: String,
    pub types: Vec<String>,
    pub data: Json,
}

impl Default for CaptureOptions {
    fn default() -> Self {
        Self {
            source: "rust".to_string(),
            types: vec!["raw.captured".to_string()],
            data: Json::object(),
        }
    }
}

/// Query options for [`Client::events`].
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct EventsQuery {
    /// `limit == 0` returns every matching event.
    pub limit: u32,
    pub offset: u32,
    pub event_type: Option<String>,
}

impl Default for EventsQuery {
    fn default() -> Self {
        Self {
            limit: 50,
            offset: 0,
            event_type: None,
        }
    }
}

// ---- client ---------------------------------------------------------------

#[derive(Clone, Debug)]
struct Origin {
    host: String,
    port: u16,
    base_path: String,
    authority: String,
}

#[derive(Clone, Debug)]
pub struct Client {
    origin: Origin,
    base_url: String,
    timeout: Duration,
}

impl Client {
    pub fn new(base_url: impl Into<String>) -> Result<Self, Error> {
        Self::with_timeout(base_url, DEFAULT_TIMEOUT)
    }

    pub fn with_timeout(base_url: impl Into<String>, timeout: Duration) -> Result<Self, Error> {
        if timeout.is_zero() {
            return Err(Error::Protocol("timeout must be positive".into()));
        }
        let raw = base_url.into();
        let origin = parse_base_url(&raw)?;
        let normalized = raw.trim().trim_end_matches('/').to_string();
        Ok(Client {
            origin,
            base_url: normalized,
            timeout,
        })
    }

    pub fn base_url(&self) -> &str {
        &self.base_url
    }

    pub fn timeout(&self) -> Duration {
        self.timeout
    }

    pub fn health(&self) -> Result<String, Error> {
        let response = self.request("GET", "/healthz", None, self.timeout)?;
        self.expect(&response, "GET", "/healthz", &[200])?;
        Ok(response.body.trim_end().to_string())
    }

    pub fn publish(&self, event: &Event) -> Result<PublishResult, Error> {
        self.publish_json(&event.to_json())
    }

    pub fn publish_json(&self, event: &Json) -> Result<PublishResult, Error> {
        if !event.is_object() {
            return Err(Error::Protocol("event must be a JSON object".into()));
        }
        let response = self.request("POST", "/events", Some(event), self.timeout)?;
        self.expect(&response, "POST", "/events", &[200, 201])?;
        PublishResult::from_json(&Json::parse(&response.body)?)
    }

    pub fn capture(&self, text: &str) -> Result<PublishResult, Error> {
        self.capture_with(text, CaptureOptions::default())
    }

    pub fn capture_with(
        &self,
        text: &str,
        mut options: CaptureOptions,
    ) -> Result<PublishResult, Error> {
        if !options.data.is_object() {
            return Err(Error::Protocol("capture data must be a JSON object".into()));
        }
        options.data.insert("text", text);
        self.publish(&Event::new(options.source, options.types, options.data)?)
    }

    pub fn events(&self, query: EventsQuery) -> Result<Vec<Event>, Error> {
        let mut path = format!("/events?limit={}&offset={}", query.limit, query.offset);
        if let Some(event_type) = query.event_type.as_deref() {
            if !event_type.is_empty() {
                path.push_str("&type=");
                path.push_str(&url_encode(event_type));
            }
        }

        let response = self.request("GET", &path, None, self.timeout)?;
        self.expect(&response, "GET", &path, &[200])?;
        let value = Json::parse(&response.body)?;
        let items = value
            .as_array()
            .ok_or_else(|| Error::Protocol("events response is not a JSON array".into()))?;
        items.iter().map(Event::from_json).collect()
    }

    pub fn stats(&self) -> Result<Vec<ConsumerStats>, Error> {
        let response = self.request("GET", "/stats", None, self.timeout)?;
        self.expect(&response, "GET", "/stats", &[200])?;
        let value = Json::parse(&response.body)?;
        let items = value
            .as_array()
            .ok_or_else(|| Error::Protocol("stats response is not a JSON array".into()))?;
        items.iter().map(ConsumerStats::from_json).collect()
    }

    pub fn failures(&self) -> Result<Vec<Failure>, Error> {
        let response = self.request("GET", "/failures", None, self.timeout)?;
        self.expect(&response, "GET", "/failures", &[200])?;
        let value = Json::parse(&response.body)?;
        let items = value
            .as_array()
            .ok_or_else(|| Error::Protocol("failures response is not a JSON array".into()))?;
        items.iter().map(Failure::from_json).collect()
    }

    pub fn register<T, I>(&self, name: &str, types: I) -> Result<ConsumerStats, Error>
    where
        T: Into<String>,
        I: IntoIterator<Item = T>,
    {
        if name.is_empty() {
            return Err(Error::Protocol("consumer name is required".into()));
        }
        let types = normalize_types(types);
        if types.is_empty() {
            return Err(Error::Protocol("consumer needs at least one type".into()));
        }

        let mut body = Json::object();
        body.insert(
            "types",
            Json::Array(types.into_iter().map(Json::String).collect()),
        );
        let path = consumer_path(name);
        let response = self.request("PUT", &path, Some(&body), self.timeout)?;
        self.expect(&response, "PUT", &path, &[200, 201])?;
        ConsumerStats::from_json(&Json::parse(&response.body)?)
    }

    pub fn deregister(&self, name: &str) -> Result<(), Error> {
        if name.is_empty() {
            return Err(Error::Protocol("consumer name is required".into()));
        }
        let path = consumer_path(name);
        let response = self.request("DELETE", &path, None, self.timeout)?;
        self.expect(&response, "DELETE", &path, &[204])?;
        Ok(())
    }

    pub fn next(&self, name: &str, timeout: Duration) -> Result<Option<Event>, Error> {
        if name.is_empty() {
            return Err(Error::Protocol("consumer name is required".into()));
        }
        if timeout.is_zero() {
            return Err(Error::Protocol("poll timeout must be positive".into()));
        }

        let path = format!(
            "{}/next?timeout={}",
            consumer_path(name),
            format_timeout(timeout)
        );
        let socket_timeout = self.timeout.max(timeout + Duration::from_secs(5));
        let response = self.request("GET", &path, None, socket_timeout)?;
        if response.status == 204 {
            return Ok(None);
        }
        self.expect(&response, "GET", &path, &[200])?;
        let value = Json::parse(&response.body)?;
        let event = value
            .get("event")
            .ok_or_else(|| Error::Protocol("next response does not contain an event".into()))?;
        Event::from_json(event).map(Some)
    }

    pub fn ack(&self, name: &str, event_id: Option<&str>) -> Result<(), Error> {
        if name.is_empty() {
            return Err(Error::Protocol("consumer name is required".into()));
        }
        let mut body = Json::object();
        if let Some(event_id) = event_id {
            if !event_id.is_empty() {
                body.insert("id", event_id);
            }
        }
        let path = format!("{}/ack", consumer_path(name));
        let response = self.request("POST", &path, Some(&body), self.timeout)?;
        self.expect(&response, "POST", &path, &[204])?;
        Ok(())
    }

    pub fn nack(&self, name: &str, error: &str, event_id: Option<&str>) -> Result<(), Error> {
        if name.is_empty() {
            return Err(Error::Protocol("consumer name is required".into()));
        }
        let mut body = Json::object();
        body.insert("error", error);
        if let Some(event_id) = event_id {
            if !event_id.is_empty() {
                body.insert("id", event_id);
            }
        }
        let path = format!("{}/nack", consumer_path(name));
        let response = self.request("POST", &path, Some(&body), self.timeout)?;
        self.expect(&response, "POST", &path, &[204])?;
        Ok(())
    }

    fn request(
        &self,
        method: &str,
        path: &str,
        body: Option<&Json>,
        timeout: Duration,
    ) -> Result<HttpResponse, Error> {
        if timeout.is_zero() {
            return Err(Error::Protocol("request timeout must be positive".into()));
        }
        let body = body.map(Json::dump);
        let full_path = format!("{}{}", self.origin.base_path, path);
        http_request(
            &self.origin.host,
            self.origin.port,
            &self.origin.authority,
            method,
            &full_path,
            body.as_deref(),
            timeout,
        )
    }

    fn expect(
        &self,
        response: &HttpResponse,
        method: &str,
        path: &str,
        accepted: &[u16],
    ) -> Result<(), Error> {
        if accepted.contains(&response.status) {
            return Ok(());
        }

        let mut message = response.body.trim().to_string();
        if let Ok(value) = Json::parse(&response.body) {
            if let Some(Json::String(text)) = value.get("error") {
                message = text.clone();
            }
        }
        if message.is_empty() {
            message = format!("HTTP {}", response.status);
        }

        let url = format!("{}{}", self.base_url, path);
        let text = format!(
            "{} {} failed with HTTP {}: {}",
            method, url, response.status, message
        );
        let error = HttpError {
            status: response.status,
            body: response.body.clone(),
            message: text,
        };
        if response.status == 409 {
            Err(Error::Conflict(error))
        } else {
            Err(Error::Http(error))
        }
    }
}

// ---- consumer -------------------------------------------------------------

#[derive(Debug)]
pub struct Consumer<'a> {
    client: &'a Client,
    name: String,
    types: Vec<String>,
    poll_timeout: Duration,
    closed: bool,
    // A consumer owns a single server-side inflight slot; sharing it across
    // threads would race on that slot. Moving it to another thread is fine.
    _not_sync: PhantomData<Cell<()>>,
}

impl<'a> Consumer<'a> {
    pub fn new<T, I>(client: &'a Client, name: impl Into<String>, types: I) -> Result<Self, Error>
    where
        T: Into<String>,
        I: IntoIterator<Item = T>,
    {
        Self::with_timeout(client, name, types, DEFAULT_POLL_TIMEOUT)
    }

    pub fn with_timeout<T, I>(
        client: &'a Client,
        name: impl Into<String>,
        types: I,
        poll_timeout: Duration,
    ) -> Result<Self, Error>
    where
        T: Into<String>,
        I: IntoIterator<Item = T>,
    {
        if poll_timeout.is_zero() {
            return Err(Error::Protocol("poll timeout must be positive".into()));
        }
        let name = name.into();
        if name.is_empty() {
            return Err(Error::Protocol("consumer name is required".into()));
        }
        let types = normalize_types(types);
        if types.is_empty() {
            return Err(Error::Protocol("consumer needs at least one type".into()));
        }
        client.register(&name, types.clone())?;
        Ok(Consumer {
            client,
            name,
            types,
            poll_timeout,
            closed: false,
            _not_sync: PhantomData,
        })
    }

    pub fn name(&self) -> &str {
        &self.name
    }

    pub fn types(&self) -> &[String] {
        &self.types
    }

    pub fn is_closed(&self) -> bool {
        self.closed
    }

    pub fn stats(&self) -> Result<ConsumerStats, Error> {
        self.client
            .stats()?
            .into_iter()
            .find(|stats| stats.name == self.name)
            .ok_or_else(|| Error::Protocol(format!("consumer '{}' not found in stats", self.name)))
    }

    pub fn next(&self) -> Result<Option<Message<'a>>, Error> {
        self.next_with_timeout(self.poll_timeout)
    }

    pub fn next_with_timeout(&self, timeout: Duration) -> Result<Option<Message<'a>>, Error> {
        if self.closed {
            return Err(Error::Protocol(format!(
                "consumer '{}' is closed",
                self.name
            )));
        }
        match self.client.next(&self.name, timeout)? {
            Some(event) => Ok(Some(Message {
                client: self.client,
                consumer: self.name.clone(),
                event,
            })),
            None => Ok(None),
        }
    }

    pub fn close(&mut self) {
        self.closed = true;
    }

    pub fn deregister(&mut self) -> Result<(), Error> {
        self.client.deregister(&self.name)?;
        self.closed = true;
        Ok(())
    }
}

#[derive(Clone, Debug)]
pub struct Message<'a> {
    client: &'a Client,
    consumer: String,
    event: Event,
}

impl<'a> Message<'a> {
    pub fn event(&self) -> &Event {
        &self.event
    }

    pub fn consumer(&self) -> &str {
        &self.consumer
    }

    pub fn ack(&self) -> Result<(), Error> {
        self.client.ack(&self.consumer, Some(&self.event.id))
    }

    pub fn nack(&self, error: &str) -> Result<(), Error> {
        self.client
            .nack(&self.consumer, error, Some(&self.event.id))
    }
}

// ---- HTTP -----------------------------------------------------------------

struct HttpResponse {
    status: u16,
    body: String,
}

fn http_request(
    host: &str,
    port: u16,
    authority: &str,
    method: &str,
    path: &str,
    body: Option<&str>,
    timeout: Duration,
) -> Result<HttpResponse, Error> {
    let addresses = (host, port).to_socket_addrs()?.collect::<Vec<_>>();
    if addresses.is_empty() {
        return Err(Error::Protocol("cannot resolve the service host".into()));
    }

    let mut stream = None;
    let mut last_error = None;
    for address in addresses {
        match TcpStream::connect_timeout(&address, timeout) {
            Ok(connected) => {
                stream = Some(connected);
                break;
            }
            Err(error) => last_error = Some(error),
        }
    }
    let mut stream = stream.ok_or_else(|| {
        Error::Io(last_error.unwrap_or_else(|| io::Error::other("no usable address")))
    })?;

    stream.set_read_timeout(Some(timeout))?;
    stream.set_write_timeout(Some(timeout))?;
    let _ = stream.set_nodelay(true);

    let mut request = format!(
        "{method} {path} HTTP/1.1\r\n\
         Host: {authority}\r\n\
         Accept: application/json\r\n\
         Connection: close\r\n\
         User-Agent: yi-rust/{VERSION}\r\n"
    );
    if let Some(body) = body {
        request.push_str("Content-Type: application/json\r\n");
        request.push_str(&format!("Content-Length: {}\r\n", body.len()));
    }
    request.push_str("\r\n");

    stream.write_all(request.as_bytes())?;
    if let Some(body) = body {
        stream.write_all(body.as_bytes())?;
    }
    stream.flush()?;

    read_response(&mut stream)
}

fn read_response(stream: &mut TcpStream) -> Result<HttpResponse, Error> {
    let mut buffer = Vec::new();
    let header_end = loop {
        if let Some(position) = find_bytes(&buffer, b"\r\n\r\n") {
            break position;
        }
        if buffer.len() > 64 * 1024 {
            return Err(Error::Protocol("response headers are too large".into()));
        }
        read_more(stream, &mut buffer)?;
    };

    let head = std::str::from_utf8(&buffer[..header_end])
        .map_err(|_| Error::Protocol("response headers are not valid UTF-8".into()))?;
    let mut lines = head.split("\r\n");
    let status_line = lines.next().unwrap_or("");
    let mut parts = status_line.split_whitespace();
    let version = parts.next().unwrap_or("");
    if !version.starts_with("HTTP/") {
        return Err(Error::Protocol("invalid HTTP status line".into()));
    }
    let status = parts
        .next()
        .and_then(|value| value.parse::<u16>().ok())
        .ok_or_else(|| Error::Protocol("invalid HTTP status".into()))?;

    let mut content_length = None;
    let mut chunked = false;
    for line in lines {
        if line.is_empty() {
            continue;
        }
        let (name, value) = line
            .split_once(':')
            .ok_or_else(|| Error::Protocol("invalid HTTP header".into()))?;
        match name.trim().to_ascii_lowercase().as_str() {
            "content-length" => {
                content_length = Some(
                    value
                        .trim()
                        .parse::<usize>()
                        .map_err(|_| Error::Protocol("invalid content-length".into()))?,
                );
            }
            "transfer-encoding" if value.to_ascii_lowercase().contains("chunked") => {
                chunked = true;
            }
            _ => {}
        }
    }

    let mut body = buffer[header_end + 4..].to_vec();
    if status == 204 || status == 304 {
        return Ok(HttpResponse {
            status,
            body: String::new(),
        });
    }
    if chunked {
        body = read_chunked(stream, body)?;
    } else if let Some(length) = content_length {
        body = read_exact_body(stream, body, length)?;
    } else {
        body = read_until_eof(stream, body)?;
    }

    let body = String::from_utf8(body)
        .map_err(|_| Error::Protocol("response body is not valid UTF-8".into()))?;
    Ok(HttpResponse { status, body })
}

fn read_exact_body(
    stream: &mut TcpStream,
    mut body: Vec<u8>,
    length: usize,
) -> Result<Vec<u8>, Error> {
    if body.len() >= length {
        body.truncate(length);
        return Ok(body);
    }
    while body.len() < length {
        read_more(stream, &mut body)?;
    }
    body.truncate(length);
    Ok(body)
}

fn read_until_eof(stream: &mut TcpStream, mut body: Vec<u8>) -> Result<Vec<u8>, Error> {
    let mut chunk = [0u8; 4096];
    loop {
        let read = stream.read(&mut chunk)?;
        if read == 0 {
            break;
        }
        body.extend_from_slice(&chunk[..read]);
    }
    Ok(body)
}

fn read_chunked(stream: &mut TcpStream, mut buffer: Vec<u8>) -> Result<Vec<u8>, Error> {
    let mut body = Vec::new();
    let mut position = 0usize;
    loop {
        let line_end = loop {
            if let Some(offset) = find_bytes(&buffer[position..], b"\r\n") {
                break position + offset;
            }
            read_more(stream, &mut buffer)?;
        };
        let line = std::str::from_utf8(&buffer[position..line_end])
            .map_err(|_| Error::Protocol("invalid chunk header".into()))?;
        let size_text = line.split(';').next().unwrap_or("").trim();
        let size = usize::from_str_radix(size_text, 16)
            .map_err(|_| Error::Protocol("invalid chunk size".into()))?;
        position = line_end + 2;
        if size == 0 {
            break;
        }
        while buffer.len() < position + size + 2 {
            read_more(stream, &mut buffer)?;
        }
        body.extend_from_slice(&buffer[position..position + size]);
        position += size;
        if &buffer[position..position + 2] != b"\r\n" {
            return Err(Error::Protocol("invalid chunk terminator".into()));
        }
        position += 2;
    }
    Ok(body)
}

fn read_more(stream: &mut TcpStream, buffer: &mut Vec<u8>) -> Result<(), Error> {
    let mut chunk = [0u8; 4096];
    let read = stream.read(&mut chunk)?;
    if read == 0 {
        return Err(Error::Protocol("unexpected end of response".into()));
    }
    buffer.extend_from_slice(&chunk[..read]);
    Ok(())
}

fn find_bytes(haystack: &[u8], needle: &[u8]) -> Option<usize> {
    haystack
        .windows(needle.len())
        .position(|window| window == needle)
}

// ---- URL, ids and time ----------------------------------------------------

fn parse_base_url(input: &str) -> Result<Origin, Error> {
    let trimmed = input.trim();
    let rest = if let Some(rest) = trimmed.strip_prefix("http://") {
        rest
    } else if trimmed.starts_with("https://") {
        return Err(Error::InvalidUrl(
            "https is not supported; terminate TLS in a proxy".into(),
        ));
    } else {
        return Err(Error::InvalidUrl("expected an http:// URL".into()));
    };

    let (authority, path) = match rest.split_once('/') {
        Some((authority, path)) => (authority, format!("/{path}")),
        None => (rest, String::new()),
    };
    if authority.is_empty() {
        return Err(Error::InvalidUrl("missing host".into()));
    }
    if authority.contains('@') {
        return Err(Error::InvalidUrl(
            "userinfo in the base URL is not supported".into(),
        ));
    }

    let (host, port) = parse_authority(authority)?;
    let mut base_path = path;
    while base_path.ends_with('/') {
        base_path.pop();
    }
    Ok(Origin {
        host,
        port,
        base_path,
        authority: authority.to_string(),
    })
}

fn parse_authority(authority: &str) -> Result<(String, u16), Error> {
    if let Some(rest) = authority.strip_prefix('[') {
        let end = rest
            .find(']')
            .ok_or_else(|| Error::InvalidUrl("unterminated IPv6 address".into()))?;
        let host = &rest[..end];
        let tail = &rest[end + 1..];
        if host.is_empty() {
            return Err(Error::InvalidUrl("missing host".into()));
        }
        let port = if tail.is_empty() {
            80
        } else if let Some(port) = tail.strip_prefix(':') {
            parse_port(port)?
        } else {
            return Err(Error::InvalidUrl("invalid IPv6 authority".into()));
        };
        return Ok((host.to_string(), port));
    }

    if let Some((host, port)) = authority.rsplit_once(':') {
        if host.contains(':') {
            return Err(Error::InvalidUrl(
                "IPv6 addresses must be wrapped in []".into(),
            ));
        }
        if host.is_empty() {
            return Err(Error::InvalidUrl("missing host".into()));
        }
        Ok((host.to_string(), parse_port(port)?))
    } else {
        if authority.is_empty() {
            return Err(Error::InvalidUrl("missing host".into()));
        }
        Ok((authority.to_string(), 80))
    }
}

fn parse_port(value: &str) -> Result<u16, Error> {
    let port = value
        .parse::<u16>()
        .map_err(|_| Error::InvalidUrl("invalid port".into()))?;
    if port == 0 {
        return Err(Error::InvalidUrl("port must be positive".into()));
    }
    Ok(port)
}

fn url_encode(value: &str) -> String {
    let mut output = String::with_capacity(value.len());
    for byte in value.as_bytes() {
        let byte = *byte;
        if byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'_' | b'.' | b'~') {
            output.push(byte as char);
        } else {
            output.push_str(&format!("%{byte:02X}"));
        }
    }
    output
}

fn consumer_path(name: &str) -> String {
    format!("/consumers/{}", url_encode(name))
}

fn format_timeout(timeout: Duration) -> String {
    let millis = timeout.as_millis().max(1);
    format!("{}.{:03}", millis / 1000, millis % 1000)
}

fn new_id() -> String {
    let mut bytes = [0u8; 16];
    let mut filled = false;

    #[cfg(unix)]
    {
        if let Ok(mut file) = std::fs::File::open("/dev/urandom") {
            filled = file.read_exact(&mut bytes).is_ok();
        }
    }

    if !filled {
        fill_fallback(&mut bytes);
    }

    let mut output = String::with_capacity(32);
    for byte in bytes {
        output.push_str(&format!("{byte:02x}"));
    }
    output
}

fn fill_fallback(bytes: &mut [u8; 16]) {
    static COUNTER: AtomicU64 = AtomicU64::new(0);
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|duration| duration.as_nanos() as u64)
        .unwrap_or(0);
    let mut state =
        nanos ^ (u64::from(std::process::id()) << 32) ^ COUNTER.fetch_add(1, Ordering::Relaxed);
    for byte in bytes {
        state ^= state << 13;
        state ^= state >> 7;
        state ^= state << 17;
        *byte = (state >> 24) as u8;
    }
}

fn now_rfc3339() -> String {
    let seconds = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|duration| duration.as_secs() as i64)
        .unwrap_or(0);
    format_rfc3339(seconds)
}

fn format_rfc3339(seconds: i64) -> String {
    let days = seconds.div_euclid(86_400);
    let seconds_of_day = seconds.rem_euclid(86_400);
    let (year, month, day) = civil_from_days(days);
    format!(
        "{year:04}-{month:02}-{day:02}T{:02}:{:02}:{:02}Z",
        seconds_of_day / 3_600,
        (seconds_of_day % 3_600) / 60,
        seconds_of_day % 60
    )
}

// Howard Hinnant's civil-from-days algorithm. Days are relative to 1970-01-01.
fn civil_from_days(days: i64) -> (i64, u32, u32) {
    let shifted = days + 719_468;
    let era = if shifted >= 0 {
        shifted
    } else {
        shifted - 146_096
    } / 146_097;
    let day_of_era = shifted - era * 146_097;
    let year_of_era =
        (day_of_era - day_of_era / 1_460 + day_of_era / 36_524 - day_of_era / 146_096) / 365;
    let year = year_of_era + era * 400;
    let day_of_year = day_of_era - (365 * year_of_era + year_of_era / 4 - year_of_era / 100);
    let month_prime = (5 * day_of_year + 2) / 153;
    let day = day_of_year - (153 * month_prime + 2) / 5 + 1;
    let month = if month_prime < 10 {
        month_prime + 3
    } else {
        month_prime - 9
    };
    (year + i64::from(month <= 2), month as u32, day as u32)
}

// ---- tests ----------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use std::net::TcpListener;
    use std::thread;

    #[test]
    fn json_round_trip() {
        let input = r#"{
            "a": [1, -2, 3.5, true, false, null],
            "b": "line\nquote\" and \u4f60\u597d\uD83D\uDE00",
            "c": {"d": "汉字"},
            "e": 18446744073709551615
        }"#;
        let value = Json::parse(input).unwrap();
        let dumped = value.dump();
        let parsed = Json::parse(&dumped).unwrap();
        assert_eq!(value, parsed);
        assert_eq!(
            parsed.get("b").and_then(Json::as_str),
            Some("line\nquote\" and 你好😀")
        );
    }

    #[test]
    fn json_rejects_invalid_documents() {
        assert!(Json::parse("").is_err());
        assert!(Json::parse("{").is_err());
        assert!(Json::parse("{\"a\":1} trailing").is_err());
        assert!(Json::parse("01").is_err());
        assert!(Json::parse("1.").is_err());
        assert!(Json::parse("\"\\uD800\"").is_err());
        assert!(Json::parse("[1,]").is_err());
    }

    #[test]
    fn event_new_and_round_trip() {
        let event = Event::new(
            "phone",
            ["note", "raw.captured", "note", "  "],
            Json::from_pairs([("text", Json::from("hello"))]),
        )
        .unwrap();

        assert_eq!(event.id.len(), 32);
        assert_eq!(event.source, "phone");
        assert_eq!(event.types, vec!["note", "raw.captured"]);
        assert!(event.time.ends_with('Z'));
        assert_eq!(event.time.len(), 20);
        assert_eq!(event.data.get("text").and_then(Json::as_str), Some("hello"));

        let parsed = Event::from_json(&event.to_json()).unwrap();
        assert_eq!(parsed, event);
    }

    #[test]
    fn event_legacy_type_and_validation() {
        let legacy = Event::from_json(&Json::from_pairs([
            ("id", Json::from("e1")),
            ("source", Json::from("s")),
            ("type", Json::from("note")),
            ("time", Json::from("2026-11-15T10:00:00Z")),
        ]))
        .unwrap();
        assert_eq!(legacy.types, vec!["note"]);
        assert!(legacy.data.is_object());

        assert!(Event::from_json(&Json::from_pairs([
            ("id", Json::from("e1")),
            ("data", Json::Array(vec![])),
        ]))
        .is_err());

        assert!(Event::new("", ["note"], Json::object()).is_err());
        assert!(Event::new("s", Vec::<String>::new(), Json::object()).is_err());
        assert!(Event::new("s", ["note"], Json::Array(vec![])).is_err());
    }

    #[test]
    fn models_parse() {
        let publish = PublishResult::from_json(&Json::from_pairs([
            ("duplicate", Json::from(true)),
            ("matched", Json::from(3_i64)),
        ]))
        .unwrap();
        assert!(publish.duplicate);
        assert_eq!(publish.matched, 3);

        let stats = ConsumerStats::from_json(&Json::from_pairs([
            ("name", Json::from("c")),
            ("types", Json::Array(vec![Json::from("raw.captured")])),
            ("cursor", Json::from(5_i64)),
            ("pending", Json::from(2_i64)),
            ("delivered", Json::from(7_i64)),
            ("failed", Json::from(1_i64)),
            ("last_error", Json::from("boom")),
            ("last_event", Json::from("e1")),
        ]))
        .unwrap();
        assert_eq!(stats.name, "c");
        assert_eq!(stats.cursor, 5);
        assert_eq!(stats.pending, 2);
        assert_eq!(stats.delivered, 7);
        assert_eq!(stats.failed, 1);
        assert_eq!(stats.last_error.as_deref(), Some("boom"));
        assert_eq!(stats.last_event.as_deref(), Some("e1"));
        assert!(stats.last_at.is_none());

        let failure = Failure::from_json(&Json::from_pairs([
            ("consumer", Json::from("c")),
            ("event_id", Json::from("e1")),
            ("source", Json::from("phone")),
            ("error", Json::from("bad")),
            ("at", Json::from("now")),
        ]))
        .unwrap();
        assert_eq!(failure.consumer, "c");
        assert_eq!(failure.event_id, "e1");
        assert_eq!(failure.error, "bad");
    }

    #[test]
    fn integers_round_trip() {
        assert_eq!(Json::from(3_u64), Json::parse("3").unwrap());
        assert_eq!(Json::from(3_i64), Json::parse("3").unwrap());
        assert_eq!(
            Json::from(u64::MAX),
            Json::parse("18446744073709551615").unwrap()
        );
        assert_eq!(Json::from(-3_i64).dump(), "-3");
    }

    #[test]
    fn urls_and_timeouts() {
        assert_eq!(url_encode("a b/c"), "a%20b%2Fc");
        assert_eq!(consumer_path("a b"), "/consumers/a%20b");
        assert_eq!(format_timeout(Duration::from_millis(2500)), "2.500");

        let origin = parse_base_url("http://127.0.0.1:8081/").unwrap();
        assert_eq!(origin.host, "127.0.0.1");
        assert_eq!(origin.port, 8081);
        assert_eq!(origin.base_path, "");

        let origin = parse_base_url("http://example.com/api/").unwrap();
        assert_eq!(origin.host, "example.com");
        assert_eq!(origin.port, 80);
        assert_eq!(origin.base_path, "/api");

        let origin = parse_base_url("http://[::1]:8081").unwrap();
        assert_eq!(origin.host, "::1");
        assert_eq!(origin.port, 8081);

        assert!(parse_base_url("https://example.com").is_err());
        assert!(parse_base_url("example.com").is_err());
        assert!(parse_base_url("http://").is_err());

        assert_eq!(format_rfc3339(0), "1970-01-01T00:00:00Z");
        assert_eq!(format_rfc3339(1_700_000_000), "2023-11-14T22:13:20Z");
    }

    #[test]
    fn reads_chunked_http_response() {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let address = listener.local_addr().unwrap();
        let server = thread::spawn(move || {
            let (mut stream, _) = listener.accept().unwrap();
            let mut request = [0u8; 1024];
            let _ = stream.read(&mut request);
            let body = "ok\n";
            let response = format!(
                "HTTP/1.1 200 OK\r\n\
                 Transfer-Encoding: chunked\r\n\
                 Connection: close\r\n\
                 \r\n\
                 {:X}\r\n{}\r\n0\r\n\r\n",
                body.len(),
                body
            );
            stream.write_all(response.as_bytes()).unwrap();
        });

        let client = Client::new(format!("http://{address}")).unwrap();
        assert_eq!(client.health().unwrap(), "ok");
        server.join().unwrap();
    }

    #[test]
    fn reads_split_chunked_response() {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let address = listener.local_addr().unwrap();
        let server = thread::spawn(move || {
            let (mut stream, _) = listener.accept().unwrap();
            let mut request = [0u8; 1024];
            let _ = stream.read(&mut request);
            let body = "ok\n";
            let response = format!(
                "HTTP/1.1 200 OK\r\n\
                 Transfer-Encoding: chunked\r\n\
                 Connection: close\r\n\
                 \r\n\
                 {:X}\r\n{}\r\n0\r\n\r\n",
                body.len(),
                body
            );
            for part in response.as_bytes().chunks(7) {
                stream.write_all(part).unwrap();
                stream.flush().unwrap();
                thread::sleep(Duration::from_millis(1));
            }
        });

        let client = Client::new(format!("http://{address}")).unwrap();
        assert_eq!(client.health().unwrap(), "ok");
        server.join().unwrap();
    }

    #[test]
    fn reads_content_length_http_response() {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let address = listener.local_addr().unwrap();
        let server = thread::spawn(move || {
            let (mut stream, _) = listener.accept().unwrap();
            let mut request = [0u8; 1024];
            let _ = stream.read(&mut request);
            let body = "ok\n";
            let response = format!(
                "HTTP/1.1 200 OK\r\n\
                 Content-Length: {}\r\n\
                 Connection: close\r\n\
                 \r\n{}",
                body.len(),
                body
            );
            stream.write_all(response.as_bytes()).unwrap();
        });

        let client = Client::new(format!("http://{address}")).unwrap();
        assert_eq!(client.health().unwrap(), "ok");
        server.join().unwrap();
    }

    #[test]
    fn live_e2e() {
        let base = match std::env::var("YI_BASE_URL") {
            Ok(value) => value,
            Err(_) => {
                eprintln!("skip live_e2e: set YI_BASE_URL to run it");
                return;
            }
        };

        let client = Client::new(base).unwrap();
        assert_eq!(client.health().unwrap(), "ok");

        let suffix = format!(
            "{}-{}",
            std::process::id(),
            SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        );
        let event_type = format!("rust.e2e.{suffix}");
        let name = format!("rust-e2e-{suffix}");

        let event = Event::new(
            "rust-lib",
            [event_type.clone()],
            Json::from_pairs([("text", Json::from("explicit"))]),
        )
        .unwrap();
        assert!(!client.publish(&event).unwrap().duplicate);
        assert!(client.publish(&event).unwrap().duplicate);

        let capture = CaptureOptions {
            source: "rust-lib".to_string(),
            types: vec![event_type.clone()],
            data: Json::from_pairs([("tag", Json::from("capture"))]),
        };
        client.capture_with("hello", capture).unwrap();

        let mut consumer = Consumer::new(&client, &name, [event_type.clone()]).unwrap();

        let first = consumer.next().unwrap().unwrap();
        assert_eq!(first.event().id, event.id);
        first.ack().unwrap();

        let second = consumer.next().unwrap().unwrap();
        assert_eq!(
            second.event().data.get("text").and_then(Json::as_str),
            Some("hello")
        );
        second.ack().unwrap();

        let stats = consumer.stats().unwrap();
        assert_eq!(stats.delivered, 2);
        assert_eq!(stats.pending, 0);

        let conflict =
            Consumer::with_timeout(&client, &name, ["other.type"], Duration::from_secs(1));
        assert!(matches!(conflict, Err(Error::Conflict(_))));

        let events = client
            .events(EventsQuery {
                limit: 0,
                offset: 0,
                event_type: Some(event_type.clone()),
            })
            .unwrap();
        assert_eq!(events.len(), 2);

        let missing = client
            .events(EventsQuery {
                event_type: Some(format!("{event_type}.missing")),
                ..EventsQuery::default()
            })
            .unwrap();
        assert!(missing.is_empty());

        let nacked = Event::new(
            "rust-lib",
            [event_type.clone()],
            Json::from_pairs([("text", Json::from("nack me"))]),
        )
        .unwrap();
        client.publish(&nacked).unwrap();
        let third = consumer.next().unwrap().unwrap();
        third.nack("e2e nack").unwrap();
        let stats = consumer.stats().unwrap();
        assert_eq!(stats.failed, 1);
        assert_eq!(stats.last_error.as_deref(), Some("e2e nack"));

        let failures = client.failures().unwrap();
        assert!(failures
            .iter()
            .any(|failure| failure.consumer == name && failure.error == "e2e nack"));

        let idle_name = format!("{name}-idle");
        client
            .register(&idle_name, [format!("{event_type}.idle")])
            .unwrap();
        let idle = client.next(&idle_name, Duration::from_millis(100)).unwrap();
        assert!(idle.is_none());
        client.deregister(&idle_name).unwrap();

        consumer.close();
        assert!(consumer.next().is_err());
        consumer.deregister().unwrap();
    }
}
