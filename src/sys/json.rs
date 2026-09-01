//! Minimal JSON with **Python-parity canonical serialization**.
//!
//! The journal's hash chain hashes `json.dumps(row, sort_keys=True,
//! separators=(",", ":"), default=str)` bytes (see `arena/journal.py`). For Rust
//! to verify and extend Python-written journals *and* for Python to verify Rust
//! ones, this module reproduces CPython's `json` output exactly:
//!
//! * objects serialize with **sorted keys** (`sort_keys=True`) — `BTreeMap`
//!   ordering equals CPython's (both are code-point order);
//! * compact separators `(",", ":")` for the canonical form; `indent=2` with
//!   `": "` for the pretty form (CLI output);
//! * `ensure_ascii=True` semantics: every non-ASCII code point escapes to
//!   `\uXXXX` with lowercase hex, astral characters as surrogate pairs;
//! * floats render through [`py_float_repr`] (CPython `repr(float)` algorithm);
//! * integers stay integers (Python `json` never coerces int→float).
//!
//! Numbers parse to [`JValue::Int`] when the literal has no `.`, `e`, `E`
//! (matching what Python emits for ints); otherwise [`JValue::Float`].

use std::collections::BTreeMap;
use std::fmt;

pub type JMap = BTreeMap<String, JValue>;

#[derive(Debug, Clone, PartialEq)]
pub enum JValue {
    Null,
    Bool(bool),
    Int(i64),
    Float(f64),
    Str(String),
    Arr(Vec<JValue>),
    Obj(JMap),
}

impl JValue {
    pub fn obj(map: JMap) -> JValue {
        JValue::Obj(map)
    }
    pub fn get(&self, key: &str) -> Option<&JValue> {
        match self {
            JValue::Obj(m) => m.get(key),
            _ => None,
        }
    }
    pub fn idx(&self, i: usize) -> Option<&JValue> {
        match self {
            JValue::Arr(v) => v.get(i),
            _ => None,
        }
    }
    pub fn as_str(&self) -> Option<&str> {
        match self {
            JValue::Str(s) => Some(s.as_str()),
            _ => None,
        }
    }
    pub fn as_bool(&self) -> Option<bool> {
        match self {
            JValue::Bool(b) => Some(*b),
            _ => None,
        }
    }
    /// Numeric accessor: Int or Float both yield f64 (Python's looseness where
    /// the fold reads numbers; call sites that must distinguish do so via
    /// [`JValue::as_int`]).
    pub fn as_f64(&self) -> Option<f64> {
        match self {
            JValue::Int(i) => Some(*i as f64),
            JValue::Float(f) => Some(*f),
            _ => None,
        }
    }
    pub fn as_int(&self) -> Option<i64> {
        match self {
            JValue::Int(i) => Some(*i),
            JValue::Float(f) if f.fract() == 0.0 && f.is_finite() => Some(*f as i64),
            _ => None,
        }
    }
    pub fn is_null(&self) -> bool {
        matches!(self, JValue::Null)
    }
    /// Python truthiness of the value (used by fold's `if p.get(...)` guards).
    pub fn truthy(&self) -> bool {
        match self {
            JValue::Null => false,
            JValue::Bool(b) => *b,
            JValue::Int(i) => *i != 0,
            JValue::Float(f) => *f != 0.0,
            JValue::Str(s) => !s.is_empty(),
            JValue::Arr(v) => !v.is_empty(),
            JValue::Obj(m) => !m.is_empty(),
        }
    }
    pub fn str_or(&self, key: &str, default: &str) -> String {
        self.get(key)
            .and_then(JValue::as_str)
            .unwrap_or(default)
            .to_string()
    }
    pub fn opt_str(&self, key: &str) -> Option<String> {
        match self.get(key) {
            Some(JValue::Str(s)) => Some(s.clone()),
            Some(JValue::Null) | None => None,
            Some(v) => Some(v.to_canon_string()), // Python `default=str` spirit
        }
    }

    /// Canonical form: sorted keys, compact separators, ASCII-escaped.
    pub fn to_canon_string(&self) -> String {
        let mut out = String::new();
        write_val(self, &mut out, None, 0);
        out
    }

    /// Pretty form matching `json.dumps(v, indent=2)`.
    pub fn to_pretty_string(&self) -> String {
        let mut out = String::new();
        write_val(self, &mut out, Some(2), 0);
        out
    }
}

impl From<&str> for JValue {
    fn from(s: &str) -> Self {
        JValue::Str(s.to_string())
    }
}
impl From<String> for JValue {
    fn from(s: String) -> Self {
        JValue::Str(s)
    }
}
impl From<bool> for JValue {
    fn from(b: bool) -> Self {
        JValue::Bool(b)
    }
}
impl From<i64> for JValue {
    fn from(i: i64) -> Self {
        JValue::Int(i)
    }
}
impl From<usize> for JValue {
    fn from(i: usize) -> Self {
        JValue::Int(i as i64)
    }
}
impl From<f64> for JValue {
    fn from(f: f64) -> Self {
        JValue::Float(f)
    }
}
impl From<Vec<JValue>> for JValue {
    fn from(v: Vec<JValue>) -> Self {
        JValue::Arr(v)
    }
}

impl fmt::Display for JValue {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.to_canon_string())
    }
}

/// Build a `JMap` from `(key, value)` pairs: `jmap! {"a" => 1i64, "b" => "x"}`.
#[macro_export]
macro_rules! jmap {
    () => { $crate::sys::json::JMap::new() };
    ($($k:expr => $v:expr),+ $(,)?) => {{
        let mut m = $crate::sys::json::JMap::new();
        $( m.insert($k.to_string(), $crate::sys::json::JValue::from($v)); )+
        m
    }};
}

// ------------------------------------------------------------------ writing

fn write_val(v: &JValue, out: &mut String, indent: Option<usize>, depth: usize) {
    match v {
        JValue::Null => out.push_str("null"),
        JValue::Bool(true) => out.push_str("true"),
        JValue::Bool(false) => out.push_str("false"),
        JValue::Int(i) => out.push_str(&i.to_string()),
        JValue::Float(f) => out.push_str(&py_float_repr(*f)),
        JValue::Str(s) => write_json_string(s, out),
        JValue::Arr(items) => {
            if items.is_empty() {
                out.push_str("[]");
                return;
            }
            out.push('[');
            let n_items = items.len();
            for (i, item) in items.iter().enumerate() {
                if let Some(n) = indent {
                    out.push('\n');
                    out.push_str(&" ".repeat(n * (depth + 1)));
                }
                write_val(item, out, indent, depth + 1);
                if i + 1 < n_items {
                    out.push(',');
                }
            }
            if let Some(n) = indent {
                out.push('\n');
                out.push_str(&" ".repeat(n * depth));
            }
            out.push(']');
        }
        JValue::Obj(map) => {
            if map.is_empty() {
                out.push_str("{}");
                return;
            }
            out.push('{');
            let n_items = map.len();
            for (i, (k, val)) in map.iter().enumerate() {
                if let Some(n) = indent {
                    out.push('\n');
                    out.push_str(&" ".repeat(n * (depth + 1)));
                }
                write_json_string(k, out);
                out.push(':');
                if indent.is_some() {
                    out.push(' ');
                }
                write_val(val, out, indent, depth + 1);
                if i + 1 < n_items {
                    out.push(',');
                }
            }
            if let Some(n) = indent {
                out.push('\n');
                out.push_str(&" ".repeat(n * depth));
            }
            out.push('}');
        }
    }
}

fn write_json_string(s: &str, out: &mut String) {
    out.push('"');
    for ch in s.chars() {
        match ch {
            '"' => out.push_str("\\\""),
            '\\' => out.push_str("\\\\"),
            '\n' => out.push_str("\\n"),
            '\r' => out.push_str("\\r"),
            '\t' => out.push_str("\\t"),
            '\u{8}' => out.push_str("\\b"),
            '\u{c}' => out.push_str("\\f"),
            c if (c as u32) < 0x20 => {
                out.push_str(&format!("\\u{:04x}", c as u32));
            }
            c if (c as u32) < 0x7f => out.push(c),
            c => {
                // ensure_ascii: BMP -> \uXXXX (lowercase), astral -> surrogate pair
                let cp = c as u32;
                if cp <= 0xFFFF {
                    out.push_str(&format!("\\u{:04x}", cp));
                } else {
                    let v = cp - 0x10000;
                    let hi = 0xD800 + (v >> 10);
                    let lo = 0xDC00 + (v & 0x3FF);
                    out.push_str(&format!("\\u{:04x}\\u{:04x}", hi, lo));
                }
            }
        }
    }
    out.push('"');
}

/// CPython `repr(float)` / `json.dumps(float)` rendering.
///
/// Shortest round-trip digits (Rust's `{:e}` gives exactly that), presented in
/// fixed notation when `1e-4 <= |x| < 1e16`, scientific otherwise, integral
/// values always carry `.0`, exponents carry a sign and at least two digits.
pub fn py_float_repr(x: f64) -> String {
    if x.is_nan() {
        return "NaN".into();
    }
    if x.is_infinite() {
        return if x > 0.0 {
            "Infinity".into()
        } else {
            "-Infinity".into()
        };
    }
    if x == 0.0 {
        return if x.is_sign_negative() {
            "-0.0".into()
        } else {
            "0.0".into()
        };
    }
    let neg = x < 0.0;
    let ax = x.abs();
    // Rust's Lower-Exp produces the shortest digit representation: "1.5e-5", "3e0"
    let sci = format!("{:e}", ax);
    let (mant, exp) = sci.split_once('e').expect("exp form");
    let exp: i32 = exp.parse().expect("exp int");
    let digits: String = mant.chars().filter(|c| *c != '.').collect();
    let digits = digits.trim_end_matches('0');
    let digits = if digits.is_empty() { "0" } else { digits };
    let sign = if neg { "-" } else { "" };

    // scientific thresholds per CPython: repr switches when exp < -4 or exp >= 16
    // CPython repr switches to exponent form outside [1e-4, 1e16)
    if !(-4..16).contains(&exp) {
        let m = if digits.len() == 1 {
            digits.to_string()
        } else {
            format!("{}.{}", &digits[..1], &digits[1..])
        };
        let esign = if exp < 0 { '-' } else { '+' };
        format!("{sign}{m}e{esign}{:02}", exp.abs())
    } else if exp >= 0 {
        let point = (exp as usize) + 1;
        if digits.len() <= point {
            let mut s = digits.to_string();
            s.push_str(&"0".repeat(point - digits.len()));
            s.push_str(".0");
            format!("{sign}{s}")
        } else {
            format!("{sign}{}.{}", &digits[..point], &digits[point..])
        }
    } else {
        let zeros = (-exp - 1) as usize;
        format!("{sign}0.{}{}", "0".repeat(zeros), digits)
    }
}

/// Python `round(x, 6)`: correctly-rounded to 6 decimals, returned as the
/// double nearest that decimal (matches CPython for all values the runtime
/// produces; differential-tested against the reference).
pub fn py_round(x: f64, digits: u32) -> f64 {
    let s = format!("{:.*}", digits as usize, x);
    s.parse::<f64>().unwrap_or(x)
}

// ------------------------------------------------------------------ parsing

#[derive(Debug)]
pub struct ParseError(pub String);

impl fmt::Display for ParseError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "json parse error: {}", self.0)
    }
}
impl std::error::Error for ParseError {}

pub fn parse(text: &str) -> Result<JValue, ParseError> {
    let bytes: Vec<char> = text.chars().collect();
    let mut p = Parser { s: &bytes, i: 0 };
    p.ws();
    let v = p.value()?;
    p.ws();
    if p.i != p.s.len() {
        return Err(ParseError(format!("trailing data at char {}", p.i)));
    }
    Ok(v)
}

struct Parser<'a> {
    s: &'a [char],
    i: usize,
}

impl<'a> Parser<'a> {
    fn ws(&mut self) {
        while self.i < self.s.len() && matches!(self.s[self.i], ' ' | '\t' | '\n' | '\r') {
            self.i += 1;
        }
    }
    fn peek(&self) -> Option<char> {
        self.s.get(self.i).copied()
    }
    fn eat(&mut self, lit: &str) -> bool {
        let chars: Vec<char> = lit.chars().collect();
        if self.s.len() >= self.i + chars.len() && self.s[self.i..self.i + chars.len()] == chars[..]
        {
            self.i += chars.len();
            true
        } else {
            false
        }
    }
    fn value(&mut self) -> Result<JValue, ParseError> {
        match self.peek() {
            None => Err(ParseError("unexpected end".into())),
            Some('n') if self.eat("null") => Ok(JValue::Null),
            Some('t') if self.eat("true") => Ok(JValue::Bool(true)),
            Some('f') if self.eat("false") => Ok(JValue::Bool(false)),
            Some('"') => Ok(JValue::Str(self.string()?)),
            Some('[') => self.array(),
            Some('{') => self.object(),
            Some(c) if c == '-' || c.is_ascii_digit() => self.number(),
            Some(c) => Err(ParseError(format!("unexpected char {c:?}"))),
        }
    }
    fn array(&mut self) -> Result<JValue, ParseError> {
        self.i += 1; // '['
        let mut out = Vec::new();
        self.ws();
        if self.peek() == Some(']') {
            self.i += 1;
            return Ok(JValue::Arr(out));
        }
        loop {
            self.ws();
            out.push(self.value()?);
            self.ws();
            match self.peek() {
                Some(',') => self.i += 1,
                Some(']') => {
                    self.i += 1;
                    return Ok(JValue::Arr(out));
                }
                _ => return Err(ParseError("expected , or ]".into())),
            }
        }
    }
    fn object(&mut self) -> Result<JValue, ParseError> {
        self.i += 1; // '{'
        let mut map = JMap::new();
        self.ws();
        if self.peek() == Some('}') {
            self.i += 1;
            return Ok(JValue::Obj(map));
        }
        loop {
            self.ws();
            if self.peek() != Some('"') {
                return Err(ParseError("expected key string".into()));
            }
            let k = self.string()?;
            self.ws();
            if self.peek() != Some(':') {
                return Err(ParseError("expected :".into()));
            }
            self.i += 1;
            self.ws();
            let v = self.value()?;
            map.insert(k, v);
            self.ws();
            match self.peek() {
                Some(',') => self.i += 1,
                Some('}') => {
                    self.i += 1;
                    return Ok(JValue::Obj(map));
                }
                _ => return Err(ParseError("expected , or }".into())),
            }
        }
    }
    fn string(&mut self) -> Result<String, ParseError> {
        self.i += 1; // '"'
        let mut out = String::new();
        loop {
            let c = self
                .peek()
                .ok_or(ParseError("unterminated string".into()))?;
            self.i += 1;
            match c {
                '"' => return Ok(out),
                '\\' => {
                    let e = self.peek().ok_or(ParseError("bad escape".into()))?;
                    self.i += 1;
                    match e {
                        '"' => out.push('"'),
                        '\\' => out.push('\\'),
                        '/' => out.push('/'),
                        'b' => out.push('\u{8}'),
                        'f' => out.push('\u{c}'),
                        'n' => out.push('\n'),
                        'r' => out.push('\r'),
                        't' => out.push('\t'),
                        'u' => {
                            let cp = self.hex4()?;
                            if (0xD800..0xDC00).contains(&cp) {
                                // high surrogate: require \uXXXX low surrogate
                                if self.peek() == Some('\\') {
                                    self.i += 1;
                                    if self.peek() == Some('u') {
                                        self.i += 1;
                                        let lo = self.hex4()?;
                                        if (0xDC00..0xE000).contains(&lo) {
                                            let c = 0x10000 + ((cp - 0xD800) << 10) + (lo - 0xDC00);
                                            out.push(char::from_u32(c).unwrap_or('\u{FFFD}'));
                                            continue;
                                        }
                                    }
                                }
                                out.push('\u{FFFD}');
                            } else {
                                out.push(char::from_u32(cp).unwrap_or('\u{FFFD}'));
                            }
                        }
                        _ => return Err(ParseError(format!("bad escape \\{e}"))),
                    }
                }
                c => out.push(c),
            }
        }
    }
    fn hex4(&mut self) -> Result<u32, ParseError> {
        if self.i + 4 > self.s.len() {
            return Err(ParseError("bad \\u escape".into()));
        }
        let txt: String = self.s[self.i..self.i + 4].iter().collect();
        self.i += 4;
        u32::from_str_radix(&txt, 16).map_err(|_| ParseError("bad \\u escape".into()))
    }
    fn number(&mut self) -> Result<JValue, ParseError> {
        let start = self.i;
        if self.peek() == Some('-') {
            self.i += 1;
        }
        let mut is_float = false;
        while let Some(c) = self.peek() {
            match c {
                '0'..='9' => self.i += 1,
                '.' | 'e' | 'E' | '+' | '-' => {
                    is_float = true;
                    self.i += 1;
                }
                _ => break,
            }
        }
        let txt: String = self.s[start..self.i].iter().collect();
        if !is_float {
            if let Ok(i) = txt.parse::<i64>() {
                return Ok(JValue::Int(i));
            }
        }
        txt.parse::<f64>()
            .map(JValue::Float)
            .map_err(|_| ParseError(format!("bad number {txt:?}")))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn float_repr_matches_python() {
        // values cross-checked against CPython 3.11 repr()
        let cases: &[(f64, &str)] = &[
            (0.0, "0.0"),
            (-0.0, "-0.0"),
            (1.0, "1.0"),
            (-1.0, "-1.0"),
            (0.01, "0.01"),
            (0.001, "0.001"),
            (0.0001, "0.0001"),
            (0.00001, "1e-05"),
            (1e-7, "1e-07"),
            (1.5e-5, "1.5e-05"),
            (1e15, "1000000000000000.0"),
            (1e16, "1e+16"),
            (1.23e20, "1.23e+20"),
            (0.1, "0.1"),
            (2.5, "2.5"),
            (100.0, "100.0"),
            (0.0015392303466796875, "0.0015392303466796875"),
            (3.0e-3, "0.003"),
            (9.999999999999999e-5, "9.999999999999999e-05"),
            (123.456, "123.456"),
            (1e-4, "0.0001"),
            (f64::INFINITY, "Infinity"),
            (f64::NEG_INFINITY, "-Infinity"),
        ];
        for (v, want) in cases {
            assert_eq!(&py_float_repr(*v), want, "value {v}");
        }
    }

    #[test]
    fn canonical_form_matches_python_json_dumps() {
        // python: json.dumps({"b":1,"a":[1,2.5,None,True,"x\n"]},sort_keys=True,separators=(",",":"))
        let mut m = JMap::new();
        m.insert("b".into(), JValue::Int(1));
        m.insert(
            "a".into(),
            JValue::Arr(vec![
                JValue::Int(1),
                JValue::Float(2.5),
                JValue::Null,
                JValue::Bool(true),
                JValue::Str("x\n".into()),
            ]),
        );
        assert_eq!(
            JValue::Obj(m).to_canon_string(),
            r#"{"a":[1,2.5,null,true,"x\n"],"b":1}"#
        );
    }

    #[test]
    fn ascii_escaping_matches_python() {
        let v = JValue::Str("héllo → 😀".into());
        assert_eq!(
            v.to_canon_string(),
            "\"h\\u00e9llo \\u2192 \\ud83d\\ude00\""
        );
    }

    #[test]
    fn parse_roundtrip() {
        let txt = r#"{"a":[1,2.5,null,true,"x\n"],"b":1,"u":"é😀"}"#;
        let v = parse(txt).unwrap();
        // CPython default is ensure_ascii=True: non-ASCII escapes to lowercase
        // \uXXXX with surrogate pairs for astral chars.
        assert_eq!(
            v.to_canon_string(),
            r#"{"a":[1,2.5,null,true,"x\n"],"b":1,"u":"\u00e9\ud83d\ude00"}"#
        );
        // and the escaped form parses back to the same value
        let v2 = parse(r#"{"a":[1,2.5,null,true,"x\n"],"b":1,"u":"\u00e9\ud83d\ude00"}"#).unwrap();
        assert_eq!(v, v2);
        assert_eq!(v.get("b").unwrap().as_int(), Some(1));
        assert_eq!(v.get("a").unwrap().idx(1).unwrap().as_f64(), Some(2.5));
    }

    #[test]
    fn py_round_matches_python() {
        assert_eq!(py_round(0.30000000000000004, 6), 0.3);
        assert_eq!(py_round(1.0000005, 6), 1.000001_f64); // binary value above tie
        assert_eq!(py_round(2.5, 0), 2.0);
        assert_eq!(py_round(3.5, 0), 4.0); // banker's rounding
    }

    #[test]
    fn pretty_form_matches_python_indent2() {
        let v = parse(r#"{"a":1,"b":[1,2]}"#).unwrap();
        assert_eq!(
            v.to_pretty_string(),
            "{\n  \"a\": 1,\n  \"b\": [\n    1,\n    2\n  ]\n}"
        );
    }
}
