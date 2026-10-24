//! Fed rows read straight into columns, at the shapes their input declares.
//!
//! A fed row is text: a key, and optionally `val=` and `diff=`. Each value is read by its
//! declared shape directly into column builders, so a fed string goes from the wire to its column
//! as bytes, never as a `Value` per byte. The syntax, by shape:
//!
//! - `int`: an integer, `-7` or `+7`.
//! - `float`: a decimal number, `278.6` or `-1e3`.
//! - a tuple: `tuple(a,b)`, and `tuple()` for unit.
//! - a list: `list(a,b)`.
//! - a sum: `inject(lane,payload)`, the lane counted from zero.
//!
//! A whole key or value may also be written without its wrapper: `1,2,3` for a tuple of
//! integers, and nothing or `_` for unit.

use corgi::{Shape, Value as CValue};

use crate::runtime::container::Updates;
use crate::ir::Diff;

/// Rows of an input's key and value shapes, read from text into columns.
pub struct FeedDecoder {
    keys: Col,
    vals: Col,
    diffs: Vec<Diff>,
}

impl FeedDecoder {
    pub fn new(key: &Shape, val: &Shape) -> Self {
        FeedDecoder { keys: Col::new(key), vals: Col::new(val), diffs: Vec::new() }
    }

    /// Append one row, `val` absent meaning unit. On an error the decoder may hold part of the
    /// row; the feed it was reading is refused whole.
    pub fn push(&mut self, key: &str, val: Option<&str>, diff: Diff) -> Result<(), String> {
        let val = val.unwrap_or("");
        self.keys.push_top(key.as_bytes()).map_err(|e| format!("key {key:?}: {e}"))?;
        self.vals.push_top(val.as_bytes()).map_err(|e| format!("val {val:?}: {e}"))?;
        self.diffs.push(diff);
        Ok(())
    }

    /// Append one row written as a line of a framed feed: `<key> [val=<v>] [diff=<d>]`. The
    /// enclosing feed supplies its time, so a line names none.
    pub fn push_line(&mut self, line: &str) -> Result<(), String> {
        let mut args = line.split_whitespace();
        let Some(key) = args.next() else {
            return Err("expected `<key> [val=<v>] [diff=<d>]`".into());
        };
        let (mut val, mut diff) = (None, 1);
        for arg in args {
            if let Some(value) = arg.strip_prefix("val=") {
                val = Some(value);
            } else if let Some(value) = arg.strip_prefix("diff=") {
                diff = value.parse().map_err(|_| format!("diff= must be an integer, got {value:?}"))?;
            } else if arg.starts_with("time=") {
                return Err("time= is not allowed; a feed of several rows takes one time".into());
            } else {
                return Err(format!("unrecognized argument {arg:?}"));
            }
        }
        self.push(key, val, diff)
    }

    pub fn len(&self) -> usize {
        self.diffs.len()
    }

    pub fn is_empty(&self) -> bool {
        self.diffs.is_empty()
    }

    pub fn finish(self) -> Updates {
        Updates { keys: self.keys.finish(), vals: self.vals.finish(), diffs: self.diffs }
    }
}

/// A column under construction, one builder per shape.
enum Col {
    Int(Vec<i64>),
    Float(Vec<f64>),
    Unit(usize),
    Prod(Vec<Col>),
    /// Each row's end in the element column, and the elements.
    List(Vec<usize>, Box<Col>),
    /// Each row's lane, and each lane's rows.
    Sum(Vec<usize>, Vec<Col>),
    /// Rows of a list, written as the list rows they name. Inputs declare none, but a `peek`'s
    /// key is read at its export's key shape, which may hold one.
    Ref(Box<Col>),
}

impl Col {
    fn new(shape: &Shape) -> Col {
        match shape {
            Shape::Int => Col::Int(Vec::new()),
            Shape::Float => Col::Float(Vec::new()),
            Shape::Unit => Col::Unit(0),
            Shape::Prod(fields) => Col::Prod(fields.iter().map(Col::new).collect()),
            Shape::List(elem) => Col::List(Vec::new(), Box::new(Col::new(elem))),
            Shape::Sum(lanes) => Col::Sum(Vec::new(), lanes.iter().map(Col::new).collect()),
            Shape::Ref(list) => Col::Ref(Box::new(Col::new(list))),
        }
    }

    fn rows(&self) -> usize {
        match self {
            Col::Int(xs) => xs.len(),
            Col::Float(xs) => xs.len(),
            Col::Unit(n) => *n,
            Col::Prod(fields) => fields.first().map_or(0, Col::rows),
            Col::List(ends, _) => ends.len(),
            Col::Sum(tags, _) => tags.len(),
            Col::Ref(list) => list.rows(),
        }
    }

    /// Append a whole key or value, whose wrapper may be left off: nothing or `_` for unit, and
    /// `1,2,3` for a tuple of integers.
    fn push_top(&mut self, text: &[u8]) -> Result<(), String> {
        match self {
            Col::Unit(n) if text.is_empty() || text == b"_" => {
                *n += 1;
                return Ok(());
            }
            Col::Prod(fields) if text.iter().all(|&b| b.is_ascii_digit() || matches!(b, b',' | b'-' | b'+')) && fields.iter().all(|f| matches!(f, Col::Int(_))) => {
                let parts = text.split(|&b| b == b',');
                if parts.clone().count() != fields.len() {
                    return Err(format!("expected {} comma-separated integers", fields.len()));
                }
                for (field, part) in fields.iter_mut().zip(parts) {
                    let mut at = Cursor { text: part, at: 0 };
                    field.push(&mut at)?;
                    at.done()?;
                }
                return Ok(());
            }
            _ => {}
        }
        let mut at = Cursor { text, at: 0 };
        self.push(&mut at)?;
        at.done()
    }

    /// Append the value at the cursor.
    fn push(&mut self, at: &mut Cursor) -> Result<(), String> {
        match self {
            Col::Int(xs) => xs.push(at.int()?),
            Col::Float(xs) => xs.push(at.float()?),
            Col::Unit(n) => {
                at.expect(b"tuple()")?;
                *n += 1;
            }
            Col::Prod(fields) => {
                at.expect(b"tuple(")?;
                for (i, field) in fields.iter_mut().enumerate() {
                    if i > 0 { at.expect(b",")? }
                    field.push(at)?;
                }
                at.expect(b")")?;
            }
            Col::List(ends, elems) => {
                at.expect(b"list(")?;
                if !at.eat(b")") {
                    loop {
                        elems.push(at)?;
                        if at.eat(b")") { break }
                        at.expect(b",")?;
                    }
                }
                ends.push(elems.rows());
            }
            Col::Sum(tags, lanes) => {
                at.expect(b"inject(")?;
                let start = at.at;
                let lane = at.int()?;
                let Some(lane) = usize::try_from(lane).ok().filter(|&l| l < lanes.len()) else {
                    at.at = start;
                    return Err(at.expected(&format!("a lane below {}", lanes.len())));
                };
                at.expect(b",")?;
                lanes[lane].push(at)?;
                at.expect(b")")?;
                tags.push(lane);
            }
            Col::Ref(list) => list.push(at)?,
        }
        Ok(())
    }

    fn finish(self) -> CValue {
        match self {
            Col::Int(xs) => CValue::int(xs),
            Col::Float(xs) => CValue::f64(xs),
            Col::Unit(n) => CValue::Unit(n),
            Col::Prod(fields) => CValue::Prod(fields.into_iter().map(Col::finish).collect()),
            Col::List(ends, elems) => CValue::List(ends.into(), Box::new(elems.finish())),
            Col::Sum(tags, lanes) => CValue::sum(tags, lanes.into_iter().map(Col::finish).collect()),
            Col::Ref(list) => {
                let list = list.finish();
                let rows = (0..list.len()).collect();
                CValue::Ref(std::sync::Arc::new(list), std::sync::Arc::new(rows))
            }
        }
    }
}

/// Row `row` of `v` written as a feed writes it, with every wrapper: `tuple(1,list(2,3))`,
/// `tuple()`, `inject(1,7)`. A float is written as Rust writes it, so `NaN` and `inf` do not read
/// back.
pub(crate) fn print(v: &CValue, row: usize) -> String {
    let mut out = String::new();
    write(v, row, &mut out);
    out
}

fn write(v: &CValue, row: usize, out: &mut String) {
    use std::fmt::Write;
    match v {
        CValue::Prim(_) => {
            let one = corgi::arrange::gather(v, &[row]);
            match corgi::arrange::leaf_slice(&one) {
                Some(x) => write!(out, "{}", x[0]).unwrap(),
                None => write!(out, "{:?}", one.as_f64("print").unwrap()[0]).unwrap(),
            }
        }
        CValue::Unit(_) => out.push_str("tuple()"),
        CValue::Prod(fields) => wrapped(out, "tuple(", fields.iter().map(|f| (f, row))),
        CValue::List(bounds, elems) => {
            let end = |i: usize| match bounds { corgi::Bounds::Offsets(ends) => ends[i], corgi::Bounds::Stride(k, _) => (i + 1) * k };
            let start = if row == 0 { 0 } else { end(row - 1) };
            wrapped(out, "list(", (start..end(row)).map(|i| (&**elems, i)))
        }
        CValue::Sum(tags, lanes) => {
            write!(out, "inject({},", tags.tag_at(row)).unwrap();
            write(&lanes[tags.tag_at(row)], tags.offset_at(row), out);
            out.push(')');
        }
        CValue::Ref(arena, rows) => write(arena, rows[row], out),
    }
}

/// `word`, then the rows `parts` names separated by commas, then `)`.
fn wrapped<'a>(out: &mut String, word: &str, parts: impl Iterator<Item = (&'a CValue, usize)>) {
    out.push_str(word);
    for (i, (part, at)) in parts.enumerate() {
        if i > 0 { out.push(',') }
        write(part, at, out);
    }
    out.push(')');
}

struct Cursor<'a> {
    text: &'a [u8],
    at: usize,
}

impl Cursor<'_> {
    fn eat(&mut self, literal: &[u8]) -> bool {
        let found = self.text[self.at..].starts_with(literal);
        if found { self.at += literal.len() }
        found
    }

    fn expect(&mut self, literal: &[u8]) -> Result<(), String> {
        if self.eat(literal) { Ok(()) } else { Err(self.expected(&format!("`{}`", String::from_utf8_lossy(literal)))) }
    }

    /// The error for finding something other than `what` here.
    fn expected(&self, what: &str) -> String {
        match self.text.get(self.at..).filter(|rest| !rest.is_empty()) {
            Some(rest) => format!("expected {what} at byte {}, found `{}`", self.at, String::from_utf8_lossy(rest)),
            None => format!("expected {what} at the end"),
        }
    }

    /// The bytes from here while `more` holds, consumed.
    fn token(&mut self, more: impl Fn(u8) -> bool) -> &[u8] {
        let start = self.at;
        while self.text.get(self.at).is_some_and(|&b| more(b)) { self.at += 1 }
        &self.text[start..self.at]
    }

    fn int(&mut self) -> Result<i64, String> {
        let start = self.at;
        let sign = usize::from(matches!(self.text.get(self.at), Some(b'-' | b'+')));
        self.at += sign;
        let digits = self.token(|b| b.is_ascii_digit()).len();
        let parsed = (digits > 0).then(|| std::str::from_utf8(&self.text[start..self.at]).ok()?.parse().ok()).flatten();
        parsed.ok_or_else(|| { self.at = start; self.expected("an integer") })
    }

    fn float(&mut self) -> Result<f64, String> {
        let start = self.at;
        let token = self.token(|b| b.is_ascii_digit() || matches!(b, b'-' | b'+' | b'.' | b'e' | b'E'));
        let parsed = std::str::from_utf8(token).ok().and_then(|t| t.parse().ok());
        parsed.ok_or_else(|| { self.at = start; self.expected("a number") })
    }

    fn done(&self) -> Result<(), String> {
        if self.at == self.text.len() { Ok(()) } else { Err(self.expected("the end of the value")) }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn int() -> Shape { Shape::Int }
    fn list(s: Shape) -> Shape { Shape::List(Box::new(s)) }

    /// Each row prints with every wrapper, as text that reads back as the row.
    #[test]
    fn rows_print_as_they_read() {
        let node = Shape::Sum(vec![int(), Shape::Prod(vec![int(), int()]), Shape::Unit]);
        let key = Shape::Prod(vec![int(), list(int()), node]);
        let val = Shape::Prod(vec![Shape::Float, list(list(int()))]);
        let rows = [
            ("tuple(1,list(104,105),inject(0,-7))", "tuple(2.5,list(list(1),list()))"),
            ("tuple(2,list(),inject(1,tuple(300,4)))", "tuple(-1000.0,list())"),
            ("tuple(-3,list(255),inject(2,tuple()))", "tuple(0.0,list(list()))"),
        ];
        let mut decoder = FeedDecoder::new(&key, &val);
        for (k, v) in rows { decoder.push(k, Some(v), 1).unwrap() }
        let columns = decoder.finish();
        assert_eq!((corgi::shape_of_value(&columns.keys), corgi::shape_of_value(&columns.vals)), (key, val));
        assert_eq!(columns.rows(), rows.map(|(k, v)| (k.to_string(), v.to_string(), 1)));
    }

    /// An integer may carry a `+` sign, as Rust's integers read: `+1,-2` is `1,-2`.
    #[test]
    fn integers_may_carry_a_plus_sign() {
        let pair = Shape::Prod(vec![int(), int()]);
        let read = |row: &str| {
            let mut decoder = FeedDecoder::new(&pair, &Shape::Unit);
            decoder.push(row, None, 1).unwrap();
            decoder.finish().keys
        };
        assert_eq!(read("+1,-2"), read("1,-2"));
        assert_eq!(read("tuple(+1,+0)"), read("1,0"));
    }

    /// The compact forms read as their wrapped ones do.
    #[test]
    fn wrappers_may_be_left_off() {
        let pair = Shape::Prod(vec![int(), int()]);
        let mut compact = FeedDecoder::new(&pair, &Shape::Unit);
        compact.push("1,-2", None, 1).unwrap();
        compact.push("3,4", Some("_"), 1).unwrap();
        let mut wrapped = FeedDecoder::new(&pair, &Shape::Unit);
        wrapped.push("tuple(1,-2)", Some("tuple()"), 1).unwrap();
        wrapped.push("tuple(3,4)", Some(""), 1).unwrap();
        let (compact, wrapped) = (compact.finish(), wrapped.finish());
        assert_eq!((compact.keys, compact.vals), (wrapped.keys, wrapped.vals));
    }

    /// Text that is not a value of the shape is an error that says where.
    #[test]
    fn a_misfit_is_an_error() {
        let key = Shape::Prod(vec![int(), list(int())]);
        let sum = Shape::Sum(vec![int(), Shape::Unit]);
        let refused = |shape: &Shape, text: &str| FeedDecoder::new(shape, &Shape::Unit).push(text, None, 1).unwrap_err();
        assert_eq!(refused(&key, "tuple(4,list(5,inject(0,6)))"), r#"key "tuple(4,list(5,inject(0,6)))": expected an integer at byte 15, found `inject(0,6)))`"#);
        assert_eq!(refused(&key, "4,5"), r#"key "4,5": expected `tuple(` at byte 0, found `4,5`"#);
        assert_eq!(refused(&key, "tuple(4,list())x"), r#"key "tuple(4,list())x": expected the end of the value at byte 15, found `x`"#);
        assert_eq!(refused(&sum, "inject(2,tuple())"), r#"key "inject(2,tuple())": expected a lane below 2 at byte 7, found `2,tuple())`"#);
        assert_eq!(refused(&Shape::Prod(vec![int(), int()]), "1,2,3"), r#"key "1,2,3": expected 2 comma-separated integers"#);
        assert_eq!(refused(&int(), "tuple(1"), r#"key "tuple(1": expected an integer at byte 0, found `tuple(1`"#);
    }

    /// An empty list has its declared element shape, also in a part of the feed with no elements.
    #[test]
    fn empty_lists_have_the_declared_element_shape() {
        let key = Shape::Prod(vec![list(Shape::Prod(vec![int(), int()])), list(int())]);
        let mut decoder = FeedDecoder::new(&key, &Shape::Unit);
        decoder.push("tuple(list(),list(1))", None, 1).unwrap();
        decoder.push("tuple(list(),list())", None, -1).unwrap();
        let columns = decoder.finish();
        assert_eq!(corgi::shape_of_value(&columns.keys), key);
        assert_eq!(corgi::shape_of_value(&columns.select(&[1]).keys), key);
        assert_eq!(columns.diffs, vec![1, -1]);
    }
}
