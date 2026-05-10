//! Hand-written recursive-descent parser for the query DSL.
//!
//! Grammar (simplified, case-insensitive keywords):
//!
//! ```text
//! query    := SELECT projection_list FROM source [WHERE expr] [GROUP BY col_list] [LIMIT n]
//! projection_list := projection (',' projection)*
//! projection := '*' | aggregate | column [AS alias]
//! aggregate  := func '(' ('*' | column) ')' [AS alias]
//! func       := COUNT | SUM | AVG | MIN | MAX
//! source     := TRACES | METRICS | LOGS
//! expr       := and_expr (OR and_expr)*
//! and_expr   := primary_expr (AND primary_expr)*
//! primary_expr := '(' expr ')' | comparison
//! comparison := column op value
//! op         := '=' | '!=' | '<>' | '<' | '<=' | '>' | '>=' | LIKE
//! col_list   := column (',' column)*
//! column     := [a-zA-Z_][a-zA-Z_0-9.]*
//! value      := string | number
//! string     := '\'' ... '\'' | '"' ... '"'
//! number     := -?[0-9]+ ( '.' [0-9]+ )?
//! ```

use thiserror::Error;

use super::ast::{AggArg, AggFunc, CompareOp, Expr, Projection, SelectStmt, Source, Value};

#[derive(Debug, Error, PartialEq)]
pub enum ParseError {
    #[error("unexpected end of input (expected {0})")]
    UnexpectedEof(&'static str),
    #[error("unexpected token {found:?} at offset {offset} (expected {expected})")]
    Unexpected {
        found: String,
        expected: &'static str,
        offset: usize,
    },
    #[error("unknown aggregate function '{0}'")]
    UnknownAggregate(String),
    #[error("unknown source '{0}', expected traces/metrics/logs")]
    UnknownSource(String),
    #[error("invalid number literal '{0}'")]
    InvalidNumber(String),
    #[error("LIMIT must be a non-negative integer (got '{0}')")]
    InvalidLimit(String),
    #[error("unterminated string literal starting at offset {0}")]
    UnterminatedString(usize),
}

#[derive(Debug, Clone, PartialEq)]
enum Tok {
    Ident(String),
    Number(f64),
    Str(String),
    LParen,
    RParen,
    Comma,
    Star,
    Eq,
    Ne,
    Lt,
    Le,
    Gt,
    Ge,
}

#[derive(Debug, Clone, PartialEq)]
struct Spanned {
    tok: Tok,
    /// Byte offset of the token start in the input string.
    offset: usize,
}

/// Tokenises the input. Whitespace is skipped. Identifiers are kept in their
/// original case; uppercased lookups happen during parsing.
fn tokenize(src: &str) -> Result<Vec<Spanned>, ParseError> {
    let bytes = src.as_bytes();
    let mut toks = Vec::new();
    let mut i = 0usize;

    while i < bytes.len() {
        let c = bytes[i];
        if c.is_ascii_whitespace() {
            i += 1;
            continue;
        }

        let start = i;
        match c {
            b'(' => {
                toks.push(Spanned {
                    tok: Tok::LParen,
                    offset: start,
                });
                i += 1;
            }
            b')' => {
                toks.push(Spanned {
                    tok: Tok::RParen,
                    offset: start,
                });
                i += 1;
            }
            b',' => {
                toks.push(Spanned {
                    tok: Tok::Comma,
                    offset: start,
                });
                i += 1;
            }
            b'*' => {
                toks.push(Spanned {
                    tok: Tok::Star,
                    offset: start,
                });
                i += 1;
            }
            b'=' => {
                toks.push(Spanned {
                    tok: Tok::Eq,
                    offset: start,
                });
                i += 1;
            }
            b'!' if i + 1 < bytes.len() && bytes[i + 1] == b'=' => {
                toks.push(Spanned {
                    tok: Tok::Ne,
                    offset: start,
                });
                i += 2;
            }
            b'<' if i + 1 < bytes.len() && bytes[i + 1] == b'=' => {
                toks.push(Spanned {
                    tok: Tok::Le,
                    offset: start,
                });
                i += 2;
            }
            b'<' if i + 1 < bytes.len() && bytes[i + 1] == b'>' => {
                toks.push(Spanned {
                    tok: Tok::Ne,
                    offset: start,
                });
                i += 2;
            }
            b'<' => {
                toks.push(Spanned {
                    tok: Tok::Lt,
                    offset: start,
                });
                i += 1;
            }
            b'>' if i + 1 < bytes.len() && bytes[i + 1] == b'=' => {
                toks.push(Spanned {
                    tok: Tok::Ge,
                    offset: start,
                });
                i += 2;
            }
            b'>' => {
                toks.push(Spanned {
                    tok: Tok::Gt,
                    offset: start,
                });
                i += 1;
            }
            b'\'' | b'"' => {
                let quote = c;
                i += 1;
                let str_start = i;
                while i < bytes.len() && bytes[i] != quote {
                    i += 1;
                }
                if i >= bytes.len() {
                    return Err(ParseError::UnterminatedString(start));
                }
                let s = std::str::from_utf8(&bytes[str_start..i])
                    .map_err(|_| ParseError::Unexpected {
                        found: "<invalid utf-8>".to_string(),
                        expected: "valid utf-8 string literal",
                        offset: start,
                    })?
                    .to_string();
                i += 1; // consume closing quote
                toks.push(Spanned {
                    tok: Tok::Str(s),
                    offset: start,
                });
            }
            b'-' | b'0'..=b'9' => {
                let mut j = i;
                if bytes[j] == b'-' {
                    j += 1;
                }
                while j < bytes.len() && bytes[j].is_ascii_digit() {
                    j += 1;
                }
                if j < bytes.len() && bytes[j] == b'.' {
                    j += 1;
                    while j < bytes.len() && bytes[j].is_ascii_digit() {
                        j += 1;
                    }
                }
                let txt =
                    std::str::from_utf8(&bytes[i..j]).map_err(|_| ParseError::Unexpected {
                        found: "<invalid utf-8>".to_string(),
                        expected: "number literal",
                        offset: start,
                    })?;
                // Reject lone '-' (caller likely meant something else).
                if txt == "-" {
                    return Err(ParseError::Unexpected {
                        found: "-".to_string(),
                        expected: "number literal",
                        offset: start,
                    });
                }
                let n: f64 = txt
                    .parse()
                    .map_err(|_| ParseError::InvalidNumber(txt.to_string()))?;
                toks.push(Spanned {
                    tok: Tok::Number(n),
                    offset: start,
                });
                i = j;
            }
            b if (b.is_ascii_alphabetic() || b == b'_') => {
                let mut j = i;
                while j < bytes.len()
                    && (bytes[j].is_ascii_alphanumeric() || bytes[j] == b'_' || bytes[j] == b'.')
                {
                    j += 1;
                }
                let txt =
                    std::str::from_utf8(&bytes[i..j]).map_err(|_| ParseError::Unexpected {
                        found: "<invalid utf-8>".to_string(),
                        expected: "identifier",
                        offset: start,
                    })?;
                toks.push(Spanned {
                    tok: Tok::Ident(txt.to_string()),
                    offset: start,
                });
                i = j;
            }
            other => {
                return Err(ParseError::Unexpected {
                    found: (other as char).to_string(),
                    expected: "valid token",
                    offset: start,
                });
            }
        }
    }

    Ok(toks)
}

/// Parser cursor. Pull tokens from the front; record offsets for diagnostics.
struct Parser {
    toks: Vec<Spanned>,
    pos: usize,
    end_offset: usize,
}

impl Parser {
    fn new(toks: Vec<Spanned>, end_offset: usize) -> Self {
        Parser {
            toks,
            pos: 0,
            end_offset,
        }
    }

    fn peek(&self) -> Option<&Spanned> {
        self.toks.get(self.pos)
    }

    fn bump(&mut self) -> Option<Spanned> {
        let s = self.toks.get(self.pos).cloned();
        if s.is_some() {
            self.pos += 1;
        }
        s
    }

    fn current_offset(&self) -> usize {
        self.peek().map(|s| s.offset).unwrap_or(self.end_offset)
    }

    /// Returns the identifier at the cursor if it matches `kw` (case-insensitive)
    /// without consuming it.
    fn peek_kw(&self, kw: &str) -> bool {
        matches!(self.peek(), Some(s) if matches!(&s.tok, Tok::Ident(name) if name.eq_ignore_ascii_case(kw)))
    }

    /// Consume the cursor if it is a keyword that matches `kw`.
    fn eat_kw(&mut self, kw: &str) -> bool {
        if self.peek_kw(kw) {
            self.pos += 1;
            return true;
        }
        false
    }

    fn expect_kw(&mut self, kw: &'static str) -> Result<(), ParseError> {
        if self.eat_kw(kw) {
            return Ok(());
        }
        match self.peek() {
            Some(s) => Err(ParseError::Unexpected {
                found: format!("{:?}", s.tok),
                expected: kw,
                offset: s.offset,
            }),
            None => Err(ParseError::UnexpectedEof(kw)),
        }
    }
}

/// Parses an entire SELECT statement. Trailing tokens are an error.
pub fn parse(src: &str) -> Result<SelectStmt, ParseError> {
    let toks = tokenize(src)?;
    let end_offset = src.len();
    let mut p = Parser::new(toks, end_offset);

    let stmt = parse_select(&mut p)?;
    if let Some(remaining) = p.peek() {
        return Err(ParseError::Unexpected {
            found: format!("{:?}", remaining.tok),
            expected: "end of input",
            offset: remaining.offset,
        });
    }
    Ok(stmt)
}

fn parse_select(p: &mut Parser) -> Result<SelectStmt, ParseError> {
    p.expect_kw("SELECT")?;

    let projections = parse_projection_list(p)?;

    p.expect_kw("FROM")?;
    let from = parse_source(p)?;

    let where_clause = if p.eat_kw("WHERE") {
        Some(parse_or_expr(p)?)
    } else {
        None
    };

    let group_by = if p.eat_kw("GROUP") {
        p.expect_kw("BY")?;
        parse_column_list(p)?
    } else {
        Vec::new()
    };

    let limit = if p.eat_kw("LIMIT") {
        match p.bump() {
            Some(Spanned {
                tok: Tok::Number(n),
                ..
            }) if n >= 0.0 && n.fract() == 0.0 => Some(n as usize),
            Some(s) => {
                return Err(ParseError::InvalidLimit(format!("{:?}", s.tok)));
            }
            None => return Err(ParseError::UnexpectedEof("LIMIT integer")),
        }
    } else {
        None
    };

    Ok(SelectStmt {
        projections,
        from,
        where_clause,
        group_by,
        limit,
    })
}

fn parse_projection_list(p: &mut Parser) -> Result<Vec<Projection>, ParseError> {
    let mut out = vec![parse_projection(p)?];
    while matches!(p.peek().map(|s| &s.tok), Some(Tok::Comma)) {
        p.bump();
        out.push(parse_projection(p)?);
    }
    Ok(out)
}

fn parse_projection(p: &mut Parser) -> Result<Projection, ParseError> {
    if matches!(p.peek().map(|s| &s.tok), Some(Tok::Star)) {
        p.bump();
        return Ok(Projection::Star);
    }

    let Some(first) = p.peek().cloned() else {
        return Err(ParseError::UnexpectedEof("projection"));
    };
    let Tok::Ident(name) = first.tok.clone() else {
        return Err(ParseError::Unexpected {
            found: format!("{:?}", first.tok),
            expected: "column or aggregate name",
            offset: first.offset,
        });
    };

    // Look ahead for `(` to disambiguate aggregate vs. column.
    let next = p.toks.get(p.pos + 1);
    if matches!(next.map(|s| &s.tok), Some(Tok::LParen)) {
        p.bump(); // function name
        p.bump(); // (
        let func = parse_agg_func(&name).ok_or(ParseError::UnknownAggregate(name.clone()))?;
        let arg = match p.peek().map(|s| &s.tok) {
            Some(Tok::Star) => {
                p.bump();
                AggArg::Star
            }
            Some(Tok::Ident(col)) => {
                let col = col.clone();
                p.bump();
                AggArg::Column(col)
            }
            Some(other) => {
                return Err(ParseError::Unexpected {
                    found: format!("{other:?}"),
                    expected: "* or column inside aggregate",
                    offset: p.current_offset(),
                });
            }
            None => return Err(ParseError::UnexpectedEof("aggregate argument")),
        };
        match p.bump() {
            Some(Spanned {
                tok: Tok::RParen, ..
            }) => {}
            Some(s) => {
                return Err(ParseError::Unexpected {
                    found: format!("{:?}", s.tok),
                    expected: "')'",
                    offset: s.offset,
                });
            }
            None => return Err(ParseError::UnexpectedEof("')'")),
        };

        let alias = parse_optional_alias(p)?.unwrap_or_else(|| match &arg {
            AggArg::Star => format!("{}(*)", func.as_str()),
            AggArg::Column(c) => format!("{}({})", func.as_str(), c),
        });

        Ok(Projection::Aggregate { func, arg, alias })
    } else {
        p.bump();
        let alias_offset = p.current_offset();
        if let Some(alias) = parse_optional_alias(p)? {
            return Err(ParseError::Unexpected {
                found: alias,
                expected: "no alias for plain column",
                offset: alias_offset,
            });
        }
        Ok(Projection::Column(name))
    }
}

fn parse_optional_alias(p: &mut Parser) -> Result<Option<String>, ParseError> {
    if !p.eat_kw("AS") {
        return Ok(None);
    }
    match p.bump() {
        Some(Spanned {
            tok: Tok::Ident(name),
            ..
        }) => Ok(Some(name)),
        Some(Spanned {
            tok: Tok::Str(s), ..
        }) => Ok(Some(s)),
        Some(s) => Err(ParseError::Unexpected {
            found: format!("{:?}", s.tok),
            expected: "alias identifier",
            offset: s.offset,
        }),
        None => Err(ParseError::UnexpectedEof("alias identifier")),
    }
}

fn parse_agg_func(name: &str) -> Option<AggFunc> {
    match name.to_ascii_lowercase().as_str() {
        "count" => Some(AggFunc::Count),
        "sum" => Some(AggFunc::Sum),
        "avg" => Some(AggFunc::Avg),
        "min" => Some(AggFunc::Min),
        "max" => Some(AggFunc::Max),
        _ => None,
    }
}

fn parse_source(p: &mut Parser) -> Result<Source, ParseError> {
    match p.bump() {
        Some(Spanned {
            tok: Tok::Ident(name),
            ..
        }) => match name.to_ascii_lowercase().as_str() {
            "traces" => Ok(Source::Traces),
            "metrics" => Ok(Source::Metrics),
            "logs" => Ok(Source::Logs),
            other => Err(ParseError::UnknownSource(other.to_string())),
        },
        Some(s) => Err(ParseError::Unexpected {
            found: format!("{:?}", s.tok),
            expected: "source (traces, metrics, logs)",
            offset: s.offset,
        }),
        None => Err(ParseError::UnexpectedEof("source name")),
    }
}

fn parse_column_list(p: &mut Parser) -> Result<Vec<String>, ParseError> {
    let mut out = vec![parse_column_name(p)?];
    while matches!(p.peek().map(|s| &s.tok), Some(Tok::Comma)) {
        p.bump();
        out.push(parse_column_name(p)?);
    }
    Ok(out)
}

fn parse_column_name(p: &mut Parser) -> Result<String, ParseError> {
    match p.bump() {
        Some(Spanned {
            tok: Tok::Ident(s), ..
        }) => Ok(s),
        Some(s) => Err(ParseError::Unexpected {
            found: format!("{:?}", s.tok),
            expected: "column name",
            offset: s.offset,
        }),
        None => Err(ParseError::UnexpectedEof("column name")),
    }
}

fn parse_or_expr(p: &mut Parser) -> Result<Expr, ParseError> {
    let mut left = parse_and_expr(p)?;
    while p.eat_kw("OR") {
        let right = parse_and_expr(p)?;
        left = Expr::Or(Box::new(left), Box::new(right));
    }
    Ok(left)
}

fn parse_and_expr(p: &mut Parser) -> Result<Expr, ParseError> {
    let mut left = parse_primary_expr(p)?;
    while p.eat_kw("AND") {
        let right = parse_primary_expr(p)?;
        left = Expr::And(Box::new(left), Box::new(right));
    }
    Ok(left)
}

fn parse_primary_expr(p: &mut Parser) -> Result<Expr, ParseError> {
    if matches!(p.peek().map(|s| &s.tok), Some(Tok::LParen)) {
        p.bump();
        let inner = parse_or_expr(p)?;
        match p.bump() {
            Some(Spanned {
                tok: Tok::RParen, ..
            }) => Ok(inner),
            Some(s) => Err(ParseError::Unexpected {
                found: format!("{:?}", s.tok),
                expected: "')'",
                offset: s.offset,
            }),
            None => Err(ParseError::UnexpectedEof("')'")),
        }
    } else {
        parse_comparison(p)
    }
}

fn parse_comparison(p: &mut Parser) -> Result<Expr, ParseError> {
    let column = parse_column_name(p)?;
    let op = parse_compare_op(p)?;
    let value = parse_value(p)?;
    Ok(Expr::Compare { column, op, value })
}

fn parse_compare_op(p: &mut Parser) -> Result<CompareOp, ParseError> {
    if p.eat_kw("LIKE") {
        return Ok(CompareOp::Like);
    }
    let s = p
        .bump()
        .ok_or(ParseError::UnexpectedEof("comparison operator"))?;
    match s.tok {
        Tok::Eq => Ok(CompareOp::Eq),
        Tok::Ne => Ok(CompareOp::Ne),
        Tok::Lt => Ok(CompareOp::Lt),
        Tok::Le => Ok(CompareOp::Le),
        Tok::Gt => Ok(CompareOp::Gt),
        Tok::Ge => Ok(CompareOp::Ge),
        other => Err(ParseError::Unexpected {
            found: format!("{other:?}"),
            expected: "comparison operator (=, !=, <, <=, >, >=, LIKE)",
            offset: s.offset,
        }),
    }
}

fn parse_value(p: &mut Parser) -> Result<Value, ParseError> {
    match p.bump() {
        Some(Spanned {
            tok: Tok::Str(s), ..
        }) => Ok(Value::Str(s)),
        Some(Spanned {
            tok: Tok::Number(n),
            ..
        }) => Ok(Value::Number(n)),
        Some(Spanned {
            tok: Tok::Ident(name),
            offset,
        }) => Err(ParseError::Unexpected {
            found: name,
            expected: "string or number literal",
            offset,
        }),
        Some(s) => Err(ParseError::Unexpected {
            found: format!("{:?}", s.tok),
            expected: "literal value",
            offset: s.offset,
        }),
        None => Err(ParseError::UnexpectedEof("literal value")),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_select_star_from_traces() {
        let stmt = parse("SELECT * FROM traces").unwrap();
        assert_eq!(stmt.from, Source::Traces);
        assert_eq!(stmt.projections, vec![Projection::Star]);
        assert!(stmt.where_clause.is_none());
        assert!(stmt.group_by.is_empty());
        assert!(stmt.limit.is_none());
    }

    #[test]
    fn parse_count_star() {
        let stmt = parse("SELECT count(*) FROM metrics").unwrap();
        assert_eq!(stmt.from, Source::Metrics);
        assert_eq!(
            stmt.projections,
            vec![Projection::Aggregate {
                func: AggFunc::Count,
                arg: AggArg::Star,
                alias: "count(*)".to_string(),
            }]
        );
    }

    #[test]
    fn parse_multiple_projections_with_aggregate_and_group_by() {
        let stmt = parse("select count(*), service_name from logs group by service_name").unwrap();
        assert_eq!(stmt.from, Source::Logs);
        assert_eq!(stmt.projections.len(), 2);
        assert!(matches!(
            stmt.projections[0],
            Projection::Aggregate {
                func: AggFunc::Count,
                ..
            }
        ));
        assert_eq!(
            stmt.projections[1],
            Projection::Column("service_name".to_string())
        );
        assert_eq!(stmt.group_by, vec!["service_name".to_string()]);
    }

    #[test]
    fn parse_where_and_limit() {
        let stmt = parse(
            "SELECT count(*) FROM traces WHERE service_name = 'api' AND duration_ms > 100 LIMIT 10",
        )
        .unwrap();
        let where_clause = stmt.where_clause.unwrap();
        let Expr::And(left, right) = where_clause else {
            panic!("expected AND expression");
        };
        assert_eq!(
            *left,
            Expr::Compare {
                column: "service_name".to_string(),
                op: CompareOp::Eq,
                value: Value::Str("api".to_string()),
            }
        );
        assert_eq!(
            *right,
            Expr::Compare {
                column: "duration_ms".to_string(),
                op: CompareOp::Gt,
                value: Value::Number(100.0),
            }
        );
        assert_eq!(stmt.limit, Some(10));
    }

    #[test]
    fn parse_or_with_parens() {
        let stmt = parse(
            "SELECT service_name FROM traces WHERE (service_name = 'a' OR service_name = 'b') AND duration_ms >= 50",
        )
        .unwrap();
        let where_clause = stmt.where_clause.unwrap();
        // The top-level should be AND because of operator precedence.
        let Expr::And(left, right) = where_clause else {
            panic!("expected top-level AND");
        };
        let Expr::Or(_, _) = *left else {
            panic!("expected OR on left side");
        };
        assert_eq!(
            *right,
            Expr::Compare {
                column: "duration_ms".to_string(),
                op: CompareOp::Ge,
                value: Value::Number(50.0),
            }
        );
    }

    #[test]
    fn parse_avg_sum_with_alias() {
        let stmt =
            parse("SELECT avg(duration_ms) AS avg_dur, sum(duration_ms) AS total FROM traces")
                .unwrap();
        assert_eq!(
            stmt.projections,
            vec![
                Projection::Aggregate {
                    func: AggFunc::Avg,
                    arg: AggArg::Column("duration_ms".to_string()),
                    alias: "avg_dur".to_string(),
                },
                Projection::Aggregate {
                    func: AggFunc::Sum,
                    arg: AggArg::Column("duration_ms".to_string()),
                    alias: "total".to_string(),
                },
            ]
        );
    }

    #[test]
    fn parse_reject_unknown_source() {
        let err = parse("SELECT * FROM events").unwrap_err();
        assert_eq!(err, ParseError::UnknownSource("events".to_string()));
    }

    #[test]
    fn parse_reject_missing_from() {
        let err = parse("SELECT *").unwrap_err();
        assert_eq!(err, ParseError::UnexpectedEof("FROM"));
    }

    #[test]
    fn parse_reject_unknown_aggregate() {
        let err = parse("SELECT median(duration_ms) FROM traces").unwrap_err();
        assert_eq!(err, ParseError::UnknownAggregate("median".to_string()));
    }

    #[test]
    fn parse_reject_trailing_garbage() {
        let err = parse("SELECT * FROM traces garbage").unwrap_err();
        assert!(matches!(err, ParseError::Unexpected { .. }));
    }

    #[test]
    fn parse_string_with_double_quotes() {
        let stmt = parse(r#"SELECT * FROM logs WHERE service_name = "api""#).unwrap();
        let Expr::Compare { value, .. } = stmt.where_clause.unwrap() else {
            panic!("expected comparison");
        };
        assert_eq!(value, Value::Str("api".to_string()));
    }

    #[test]
    fn parse_negative_number() {
        let stmt = parse("SELECT * FROM metrics WHERE duration_ms > -1").unwrap();
        let Expr::Compare { value, .. } = stmt.where_clause.unwrap() else {
            panic!("expected comparison");
        };
        assert_eq!(value, Value::Number(-1.0));
    }

    #[test]
    fn parse_ne_operators() {
        for op in ["!=", "<>"] {
            let sql = format!("SELECT * FROM traces WHERE service_name {op} 'x'");
            let stmt = parse(&sql).unwrap();
            let Expr::Compare { op: parsed_op, .. } = stmt.where_clause.unwrap() else {
                panic!("expected comparison for {sql}");
            };
            assert_eq!(parsed_op, CompareOp::Ne, "{sql}");
        }
    }

    #[test]
    fn parse_like_operator() {
        let stmt = parse("SELECT * FROM logs WHERE service_name LIKE 'api%'").unwrap();
        let Expr::Compare { op, .. } = stmt.where_clause.unwrap() else {
            panic!("expected comparison");
        };
        assert_eq!(op, CompareOp::Like);
    }

    #[test]
    fn parse_unterminated_string() {
        let err = parse("SELECT * FROM traces WHERE service_name = 'oops").unwrap_err();
        assert!(matches!(err, ParseError::UnterminatedString(_)));
    }

    #[test]
    fn parse_reject_alias_on_plain_column() {
        let err = parse("SELECT service_name AS svc FROM traces").unwrap_err();
        assert!(
            matches!(err, ParseError::Unexpected { expected, .. } if expected == "no alias for plain column"),
            "expected alias rejection, got: {err:?}"
        );
    }
}
