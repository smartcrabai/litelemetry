//! In-memory executor for the query DSL.
//!
//! Given a `SelectStmt` and an iterator of `NormalizedEntry` rows, evaluate
//! the statement and produce a result table (columns + rows).
//!
//! Design notes:
//!   * The executor pulls a small set of derived fields from each entry
//!     (service_name, observed_at_ms, signal, payload text, duration_ms).
//!   * `WHERE` is evaluated row-by-row.
//!   * Aggregations are computed eagerly. When `GROUP BY` is omitted but
//!     aggregates are present, every input row collapses into a single output
//!     row.
//!   * `LIMIT` clamps the final result set after grouping.

use std::collections::BTreeMap;

use serde::Serialize;
use serde_json::Value as JsonValue;
use thiserror::Error;

use super::ast::{AggArg, AggFunc, CompareOp, Expr, Projection, SelectStmt, Source, Value};
use crate::domain::telemetry::{NormalizedEntry, Signal};
use crate::ingest::otlp_pb::payload_as_value;
use crate::viewer_runtime::compiler::extract_searchable_payload_text;

/// Hard cap on the number of rows returned to clients.
pub const MAX_RESULT_ROWS: usize = 10_000;

#[derive(Debug, Error)]
pub enum ExecuteError {
    #[error("unknown column '{0}'")]
    UnknownColumn(String),
    #[error("GROUP BY column '{0}' must also appear in SELECT")]
    GroupByNotInSelect(String),
    #[error("non-aggregate column '{0}' must appear in GROUP BY")]
    NonAggregateNotInGroupBy(String),
}

/// A single value rendered into a JSON-friendly form.
#[derive(Debug, Clone, PartialEq, Serialize)]
#[serde(untagged)]
pub enum Cell {
    Null,
    Bool(bool),
    Number(f64),
    String(String),
}

impl Cell {
    fn from_field(value: &FieldValue) -> Cell {
        match value {
            FieldValue::Null => Cell::Null,
            FieldValue::Str(s) => Cell::String(s.clone()),
            FieldValue::Number(n) => Cell::Number(*n),
        }
    }
}

/// Result of executing a statement.
#[derive(Debug, Clone, Serialize)]
pub struct QueryResult {
    pub columns: Vec<String>,
    pub rows: Vec<Vec<Cell>>,
    pub scanned: usize,
    pub matched: usize,
    pub returned: usize,
    pub truncated: bool,
}

/// Internal field representation used while evaluating a row.
#[derive(Debug, Clone)]
enum FieldValue {
    Null,
    Str(String),
    Number(f64),
}

impl FieldValue {
    fn as_number(&self) -> Option<f64> {
        match self {
            FieldValue::Number(n) => Some(*n),
            FieldValue::Str(s) => s.parse::<f64>().ok(),
            FieldValue::Null => None,
        }
    }

    fn group_key(&self) -> String {
        // BTreeMap keys must be stable. Prefix with a tag to avoid
        // collisions across types ("1" string vs 1 number).
        match self {
            FieldValue::Null => "n".to_string(),
            FieldValue::Number(n) => format!("#{n}"),
            FieldValue::Str(s) => format!("s{s}"),
        }
    }
}

/// Source signal selected by the FROM clause.
pub fn source_to_signal(source: Source) -> Signal {
    match source {
        Source::Traces => Signal::Traces,
        Source::Metrics => Signal::Metrics,
        Source::Logs => Signal::Logs,
    }
}

/// Recognised columns. Anything else is an error during validation.
fn known_columns() -> &'static [&'static str] {
    &[
        "service_name",
        "signal",
        "observed_at_ms",
        "duration_ms",
        "payload",
    ]
}

/// Validate that every referenced column is known.
fn validate_columns(stmt: &SelectStmt) -> Result<(), ExecuteError> {
    fn check(col: &str) -> Result<(), ExecuteError> {
        if known_columns().iter().any(|k| k.eq_ignore_ascii_case(col)) {
            Ok(())
        } else {
            Err(ExecuteError::UnknownColumn(col.to_string()))
        }
    }
    for p in &stmt.projections {
        match p {
            Projection::Star => {}
            Projection::Column(c) => check(c)?,
            Projection::Aggregate {
                arg: AggArg::Column(c),
                ..
            } => check(c)?,
            Projection::Aggregate {
                arg: AggArg::Star, ..
            } => {}
        }
    }
    if let Some(expr) = &stmt.where_clause {
        validate_expr_columns(expr)?;
    }
    for c in &stmt.group_by {
        check(c)?;
    }
    Ok(())
}

fn validate_expr_columns(expr: &Expr) -> Result<(), ExecuteError> {
    match expr {
        Expr::Compare { column, .. } => {
            if !known_columns()
                .iter()
                .any(|k| k.eq_ignore_ascii_case(column))
            {
                return Err(ExecuteError::UnknownColumn(column.clone()));
            }
            Ok(())
        }
        Expr::And(a, b) | Expr::Or(a, b) => {
            validate_expr_columns(a)?;
            validate_expr_columns(b)
        }
    }
}

/// Validate group-by semantics: every plain Column projection must appear in
/// `GROUP BY` whenever the SELECT list contains aggregates, and every
/// group-by column must appear in the projections.
fn validate_group_by(stmt: &SelectStmt) -> Result<(), ExecuteError> {
    let has_aggregate = stmt
        .projections
        .iter()
        .any(|p| matches!(p, Projection::Aggregate { .. }));
    let select_cols: Vec<String> = stmt
        .projections
        .iter()
        .filter_map(|p| match p {
            Projection::Column(c) => Some(c.to_lowercase()),
            _ => None,
        })
        .collect();
    let group_set: Vec<String> = stmt.group_by.iter().map(|s| s.to_lowercase()).collect();

    if has_aggregate {
        for c in &select_cols {
            if !group_set.contains(c) {
                return Err(ExecuteError::NonAggregateNotInGroupBy(c.clone()));
            }
        }
    }
    for c in &group_set {
        if !select_cols.contains(c) {
            return Err(ExecuteError::GroupByNotInSelect(c.clone()));
        }
    }
    Ok(())
}

/// Pull out a single field from a row.
fn field_value(entry: &NormalizedEntry, name: &str) -> FieldValue {
    match name.to_ascii_lowercase().as_str() {
        "service_name" => entry
            .service_name
            .clone()
            .map(FieldValue::Str)
            .unwrap_or(FieldValue::Null),
        "signal" => FieldValue::Str(
            match entry.signal {
                Signal::Traces => "traces",
                Signal::Metrics => "metrics",
                Signal::Logs => "logs",
            }
            .to_string(),
        ),
        "observed_at_ms" => FieldValue::Number(entry.observed_at.timestamp_millis() as f64),
        "duration_ms" => extract_duration_ms(entry)
            .map(FieldValue::Number)
            .unwrap_or(FieldValue::Null),
        "payload" => extract_searchable_payload_text(entry.signal, &entry.payload)
            .map(FieldValue::Str)
            .unwrap_or(FieldValue::Null),
        _ => FieldValue::Null,
    }
}

/// Best-effort extraction of a duration in milliseconds from a trace payload.
/// For traces, returns the maximum span duration found in the payload.
/// For metrics/logs, returns `None`.
fn extract_duration_ms(entry: &NormalizedEntry) -> Option<f64> {
    if entry.signal != Signal::Traces {
        return None;
    }
    let value = payload_as_value(entry.signal, &entry.payload)?;
    let resource_spans = value.get("resourceSpans")?.as_array()?;
    let mut max_ms: Option<f64> = None;
    for rs in resource_spans {
        let scope_spans = rs.get("scopeSpans").and_then(|v| v.as_array());
        let Some(scope_spans) = scope_spans else {
            continue;
        };
        for ss in scope_spans {
            let spans = ss.get("spans").and_then(|v| v.as_array());
            let Some(spans) = spans else { continue };
            for span in spans {
                let Some(start) = parse_unix_nano(span.get("startTimeUnixNano")) else {
                    continue;
                };
                let Some(end) = parse_unix_nano(span.get("endTimeUnixNano")) else {
                    continue;
                };
                if end >= start {
                    let dur_ms = (end - start) as f64 / 1_000_000.0;
                    max_ms = Some(max_ms.map_or(dur_ms, |prev| prev.max(dur_ms)));
                }
            }
        }
    }
    max_ms
}

/// OTLP encodes UnixNano fields as either string or number; accept both.
fn parse_unix_nano(v: Option<&JsonValue>) -> Option<u64> {
    let v = v?;
    if let Some(n) = v.as_u64() {
        return Some(n);
    }
    if let Some(s) = v.as_str() {
        return s.parse::<u64>().ok();
    }
    if let Some(f) = v.as_f64()
        && f >= 0.0
        && f.is_finite()
    {
        return Some(f as u64);
    }
    None
}

/// Evaluate the WHERE clause against a single entry.
fn eval_where(expr: &Expr, entry: &NormalizedEntry) -> bool {
    match expr {
        Expr::And(a, b) => eval_where(a, entry) && eval_where(b, entry),
        Expr::Or(a, b) => eval_where(a, entry) || eval_where(b, entry),
        Expr::Compare { column, op, value } => {
            let lhs = field_value(entry, column);
            eval_compare(&lhs, *op, value)
        }
    }
}

fn eval_compare(lhs: &FieldValue, op: CompareOp, rhs: &Value) -> bool {
    match (lhs, rhs) {
        (FieldValue::Null, _) => matches!(op, CompareOp::Ne),
        (FieldValue::Number(a), Value::Number(b)) => match op {
            CompareOp::Eq => a == b,
            CompareOp::Ne => a != b,
            CompareOp::Lt => a < b,
            CompareOp::Le => a <= b,
            CompareOp::Gt => a > b,
            CompareOp::Ge => a >= b,
            CompareOp::Like => false,
        },
        (FieldValue::Number(a), Value::Str(b)) => match b.parse::<f64>() {
            Ok(b) => eval_compare(&FieldValue::Number(*a), op, &Value::Number(b)),
            Err(_) => match op {
                CompareOp::Eq => false,
                CompareOp::Ne => true,
                _ => false,
            },
        },
        (FieldValue::Str(a), Value::Str(b)) => match op {
            CompareOp::Eq => a.eq_ignore_ascii_case(b),
            CompareOp::Ne => !a.eq_ignore_ascii_case(b),
            CompareOp::Lt => a < b,
            CompareOp::Le => a <= b,
            CompareOp::Gt => a > b,
            CompareOp::Ge => a >= b,
            CompareOp::Like => like_match(a, b),
        },
        (FieldValue::Str(a), Value::Number(b)) => match a.parse::<f64>() {
            Ok(a) => eval_compare(&FieldValue::Number(a), op, &Value::Number(*b)),
            Err(_) => match op {
                CompareOp::Eq => false,
                CompareOp::Ne => true,
                _ => false,
            },
        },
        _ => false,
    }
}

/// Trivial SQL-style LIKE: `%` matches zero-or-more chars, `_` matches one
/// char. Anchored to both ends. Case-insensitive.
fn like_match(haystack: &str, pattern: &str) -> bool {
    let h: Vec<char> = haystack.to_lowercase().chars().collect();
    let p: Vec<char> = pattern.to_lowercase().chars().collect();
    like_match_inner(&h, 0, &p, 0)
}

fn like_match_inner(h: &[char], hi: usize, p: &[char], pi: usize) -> bool {
    if pi == p.len() {
        return hi == h.len();
    }
    match p[pi] {
        '%' => {
            // Try consuming 0..=remaining haystack characters.
            for skip in hi..=h.len() {
                if like_match_inner(h, skip, p, pi + 1) {
                    return true;
                }
            }
            false
        }
        '_' => hi < h.len() && like_match_inner(h, hi + 1, p, pi + 1),
        c => hi < h.len() && h[hi] == c && like_match_inner(h, hi + 1, p, pi + 1),
    }
}

/// Shape of a single accumulator slot for an aggregate.
#[derive(Debug, Default, Clone)]
struct AggAccum {
    count: u64,
    sum: f64,
    min: Option<f64>,
    max: Option<f64>,
    /// Number of rows that contributed a numeric value (vs. null).
    numeric_count: u64,
}

impl AggAccum {
    fn observe_count(&mut self) {
        self.count += 1;
    }

    fn observe_value(&mut self, v: f64) {
        self.numeric_count += 1;
        self.sum += v;
        self.min = Some(self.min.map_or(v, |prev| prev.min(v)));
        self.max = Some(self.max.map_or(v, |prev| prev.max(v)));
    }

    fn render(&self, func: AggFunc) -> Cell {
        match func {
            AggFunc::Count => Cell::Number(self.count as f64),
            AggFunc::Sum => Cell::Number(self.sum),
            AggFunc::Avg => {
                if self.numeric_count == 0 {
                    Cell::Null
                } else {
                    Cell::Number(self.sum / self.numeric_count as f64)
                }
            }
            AggFunc::Min => self.min.map(Cell::Number).unwrap_or(Cell::Null),
            AggFunc::Max => self.max.map(Cell::Number).unwrap_or(Cell::Null),
        }
    }
}

/// Per-group accumulator state. One slot per aggregate projection.
#[derive(Debug, Clone)]
struct GroupAccum {
    /// Concrete values for the GROUP BY columns (in the order they appear in
    /// the SELECT list -- used to render the row).
    keys: BTreeMap<String, FieldValue>,
    /// Aggregate slots, indexed by their position in the projection list.
    aggs: Vec<AggAccum>,
}

/// Public entry point.
pub fn execute<I>(stmt: &SelectStmt, entries: I) -> Result<QueryResult, ExecuteError>
where
    I: IntoIterator<Item = NormalizedEntry>,
{
    validate_columns(stmt)?;
    validate_group_by(stmt)?;

    let target_signal = source_to_signal(stmt.from);
    let where_clause = stmt.where_clause.as_ref();

    // Determine the column layout for the output table.
    let columns = output_columns(stmt);

    let has_aggregates = stmt
        .projections
        .iter()
        .any(|p| matches!(p, Projection::Aggregate { .. }));
    let group_by = &stmt.group_by;
    let limit = stmt.limit;

    let mut scanned: usize = 0;
    let mut matched: usize = 0;

    if !has_aggregates {
        // Plain SELECT path -- collect rows directly.
        let mut rows: Vec<Vec<Cell>> = Vec::new();
        let mut truncated = false;
        for entry in entries.into_iter() {
            scanned += 1;
            if entry.signal != target_signal {
                continue;
            }
            if let Some(expr) = where_clause
                && !eval_where(expr, &entry)
            {
                continue;
            }
            matched += 1;
            let limit_for_collect = limit.unwrap_or(MAX_RESULT_ROWS).min(MAX_RESULT_ROWS);
            if rows.len() >= limit_for_collect {
                truncated = true;
                continue;
            }
            rows.push(render_plain_row(stmt, &entry));
        }

        let returned = rows.len();
        return Ok(QueryResult {
            columns,
            rows,
            scanned,
            matched,
            returned,
            truncated,
        });
    }

    // Aggregate path.
    let mut groups: BTreeMap<String, GroupAccum> = BTreeMap::new();
    // Cache the aggregate projection indices and their column args.
    let agg_specs: Vec<(usize, AggFunc, AggArg)> = stmt
        .projections
        .iter()
        .enumerate()
        .filter_map(|(i, p)| match p {
            Projection::Aggregate { func, arg, .. } => Some((i, *func, arg.clone())),
            _ => None,
        })
        .collect();
    let agg_count = agg_specs.len();

    for entry in entries.into_iter() {
        scanned += 1;
        if entry.signal != target_signal {
            continue;
        }
        if let Some(expr) = where_clause
            && !eval_where(expr, &entry)
        {
            continue;
        }
        matched += 1;

        let group_key = if group_by.is_empty() {
            String::new()
        } else {
            group_by
                .iter()
                .map(|c| field_value(&entry, c).group_key())
                .collect::<Vec<_>>()
                .join("\x1f")
        };

        let slot = groups.entry(group_key).or_insert_with(|| GroupAccum {
            keys: group_by
                .iter()
                .map(|c| (c.to_lowercase(), field_value(&entry, c)))
                .collect(),
            aggs: vec![AggAccum::default(); agg_count],
        });

        for (idx, (_, _, arg)) in agg_specs.iter().enumerate() {
            match arg {
                AggArg::Star => slot.aggs[idx].observe_count(),
                AggArg::Column(c) => {
                    slot.aggs[idx].observe_count();
                    if let Some(n) = field_value(&entry, c).as_number() {
                        slot.aggs[idx].observe_value(n);
                    }
                }
            }
        }
    }

    // SQL semantics: an aggregate over zero matching rows still returns a
    // single row when there is no GROUP BY (e.g. `SELECT count(*) FROM ...`
    // returns 0 rather than an empty result).
    if groups.is_empty() && group_by.is_empty() {
        groups.insert(
            String::new(),
            GroupAccum {
                keys: BTreeMap::new(),
                aggs: vec![AggAccum::default(); agg_count],
            },
        );
    }

    // Render rows in deterministic order.
    let mut rows: Vec<Vec<Cell>> = Vec::with_capacity(groups.len());
    for (_, group) in groups {
        let mut row: Vec<Cell> = Vec::with_capacity(stmt.projections.len());
        let mut agg_idx = 0;
        for projection in &stmt.projections {
            match projection {
                Projection::Star => {
                    // SELECT * + aggregate doesn't really make sense, but
                    // honour it by emitting a synthetic count.
                    row.push(Cell::Null);
                }
                Projection::Column(c) => {
                    let key = c.to_lowercase();
                    let cell = group
                        .keys
                        .get(&key)
                        .map(Cell::from_field)
                        .unwrap_or(Cell::Null);
                    row.push(cell);
                }
                Projection::Aggregate { func, .. } => {
                    let cell = group.aggs[agg_idx].render(*func);
                    agg_idx += 1;
                    row.push(cell);
                }
            }
        }
        rows.push(row);
    }

    let limit_val = limit.unwrap_or(MAX_RESULT_ROWS).min(MAX_RESULT_ROWS);
    let pre_limit = rows.len();
    if rows.len() > limit_val {
        rows.truncate(limit_val);
    }
    let truncated = pre_limit > rows.len();
    let returned = rows.len();

    Ok(QueryResult {
        columns,
        rows,
        scanned,
        matched,
        returned,
        truncated,
    })
}

fn output_columns(stmt: &SelectStmt) -> Vec<String> {
    let mut cols = Vec::new();
    for p in &stmt.projections {
        match p {
            Projection::Star => {
                cols.extend([
                    "observed_at_ms".to_string(),
                    "signal".to_string(),
                    "service_name".to_string(),
                    "duration_ms".to_string(),
                ]);
            }
            Projection::Column(c) => cols.push(c.clone()),
            Projection::Aggregate { alias, .. } => cols.push(alias.clone()),
        }
    }
    cols
}

fn render_plain_row(stmt: &SelectStmt, entry: &NormalizedEntry) -> Vec<Cell> {
    let mut out = Vec::new();
    for p in &stmt.projections {
        match p {
            Projection::Star => {
                out.push(Cell::Number(entry.observed_at.timestamp_millis() as f64));
                out.push(Cell::String(
                    match entry.signal {
                        Signal::Traces => "traces",
                        Signal::Metrics => "metrics",
                        Signal::Logs => "logs",
                    }
                    .to_string(),
                ));
                out.push(match &entry.service_name {
                    Some(s) => Cell::String(s.clone()),
                    None => Cell::Null,
                });
                out.push(
                    extract_duration_ms(entry)
                        .map(Cell::Number)
                        .unwrap_or(Cell::Null),
                );
            }
            Projection::Column(c) => {
                out.push(Cell::from_field(&field_value(entry, c)));
            }
            Projection::Aggregate { .. } => {
                // Should not be reached -- the aggregate path handles this.
                out.push(Cell::Null);
            }
        }
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::query::parser::parse;
    use serde_json::Map;

    /// Test helper: convert the result rows to a `Vec<JsonValue>` (object form).
    fn rows_as_objects(result: &QueryResult) -> Vec<JsonValue> {
        result
            .rows
            .iter()
            .map(|row| {
                let mut obj = Map::new();
                for (i, col) in result.columns.iter().enumerate() {
                    let cell = row.get(i).cloned().unwrap_or(Cell::Null);
                    let value = match cell {
                        Cell::Null => JsonValue::Null,
                        Cell::Bool(b) => JsonValue::Bool(b),
                        Cell::Number(n) => serde_json::Number::from_f64(n)
                            .map(JsonValue::Number)
                            .unwrap_or(JsonValue::Null),
                        Cell::String(s) => JsonValue::String(s),
                    };
                    obj.insert(col.clone(), value);
                }
                JsonValue::Object(obj)
            })
            .collect()
    }
    use bytes::Bytes;
    use chrono::{Duration, Utc};

    fn entry(signal: Signal, service: Option<&str>, age_ms: i64) -> NormalizedEntry {
        NormalizedEntry {
            signal,
            observed_at: Utc::now() - Duration::milliseconds(age_ms),
            service_name: service.map(String::from),
            payload: Bytes::from_static(b"{}"),
        }
    }

    fn trace_with_duration(service: &str, dur_ms: u64) -> NormalizedEntry {
        let start: u64 = 1_700_000_000_000_000_000;
        let end: u64 = start + dur_ms * 1_000_000;
        let payload = serde_json::json!({
            "resourceSpans": [{
                "scopeSpans": [{
                    "spans": [{
                        "name": "op",
                        "startTimeUnixNano": start.to_string(),
                        "endTimeUnixNano": end.to_string(),
                    }]
                }]
            }]
        })
        .to_string();
        NormalizedEntry {
            signal: Signal::Traces,
            observed_at: Utc::now(),
            service_name: Some(service.to_string()),
            payload: Bytes::from(payload),
        }
    }

    #[test]
    fn execute_count_star() {
        let stmt = parse("SELECT count(*) FROM traces").unwrap();
        let entries = vec![
            entry(Signal::Traces, Some("a"), 0),
            entry(Signal::Traces, Some("b"), 0),
            entry(Signal::Logs, Some("a"), 0), // filtered out
        ];
        let result = execute(&stmt, entries).unwrap();
        assert_eq!(result.columns, vec!["count(*)"]);
        assert_eq!(result.rows, vec![vec![Cell::Number(2.0)]]);
        assert_eq!(result.matched, 2);
    }

    #[test]
    fn execute_select_star_returns_basic_columns() {
        let stmt = parse("SELECT * FROM logs").unwrap();
        let entries = vec![entry(Signal::Logs, Some("api"), 0)];
        let result = execute(&stmt, entries).unwrap();
        assert_eq!(
            result.columns,
            vec!["observed_at_ms", "signal", "service_name", "duration_ms"]
        );
        assert_eq!(result.rows.len(), 1);
        assert_eq!(result.rows[0][1], Cell::String("logs".to_string()));
        assert_eq!(result.rows[0][2], Cell::String("api".to_string()));
    }

    #[test]
    fn execute_filter_by_service_name_eq() {
        let stmt = parse("SELECT count(*) FROM traces WHERE service_name = 'api'").unwrap();
        let entries = vec![
            entry(Signal::Traces, Some("api"), 0),
            entry(Signal::Traces, Some("API"), 0), // case-insensitive match
            entry(Signal::Traces, Some("other"), 0),
        ];
        let result = execute(&stmt, entries).unwrap();
        assert_eq!(result.rows[0][0], Cell::Number(2.0));
    }

    #[test]
    fn execute_group_by_service_name() {
        let stmt =
            parse("SELECT count(*), service_name FROM traces GROUP BY service_name").unwrap();
        let entries = vec![
            entry(Signal::Traces, Some("a"), 0),
            entry(Signal::Traces, Some("a"), 0),
            entry(Signal::Traces, Some("b"), 0),
        ];
        let result = execute(&stmt, entries).unwrap();
        assert_eq!(result.columns, vec!["count(*)", "service_name"]);
        // Order is BTreeMap (sorted by group key prefix).
        assert_eq!(result.rows.len(), 2);
        let counts: Vec<f64> = result
            .rows
            .iter()
            .map(|r| match r[0] {
                Cell::Number(n) => n,
                _ => panic!("expected number"),
            })
            .collect();
        assert!(counts.contains(&2.0));
        assert!(counts.contains(&1.0));
    }

    #[test]
    fn execute_avg_and_sum_duration_ms() {
        let stmt = parse(
            "SELECT avg(duration_ms), sum(duration_ms), service_name FROM traces GROUP BY service_name",
        )
        .unwrap();
        let entries = vec![
            trace_with_duration("api", 100),
            trace_with_duration("api", 200),
            trace_with_duration("worker", 50),
        ];
        let result = execute(&stmt, entries).unwrap();
        assert_eq!(result.rows.len(), 2);
        // Find the api row and validate avg=150, sum=300.
        let api_row = result
            .rows
            .iter()
            .find(|r| r[2] == Cell::String("api".to_string()))
            .expect("api row");
        assert_eq!(api_row[0], Cell::Number(150.0));
        assert_eq!(api_row[1], Cell::Number(300.0));
    }

    #[test]
    fn execute_where_duration_gt() {
        let stmt = parse("SELECT count(*) FROM traces WHERE duration_ms > 100").unwrap();
        let entries = vec![
            trace_with_duration("a", 50),
            trace_with_duration("a", 150),
            trace_with_duration("a", 250),
        ];
        let result = execute(&stmt, entries).unwrap();
        assert_eq!(result.rows[0][0], Cell::Number(2.0));
    }

    #[test]
    fn execute_unknown_column_errors() {
        let stmt = parse("SELECT bogus FROM traces").unwrap();
        let entries: Vec<NormalizedEntry> = vec![];
        let err = execute(&stmt, entries).unwrap_err();
        assert!(matches!(err, ExecuteError::UnknownColumn(_)));
    }

    #[test]
    fn execute_select_non_aggregate_without_group_by_errors() {
        let stmt = parse("SELECT count(*), service_name FROM traces").unwrap();
        let err = execute(&stmt, Vec::<NormalizedEntry>::new()).unwrap_err();
        assert!(matches!(err, ExecuteError::NonAggregateNotInGroupBy(_)));
    }

    #[test]
    fn execute_limit_truncates() {
        let stmt = parse("SELECT service_name FROM traces LIMIT 2").unwrap();
        let entries = vec![
            entry(Signal::Traces, Some("a"), 0),
            entry(Signal::Traces, Some("b"), 0),
            entry(Signal::Traces, Some("c"), 0),
        ];
        let result = execute(&stmt, entries).unwrap();
        assert_eq!(result.rows.len(), 2);
        assert!(result.truncated);
    }

    #[test]
    fn execute_like_matches_pattern() {
        let stmt = parse("SELECT count(*) FROM traces WHERE service_name LIKE 'api%'").unwrap();
        let entries = vec![
            entry(Signal::Traces, Some("api"), 0),
            entry(Signal::Traces, Some("api-v2"), 0),
            entry(Signal::Traces, Some("worker"), 0),
        ];
        let result = execute(&stmt, entries).unwrap();
        assert_eq!(result.rows[0][0], Cell::Number(2.0));
    }

    #[test]
    fn execute_or_clause() {
        let stmt =
            parse("SELECT count(*) FROM traces WHERE service_name = 'a' OR service_name = 'b'")
                .unwrap();
        let entries = vec![
            entry(Signal::Traces, Some("a"), 0),
            entry(Signal::Traces, Some("b"), 0),
            entry(Signal::Traces, Some("c"), 0),
        ];
        let result = execute(&stmt, entries).unwrap();
        assert_eq!(result.rows[0][0], Cell::Number(2.0));
    }

    #[test]
    fn execute_signal_string_filter() {
        // signal column should let users discriminate without changing FROM.
        let stmt = parse("SELECT count(*) FROM traces WHERE signal = 'traces'").unwrap();
        let entries = vec![
            entry(Signal::Traces, Some("a"), 0),
            entry(Signal::Logs, Some("a"), 0),
        ];
        let result = execute(&stmt, entries).unwrap();
        assert_eq!(result.rows[0][0], Cell::Number(1.0));
    }

    #[test]
    fn rows_as_objects_serialises_correctly() {
        let stmt =
            parse("SELECT count(*), service_name FROM traces GROUP BY service_name").unwrap();
        let entries = vec![
            entry(Signal::Traces, Some("a"), 0),
            entry(Signal::Traces, Some("a"), 0),
        ];
        let result = execute(&stmt, entries).unwrap();
        let objs = rows_as_objects(&result);
        assert_eq!(objs.len(), 1);
        let obj = objs[0].as_object().unwrap();
        assert_eq!(
            obj.get("service_name"),
            Some(&JsonValue::String("a".into()))
        );
        assert!(obj.contains_key("count(*)"));
    }
}
