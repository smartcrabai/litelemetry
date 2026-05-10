//! SQL-like Query DSL: parser, AST, and in-memory executor.
//!
//! Supports a tiny subset of SQL designed for ad-hoc telemetry analysis:
//!
//! ```sql
//! SELECT count(*), service_name
//! FROM traces
//! WHERE service_name = 'api' AND duration_ms > 100
//! GROUP BY service_name
//! LIMIT 100
//! ```
//!
//! Supported sources: `traces`, `metrics`, `logs`.
//!
//! Supported projections: `*`, plain columns, aggregates `count(*)`, `count(col)`,
//! `sum(col)`, `avg(col)`.
//!
//! Supported `WHERE` ops: `=`, `!=`, `<>`, `>`, `<`, `>=`, `<=`, `LIKE` combined with
//! `AND` / `OR`.
//!
//! Supported columns:
//!   * `service_name` -- string
//!   * `signal` -- string (`traces`/`metrics`/`logs`)
//!   * `payload` -- string match against the searchable payload text
//!   * `observed_at_ms` -- numeric (UTC milliseconds)
//!   * `duration_ms` -- numeric, derived from trace span duration (sum of
//!     `endTimeUnixNano - startTimeUnixNano` divided by 1_000_000)

pub mod ast;
pub mod executor;
pub mod parser;

pub use ast::{AggFunc, CompareOp, Expr, Projection, SelectStmt, Source, Value};
pub use executor::{ExecuteError, QueryResult, execute};
pub use parser::{ParseError, parse};
