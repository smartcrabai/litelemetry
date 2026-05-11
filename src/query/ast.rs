//! AST types for the query DSL.

/// Top-level parsed `SELECT` statement.
#[derive(Debug, Clone, PartialEq)]
pub struct SelectStmt {
    pub projections: Vec<Projection>,
    pub from: Source,
    pub where_clause: Option<Expr>,
    pub group_by: Vec<String>,
    pub limit: Option<usize>,
}

/// FROM source: which signal stream to query.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Source {
    Traces,
    Metrics,
    Logs,
}

/// A single projection in the SELECT list.
#[derive(Debug, Clone, PartialEq)]
pub enum Projection {
    /// `*` -- selects an opaque "row" representation.
    Star,
    /// A bare column reference, e.g. `service_name`.
    Column(String),
    /// An aggregate function, e.g. `count(*)`, `sum(duration_ms)`.
    Aggregate {
        func: AggFunc,
        /// `None` for `*`, `Some("col")` for `count(col)` etc.
        arg: AggArg,
        /// Display alias (defaults to `func(arg)`).
        alias: String,
    },
}

#[derive(Debug, Clone, PartialEq)]
pub enum AggArg {
    Star,
    Column(String),
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AggFunc {
    Count,
    Sum,
    Avg,
    Min,
    Max,
}

impl AggFunc {
    pub fn as_str(self) -> &'static str {
        match self {
            AggFunc::Count => "count",
            AggFunc::Sum => "sum",
            AggFunc::Avg => "avg",
            AggFunc::Min => "min",
            AggFunc::Max => "max",
        }
    }
}

/// A WHERE expression. Supports flat AND/OR combinations of comparisons.
#[derive(Debug, Clone, PartialEq)]
pub enum Expr {
    Compare {
        column: String,
        op: CompareOp,
        value: Value,
    },
    And(Box<Expr>, Box<Expr>),
    Or(Box<Expr>, Box<Expr>),
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CompareOp {
    Eq,
    Ne,
    Gt,
    Lt,
    Ge,
    Le,
    Like,
}

#[derive(Debug, Clone, PartialEq)]
pub enum Value {
    Str(String),
    Number(f64),
}
