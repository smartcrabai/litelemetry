use crate::domain::telemetry::Signal;
use thiserror::Error;

/// OTLP リクエストのエンコーディング種別
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ContentType {
    Protobuf,
    Json,
}

#[derive(Debug, Error)]
pub enum DecodeError {
    #[error("unsupported content type: {0}")]
    UnsupportedContentType(String),
}

/// Content-Type ヘッダ値を解析して ContentType を返す。
/// "application/x-protobuf" -> Protobuf
/// "application/json" (charset 付き可) -> Json
pub fn parse_content_type(content_type: &str) -> Result<ContentType, DecodeError> {
    let mime_type = content_type.split(';').next().unwrap_or("").trim();
    match mime_type {
        "application/x-protobuf" => Ok(ContentType::Protobuf),
        "application/json" => Ok(ContentType::Json),
        other => Err(DecodeError::UnsupportedContentType(other.to_string())),
    }
}

/// OTLP/HTTP エンドポイントパスから Signal を判定する。
/// "/v1/traces" -> Some(Signal::Traces)
/// "/v1/metrics" -> Some(Signal::Metrics)
/// "/v1/logs" -> Some(Signal::Logs)
/// その他 -> None
pub fn signal_from_path(path: &str) -> Option<Signal> {
    match path {
        "/v1/traces" => Some(Signal::Traces),
        "/v1/metrics" => Some(Signal::Metrics),
        "/v1/logs" => Some(Signal::Logs),
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    // ─── parse_content_type ───────────────────────────────────────────────────

    #[test]
    fn test_parse_content_type_protobuf() {
        // Given: application/x-protobuf
        // When: parse
        // Then: ContentType::Protobuf
        let result = parse_content_type("application/x-protobuf");
        assert_eq!(result.unwrap(), ContentType::Protobuf);
    }

    #[test]
    fn test_parse_content_type_json() {
        // Given: application/json
        // When: parse
        // Then: ContentType::Json
        let result = parse_content_type("application/json");
        assert_eq!(result.unwrap(), ContentType::Json);
    }

    #[test]
    fn test_parse_content_type_json_with_charset() {
        // Given: application/json; charset=utf-8
        // When: parse
        // Then: ContentType::Json (charset パラメータは無視)
        let result = parse_content_type("application/json; charset=utf-8");
        assert_eq!(result.unwrap(), ContentType::Json);
    }

    #[test]
    fn test_parse_content_type_json_with_whitespace() {
        // Given: application/json;charset=utf-8 (空白なし)
        // When: parse
        // Then: ContentType::Json
        let result = parse_content_type("application/json;charset=utf-8");
        assert_eq!(result.unwrap(), ContentType::Json);
    }

    #[test]
    fn test_parse_content_type_unknown_returns_error() {
        // Given: text/plain (未対応 content-type)
        // When: parse
        // Then: UnsupportedContentType エラー
        let result = parse_content_type("text/plain");
        assert!(
            matches!(result, Err(DecodeError::UnsupportedContentType(_))),
            "expected UnsupportedContentType, got {result:?}"
        );
    }

    #[test]
    fn test_parse_content_type_empty_returns_error() {
        // Given: 空文字列
        // When: parse
        // Then: UnsupportedContentType エラー
        let result = parse_content_type("");
        assert!(matches!(
            result,
            Err(DecodeError::UnsupportedContentType(_))
        ));
    }

    // ─── signal_from_path ────────────────────────────────────────────────────

    #[test]
    fn test_signal_from_traces_path() {
        // Given: /v1/traces
        // When: signal_from_path
        // Then: Signal::Traces
        assert_eq!(signal_from_path("/v1/traces"), Some(Signal::Traces));
    }

    #[test]
    fn test_signal_from_metrics_path() {
        // Given: /v1/metrics
        // When: signal_from_path
        // Then: Signal::Metrics
        assert_eq!(signal_from_path("/v1/metrics"), Some(Signal::Metrics));
    }

    #[test]
    fn test_signal_from_logs_path() {
        // Given: /v1/logs
        // When: signal_from_path
        // Then: Signal::Logs
        assert_eq!(signal_from_path("/v1/logs"), Some(Signal::Logs));
    }

    #[test]
    fn test_signal_from_unknown_path_returns_none() {
        // Given: /v1/unknown
        // When: signal_from_path
        // Then: None
        assert_eq!(signal_from_path("/v1/unknown"), None);
    }

    #[test]
    fn test_signal_from_root_path_returns_none() {
        // Given: /
        // When: signal_from_path
        // Then: None
        assert_eq!(signal_from_path("/"), None);
    }

    #[test]
    fn test_signal_from_traces_trailing_slash_returns_none() {
        // Given: /v1/traces/ (末尾スラッシュ付き)
        // When: signal_from_path
        // Then: None (厳密マッチ)
        assert_eq!(signal_from_path("/v1/traces/"), None);
    }

    #[test]
    fn test_signal_from_empty_path_returns_none() {
        // Given: 空文字列
        // When: signal_from_path
        // Then: None
        assert_eq!(signal_from_path(""), None);
    }
}
