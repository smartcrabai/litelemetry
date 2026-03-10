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
}
