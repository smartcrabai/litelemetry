/// OTel resource attribute の値型
/// (protobuf AnyValue の簡略版)
#[derive(Debug, Clone, PartialEq)]
pub enum AttributeValue {
    String(String),
    Bool(bool),
    Int(i64),
}

impl AttributeValue {
    pub fn as_string(&self) -> Option<&str> {
        match self {
            AttributeValue::String(s) => Some(s.as_str()),
            _ => None,
        }
    }
}

const SERVICE_NAME_ATTR: &str = "service.name";

/// OTLP proto の Resource を変換した内部表現
#[derive(Debug, Clone)]
pub struct ParsedResource {
    pub attributes: std::collections::HashMap<String, AttributeValue>,
}

/// resource.service.name 属性を返す。
/// - 属性が存在しない場合 → None
/// - 値が文字列型でない場合 → None
pub fn extract_service_name(resource: &ParsedResource) -> Option<&str> {
    resource
        .attributes
        .get(SERVICE_NAME_ATTR)
        .and_then(|v| v.as_string())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn make_resource(attrs: Vec<(&str, AttributeValue)>) -> ParsedResource {
        ParsedResource {
            attributes: attrs.into_iter().map(|(k, v)| (k.to_string(), v)).collect(),
        }
    }

    // ─── extract_service_name ────────────────────────────────────────────────

    #[test]
    fn test_extract_service_name_found() {
        // Given: resource に service.name (文字列) がある
        let resource = make_resource(vec![(
            "service.name",
            AttributeValue::String("my-service".to_string()),
        )]);

        // When: extract_service_name
        let name = extract_service_name(&resource);

        // Then: サービス名が返る
        assert_eq!(name, Some("my-service"));
    }

    #[test]
    fn test_extract_service_name_not_present() {
        // Given: resource に service.name がない
        let resource = make_resource(vec![(
            "service.version",
            AttributeValue::String("1.0.0".to_string()),
        )]);

        // When: extract_service_name
        let name = extract_service_name(&resource);

        // Then: None
        assert_eq!(name, None);
    }

    #[test]
    fn test_extract_service_name_non_string_type_returns_none() {
        // Given: resource に service.name があるが値が整数型
        let resource = make_resource(vec![("service.name", AttributeValue::Int(42))]);

        // When: extract_service_name
        let name = extract_service_name(&resource);

        // Then: None (文字列以外は無視)
        assert_eq!(name, None);
    }

    #[test]
    fn test_extract_service_name_empty_resource() {
        // Given: 属性が空の resource
        let resource = make_resource(vec![]);

        // When: extract_service_name
        let name = extract_service_name(&resource);

        // Then: None
        assert_eq!(name, None);
    }

    #[test]
    fn test_extract_service_name_multiple_attributes() {
        // Given: 複数属性の中に service.name がある
        let resource = make_resource(vec![
            ("host.name", AttributeValue::String("server-1".to_string())),
            (
                "service.name",
                AttributeValue::String("api-service".to_string()),
            ),
            (
                "service.version",
                AttributeValue::String("2.0.0".to_string()),
            ),
        ]);

        // When: extract_service_name
        let name = extract_service_name(&resource);

        // Then: 正しいサービス名が返る
        assert_eq!(name, Some("api-service"));
    }

    #[test]
    fn test_extract_service_name_bool_type_returns_none() {
        // Given: service.name が bool 型
        let resource = make_resource(vec![("service.name", AttributeValue::Bool(true))]);

        // When: extract_service_name
        let name = extract_service_name(&resource);

        // Then: None
        assert_eq!(name, None);
    }
}
