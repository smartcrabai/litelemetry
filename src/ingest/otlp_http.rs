use crate::domain::telemetry::{NormalizedEntry, Signal};
use crate::ingest::decode::{ContentType, DecodeError, parse_content_type};
use bytes::Bytes;
use chrono::Utc;

/// OTLP/HTTP リクエストを解析して NormalizedEntry を生成する (I/O なし)。
///
/// - content-type が未対応の場合は `DecodeError::UnsupportedContentType` を返す。
/// - 成功した場合はペイロードをそのまま保持する NormalizedEntry を返す。
///   traces の JSON payload からは `service.name` を抽出する。
pub fn parse_ingest_request(
    signal: Signal,
    content_type_header: Option<&str>,
    body: Bytes,
) -> Result<NormalizedEntry, DecodeError> {
    let ct = content_type_header.unwrap_or("");
    let content_type = parse_content_type(ct)?;
    Ok(NormalizedEntry {
        signal,
        observed_at: Utc::now(),
        service_name: extract_service_name(signal, content_type, &body),
        payload: body,
    })
}

fn extract_service_name(signal: Signal, content_type: ContentType, body: &Bytes) -> Option<String> {
    if signal != Signal::Traces || content_type != ContentType::Json {
        return None;
    }

    let value: serde_json::Value = serde_json::from_slice(body).ok()?;
    let resource_spans = value.get("resourceSpans")?.as_array()?;

    for resource_span in resource_spans {
        let Some(attributes) = resource_span
            .get("resource")
            .and_then(|resource| resource.get("attributes"))
            .and_then(serde_json::Value::as_array)
        else {
            continue;
        };

        for attribute in attributes {
            let Some(key) = attribute.get("key").and_then(serde_json::Value::as_str) else {
                continue;
            };
            if key != "service.name" {
                continue;
            }

            if let Some(service_name) = attribute
                .get("value")
                .and_then(|value| value.get("stringValue"))
                .and_then(serde_json::Value::as_str)
            {
                return Some(service_name.to_string());
            }
        }
    }

    None
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::domain::telemetry::Signal;
    use crate::ingest::decode::DecodeError;
    use bytes::Bytes;
    use chrono::Utc;

    // ─── parse_ingest_request: ハッピーパス ─────────────────────────────────

    #[test]
    fn test_parse_ingest_request_protobuf_traces_returns_entry() {
        // Given: traces signal, protobuf content-type, バイナリ payload
        let signal = Signal::Traces;
        let body = Bytes::from_static(b"\x0a\x0b\x0c");

        // When: parse
        let result = parse_ingest_request(signal, Some("application/x-protobuf"), body.clone());

        // Then: 正しい signal と payload を持つ NormalizedEntry が返る
        let entry = result.unwrap();
        assert_eq!(entry.signal, Signal::Traces);
        assert_eq!(entry.payload, body);
    }

    #[test]
    fn test_parse_ingest_request_json_traces_returns_entry() {
        // Given: traces signal, JSON content-type
        let result = parse_ingest_request(
            Signal::Traces,
            Some("application/json"),
            Bytes::from_static(b"{}"),
        );

        // Then: 成功する
        assert!(result.is_ok());
        assert_eq!(result.unwrap().signal, Signal::Traces);
    }

    #[test]
    fn test_parse_ingest_request_json_with_charset_returns_entry() {
        // Given: "application/json; charset=utf-8" (charset パラメータ付き)
        let result = parse_ingest_request(
            Signal::Traces,
            Some("application/json; charset=utf-8"),
            Bytes::new(),
        );

        // Then: charset は無視されて成功する
        assert!(result.is_ok());
    }

    #[test]
    fn test_parse_ingest_request_metrics_signal_propagates() {
        // Given: metrics signal
        let result = parse_ingest_request(
            Signal::Metrics,
            Some("application/x-protobuf"),
            Bytes::new(),
        );

        // Then: signal は Metrics として伝播する
        assert_eq!(result.unwrap().signal, Signal::Metrics);
    }

    #[test]
    fn test_parse_ingest_request_logs_signal_propagates() {
        // Given: logs signal
        let result =
            parse_ingest_request(Signal::Logs, Some("application/x-protobuf"), Bytes::new());

        // Then: signal は Logs として伝播する
        assert_eq!(result.unwrap().signal, Signal::Logs);
    }

    #[test]
    fn test_parse_ingest_request_preserves_payload_bytes() {
        // Given: 特定のバイト列
        let payload = Bytes::copy_from_slice(&[0x0a, 0x1b, 0x2c, 0x3d]);

        // When: parse
        let entry = parse_ingest_request(
            Signal::Traces,
            Some("application/x-protobuf"),
            payload.clone(),
        )
        .unwrap();

        // Then: payload が変更されずに保持される
        assert_eq!(entry.payload, payload);
    }

    #[test]
    fn test_parse_ingest_request_empty_body_succeeds() {
        // Given: 空のボディ
        let result =
            parse_ingest_request(Signal::Traces, Some("application/x-protobuf"), Bytes::new());

        // Then: 成功する (ボディの内容は検証しない)
        assert!(result.is_ok());
    }

    #[test]
    fn test_parse_ingest_request_observed_at_is_close_to_now() {
        // Given: パース実行前後の時刻を記録
        let before = Utc::now();

        // When: parse
        let entry =
            parse_ingest_request(Signal::Traces, Some("application/x-protobuf"), Bytes::new())
                .unwrap();

        let after = Utc::now();

        // Then: observed_at は before 以上・after 以下の範囲に収まる
        assert!(
            entry.observed_at >= before,
            "observed_at {:?} should be >= before {:?}",
            entry.observed_at,
            before
        );
        assert!(
            entry.observed_at <= after,
            "observed_at {:?} should be <= after {:?}",
            entry.observed_at,
            after
        );
    }

    // ─── parse_ingest_request: エラーケース ─────────────────────────────────

    #[test]
    fn test_parse_ingest_request_text_plain_returns_unsupported_error() {
        // Given: text/plain (未対応 content-type)
        let result = parse_ingest_request(Signal::Traces, Some("text/plain"), Bytes::new());

        // Then: UnsupportedContentType エラー
        assert!(
            matches!(result, Err(DecodeError::UnsupportedContentType(_))),
            "expected UnsupportedContentType, got {result:?}"
        );
    }

    #[test]
    fn test_parse_ingest_request_missing_content_type_returns_error() {
        // Given: content-type ヘッダーなし (None)
        let result = parse_ingest_request(Signal::Traces, None, Bytes::new());

        // Then: UnsupportedContentType エラー (空文字列扱い)
        assert!(
            matches!(result, Err(DecodeError::UnsupportedContentType(_))),
            "expected UnsupportedContentType, got {result:?}"
        );
    }

    #[test]
    fn test_parse_ingest_request_empty_content_type_returns_error() {
        // Given: 空文字列の content-type
        let result = parse_ingest_request(Signal::Traces, Some(""), Bytes::new());

        // Then: UnsupportedContentType エラー
        assert!(
            matches!(result, Err(DecodeError::UnsupportedContentType(_))),
            "expected UnsupportedContentType, got {result:?}"
        );
    }

    #[test]
    fn test_parse_ingest_request_multipart_returns_error() {
        // Given: multipart/form-data (未対応)
        let result =
            parse_ingest_request(Signal::Traces, Some("multipart/form-data"), Bytes::new());

        // Then: UnsupportedContentType エラー
        assert!(matches!(
            result,
            Err(DecodeError::UnsupportedContentType(_))
        ));
    }

    // ─── parse_ingest_request: 境界値 ───────────────────────────────────────

    #[test]
    fn test_parse_ingest_request_service_name_is_none_for_protobuf_traces() {
        // Given: プロトバイナリからの service_name 抽出は未実装
        let entry =
            parse_ingest_request(Signal::Traces, Some("application/x-protobuf"), Bytes::new())
                .unwrap();

        // Then: service_name は None
        assert_eq!(entry.service_name, None);
    }

    #[test]
    fn test_parse_ingest_request_extracts_service_name_from_trace_json() {
        let body = Bytes::from_static(
            br#"{
                "resourceSpans": [
                    {
                        "resource": {
                            "attributes": [
                                {
                                    "key": "service.name",
                                    "value": { "stringValue": "checkout-ui" }
                                }
                            ]
                        }
                    }
                ]
            }"#,
        );

        let entry = parse_ingest_request(Signal::Traces, Some("application/json"), body).unwrap();

        assert_eq!(entry.service_name.as_deref(), Some("checkout-ui"));
    }

    #[test]
    fn test_parse_ingest_request_extracts_service_name_from_later_resource_span() {
        let body = Bytes::from_static(
            br#"{
                "resourceSpans": [
                    {
                        "resource": {}
                    },
                    {
                        "resource": {
                            "attributes": [
                                {
                                    "key": "service.name",
                                    "value": { "stringValue": "payments-api" }
                                }
                            ]
                        }
                    }
                ]
            }"#,
        );

        let entry = parse_ingest_request(Signal::Traces, Some("application/json"), body).unwrap();

        assert_eq!(entry.service_name.as_deref(), Some("payments-api"));
    }
}
