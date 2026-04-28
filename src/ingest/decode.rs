use bytes::Bytes;
use flate2::read::GzDecoder;
use std::io::Read;
use thiserror::Error;

/// Encoding type of an OTLP request
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ContentType {
    Protobuf,
    Json,
}

/// Transfer encoding applied to an OTLP request body
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ContentEncoding {
    Identity,
    Gzip,
    Snappy,
    Zstd,
    Lz4,
}

#[derive(Debug, Error)]
pub enum DecodeError {
    #[error("unsupported content type: {0}")]
    UnsupportedContentType(String),
    #[error("unsupported content encoding: {0}")]
    UnsupportedContentEncoding(String),
    #[error("failed to decompress body: {0}")]
    Decompression(String),
}

/// Parses a Content-Type header value and returns a ContentType.
/// "application/x-protobuf" -> Protobuf
/// "application/json" (charset parameter allowed) -> Json
pub fn parse_content_type(content_type: &str) -> Result<ContentType, DecodeError> {
    let mime_type = content_type.split(';').next().unwrap_or("").trim();
    match mime_type {
        "application/x-protobuf" => Ok(ContentType::Protobuf),
        "application/json" => Ok(ContentType::Json),
        other => Err(DecodeError::UnsupportedContentType(other.to_string())),
    }
}

/// Parses a Content-Encoding header value and returns a ContentEncoding.
/// `None`, empty string, or "identity" -> Identity (no decompression)
/// "gzip" -> Gzip
/// "snappy" -> Snappy (framed format, per OTel Collector convention)
/// "zstd" -> Zstd
/// "lz4" -> Lz4 (frame format, per OTel Collector convention)
pub fn parse_content_encoding(header: Option<&str>) -> Result<ContentEncoding, DecodeError> {
    match header.unwrap_or("").trim() {
        "" | "identity" => Ok(ContentEncoding::Identity),
        "gzip" => Ok(ContentEncoding::Gzip),
        "snappy" => Ok(ContentEncoding::Snappy),
        "zstd" => Ok(ContentEncoding::Zstd),
        "lz4" => Ok(ContentEncoding::Lz4),
        other => Err(DecodeError::UnsupportedContentEncoding(other.to_string())),
    }
}

/// Decompresses `body` according to `encoding`. For `Identity`, the body is returned as-is.
pub fn decompress_body(encoding: ContentEncoding, body: Bytes) -> Result<Bytes, DecodeError> {
    match encoding {
        ContentEncoding::Identity => Ok(body),
        ContentEncoding::Gzip => {
            let mut decoder = GzDecoder::new(body.as_ref());
            let mut out = Vec::new();
            decoder
                .read_to_end(&mut out)
                .map_err(|error| DecodeError::Decompression(error.to_string()))?;
            Ok(Bytes::from(out))
        }
        ContentEncoding::Snappy => {
            let mut decoder = snap::read::FrameDecoder::new(body.as_ref());
            let mut out = Vec::new();
            decoder
                .read_to_end(&mut out)
                .map_err(|error| DecodeError::Decompression(error.to_string()))?;
            Ok(Bytes::from(out))
        }
        ContentEncoding::Zstd => {
            let mut decoder = zstd::stream::read::Decoder::new(body.as_ref())
                .map_err(|error| DecodeError::Decompression(error.to_string()))?;
            let mut out = Vec::new();
            decoder
                .read_to_end(&mut out)
                .map_err(|error| DecodeError::Decompression(error.to_string()))?;
            Ok(Bytes::from(out))
        }
        ContentEncoding::Lz4 => {
            let mut decoder = lz4_flex::frame::FrameDecoder::new(body.as_ref());
            let mut out = Vec::new();
            decoder
                .read_to_end(&mut out)
                .map_err(|error| DecodeError::Decompression(error.to_string()))?;
            Ok(Bytes::from(out))
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    // --- parse_content_type --------------------------------------------------

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
        // Then: ContentType::Json (charset parameter is ignored)
        let result = parse_content_type("application/json; charset=utf-8");
        assert_eq!(result.unwrap(), ContentType::Json);
    }

    #[test]
    fn test_parse_content_type_json_with_whitespace() {
        // Given: application/json;charset=utf-8 (no whitespace)
        // When: parse
        // Then: ContentType::Json
        let result = parse_content_type("application/json;charset=utf-8");
        assert_eq!(result.unwrap(), ContentType::Json);
    }

    #[test]
    fn test_parse_content_type_unknown_returns_error() {
        // Given: text/plain (unsupported content-type)
        // When: parse
        // Then: UnsupportedContentType error
        let result = parse_content_type("text/plain");
        assert!(
            matches!(result, Err(DecodeError::UnsupportedContentType(_))),
            "expected UnsupportedContentType, got {result:?}"
        );
    }

    #[test]
    fn test_parse_content_type_empty_returns_error() {
        // Given: empty string
        // When: parse
        // Then: UnsupportedContentType error
        let result = parse_content_type("");
        assert!(matches!(
            result,
            Err(DecodeError::UnsupportedContentType(_))
        ));
    }

    // --- parse_content_encoding ----------------------------------------------

    #[test]
    fn test_parse_content_encoding_missing_is_identity() {
        assert_eq!(
            parse_content_encoding(None).unwrap(),
            ContentEncoding::Identity
        );
    }

    #[test]
    fn test_parse_content_encoding_empty_is_identity() {
        assert_eq!(
            parse_content_encoding(Some("")).unwrap(),
            ContentEncoding::Identity
        );
    }

    #[test]
    fn test_parse_content_encoding_identity_is_identity() {
        assert_eq!(
            parse_content_encoding(Some("identity")).unwrap(),
            ContentEncoding::Identity
        );
    }

    #[test]
    fn test_parse_content_encoding_gzip() {
        assert_eq!(
            parse_content_encoding(Some("gzip")).unwrap(),
            ContentEncoding::Gzip
        );
    }

    #[test]
    fn test_parse_content_encoding_gzip_with_whitespace() {
        assert_eq!(
            parse_content_encoding(Some("  gzip  ")).unwrap(),
            ContentEncoding::Gzip
        );
    }

    #[test]
    fn test_parse_content_encoding_snappy() {
        assert_eq!(
            parse_content_encoding(Some("snappy")).unwrap(),
            ContentEncoding::Snappy
        );
    }

    #[test]
    fn test_parse_content_encoding_zstd() {
        assert_eq!(
            parse_content_encoding(Some("zstd")).unwrap(),
            ContentEncoding::Zstd
        );
    }

    #[test]
    fn test_parse_content_encoding_lz4() {
        assert_eq!(
            parse_content_encoding(Some("lz4")).unwrap(),
            ContentEncoding::Lz4
        );
    }

    #[test]
    fn test_parse_content_encoding_unknown_returns_error() {
        let result = parse_content_encoding(Some("br"));
        assert!(matches!(
            result,
            Err(DecodeError::UnsupportedContentEncoding(_))
        ));
    }

    // --- decompress_body -----------------------------------------------------

    #[test]
    fn test_decompress_body_identity_returns_input_unchanged() {
        let body = Bytes::from_static(b"hello world");
        let out = decompress_body(ContentEncoding::Identity, body.clone()).unwrap();
        assert_eq!(out, body);
    }

    #[test]
    fn test_decompress_body_gzip_decodes_payload() {
        use flate2::Compression;
        use flate2::write::GzEncoder;
        use std::io::Write;

        let original = b"{\"resourceSpans\":[]}";
        let mut encoder = GzEncoder::new(Vec::new(), Compression::default());
        encoder.write_all(original).unwrap();
        let compressed = Bytes::from(encoder.finish().unwrap());

        let out = decompress_body(ContentEncoding::Gzip, compressed).unwrap();
        assert_eq!(out.as_ref(), original);
    }

    #[test]
    fn test_decompress_body_gzip_invalid_returns_error() {
        let body = Bytes::from_static(b"not gzipped at all");
        let result = decompress_body(ContentEncoding::Gzip, body);
        assert!(matches!(result, Err(DecodeError::Decompression(_))));
    }

    #[test]
    fn test_decompress_body_snappy_decodes_payload() {
        use std::io::Write;

        let original = b"{\"resourceSpans\":[]}";
        let mut encoder = snap::write::FrameEncoder::new(Vec::new());
        encoder.write_all(original).unwrap();
        let compressed = Bytes::from(encoder.into_inner().unwrap());

        let out = decompress_body(ContentEncoding::Snappy, compressed).unwrap();
        assert_eq!(out.as_ref(), original);
    }

    #[test]
    fn test_decompress_body_snappy_invalid_returns_error() {
        let body = Bytes::from_static(b"not snappy at all");
        let result = decompress_body(ContentEncoding::Snappy, body);
        assert!(matches!(result, Err(DecodeError::Decompression(_))));
    }

    #[test]
    fn test_decompress_body_zstd_decodes_payload() {
        let original = b"{\"resourceSpans\":[]}";
        let compressed = Bytes::from(zstd::stream::encode_all(&original[..], 0).unwrap());

        let out = decompress_body(ContentEncoding::Zstd, compressed).unwrap();
        assert_eq!(out.as_ref(), original);
    }

    #[test]
    fn test_decompress_body_zstd_invalid_returns_error() {
        let body = Bytes::from_static(b"not zstd at all");
        let result = decompress_body(ContentEncoding::Zstd, body);
        assert!(matches!(result, Err(DecodeError::Decompression(_))));
    }

    #[test]
    fn test_decompress_body_lz4_decodes_payload() {
        use std::io::Write;

        let original = b"{\"resourceSpans\":[]}";
        let mut encoder = lz4_flex::frame::FrameEncoder::new(Vec::new());
        encoder.write_all(original).unwrap();
        let compressed = Bytes::from(encoder.finish().unwrap());

        let out = decompress_body(ContentEncoding::Lz4, compressed).unwrap();
        assert_eq!(out.as_ref(), original);
    }

    #[test]
    fn test_decompress_body_lz4_invalid_returns_error() {
        let body = Bytes::from_static(b"not lz4 at all");
        let result = decompress_body(ContentEncoding::Lz4, body);
        assert!(matches!(result, Err(DecodeError::Decompression(_))));
    }
}
