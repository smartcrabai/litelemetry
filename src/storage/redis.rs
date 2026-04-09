use crate::domain::telemetry::{NormalizedEntry, Signal};
use bytes::Bytes;

/// Compares Redis stream IDs (`<ms>-<seq>`) as numeric tuples.
///
/// Lexicographic string comparison is incorrect when seq exceeds one digit (`"10" < "9"`),
/// so numeric comparison is required.
pub(crate) fn parse_stream_id(id: &str) -> Option<(u64, u64)> {
    let (ms, seq) = id.split_once('-')?;
    Some((ms.parse().ok()?, seq.parse().ok()?))
}

pub fn cmp_stream_id(a: &str, b: &str) -> std::cmp::Ordering {
    fn expect_stream_id_parts(id: &str) -> (u64, u64) {
        parse_stream_id(id)
            .unwrap_or_else(|| panic!("invalid Redis stream ID {id:?}; expected <ms>-<seq>"))
    }

    expect_stream_id_parts(a).cmp(&expect_stream_id_parts(b))
}

/// Returns the Redis stream key name corresponding to the given signal.
///
/// Key naming convention: `lt:stream:{signal}`
///   - traces  -> `lt:stream:traces`
///   - metrics -> `lt:stream:metrics`
///   - logs    -> `lt:stream:logs`
pub fn stream_key_for_signal(signal: Signal) -> &'static str {
    match signal {
        Signal::Traces => "lt:stream:traces",
        Signal::Metrics => "lt:stream:metrics",
        Signal::Logs => "lt:stream:logs",
    }
}

/// Store responsible for adding and reading entries in a Redis stream
#[derive(Clone)]
pub struct RedisStore {
    conn: redis::aio::MultiplexedConnection,
}

impl RedisStore {
    pub async fn new(url: &str) -> Result<Self, redis::RedisError> {
        let client = redis::Client::open(url)?;
        let conn = client.get_multiplexed_async_connection().await?;
        Ok(Self { conn })
    }

    /// XADDs a NormalizedEntry to the corresponding stream and returns the generated stream ID.
    pub async fn append_entry(
        &mut self,
        entry: &NormalizedEntry,
    ) -> Result<String, redis::RedisError> {
        let key = stream_key_for_signal(entry.signal);
        let observed_at = entry.observed_at.to_rfc3339();
        let service_name = entry.service_name.as_deref().unwrap_or("");
        let payload: &[u8] = &entry.payload;

        let id: String = redis::cmd("XADD")
            .arg(key)
            .arg("*")
            .arg("observed_at")
            .arg(&observed_at)
            .arg("service_name")
            .arg(service_name)
            .arg("payload")
            .arg(payload)
            .query_async(&mut self.conn)
            .await?;

        Ok(id)
    }

    /// XREADs and returns entries from the given cursor onwards.
    ///
    /// XREAD returns entries after the specified ID (exclusive).
    /// When cursor is None, reads from the beginning of the stream ("0-0").
    pub async fn read_entries_since(
        &mut self,
        signal: Signal,
        cursor: Option<&str>,
        count: usize,
    ) -> Result<Vec<(String, NormalizedEntry)>, redis::RedisError> {
        let key = stream_key_for_signal(signal);
        // XREAD returns entries after cursor_id (exclusive).
        // When cursor = None, specify "0-0" to fetch all entries (IDs greater than 0-0 = all entries).
        let start_id = cursor.unwrap_or("0-0");

        let raw: redis::Value = redis::cmd("XREAD")
            .arg("COUNT")
            .arg(count)
            .arg("STREAMS")
            .arg(key)
            .arg(start_id)
            .query_async(&mut self.conn)
            .await?;

        parse_xread_reply(signal, raw)
    }
}

/// Parses the raw response from XREAD.
/// Returns an empty Vec if the stream does not exist or has no entries.
fn parse_xread_reply(
    signal: Signal,
    value: redis::Value,
) -> Result<Vec<(String, NormalizedEntry)>, redis::RedisError> {
    use redis::FromRedisValue;

    // XREAD returns Nil when there are no matching entries
    if matches!(value, redis::Value::Nil) {
        return Ok(Vec::new());
    }

    let reply = redis::streams::StreamReadReply::from_redis_value(value)?;
    let mut results = Vec::new();
    for key_data in reply.keys {
        for item in key_data.ids {
            let id = item.id.clone();
            if let Some(entry) = parse_stream_entry(signal, &item) {
                results.push((id, entry));
            }
        }
    }
    Ok(results)
}

fn parse_stream_entry(signal: Signal, item: &redis::streams::StreamId) -> Option<NormalizedEntry> {
    let observed_at_str: String = item.get("observed_at")?;
    let observed_at = chrono::DateTime::parse_from_rfc3339(&observed_at_str)
        .ok()?
        .with_timezone(&chrono::Utc);
    let service_name_str: String = item.get("service_name").unwrap_or_default();
    let service_name = if service_name_str.is_empty() {
        None
    } else {
        Some(service_name_str)
    };
    let payload_bytes: Vec<u8> = item.get("payload")?;

    Some(NormalizedEntry {
        signal,
        observed_at,
        service_name,
        payload: Bytes::from(payload_bytes),
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::domain::telemetry::Signal;

    // --- stream_key_for_signal -----------------------------------------------

    #[test]
    fn test_stream_key_for_traces() {
        assert_eq!(stream_key_for_signal(Signal::Traces), "lt:stream:traces");
    }

    #[test]
    fn test_stream_key_for_metrics() {
        assert_eq!(stream_key_for_signal(Signal::Metrics), "lt:stream:metrics");
    }

    #[test]
    fn test_stream_key_for_logs() {
        assert_eq!(stream_key_for_signal(Signal::Logs), "lt:stream:logs");
    }

    #[test]
    fn test_stream_keys_are_distinct() {
        let traces = stream_key_for_signal(Signal::Traces);
        let metrics = stream_key_for_signal(Signal::Metrics);
        let logs = stream_key_for_signal(Signal::Logs);

        assert_ne!(traces, metrics);
        assert_ne!(metrics, logs);
        assert_ne!(traces, logs);
    }

    #[test]
    fn test_stream_key_has_lt_prefix() {
        for signal in [Signal::Traces, Signal::Metrics, Signal::Logs] {
            let key = stream_key_for_signal(signal);
            assert!(
                key.starts_with("lt:stream:"),
                "key {key:?} should start with 'lt:stream:'"
            );
        }
    }

    #[test]
    fn test_cmp_stream_id_uses_numeric_sequence_order() {
        assert!(cmp_stream_id("1710000000000-10", "1710000000000-9").is_gt());
    }

    #[test]
    fn test_parse_stream_id_rejects_invalid_ids() {
        assert_eq!(
            parse_stream_id("1710000000000-1"),
            Some((1_710_000_000_000, 1))
        );
        assert_eq!(parse_stream_id("1710000000000"), None);
        assert_eq!(parse_stream_id("1710000000000-"), None);
        assert_eq!(parse_stream_id("abc-1"), None);
    }

    #[test]
    #[should_panic(expected = "invalid Redis stream ID")]
    fn test_cmp_stream_id_panics_for_invalid_ids() {
        let _ = cmp_stream_id("invalid", "1710000000000-1");
    }
}
