use crate::domain::telemetry::{NormalizedEntry, Signal};
use bytes::Bytes;

/// Redis stream ID (`<ms>-<seq>`) を数値タプルとして比較する。
///
/// 文字列辞書順は seq が 2 桁以上になると誤るため (`"10" < "9"`) 数値比較が必要。
pub fn cmp_stream_id(a: &str, b: &str) -> std::cmp::Ordering {
    fn parse(s: &str) -> (u64, u64) {
        let mut p = s.splitn(2, '-');
        let ms = p.next().and_then(|x| x.parse().ok()).unwrap_or(0);
        let seq = p.next().and_then(|x| x.parse().ok()).unwrap_or(0);
        (ms, seq)
    }
    parse(a).cmp(&parse(b))
}

/// signal に対応する Redis stream キー名を返す。
///
/// キー命名規則: `lt:stream:{signal}`
///   - traces  → `lt:stream:traces`
///   - metrics → `lt:stream:metrics`
///   - logs    → `lt:stream:logs`
pub fn stream_key_for_signal(signal: Signal) -> &'static str {
    match signal {
        Signal::Traces => "lt:stream:traces",
        Signal::Metrics => "lt:stream:metrics",
        Signal::Logs => "lt:stream:logs",
    }
}

/// Redis stream へのエントリ追加と読み出しを担うストア
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

    /// NormalizedEntry を対応 stream に XADD し、生成された stream ID を返す。
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

    /// cursor 以降のエントリを XREAD して返す。
    ///
    /// XREAD は指定した ID より後 (exclusive) のエントリを返す。
    /// cursor が None の場合はストリームの先頭 ("0-0") から読む。
    pub async fn read_entries_since(
        &mut self,
        signal: Signal,
        cursor: Option<&str>,
        count: usize,
    ) -> Result<Vec<(String, NormalizedEntry)>, redis::RedisError> {
        let key = stream_key_for_signal(signal);
        // XREAD は cursor_id より後 (exclusive) を返す。
        // cursor = None のときは "0-0" を指定して全件取得 (0-0 より大きい ID = 全エントリ)。
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

/// XREAD の生レスポンスをパースする。
/// ストリームが存在しない・エントリなしの場合は空 Vec を返す。
fn parse_xread_reply(
    signal: Signal,
    value: redis::Value,
) -> Result<Vec<(String, NormalizedEntry)>, redis::RedisError> {
    use redis::FromRedisValue;

    // XREAD は該当エントリなしのとき Nil を返す
    if matches!(value, redis::Value::Nil) {
        return Ok(Vec::new());
    }

    let reply = redis::streams::StreamReadReply::from_redis_value(&value)?;
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

    // ─── stream_key_for_signal ────────────────────────────────────────────────

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
}
