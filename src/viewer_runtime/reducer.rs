use crate::domain::telemetry::NormalizedEntry;
use crate::viewer_runtime::compiler::CompiledViewer;
use crate::viewer_runtime::state::ViewerState;
use chrono::{DateTime, Utc};

/// エントリを viewer に適用する。
/// - viewer の signal_mask に合致しないエントリは無視する。
/// - 合致した場合は state.entries に追加する。
///
/// **前提条件**: 呼び出し元は entry を時刻昇順（古い順）で渡す責任を持つ。
/// prune_stale_buckets は entries が時刻昇順であることを前提とするため、
/// 順序が乱れると刈り込みが正しく動作しない。
pub fn apply_entry(state: &mut ViewerState, viewer: &CompiledViewer, entry: NormalizedEntry) {
    if viewer.matches_signal(entry.signal) {
        state.entries.push(entry);
    }
}

/// now から lookback_ms より前のエントリを state から削除する。
/// - 削除したエントリ数を返す。
/// - `entry.observed_at <= now - lookback_ms` が削除対象 (境界も含む)。
/// - state.entries が時刻昇順（古い順）であることを前提とする。
pub fn prune_stale_buckets(state: &mut ViewerState, lookback_ms: i64, now: DateTime<Utc>) -> usize {
    let cutoff = now - chrono::Duration::milliseconds(lookback_ms);
    // 時刻昇順を前提に二分探索でカット位置を特定し O(log n + 削除数) で処理する
    let pos = state.entries.partition_point(|e| e.observed_at <= cutoff);
    state.entries.drain(..pos);
    pos
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::domain::telemetry::Signal;
    use crate::domain::viewer::ViewerDefinition;
    use crate::viewer_runtime::compiler::compile;
    use bytes::Bytes;
    use chrono::{Duration, Utc};
    use serde_json::json;
    use uuid::Uuid;

    fn make_compiled_viewer(signal_mask: crate::domain::telemetry::SignalMask) -> CompiledViewer {
        compile(ViewerDefinition {
            id: Uuid::new_v4(),
            slug: "test".to_string(),
            name: "Test".to_string(),
            refresh_interval_ms: 5_000,
            lookback_ms: 60_000,
            signal_mask,
            definition_json: json!({}),
            layout_json: json!({}),
            revision: 1,
            enabled: true,
        })
        .unwrap()
    }

    fn make_entry(signal: Signal, age_ms: i64) -> NormalizedEntry {
        NormalizedEntry {
            signal,
            observed_at: Utc::now() - Duration::milliseconds(age_ms),
            service_name: Some("test-service".to_string()),
            payload: Bytes::from_static(b"test"),
        }
    }

    fn make_state() -> ViewerState {
        ViewerState::new(Uuid::new_v4(), 1)
    }

    // ─── apply_entry ─────────────────────────────────────────────────────────

    #[test]
    fn test_apply_entry_matching_signal_is_added() {
        // Given: traces を対象とする viewer、空の state
        let viewer = make_compiled_viewer(Signal::Traces.into());
        let mut state = make_state();
        let entry = make_entry(Signal::Traces, 0);

        // When: traces エントリを適用
        apply_entry(&mut state, &viewer, entry);

        // Then: エントリが追加される
        assert_eq!(state.entries.len(), 1);
    }

    #[test]
    fn test_apply_entry_non_matching_signal_is_ignored() {
        // Given: traces のみを対象とする viewer
        let viewer = make_compiled_viewer(Signal::Traces.into());
        let mut state = make_state();
        let entry = make_entry(Signal::Metrics, 0);

        // When: metrics エントリを適用
        apply_entry(&mut state, &viewer, entry);

        // Then: エントリは無視される
        assert_eq!(state.entries.len(), 0);
    }

    #[test]
    fn test_apply_multiple_entries_all_matching() {
        // Given: 全シグナルを対象とする viewer
        let viewer = make_compiled_viewer(Signal::Traces | Signal::Metrics | Signal::Logs);
        let mut state = make_state();

        // When: 各シグナルのエントリを適用
        apply_entry(&mut state, &viewer, make_entry(Signal::Traces, 0));
        apply_entry(&mut state, &viewer, make_entry(Signal::Metrics, 0));
        apply_entry(&mut state, &viewer, make_entry(Signal::Logs, 0));

        // Then: 全エントリが追加される
        assert_eq!(state.entries.len(), 3);
    }

    #[test]
    fn test_apply_entry_logs_ignored_by_traces_viewer() {
        // Given: traces のみの viewer
        let viewer = make_compiled_viewer(Signal::Traces.into());
        let mut state = make_state();

        // When: logs エントリを適用
        apply_entry(&mut state, &viewer, make_entry(Signal::Logs, 0));

        // Then: 無視される
        assert_eq!(state.entries.len(), 0);
    }

    // ─── prune_stale_buckets ─────────────────────────────────────────────────
    // 注意: entries は時刻昇順（古い順）で挿入する必要がある。

    #[test]
    fn test_prune_removes_entries_older_than_lookback() {
        // Given: 2件の古いエントリ (120s, 90s前) と 1件の新しいエントリ (10s前)
        //        昇順 (古い→新しい) で挿入。lookback = 60_000ms
        let viewer = make_compiled_viewer(Signal::Traces.into());
        let mut state = make_state();
        let lookback_ms = 60_000i64;
        let now = Utc::now();

        apply_entry(&mut state, &viewer, make_entry(Signal::Traces, 120_000));
        apply_entry(&mut state, &viewer, make_entry(Signal::Traces, 90_000));
        apply_entry(&mut state, &viewer, make_entry(Signal::Traces, 10_000));

        // When: prune
        let pruned = prune_stale_buckets(&mut state, lookback_ms, now);

        // Then: 古い 2件が削除され、新しい 1件が残る
        assert_eq!(pruned, 2);
        assert_eq!(state.entries.len(), 1);
    }

    #[test]
    fn test_prune_empty_state_returns_zero() {
        // Given: 空の state
        let mut state = make_state();
        let now = Utc::now();

        // When: prune
        let pruned = prune_stale_buckets(&mut state, 60_000, now);

        // Then: 0件削除
        assert_eq!(pruned, 0);
        assert_eq!(state.entries.len(), 0);
    }

    #[test]
    fn test_prune_all_within_lookback_removes_nothing() {
        // Given: 全エントリが lookback 内 (30s, 10s前) - 昇順で挿入
        let viewer = make_compiled_viewer(Signal::Traces.into());
        let mut state = make_state();
        let lookback_ms = 60_000i64;
        let now = Utc::now();

        apply_entry(&mut state, &viewer, make_entry(Signal::Traces, 30_000));
        apply_entry(&mut state, &viewer, make_entry(Signal::Traces, 10_000));

        // When: prune
        let pruned = prune_stale_buckets(&mut state, lookback_ms, now);

        // Then: 削除なし
        assert_eq!(pruned, 0);
        assert_eq!(state.entries.len(), 2);
    }

    #[test]
    fn test_prune_all_stale_removes_all() {
        // Given: 全エントリが lookback 外 (180s, 90s前) - 昇順で挿入
        let viewer = make_compiled_viewer(Signal::Traces.into());
        let mut state = make_state();
        let lookback_ms = 60_000i64;
        let now = Utc::now();

        apply_entry(&mut state, &viewer, make_entry(Signal::Traces, 180_000));
        apply_entry(&mut state, &viewer, make_entry(Signal::Traces, 90_000));

        // When: prune
        let pruned = prune_stale_buckets(&mut state, lookback_ms, now);

        // Then: 全件削除
        assert_eq!(pruned, 2);
        assert_eq!(state.entries.len(), 0);
    }

    #[test]
    fn test_prune_boundary_entry_is_removed() {
        // Given: ちょうど lookback_ms 前のエントリ 1件と、それより新しい 1件
        //        境界 (= lookback_ms 前) は削除対象 (境界含む)
        let viewer = make_compiled_viewer(Signal::Traces.into());
        let mut state = make_state();
        let lookback_ms = 60_000i64;
        let now = Utc::now();

        // ちょうど境界 (60s 前) - 古い方を先に追加
        let boundary_entry = NormalizedEntry {
            signal: Signal::Traces,
            observed_at: now - Duration::milliseconds(lookback_ms),
            service_name: None,
            payload: Bytes::from_static(b"boundary"),
        };
        state.entries.push(boundary_entry);

        // 境界より新しい (30s 前)
        apply_entry(&mut state, &viewer, make_entry(Signal::Traces, 30_000));

        // When: prune
        let pruned = prune_stale_buckets(&mut state, lookback_ms, now);

        // Then: 境界エントリが削除され、新しい 1件のみ残る
        assert_eq!(pruned, 1);
        assert_eq!(state.entries.len(), 1);
    }
}
