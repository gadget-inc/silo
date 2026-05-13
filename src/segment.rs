//! Segment-oriented compaction support (slatedb RFC-0024).
//!
//! When enabled at db-open time, every silo key gets a fixed-length
//! segment-prefix prepended. slatedb keeps a separate L0/SR tree per
//! segment, so compaction work is bounded to the size of one segment
//! instead of the whole database.
//!
//! Segments are derived from data at key-construction time, not from
//! wall-clock-now at write time, so a key written today is rediscoverable
//! tomorrow: the same job_id always maps to the same segment.
//!
//! Two strategies are supported:
//! - [`SegmentStrategy::Date`]: 8 ASCII bytes `"YYYYMMDD"` from the UUIDv7
//!   timestamp in the entity id.
//! - [`SegmentStrategy::Minute`]: 1 byte (`0..=59`) holding the minute-of-hour
//!   from the UUIDv7 timestamp. Intended for short-lived test/dev shards
//!   where date buckets are too coarse.
//!
//! Entities without a timestamp-bearing id (counters, leases, cleanup
//! markers) land in a single sentinel segment — `"99999999"` for date
//! and `60` for minute — so they are still routable through the same
//! fixed-length extractor.

use std::sync::OnceLock;

use serde::{Deserialize, Serialize};
use slatedb::prefix_extractor::{PrefixExtractor, PrefixTarget};
use uuid::Uuid;

#[derive(Debug, Deserialize, Serialize, Clone, Copy, PartialEq, Eq)]
#[serde(rename_all = "lowercase")]
pub enum SegmentStrategy {
    Date,
    Minute,
}

impl Default for SegmentStrategy {
    fn default() -> Self {
        Self::Date
    }
}

impl SegmentStrategy {
    pub fn prefix_len(self) -> usize {
        match self {
            Self::Date => 8,
            Self::Minute => 1,
        }
    }

    pub fn name(self) -> &'static str {
        match self {
            Self::Date => "silo-date-yyyymmdd",
            Self::Minute => "silo-minute",
        }
    }

    fn prefix_bytes_from_unix_ms(self, ms: i64) -> Vec<u8> {
        match self {
            Self::Date => {
                let days = ms.div_euclid(86_400_000);
                let (y, m, d) = civil_from_days(days);
                let s = format!("{:04}{:02}{:02}", y, m, d);
                s.into_bytes()
            }
            Self::Minute => {
                let minutes = ms.div_euclid(60_000);
                let minute_of_hour = minutes.rem_euclid(60) as u8;
                vec![minute_of_hour]
            }
        }
    }

    fn sentinel_bytes(self) -> Vec<u8> {
        match self {
            // 8 ASCII '9' bytes — larger lex value than any real
            // YYYYMMDD bucket, so sentinel keys sort after dated ones.
            Self::Date => vec![b'9'; 8],
            // 60 is out-of-range for minute-of-hour [0..=59], so we
            // can spot sentinel-routed keys at a glance in dumps.
            Self::Minute => vec![60u8],
        }
    }
}

/// Howard Hinnant's civil_from_days. `days` is the count of days since
/// 1970-01-01 (negative for pre-epoch). Returns `(year, month, day)`
/// where month is 1..=12 and day is 1..=31. Pure integer math, valid
/// over the full proleptic Gregorian range.
fn civil_from_days(days: i64) -> (i32, u32, u32) {
    let z = days + 719_468;
    let era = z.div_euclid(146_097);
    let doe = (z - era * 146_097) as u32;
    let yoe = (doe.saturating_sub(doe / 1460) + doe / 36_524 - doe / 146_096) / 365;
    let y = yoe as i64 + era * 400;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
    let mp = (5 * doy + 2) / 153;
    let d = doy - (153 * mp + 2) / 5 + 1;
    let m = if mp < 10 { mp + 3 } else { mp - 9 };
    let y = if m <= 2 { y + 1 } else { y };
    (y as i32, m, d)
}

static STRATEGY: OnceLock<SegmentStrategy> = OnceLock::new();

/// Initialize the process-wide segment strategy. The first call wins.
/// Later calls that match the existing strategy are a no-op; mismatched
/// ones return Err with the previously-set value (so an operator who
/// reconfigures a running silo and reopens shards trips a clear error
/// instead of silently fragmenting the manifest).
pub fn init(s: SegmentStrategy) -> Result<(), SegmentStrategy> {
    match STRATEGY.set(s) {
        Ok(()) => Ok(()),
        Err(_) => {
            let existing = *STRATEGY.get().expect("set failed implies value present");
            if existing == s { Ok(()) } else { Err(existing) }
        }
    }
}

/// Returns the configured strategy, or `None` when segmentation is off.
pub fn current() -> Option<SegmentStrategy> {
    STRATEGY.get().copied()
}

/// Length of the segment prefix prepended to every key when
/// segmentation is enabled, or `0` when disabled. Parse functions use
/// this to skip past the prefix.
pub fn prefix_len() -> usize {
    STRATEGY.get().map(|s| s.prefix_len()).unwrap_or(0)
}

/// Build the segment-prefix bytes for an entity-id string. The id is
/// parsed as a UUID; if it is a UUIDv7 we use its embedded timestamp,
/// otherwise we fall back to the sentinel segment. Returns an empty
/// vector when segmentation is disabled.
pub fn prefix_for_id(id: &str) -> Vec<u8> {
    let Some(strat) = current() else {
        return Vec::new();
    };
    match Uuid::parse_str(id) {
        Ok(u) if u.get_version_num() == 7 => {
            let (secs, nanos) = u.get_timestamp().expect("v7 has timestamp").to_unix();
            let ms = (secs as i64) * 1000 + (nanos as i64) / 1_000_000;
            strat.prefix_bytes_from_unix_ms(ms)
        }
        _ => strat.sentinel_bytes(),
    }
}

/// Build the segment-prefix bytes from a Unix-ms timestamp. Used by
/// keys that already carry a timestamp in their components
/// (e.g. `idx_status_time_key`). Returns an empty vector when
/// segmentation is disabled.
pub fn prefix_for_unix_ms(ms: i64) -> Vec<u8> {
    match current() {
        Some(s) => s.prefix_bytes_from_unix_ms(ms),
        None => Vec::new(),
    }
}

/// Sentinel segment-prefix bytes for keys that lack a natural
/// timestamp source (counters, cleanup state, shard metadata).
/// Returns an empty vector when segmentation is disabled.
pub fn prefix_sentinel() -> Vec<u8> {
    match current() {
        Some(s) => s.sentinel_bytes(),
        None => Vec::new(),
    }
}

/// PrefixExtractor implementation wired into `DbBuilder`. The extractor
/// name is part of the slatedb manifest contract — changing it after
/// data is written fails open. See `SegmentStrategy::name`.
#[derive(Debug)]
pub struct SiloPrefixExtractor {
    name: &'static str,
    fixed_len: usize,
}

impl SiloPrefixExtractor {
    pub fn new(strategy: SegmentStrategy) -> Self {
        Self {
            name: strategy.name(),
            fixed_len: strategy.prefix_len(),
        }
    }
}

impl PrefixExtractor for SiloPrefixExtractor {
    fn name(&self) -> &str {
        self.name
    }

    fn prefix_len(&self, target: &PrefixTarget) -> Option<usize> {
        let len = match target {
            PrefixTarget::Point(b) | PrefixTarget::Prefix(b) => b.len(),
        };
        if len >= self.fixed_len {
            Some(self.fixed_len)
        } else {
            None
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn date_prefix_for_known_unix_ms() {
        let s = SegmentStrategy::Date;
        // 2024-01-15 00:00:00 UTC = 1705276800 sec = 1705276800000 ms
        let bytes = s.prefix_bytes_from_unix_ms(1_705_276_800_000);
        assert_eq!(&bytes, b"20240115");
    }

    #[test]
    fn date_prefix_handles_epoch() {
        let s = SegmentStrategy::Date;
        let bytes = s.prefix_bytes_from_unix_ms(0);
        assert_eq!(&bytes, b"19700101");
    }

    #[test]
    fn minute_prefix_cycles() {
        let s = SegmentStrategy::Minute;
        // 03:17 UTC → minute 17
        let ms = (3 * 3600 + 17 * 60) * 1000;
        assert_eq!(s.prefix_bytes_from_unix_ms(ms), vec![17]);
    }

    #[test]
    fn sentinel_is_fixed_length() {
        assert_eq!(
            SegmentStrategy::Date.sentinel_bytes().len(),
            SegmentStrategy::Date.prefix_len()
        );
        assert_eq!(
            SegmentStrategy::Minute.sentinel_bytes().len(),
            SegmentStrategy::Minute.prefix_len()
        );
    }

    #[test]
    fn extractor_reports_fixed_length_when_input_long_enough() {
        use bytes::Bytes;
        let ex = SiloPrefixExtractor::new(SegmentStrategy::Date);
        assert_eq!(
            ex.prefix_len(&PrefixTarget::Point(Bytes::from_static(b"20240115_key"))),
            Some(8)
        );
        assert_eq!(
            ex.prefix_len(&PrefixTarget::Prefix(Bytes::from_static(b"202"))),
            None
        );
    }

    #[test]
    fn segment_for_v7_uuid_extracts_date() {
        // crate-side gen: we won't initialize the OnceLock in tests.
        // Just exercise the impl helper through the strategy.
        let id = Uuid::now_v7();
        let s = SegmentStrategy::Date;
        let (secs, nanos) = id.get_timestamp().unwrap().to_unix();
        let ms = (secs as i64) * 1000 + (nanos as i64) / 1_000_000;
        let bytes = s.prefix_bytes_from_unix_ms(ms);
        assert_eq!(bytes.len(), 8);
        assert!(bytes.iter().all(|b| b.is_ascii_digit()));
    }
}
