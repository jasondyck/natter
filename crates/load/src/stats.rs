//! What subscribers measure: unique packets, duplicates and one-way
//! latency per publisher's stream. Sender and receiver share a clock (one
//! process), so latency is exact.

use std::collections::HashMap;

use hdrhistogram::Histogram;

use crate::stamp::Stamp;

/// Receipts within this many packets of the newest are checked for
/// duplicates (a resend that raced its original, say).
const WINDOW: u32 = 128;

/// Latency past which a packet counts as late; on loopback only a resend
/// (NACK) or a stall takes this long.
pub const LATE_US: u64 = 100_000;

#[derive(Default)]
pub struct Stream {
    newest: Option<u32>,
    /// Bit `i` set: `newest - i` was received.
    seen: u128,
}

impl Stream {
    /// Records a sequence number; false if it was already received.
    pub fn insert(&mut self, seq: u32) -> bool {
        let Some(newest) = self.newest else {
            self.newest = Some(seq);
            self.seen = 1;
            return true;
        };
        let ahead = seq.wrapping_sub(newest);
        if ahead != 0 && ahead < 1 << 31 {
            self.seen = if ahead >= WINDOW {
                0
            } else {
                self.seen << ahead
            };
            self.seen |= 1;
            self.newest = Some(seq);
            return true;
        }
        let behind = newest.wrapping_sub(seq);
        if behind >= WINDOW {
            return true;
        }
        let bit = 1u128 << behind;
        let fresh = self.seen & bit == 0;
        self.seen |= bit;
        fresh
    }
}

pub struct Receipts {
    pub unique: u64,
    pub duplicates: u64,
    pub late: u64,
    /// Microseconds from send to receipt, unique packets only.
    pub latency: Histogram<u64>,
}

impl Default for Receipts {
    fn default() -> Self {
        Receipts {
            unique: 0,
            duplicates: 0,
            late: 0,
            latency: Histogram::new_with_bounds(1, 60_000_000, 3).expect("valid bounds"),
        }
    }
}

impl Receipts {
    /// Counts a measured packet; `fresh` is what [`Stream::insert`] said.
    pub fn record(&mut self, fresh: bool, stamp: &Stamp, now_us: u64) {
        if !fresh {
            self.duplicates += 1;
            return;
        }
        self.unique += 1;
        let latency = now_us.saturating_sub(stamp.sent_us).max(1);
        if latency >= LATE_US {
            self.late += 1;
        }
        self.latency.saturating_record(latency);
    }

    pub fn merge(&mut self, other: &Receipts) {
        self.unique += other.unique;
        self.duplicates += other.duplicates;
        self.late += other.late;
        self.latency.add(&other.latency).expect("same bounds");
    }
}

/// A subscriber's streams, by publisher id.
pub type Streams = HashMap<u32, Stream>;

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn counts_each_sequence_once() {
        let mut s = Stream::default();
        assert!(s.insert(10));
        assert!(s.insert(12));
        assert!(!s.insert(12));
        assert!(s.insert(11), "a late packet inside the window");
        assert!(!s.insert(10));
        assert!(s.insert(10 + 500), "a jump clears the window");
        assert!(s.insert(13), "too old to tell: counted");

        let mut w = Stream::default();
        assert!(w.insert(u32::MAX) && w.insert(0) && w.insert(1));
        assert!(!w.insert(u32::MAX), "seen across the wrap");
    }
}
