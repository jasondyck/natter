//! Which shard (engine thread) each room lives on.
//!
//! Rooms never share media, so a server runs one engine per core and gives
//! each room wholly to one of them. The signalling side places a room when
//! its first participant joins. Shards fill one at a time up to `fill`
//! participants before the next is used: a busy engine handles packets
//! back to back, while one with little to do wakes up for nearly every
//! packet, which costs more than the packet. Past `fill` everywhere, the
//! least loaded shard gets the room.
//!
//! Rooms are placed when they start, with one participant, and grow after,
//! and a room can't move. So a shard's load counts each of its rooms as at
//! least [`EXPECTED_ROOM`] participants: otherwise a burst of new rooms
//! would all land on the first shard before any of them filled.
//! Every join takes a reference to the room's placement and the engine
//! drops it when that participant goes; the room itself holds one until
//! it closes. So a room can't be split across shards while it exists, and
//! a room an admin created empty stays where it was put.

use std::collections::HashMap;
use std::sync::Mutex;
use std::sync::atomic::{AtomicUsize, Ordering::Relaxed};

/// Participants a new room is assumed to grow to, for placing others.
pub const EXPECTED_ROOM: usize = 8;

pub struct Placement {
    rooms: Mutex<HashMap<String, Placed>>,
    /// Participants a shard takes before new rooms go to the next.
    fill: usize,
    /// Participants per shard, as each engine last counted them.
    load: Vec<AtomicUsize>,
}

struct Placed {
    shard: usize,
    /// Participants joined or joining.
    refs: usize,
}

impl Placement {
    pub fn new(shards: usize, fill: usize) -> Self {
        Placement {
            rooms: Mutex::default(),
            fill,
            load: (0..shards.max(1)).map(|_| AtomicUsize::new(0)).collect(),
        }
    }

    /// The shard for a joining participant, taking a reference to the
    /// room's placement. The engine must `leave` exactly once for it.
    pub fn join(&self, room: &str) -> usize {
        let mut rooms = self.rooms.lock().expect("placement lock");
        if let Some(p) = rooms.get_mut(room) {
            p.refs += 1;
            return p.shard;
        }
        let mut projected = vec![0usize; self.load.len()];
        for p in rooms.values() {
            projected[p.shard] += p.refs.max(EXPECTED_ROOM);
        }
        let load = |s: usize| projected[s].max(self.load[s].load(Relaxed));
        let shards = 0..self.load.len();
        // The fullest shard still under `fill`, else the emptiest.
        let shard = shards
            .clone()
            .filter(|&s| load(s) < self.fill)
            .max_by_key(|&s| (load(s), std::cmp::Reverse(s)))
            .or_else(|| shards.min_by_key(|&s| load(s)))
            .unwrap_or(0);
        rooms.insert(room.to_owned(), Placed { shard, refs: 1 });
        shard
    }

    /// Another reference to a placed room: the room's own, which its
    /// engine drops when the room closes.
    pub fn hold(&self, room: &str) {
        if let Some(p) = self.rooms.lock().expect("placement lock").get_mut(room) {
            p.refs += 1;
        }
    }

    /// Drops a participant's reference; the last one frees the room.
    pub fn leave(&self, room: &str) {
        let mut rooms = self.rooms.lock().expect("placement lock");
        if let Some(p) = rooms.get_mut(room) {
            p.refs = p.refs.saturating_sub(1);
            if p.refs == 0 {
                rooms.remove(room);
            }
        }
    }

    /// Where a room is, for a client resuming its session there.
    pub fn find(&self, room: &str) -> Option<usize> {
        self.rooms
            .lock()
            .expect("placement lock")
            .get(room)
            .map(|p| p.shard)
    }

    /// The rooms on this server.
    pub fn rooms(&self) -> Vec<String> {
        self.rooms
            .lock()
            .expect("placement lock")
            .keys()
            .cloned()
            .collect()
    }

    /// An engine's participant count, for placing new rooms.
    pub fn set_load(&self, shard: usize, participants: usize) {
        self.load[shard].store(participants, Relaxed);
    }

    /// Participants on the whole server.
    pub fn total(&self) -> usize {
        self.load.iter().map(|l| l.load(Relaxed)).sum()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn rooms_stay_on_one_shard_while_occupied() {
        let p = Placement::new(4, 100);
        let a = p.join("a");
        assert_eq!(p.join("a"), a);
        p.leave("a");
        assert_eq!(p.find("a"), Some(a), "one participant left");
        p.leave("a");
        assert_eq!(p.find("a"), None);
    }

    #[test]
    fn shards_fill_one_at_a_time_then_spread() {
        // Room for three expected rooms per shard.
        let p = Placement::new(3, 3 * EXPECTED_ROOM);
        let shards: Vec<usize> = (0..7).map(|i| p.join(&format!("r{i}"))).collect();
        assert_eq!(shards, [0, 0, 0, 1, 1, 1, 2]);
        // A room that grows past the expectation counts as its size.
        for _ in 0..20 {
            p.join("r6");
        }
        p.set_load(0, 30);
        p.set_load(1, 30);
        // Everyone past fill: the emptiest by its engine's own count.
        assert_eq!(p.join("x"), 2);
    }
}
