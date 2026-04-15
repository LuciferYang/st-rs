// Copyright 2025 The st-rs Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

//! Port of `util/random.h`.
//!
//! Pseudo-random number generators used throughout the engine: skiplist
//! height sampling, cache-shard selection, sample-based statistics. Fast
//! and deterministic; **not** cryptographically secure.
//!
//! Two generators are ported:
//! - [`Random`] — the classic Park–Miller 31-bit LCG from upstream
//!   `rocksdb::Random`. Period 2³¹ − 2; produces `u32`s in
//!   `[0, 2³¹ − 2)`.
//! - [`Random64`] — a 64-bit xorshift\* from upstream `rocksdb::Random64`.
//!
//! Both are single-threaded. Thread-local instances are the standard
//! pattern.

/// Park–Miller 31-bit linear congruential generator. Mirrors upstream
/// `rocksdb::Random`. Deterministic given a seed; period `2³¹ − 2`.
#[derive(Debug, Clone)]
pub struct Random {
    seed: u32,
}

impl Random {
    /// The modulus of the generator (Mersenne prime `2³¹ − 1`).
    pub const M: u32 = 2_147_483_647;
    /// The multiplier.
    pub const A: u64 = 16_807;

    /// Construct a generator. A `seed` of `0` is mapped to `1` because
    /// `0` is a fixed point of the LCG.
    pub fn new(seed: u32) -> Self {
        let seed = seed & 0x7fff_ffff;
        let seed = if seed == 0 || seed == Self::M { 1 } else { seed };
        Self { seed }
    }

    /// Produce the next `u32` in `[0, 2³¹ − 2)`. Mirrors upstream
    /// `rocksdb::Random::Next()`. Named with the `_u32` suffix to match
    /// the Rust ecosystem convention (`rand_core::RngCore::next_u32`)
    /// and avoid collision with `core::iter::Iterator::next`.
    pub fn next_u32(&mut self) -> u32 {
        // product = seed * A, where A = 16807 fits in 15 bits, and
        // seed < 2^31, so product fits in u64.
        let product = self.seed as u64 * Self::A;
        // Compute (product) mod M = 2^31 - 1 with the classic Park–Miller
        // "Schrage" trick, simplified here since we're not bandwidth-bound.
        self.seed = ((product >> 31) + (product & Self::M as u64)) as u32;
        if self.seed > Self::M {
            self.seed -= Self::M;
        }
        self.seed
    }

    /// Return a uniform number in `[0, n)`. Requires `n > 0`.
    pub fn uniform(&mut self, n: u32) -> u32 {
        assert!(n > 0, "Random::uniform requires n > 0");
        self.next_u32() % n
    }

    /// Return `true` with probability `1 / n`. Requires `n > 0`.
    pub fn one_in(&mut self, n: u32) -> bool {
        self.uniform(n) == 0
    }

    /// Return a uniform number in `[0, max_log)` by first picking a number
    /// of bits in `[0, max_log)` and then masking. Matches upstream
    /// `Skewed` — useful for sampling small counts with a long tail.
    pub fn skewed(&mut self, max_log: u32) -> u32 {
        let bits = self.uniform(max_log + 1);
        self.uniform(1u32 << bits)
    }
}

/// 64-bit xorshift\* PRNG. Mirrors upstream `rocksdb::Random64`.
#[derive(Debug, Clone)]
pub struct Random64 {
    state: u64,
}

impl Random64 {
    /// Construct a generator from a 64-bit seed. Seed `0` is mapped to
    /// a non-zero constant.
    pub fn new(seed: u64) -> Self {
        let state = if seed == 0 { 0x9e37_79b9_7f4a_7c15 } else { seed };
        Self { state }
    }

    /// Produce the next 64-bit value. Suffixed `_u64` for consistency
    /// with [`Random::next_u32`].
    pub fn next_u64(&mut self) -> u64 {
        let mut x = self.state;
        x ^= x >> 12;
        x ^= x << 25;
        x ^= x >> 27;
        self.state = x;
        x.wrapping_mul(0x2545_f491_4f6c_dd1d)
    }

    /// Uniform in `[0, n)`. Requires `n > 0`.
    pub fn uniform(&mut self, n: u64) -> u64 {
        assert!(n > 0, "Random64::uniform requires n > 0");
        self.next_u64() % n
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn random_is_deterministic() {
        let mut a = Random::new(42);
        let mut b = Random::new(42);
        for _ in 0..100 {
            assert_eq!(a.next_u32(), b.next_u32());
        }
    }

    #[test]
    fn random_zero_seed_is_nonzero() {
        let mut r = Random::new(0);
        assert!(r.next_u32() > 0);
    }

    #[test]
    fn random_uniform_stays_in_range() {
        let mut r = Random::new(1);
        for _ in 0..1000 {
            let v = r.uniform(10);
            assert!(v < 10);
        }
    }

    #[test]
    fn random_one_in_matches_probability_shape() {
        let mut r = Random::new(1);
        let mut hits = 0;
        let n = 10_000;
        for _ in 0..n {
            if r.one_in(10) {
                hits += 1;
            }
        }
        // Expected ~1000; allow generous slack.
        assert!(hits > 800 && hits < 1200, "hits = {hits}");
    }

    #[test]
    fn random64_is_deterministic() {
        let mut a = Random64::new(7);
        let mut b = Random64::new(7);
        for _ in 0..100 {
            assert_eq!(a.next_u64(), b.next_u64());
        }
    }

    #[test]
    fn random64_uniform_stays_in_range() {
        let mut r = Random64::new(7);
        for _ in 0..1000 {
            assert!(r.uniform(1024) < 1024);
        }
    }

    #[test]
    #[should_panic(expected = "Random::uniform requires n > 0")]
    fn random_uniform_zero_panics() {
        let mut r = Random::new(1);
        r.uniform(0);
    }
}
