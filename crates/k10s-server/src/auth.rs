use k10s_protocol::{ErrorCode, Hello, PROTOCOL_MAJOR, PROTOCOL_MINOR, ProtocolVersion};

use crate::config::ServerConfig;

#[derive(Debug)]
pub(crate) struct Negotiated {
    pub(crate) protocol: ProtocolVersion,
    pub(crate) capabilities: Vec<String>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum AuthenticationError {
    Unauthorized,
    IncompatibleProtocol { client_major: u16 },
}

impl AuthenticationError {
    pub(crate) fn code(self) -> ErrorCode {
        match self {
            Self::Unauthorized => ErrorCode::Unauthorized,
            Self::IncompatibleProtocol { .. } => ErrorCode::IncompatibleProtocol,
        }
    }

    pub(crate) fn safe_reason(self) -> &'static str {
        match self {
            Self::Unauthorized => "authentication failed",
            Self::IncompatibleProtocol { .. } => "incompatible protocol major",
        }
    }
}

/// Hard upper bound on the length of a configured access token, enforced at
/// startup so comparison iteration count never depends on credential length.
pub const MAX_ACCESS_TOKEN_BYTES: usize = 512;

/// Constant-time byte comparison for credential material.
///
/// The loop always runs exactly `MAX_ACCESS_TOKEN_BYTES` iterations regardless
/// of the input lengths or where the sequences diverge, reading out-of-range
/// positions as zero, and any length mismatch is folded into the same
/// accumulator. For inputs within that bound (guaranteed for one side by
/// startup validation) neither early-exit timing nor iteration count depends
/// on the input values or lengths, so credentials cannot be recovered from it.
/// Beyond the bound correctness must still hold unconditionally: only when the
/// window passed AND both lengths are equal can a tail exist that both sides
/// share at the same positions; then every byte past the fixed window is
/// compared sequentially from the constant bound, so no middle range stays
/// unexamined at any length. Returns true only when both sequences are
/// identical in length and content.
pub(crate) fn const_time_eq(left: &[u8], right: &[u8]) -> bool {
    let mut difference = left.len() ^ right.len();
    for index in 0..MAX_ACCESS_TOKEN_BYTES {
        difference |= (*left.get(index).unwrap_or(&0) ^ *right.get(index).unwrap_or(&0)) as usize;
    }
    if difference != 0 {
        return false;
    }
    // The fixed window matched and the length fold is zero, so both inputs are
    // equal-length. Within the bound that decides equality; beyond it, every
    // remaining byte must be examined at its constant position.
    for index in MAX_ACCESS_TOKEN_BYTES..left.len() {
        difference |= (left[index] ^ right[index]) as usize;
    }
    difference == 0
}

pub(crate) fn authenticate(
    config: &ServerConfig,
    hello: &Hello,
) -> Result<Negotiated, AuthenticationError> {
    if !const_time_eq(
        hello.access_token.as_bytes(),
        config.access_token.as_bytes(),
    ) {
        return Err(AuthenticationError::Unauthorized);
    }
    if hello.protocol_major != PROTOCOL_MAJOR {
        return Err(AuthenticationError::IncompatibleProtocol {
            client_major: hello.protocol_major,
        });
    }
    let protocol = ProtocolVersion {
        major: PROTOCOL_MAJOR,
        minor: hello.protocol_minor.min(PROTOCOL_MINOR),
    };
    Ok(Negotiated {
        protocol,
        capabilities: hello
            .capabilities
            .iter()
            .filter(|item| config.capabilities.contains(item))
            .filter(|item| {
                item.as_str() != k10s_protocol::CAPABILITY_POD_PORT_FORWARD
                    || protocol.minor >= k10s_protocol::GENERALIZED_PORT_FORWARD_MINOR
            })
            .cloned()
            .collect(),
    })
}

#[cfg(test)]
mod tests {
    use super::{MAX_ACCESS_TOKEN_BYTES, const_time_eq};

    #[test]
    fn equal_sequences_match() {
        assert!(const_time_eq(b"", b""));
        assert!(const_time_eq(b"k10s-secret", b"k10s-secret"));
        // Long shared prefix with one differing trailing byte must not match.
        let long = vec![b'a'; 256];
        let mut altered = long.clone();
        altered[255] = b'b';
        assert!(!const_time_eq(&long, &altered));
    }

    #[test]
    fn length_difference_never_matches() {
        assert!(!const_time_eq(b"secret", b"secr"));
        assert!(!const_time_eq(b"secr", b"secret"));
    }

    #[test]
    fn oversized_inputs_stay_correct_under_the_production_invariant() {
        // Production invariant: startup validation keeps every configured
        // token at MAX_ACCESS_TOKEN_BYTES or below, so one side of each real
        // comparison is bounded and the fixed iteration window always covers
        // it. These checks confirm correctness when inputs exceed the bound.
        let prefix = vec![b'k'; MAX_ACCESS_TOKEN_BYTES];
        let mut long_one = prefix.clone();
        long_one.extend_from_slice(b"-one");
        let mut long_two = prefix;
        long_two.extend_from_slice(b"-two-tail"); // distinct length, same prefix
        // Identical 512-byte prefixes but different lengths must NOT compare
        // equal in either orientation (the length fold rejects them).
        assert!(!const_time_eq(&long_one, &long_two));
        assert!(!const_time_eq(&long_two, &long_one));
        // An oversized input still matches an identical copy of itself.
        let long_copy = long_one.clone();
        assert!(const_time_eq(&long_one, &long_copy));
    }

    #[test]
    fn oversized_equal_prefix_tokens_are_distinguished_beyond_the_window() {
        // Regression guard for the public-ServerConfig bypass: two distinct
        // tokens longer than the window share an identical first 512-byte
        // prefix and must still compare unequal in both argument orders.
        let mut one = vec![b'k'; MAX_ACCESS_TOKEN_BYTES];
        one.extend_from_slice(&[0x61u8; 88]); // 600 bytes total

        let mut two = one.clone();
        for byte in &mut two[MAX_ACCESS_TOKEN_BYTES..] {
            *byte ^= 0xFF; // distinct tail at the same positions, equal length
        }
        assert!(!const_time_eq(&one, &two));
        assert!(!const_time_eq(&two, &one));

        // Identical oversized copies still match (no over-rejection).
        let twin = one.clone();
        assert!(
            const_time_eq(&one, &twin),
            "identical 600-byte slices must match"
        );
    }

    #[test]
    fn oversized_middle_difference_is_detected_beyond_double_bound() {
        // Regression guard for the sliding-tail bypass: at equal length L >
        // 2*MAX, a tail starting at L-MAX leaves [MAX..L-MAX) unexamined. Two
        // distinct tokens sharing identical first-512 AND last-512-byte regions
        // but differing only in the middle must compare unequal in both orders.
        let length = MAX_ACCESS_TOKEN_BYTES * 4; // 2048 > 2*MAX
        let mut one = vec![b'k'; length];
        let start = MAX_ACCESS_TOKEN_BYTES + 1;
        for byte in &mut one[start..(length - MAX_ACCESS_TOKEN_BYTES - 1)] {
            *byte ^= 0x40; // flip bits across the middle region only
        }

        let two = vec![b'k'; length];
        assert!(!const_time_eq(&one, &two));
        assert!(!const_time_eq(&two, &one));

        // Identical oversized copies still match (no over-rejection).
        let twin = one.clone();
        assert!(
            const_time_eq(&one, &twin),
            "identical 2048-byte slices must match"
        );
    }

    #[test]
    fn oversized_strict_extension_is_rejected() {
        // A token that is a strict byte-prefix of another (600 vs 601 bytes)
        // must never compare equal, in either orientation.
        let mut base = vec![b'k'; MAX_ACCESS_TOKEN_BYTES];
        base.extend_from_slice(&[0x61u8; 88]); // 600 bytes

        let mut extended = base.clone();
        extended.push(0x42); // strict extension: identical first 600 bytes

        assert!(!const_time_eq(&base, &extended));
        assert!(!const_time_eq(&extended, &base));
    }

    #[test]
    fn cross_length_pairs_traverse_the_full_span_and_reject() {
        // Empty versus non-empty in both orientations: the shorter side
        // contributes zeros for every missing position, but the length fold
        // alone already rejects.
        assert!(!const_time_eq(b"", b"secret"));
        assert!(!const_time_eq(b"secret", b""));
        // Identical short prefix with different lengths: the shorter input is
        // a full prefix of the longer one, so only the length fold separates
        // them and it must do so without matching early.
        assert!(!const_time_eq(b"k10s", b"k10s-secret-token"));
        assert!(!const_time_eq(b"k10s-secret-token", b"k10s"));
        // Equal-prefix / different-suffix pair of differing lengths: shared
        // bytes XOR to zero, the divergent byte and padding zeros do not.
        let long = vec![b'a'; 32];
        let mut short_variant = vec![b'a'; 7];
        short_variant[5] ^= 0x40;
        assert!(!const_time_eq(&long, &short_variant));
        assert!(!const_time_eq(&short_variant, &long));
    }

    #[test]
    fn any_byte_difference_rejects_at_every_position() {
        let base = b"0123456789abcdef";
        for position in 0..base.len() {
            let mut variant = base.to_vec();
            variant[position] ^= 0x01; // flip one bit at this exact position
            assert!(
                !const_time_eq(base, &variant),
                "difference at position {position} must reject"
            );
        }
    }

    #[test]
    fn matches_standard_equality_on_random_pairs() {
        let mut seed = 0x5EEDu64;
        let next = |seed: &mut u64| -> u64 {
            *seed = seed
                .wrapping_mul(6364136223846793005)
                .wrapping_add(1442695040888963407);
            seed.rotate_right(32) / 2
        };
        for _ in 0..512 {
            let mut a = vec![0u8; (next(&mut seed) % 40) as usize];
            let mut b = vec![0u8; (next(&mut seed) % 40) as usize];
            for byte in a.iter_mut() {
                *byte = next(&mut seed) as u8;
            }
            for byte in b.iter_mut() {
                *byte = next(&mut seed) as u8;
            }
            assert_eq!(const_time_eq(&a, &b), a == b);
        }
    }
}
