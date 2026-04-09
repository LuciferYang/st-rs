//! Port of the subset of `util/string_util.h` the engine actually uses.
//!
//! These are tiny helpers — nothing fancy. Most of `std`'s `&str`
//! methods are already better than upstream's hand-rolled equivalents,
//! so we port only the few that aren't in `std`.

/// Case-insensitive prefix check over bytes. Used by option parsing to
/// match things like `"snappy"` against `"SNAPPY"`. Matches upstream
/// `StartsWithCaseInsensitive`.
pub fn starts_with_ci(haystack: &[u8], needle: &[u8]) -> bool {
    if needle.len() > haystack.len() {
        return false;
    }
    haystack
        .iter()
        .zip(needle.iter())
        .all(|(a, b)| a.eq_ignore_ascii_case(b))
}

/// Parse a decimal `u64`. Accepts optional leading `'+'`, rejects `'-'`.
/// Matches upstream `ParseUint64`.
pub fn parse_u64(s: &str) -> Option<u64> {
    let s = s.strip_prefix('+').unwrap_or(s);
    s.parse::<u64>().ok()
}

/// Parse a boolean. Accepts `"true"`, `"false"`, `"1"`, `"0"`, and the
/// all-uppercase/mixed-case variants. Matches upstream `ParseBoolean`.
pub fn parse_bool(s: &str) -> Option<bool> {
    match s.to_ascii_lowercase().as_str() {
        "true" | "1" | "yes" | "on" => Some(true),
        "false" | "0" | "no" | "off" => Some(false),
        _ => None,
    }
}

/// Split `s` on `delim` into a `Vec<&str>`, discarding empty segments.
/// Matches upstream `StringSplit` without the trailing-empty edge case.
pub fn split(s: &str, delim: char) -> Vec<&str> {
    s.split(delim).filter(|seg| !seg.is_empty()).collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn starts_with_ci_matches_regardless_of_case() {
        assert!(starts_with_ci(b"SnAppY", b"snap"));
        assert!(starts_with_ci(b"snappy", b"SNAP"));
        assert!(!starts_with_ci(b"lz4", b"zstd"));
        assert!(!starts_with_ci(b"sn", b"snappy"));
        assert!(starts_with_ci(b"anything", b""));
    }

    #[test]
    fn parse_u64_accepts_plus_and_digits() {
        assert_eq!(parse_u64("42"), Some(42));
        assert_eq!(parse_u64("+42"), Some(42));
        assert_eq!(parse_u64("0"), Some(0));
        assert_eq!(parse_u64("-1"), None);
        assert_eq!(parse_u64("abc"), None);
        assert_eq!(parse_u64(""), None);
    }

    #[test]
    fn parse_bool_is_case_insensitive() {
        assert_eq!(parse_bool("true"), Some(true));
        assert_eq!(parse_bool("FALSE"), Some(false));
        assert_eq!(parse_bool("1"), Some(true));
        assert_eq!(parse_bool("0"), Some(false));
        assert_eq!(parse_bool("maybe"), None);
    }

    #[test]
    fn split_drops_empty_segments() {
        assert_eq!(split("a,b,,c", ','), vec!["a", "b", "c"]);
        assert_eq!(split("", ','), Vec::<&str>::new());
        assert_eq!(split(",,,", ','), Vec::<&str>::new());
    }
}
