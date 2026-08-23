//! Same-origin policy for browser WebSocket upgrades.
//!
//! Native desktop clients connect directly and send no `Origin` header, so they
//! are always admitted. Browsers set the header on cross-origin attempts, which
//! lets a malicious page probe this service; the check below compares the
//! origin's authority against the request's own `Host` header and admits only
//! true same-origin pairs (default ports 80/443 count as equal to their
//! implicit form). Malformed origins — including userinfo, which browsers never
//! emit, and present-but-blank values that try to borrow the native allowance
//! for a missing header — are rejected as untrusted, along with values that
//! carry path, query, or fragment components: browsers never emit anything but
//! `scheme://authority` in an Origin header. The authority itself must be one of
//! the three shapes real browsers actually produce — a bare hostname, a hostname
//! with an explicit 1-5 digit port, or a bracketed IPv6 literal optionally
//! carrying that port — and every other form fails closed.
use std::str::FromStr;

/// Whether an upgrade request satisfies the same-origin rule.
pub(crate) fn origin_matches_host(origin_header: Option<&str>, host_header: Option<&str>) -> bool {
    let Some(raw_origin) = origin_header else {
        return true; // native clients send no Origin and are unconstrained.
    };
    let origin = raw_origin.trim();
    if origin.is_empty() {
        // A present-but-blank (or whitespace-only) Origin is malformed: it
        // must not inherit the absent-header allowance for native clients.
        return false;
    }
    let Some(host) = host_header.map(str::trim).filter(|h| !h.is_empty()) else {
        return false; // a browser sending an Origin always sends Host too.
    };

    let (scheme, rest) = match origin.split_once("://") {
        Some(parts) => parts,
        None => return false,
    };
    if !matches!(scheme.to_ascii_lowercase().as_str(), "http" | "https") {
        return false;
    }
    if rest.contains(['/', '?', '#']) {
        // Browsers emit exactly scheme://authority in Origin headers; a path,
        // query, or fragment makes the value malformed for this check.
        return false;
    }
    let authority = rest;
    if authority.contains('@') {
        // Userinfo never appears in browser-emitted origins; refuse spoofed forms.
        return false;
    }

    let default_port: Option<&str> = match scheme.to_ascii_lowercase().as_str() {
        "http" => Some("80"),
        _ => Some("443"),
    };
    // Host names compare case-insensitively.
    let authority_lc = authority.to_ascii_lowercase();
    if strict_host_and_port(&authority_lc).is_none() {
        // Malformed authority (empty port, non-digit port, unterminated or
        // misshapen brackets, extra colons): fail closed immediately instead of
        // relying on the comparison below to reject it.
        return false;
    }
    let host_lc = host.to_ascii_lowercase();
    normalized(&authority_lc, default_port) == normalized(&host_lc, default_port)
}

/// Expand a missing port with the scheme's default so implicit and explicit
/// forms of one authority compare equal. Returns None for authorities not in a
/// strict browser form; such a value can never equal a valid Origin.
fn normalized(authority: &str, default_port: Option<&str>) -> Option<String> {
    let (host, port) = strict_host_and_port(authority)?;
    Some(match (port, default_port) {
        (Some(port), _) => format!("{host}:{port}"),
        (None, Some(default)) => format!("{host}:{default}"),
        (None, None) => host.to_owned(),
    })
}

/// Split an authority into its host and explicit port under the strict forms a
/// real browser emits: `hostname`, `hostname:port` with 1-5 ASCII digits, or a
/// bracketed IPv6 literal optionally followed by that same port. Returns None
/// for any other form so callers fail closed.
fn strict_host_and_port(authority: &str) -> Option<(&str, Option<&str>)> {
    if authority.starts_with('[') {
        // Bracketed form [addr] or [addr]:port needs a valid IPv6 literal and
        // nothing but an optional port after the closing bracket.
        let close = authority.find(']').filter(|&index| index > 0)?;
        if !valid_ipv6_literal(&authority[1..close]) {
            return None;
        }
        match &authority[close + 1..] {
            "" => Some((&authority[..=close], None)),
            remainder if remainder.starts_with(':') && valid_explicit_port(&remainder[1..]) => {
                let port = &remainder[1..];
                Some((&authority[..=close], Some(port)))
            }
            _ => None,
        }
    } else {
        match authority.rsplit_once(':') {
            // Bare hostname: no colons at all and a strict browser host shape.
            None if valid_bare_hostname(authority) => Some((authority, None)),
            // Exactly one colon separating host and a valid explicit port.
            Some((host_part, port))
                if !host_part.contains(':')
                    && valid_bare_hostname(host_part)
                    && valid_explicit_port(port) =>
            {
                Some((host_part, Some(port)))
            }
            _ => None,
        }
    }
}

/// A bare (non-bracketed) host must use only the characters browsers emit for
/// hostnames and dotted quads — ASCII alphanumerics, dots, and hyphens — and it
/// may not start or end with a hyphen. Anything else fails closed.
fn valid_bare_hostname(host: &str) -> bool {
    !host.is_empty()
        && host
            .chars()
            .all(|c| c.is_ascii_alphanumeric() || matches!(c, '.' | '-'))
        && !(host.starts_with('-') || host.ends_with('-'))
}

/// Structural validator for a bracketed IPv6 literal as emitted by browsers.
/// Colon-separated groups of 1-4 ASCII hex digits, plus at most one '::' run
/// standing in for one or more zero groups, and nothing else. Dotted
/// IPv4-mapped tails such as ::ffff:192.168.1.1 are rejected on purpose:
/// the fail-closed policy admits only canonical hex-group literals, which is
/// what this service's browser clients send here.
fn valid_ipv6_literal(literal: &str) -> bool {
    // Must contain at least one colon, and '::' compression marker at most once.
    if !literal.contains(':') || literal.matches("::").count() > 1 {
        return false;
    }
    let compressed = literal.contains("::");
    let halves: Vec<&str> = match literal.split_once("::") {
        Some((left, right)) => vec![left, right],
        None => vec![literal],
    };
    let mut groups = 0usize;
    for side in &halves {
        // An empty half is legal only as a side of the compression marker.
        if side.is_empty() {
            if !compressed {
                return false;
            }
            continue;
        }
        // Every colon-separated piece on either side is exactly one group:
        // 1-4 ASCII hex digits, never empty (catches stray ':' positions).
        for group in side.split(':') {
            let valid_group = !group.is_empty()
                && group.len() <= 4
                && group.chars().all(|c| c.is_ascii_hexdigit());
            if !valid_group {
                return false;
            }
            groups += 1;
        }
    }
    // '::' stands for one or more zero groups: at most 7 explicit.
    if compressed { groups <= 7 } else { groups == 8 }
}

/// An explicit port is 1-5 characters long, every character an ASCII digit, and
/// at most 65535 — the largest valid TCP/UDP port number.
fn valid_explicit_port(port: &str) -> bool {
    (1..=5).contains(&port.len())
        && port.chars().all(|c| c.is_ascii_digit())
        && u16::from_str(port).is_ok() // rejects digit runs above 65535.
}

#[cfg(test)]
mod tests {
    use super::origin_matches_host;

    #[test]
    fn absent_origin_is_admitted_for_native_clients() {
        // Only a truly missing Origin header takes the native allowance.
        assert!(origin_matches_host(None, Some("127.0.0.1:8080")));
        assert!(origin_matches_host(None, None));
    }

    #[test]
    fn blank_origin_headers_are_rejected() {
        // A present Origin that is empty or whitespace-only after trimming is
        // malformed and must never inherit the native allowance.
        let host = Some("127.0.0.1:8080");
        assert!(!origin_matches_host(Some(""), host));
        assert!(!origin_matches_host(Some("   "), host));
        assert!(!origin_matches_host(Some("\t\r \n"), host));
    }

    #[test]
    fn same_origin_pairs_are_admitted() {
        // The exact loopback development case.
        assert!(origin_matches_host(
            Some("http://127.0.0.1:8080"),
            Some("127.0.0.1:8080")
        ));
        // Default-port expansion in both directions for bare authorities.
        assert!(origin_matches_host(
            Some("http://example.com:80"),
            Some("example.com")
        ));
        assert!(origin_matches_host(
            Some("https://example.com"),
            Some("example.com:443")
        ));
        assert!(origin_matches_host(
            Some("https://Example.COM"),
            Some("example.com")
        ));
        // Well-formed bracketed IPv6 origins are admitted with an explicit port…
        assert!(origin_matches_host(
            Some("http://[2001:db8::1]:8443"),
            Some("[2001:db8::1]:8443")
        ));
        // …and without one they fall back to the default-port rule.
        assert!(origin_matches_host(Some("http://[::1]"), Some("[::1]:80")));
    }

    #[test]
    fn path_query_and_fragment_origins_are_rejected() {
        // Browsers never emit anything but scheme://authority in Origin, so
        // any trailing component is malformed and must fail closed even when
        // the embedded authority would match the Host header.
        assert!(!origin_matches_host(
            Some("http://example.com/path?x=1"),
            Some("example.com")
        ));
        assert!(!origin_matches_host(
            Some("http://example.com:80/"),
            Some("example.com")
        ));
        assert!(!origin_matches_host(
            Some("https://example.com#frag"),
            Some("example.com")
        ));
        assert!(!origin_matches_host(
            Some("http://127.0.0.1:8080/admin"),
            Some("127.0.0.1:8080")
        ));
    }

    #[test]
    fn cross_origin_pairs_are_rejected() {
        assert!(!origin_matches_host(
            Some("http://evil.example.com"),
            Some("127.0.0.1:8080")
        ));
        assert!(!origin_matches_host(
            Some("http://127.0.0.1:9090"),
            Some("127.0.0.1:8080")
        ));
    }

    #[test]
    fn malformed_or_spoofed_origins_are_rejected() {
        // Userinfo tricks must not launder a foreign host into the allowed set.
        assert!(!origin_matches_host(
            Some("http://evil.com@127.0.0.1:8080"),
            Some("127.0.0.1:8080")
        ));
        assert!(!origin_matches_host(
            None.or(Some("no-scheme-value")),
            Some("127.0.0.1:8080")
        ));
        assert!(!origin_matches_host(
            Some("ws://127.0.0.1:8080"),
            Some("127.0.0.1:8080")
        ));
    }

    #[test]
    fn malformed_authority_forms_are_rejected() {
        // A real browser Origin authority is only ever a bare hostname, one
        // with an explicit 1-5 digit port, or a bracketed IPv6 literal
        // optionally carrying that port; every other form fails closed even
        // when the Host header echoes the same malformed shape.
        assert!(!origin_matches_host(
            Some("http://127.0.0.1:"),
            Some("127.0.0.1")
        ));
        assert!(!origin_matches_host(
            Some("http://127.0.0.1:"),
            Some("127.0.0.1:") // empty port must not launder through either side
        ));
        assert!(!origin_matches_host(
            Some("http://host:notaport"),
            Some("host:notaport")
        ));
        assert!(!origin_matches_host(Some("http://[::1"), Some("[::1")));
        assert!(!origin_matches_host(Some("http://a:b:c"), Some("a:b:c")));
        // Hostname charset violations fail closed too…
        assert!(!origin_matches_host(
            Some("http://exa mple.com"),
            Some("exa mple.com")
        ));
        assert!(!origin_matches_host(
            Some("http://-example.com"),
            Some("-example.com")
        ));
        // …as do bracketed literals that are not IPv6 text (no colon, or a
        // malformed triple-colon run)…
        assert!(!origin_matches_host(Some("http://[abc]"), Some("[abc]")));
        assert!(!origin_matches_host(Some("http://[:::]"), Some("[:::]")));
        // …and bracketed literals that fail strict structural IPv6 shape: an
        // oversized group, seven groups without a compression marker, or a
        // stray leading colon inside one half.
        assert!(!origin_matches_host(
            Some("http://[00005::1]"),
            Some("[00005::1]")
        ));
        assert!(!origin_matches_host(
            Some("http://[1:2:3:4:5:6:7]"),
            Some("[1:2:3:4:5:6:7]")
        ));
        assert!(!origin_matches_host(Some("http://[:1::]"), Some("[:1::]")));
        // …and ports beyond the 65535 range, even with five digits.
        assert!(!origin_matches_host(
            Some("http://127.0.0.1:99999"),
            Some("127.0.0.1:99999")
        ));
    }

    #[test]
    fn origin_without_a_host_header_is_rejected() {
        assert!(!origin_matches_host(Some("http://example.com"), None));
        assert!(!origin_matches_host(Some("http://example.com"), Some("  ")));
    }
}
