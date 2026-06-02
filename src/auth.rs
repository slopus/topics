//! API-key authentication: optional **scopes** + **topic-name prefix allowlist**,
//! with keys stored **hashed at rest** (SHA-256) and compared in **constant
//! time**. All of this is *additive* and *back-compatible*: a bare key (no
//! scopes, no prefixes) authorizes **full access** to every topic/router, exactly
//! as before this module existed.
//!
//! # `TOPICS_API_KEYS` syntax (extended, back-compatible)
//!
//! Comma-separated entries; each entry is one of:
//!
//! ```text
//! key                       # full access (back-compat: no scopes, all topics)
//! key:scopes                # scopes only, all topics
//! key:scopes:prefixes       # scopes + a topic-name PREFIX allowlist
//! key::prefixes             # all scopes (empty scopes field), prefix-restricted
//! ```
//!
//! - **`scopes`** is a `+`-separated subset of `{read, write, delete, admin}`
//!   (also accepts the single letters `r`, `w`, `d`, `a` and the alias `rw`).
//!   An **empty** scopes field means **all** scopes (full read/write/delete/admin).
//! - **`prefixes`** is a `|`-separated list of topic-name prefixes the key may
//!   touch (e.g. `tenant42:|shared.`). An **empty** prefixes field means **any**
//!   topic name. Prefixes are matched against the raw topic/router name as a byte
//!   prefix (the `tenant:` convention in API §3 becomes a real boundary here).
//!
//! The key itself may not contain `,` or `:` (the delimiters); everything before
//! the first `:` is the secret. A leading/trailing-whitespace-trimmed empty key
//! is skipped.
//!
//! # Security properties
//!
//! - **Hashed at rest.** Only the SHA-256 digest of each key is retained
//!   ([`ApiKey::hash`]); the plaintext secret is never stored on the
//!   [`ApiKey`] and is never logged.
//! - **Constant-time compare.** A presented token is hashed and its digest
//!   compared against every configured key's digest with [`subtle`]'s
//!   `ConstantTimeEq` and no early-exit, so neither *which* key matched nor *how
//!   many leading bytes* matched is observable via timing.

use sha2::{Digest, Sha256};
use subtle::ConstantTimeEq;

/// The set of permission bits a key may carry. A key with **no** bits set
/// (`bits == 0`) is the back-compat *full-access* key.
///
/// Per-route requirements (see [`crate::http`]):
/// - **reads** (GET state/list, POST diff, watch/SSE) need [`Scope::READ`];
/// - **writes** (POST records, queue ack/nack/extend) need [`Scope::WRITE`];
/// - **queue claim / `/work`** is a *read+write* (it leases — mutates — then
///   returns jobs), so it needs **both** [`Scope::READ`] and [`Scope::WRITE`];
/// - **deletes** (DELETE topic/router, POST `.../delete`) need [`Scope::DELETE`];
/// - **control-plane** (PUT topic/router) needs [`Scope::ADMIN`].
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct Scope(u8);

impl Scope {
    pub const READ: Scope = Scope(1 << 0);
    pub const WRITE: Scope = Scope(1 << 1);
    pub const DELETE: Scope = Scope(1 << 2);
    pub const ADMIN: Scope = Scope(1 << 3);

    /// The empty scope set (no bits). On an [`ApiKey`] this is the sentinel for
    /// *full access* (back-compat); as a *requirement* it means "no scope needed".
    pub const NONE: Scope = Scope(0);

    /// All four scope bits set (what an empty `scopes` field in the env expands
    /// to, and the bitset a back-compat full-access key is treated as holding).
    pub const ALL: Scope = Scope(0b1111);

    /// Raw bits (test/inspection helper).
    pub fn bits(self) -> u8 {
        self.0
    }

    /// Whether this set contains every bit of `needed`.
    pub fn contains(self, needed: Scope) -> bool {
        self.0 & needed.0 == needed.0
    }

    /// Union of two scope sets.
    pub fn union(self, other: Scope) -> Scope {
        Scope(self.0 | other.0)
    }

    /// Parse a `+`-separated scope spec (`"read+write"`, `"rw"`, `"admin"`, …).
    /// An **empty** spec ⇒ [`Scope::ALL`] (the env "all scopes" shorthand).
    /// Returns `Err` naming the first unrecognized token.
    pub fn parse(spec: &str) -> std::result::Result<Scope, String> {
        let spec = spec.trim();
        if spec.is_empty() {
            return Ok(Scope::ALL);
        }
        let mut acc = Scope::NONE;
        for tok in spec.split('+') {
            let tok = tok.trim();
            if tok.is_empty() {
                continue;
            }
            let bit = match tok.to_ascii_lowercase().as_str() {
                "read" | "r" => Scope::READ,
                "write" | "w" => Scope::WRITE,
                "delete" | "del" | "d" => Scope::DELETE,
                "admin" | "a" => Scope::ADMIN,
                // Convenience alias for the common read+write data-plane key.
                "rw" => Scope::READ.union(Scope::WRITE),
                other => return Err(format!("unknown scope {other:?}")),
            };
            acc = acc.union(bit);
        }
        Ok(acc)
    }
}

/// A configured API key: the SHA-256 digest of the secret (never the plaintext),
/// the granted [`Scope`] set, and an optional topic-name **prefix allowlist**.
///
/// A key with **no scopes and no prefixes** (`scopes == Scope::NONE` and
/// `prefixes` empty) is a back-compat **full-access** key.
#[derive(Debug, Clone)]
pub struct ApiKey {
    /// SHA-256 of the key secret. The plaintext is intentionally not retained.
    hash: [u8; 32],
    /// Granted scope bits. [`Scope::NONE`] (no bits) ⇒ full access (back-compat).
    scopes: Scope,
    /// Topic-name prefix allowlist. Empty ⇒ any topic name is permitted.
    prefixes: Vec<String>,
}

impl ApiKey {
    /// SHA-256 digest of a secret. Pure-Rust ([`sha2`]); the input is never logged.
    fn digest(secret: &str) -> [u8; 32] {
        let mut h = Sha256::new();
        h.update(secret.as_bytes());
        h.finalize().into()
    }

    /// A full-access key (no scopes, all topics) from a plaintext secret — the
    /// back-compat shape used when an entry is a bare `key`.
    pub fn full_access(secret: &str) -> ApiKey {
        ApiKey {
            hash: Self::digest(secret),
            scopes: Scope::NONE,
            prefixes: Vec::new(),
        }
    }

    /// Build directly from a plaintext secret + parsed scopes/prefixes (used by
    /// the env parser and tests). The plaintext is hashed and dropped.
    pub fn new(secret: &str, scopes: Scope, prefixes: Vec<String>) -> ApiKey {
        ApiKey {
            hash: Self::digest(secret),
            scopes,
            prefixes,
        }
    }

    /// This key's effective scope set: a back-compat full-access key
    /// (`scopes == NONE` **and** no prefix allowlist) reports [`Scope::ALL`];
    /// otherwise the explicitly granted bits.
    pub fn effective_scopes(&self) -> Scope {
        if self.scopes == Scope::NONE && self.prefixes.is_empty() {
            Scope::ALL
        } else {
            self.scopes
        }
    }

    /// The topic-name prefix allowlist (empty ⇒ any topic name).
    pub fn prefixes(&self) -> &[String] {
        &self.prefixes
    }

    /// Whether the presented token's digest equals this key's, in constant time.
    fn digest_matches(&self, presented_digest: &[u8; 32]) -> subtle::Choice {
        self.hash.ct_eq(presented_digest)
    }

    /// Parse one `TOPICS_API_KEYS` entry (`key` | `key:scopes` |
    /// `key:scopes:prefixes`). Returns `Ok(None)` for an empty (whitespace-only)
    /// entry so the caller can skip it; `Err` names a malformed scope token.
    pub fn parse_entry(entry: &str) -> std::result::Result<Option<ApiKey>, String> {
        let entry = entry.trim();
        if entry.is_empty() {
            return Ok(None);
        }
        // Split into at most 3 fields on `:`. The secret is field 0 (it may not
        // contain `:`); scopes is field 1; prefixes is field 2.
        let mut parts = entry.splitn(3, ':');
        let secret = parts.next().unwrap_or("").trim();
        if secret.is_empty() {
            return Ok(None);
        }
        let scopes_field = parts.next();
        let prefixes_field = parts.next();

        // No scopes field at all ⇒ bare key ⇒ full access (back-compat).
        let scopes = match scopes_field {
            None => Scope::NONE,
            Some(s) => Scope::parse(s)?,
        };
        let prefixes = match prefixes_field {
            None => Vec::new(),
            Some(p) => p
                .split('|')
                .map(str::trim)
                .filter(|s| !s.is_empty())
                .map(String::from)
                .collect(),
        };
        Ok(Some(ApiKey {
            hash: Self::digest(secret),
            scopes,
            prefixes,
        }))
    }
}

/// A stable, non-secret identity for a configured key: its SHA-256 digest. Used
/// to bind a created resource (a watch session) to its creating key and to test
/// equality of two presented tokens *without* retaining either plaintext. It is
/// **not** a secret (a digest of a high-entropy key is not reversible), but it is
/// never logged or returned on the wire.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct KeyId(pub [u8; 32]);

impl KeyId {
    /// The key id for a presented token (its SHA-256 digest).
    pub fn of(token: &str) -> KeyId {
        KeyId(ApiKey::digest(token))
    }
}

/// The authenticated principal carried in request extensions: a stable
/// non-secret [`KeyId`], the matched key's effective [`Scope`] set, and its
/// topic-name prefix allowlist. Carries **no** secret (the token is not retained
/// past the match), so it is safe to clone and to bind to a created resource
/// (e.g. a watch session). The dev-mode principal has `key_id == None`.
#[derive(Debug, Clone)]
pub struct Principal {
    /// Stable non-secret identity of the matched key (the digest). `None` in dev
    /// mode (auth disabled) — there is no key to bind to.
    pub key_id: Option<KeyId>,
    pub scopes: Scope,
    pub prefixes: Vec<String>,
}

impl Principal {
    /// The dev-mode principal (auth disabled): full access, all topics, no key id.
    /// Used so a handler's scope/prefix checks are uniform whether or not auth is
    /// enabled.
    pub fn full_access() -> Principal {
        Principal {
            key_id: None,
            scopes: Scope::ALL,
            prefixes: Vec::new(),
        }
    }

    /// Whether this principal holds every bit of `needed`.
    pub fn allows_scope(&self, needed: Scope) -> bool {
        self.scopes.contains(needed)
    }

    /// Whether this principal may touch a topic/router named `name`: true when the
    /// allowlist is empty (any name) or `name` starts with one of the prefixes.
    pub fn allows_name(&self, name: &str) -> bool {
        self.prefixes.is_empty() || self.prefixes.iter().any(|p| name.starts_with(p.as_str()))
    }
}

/// The set of accepted [`ApiKey`]s. Empty ⇒ auth disabled (dev mode).
#[derive(Debug, Clone, Default)]
pub struct KeyStore {
    keys: Vec<ApiKey>,
}

impl KeyStore {
    /// Build from already-parsed keys (tests / programmatic config).
    pub fn from_keys(keys: Vec<ApiKey>) -> KeyStore {
        KeyStore { keys }
    }

    /// Parse a comma-separated `TOPICS_API_KEYS` value into a [`KeyStore`].
    /// Malformed *scope* tokens abort the parse with an error (fail-closed at
    /// startup rather than silently granting the wrong scope); empty entries are
    /// skipped.
    pub fn parse(value: &str) -> std::result::Result<KeyStore, String> {
        let mut keys = Vec::new();
        for entry in value.split(',') {
            if let Some(k) = ApiKey::parse_entry(entry)? {
                keys.push(k);
            }
        }
        Ok(KeyStore { keys })
    }

    /// Whether any key is configured (auth enabled).
    pub fn is_enabled(&self) -> bool {
        !self.keys.is_empty()
    }

    /// Number of configured keys (for boot logging — never the keys themselves).
    pub fn len(&self) -> usize {
        self.keys.len()
    }

    pub fn is_empty(&self) -> bool {
        self.keys.is_empty()
    }

    /// Authenticate a presented bearer token in **constant time** with respect to
    /// the token's bytes: hash it once (SHA-256 is fixed-time over a 32-byte
    /// digest), then compare its digest against *every* configured key's digest
    /// with [`subtle`]'s `ConstantTimeEq` and **no early-exit**. Returns the
    /// matched key's [`Principal`] (effective scopes + prefixes), or `None`.
    ///
    /// The full per-key digest scan runs before any decision, so neither *which*
    /// key matched nor *how many leading bytes* of a near-miss matched is
    /// timing-observable. Selecting the matched key's scopes/prefixes afterward
    /// branches only on a single accumulated hit bit (one collision-free match at
    /// most) — it does not compare secret bytes and so leaks nothing about them.
    pub fn authenticate(&self, token: &str) -> Option<Principal> {
        let presented = ApiKey::digest(token);
        let mut any_hit = subtle::Choice::from(0u8);
        let mut matched_idx: usize = 0;
        for (i, k) in self.keys.iter().enumerate() {
            let hit = k.digest_matches(&presented);
            // Record the (single, collision-free) matched index without an early
            // `break`, so the digest scan stays constant-time over the key set.
            if bool::from(hit) {
                matched_idx = i;
            }
            any_hit |= hit;
        }
        if bool::from(any_hit) {
            let k = &self.keys[matched_idx];
            Some(Principal {
                key_id: Some(KeyId(k.hash)),
                scopes: k.effective_scopes(),
                prefixes: k.prefixes.clone(),
            })
        } else {
            None
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn scope_parse_forms() {
        assert_eq!(Scope::parse("").unwrap(), Scope::ALL);
        assert_eq!(Scope::parse("read").unwrap(), Scope::READ);
        assert_eq!(Scope::parse("r").unwrap(), Scope::READ);
        assert_eq!(
            Scope::parse("read+write").unwrap(),
            Scope::READ.union(Scope::WRITE)
        );
        assert_eq!(Scope::parse("rw").unwrap(), Scope::READ.union(Scope::WRITE));
        assert_eq!(Scope::parse("read+write+delete+admin").unwrap(), Scope::ALL);
        assert!(Scope::parse("bogus").is_err());
    }

    #[test]
    fn scope_contains_semantics() {
        let rw = Scope::READ.union(Scope::WRITE);
        assert!(rw.contains(Scope::READ));
        assert!(rw.contains(Scope::WRITE));
        assert!(rw.contains(rw));
        assert!(!rw.contains(Scope::DELETE));
        assert!(!rw.contains(Scope::ADMIN));
        // ALL contains everything; NONE (as a requirement) is always satisfied.
        assert!(Scope::ALL.contains(Scope::ADMIN));
        assert!(rw.contains(Scope::NONE));
    }

    #[test]
    fn bare_key_is_full_access() {
        let k = ApiKey::parse_entry("s3cr3t").unwrap().unwrap();
        assert_eq!(k.effective_scopes(), Scope::ALL);
        assert!(k.prefixes().is_empty());
    }

    #[test]
    fn scoped_key_parsing() {
        let k = ApiKey::parse_entry("k:read").unwrap().unwrap();
        assert_eq!(k.effective_scopes(), Scope::READ);
        assert!(k.prefixes().is_empty());

        let k = ApiKey::parse_entry("k:read+write:tenant42:|shared.")
            .unwrap()
            .unwrap();
        // Only the FIRST `:` after the secret splits scopes; the prefixes field is
        // the remainder, so a `:` inside a prefix (e.g. `tenant42:`) is preserved.
        assert_eq!(k.effective_scopes(), Scope::READ.union(Scope::WRITE));
        assert_eq!(
            k.prefixes(),
            &["tenant42:".to_string(), "shared.".to_string()]
        );
    }

    #[test]
    fn empty_scopes_field_means_all_scopes() {
        // `key::prefix` ⇒ all scopes, prefix-restricted.
        let k = ApiKey::parse_entry("k::tenant42:").unwrap().unwrap();
        assert_eq!(k.effective_scopes(), Scope::ALL);
        assert_eq!(k.prefixes(), &["tenant42:".to_string()]);
    }

    #[test]
    fn malformed_scope_is_error() {
        assert!(ApiKey::parse_entry("k:bogus").is_err());
        assert!(KeyStore::parse("good,k:bogus").is_err());
    }

    #[test]
    fn empty_entries_skipped() {
        let store = KeyStore::parse("  , a , ,b ").unwrap();
        assert_eq!(store.len(), 2);
    }

    #[test]
    fn authenticate_returns_principal_for_match_only() {
        let store = KeyStore::parse("full,ro:read,scoped:write:tenant42:").unwrap();

        // Full-access key.
        let p = store.authenticate("full").expect("full matches");
        assert_eq!(p.scopes, Scope::ALL);
        assert!(p.prefixes.is_empty());
        assert!(p.allows_name("anything"));

        // Read-only key.
        let p = store.authenticate("ro").expect("ro matches");
        assert_eq!(p.scopes, Scope::READ);
        assert!(!p.allows_scope(Scope::WRITE));

        // Prefix-restricted write key.
        let p = store.authenticate("scoped").expect("scoped matches");
        assert!(p.allows_scope(Scope::WRITE));
        assert!(p.allows_name("tenant42:jobs"));
        assert!(!p.allows_name("tenant99:jobs"));

        // Unknown token ⇒ no principal.
        assert!(store.authenticate("nope").is_none());
        // A prefix of a real key must not match (hash compare is exact).
        assert!(store.authenticate("ful").is_none());
        assert!(store.authenticate("fullx").is_none());
        assert!(store.authenticate("").is_none());
    }

    #[test]
    fn key_secret_is_not_retained_in_plaintext() {
        // The ApiKey stores a 32-byte SHA-256 digest, not the secret. Sanity: the
        // digest of a known string is stable and not the bytes of the secret.
        let k = ApiKey::full_access("hunter2");
        let expect = {
            let mut h = Sha256::new();
            h.update(b"hunter2");
            let d: [u8; 32] = h.finalize().into();
            d
        };
        assert!(bool::from(k.digest_matches(&expect)));
        // A different secret's digest must not match.
        let other = ApiKey::digest("hunter3");
        assert!(!bool::from(k.digest_matches(&other)));
    }

    #[test]
    fn prefix_allowlist_byte_prefix_match() {
        let p = Principal {
            key_id: None,
            scopes: Scope::ALL,
            prefixes: vec!["tenant42:".to_string()],
        };
        assert!(p.allows_name("tenant42:jobs"));
        assert!(p.allows_name("tenant42:")); // exact prefix is allowed
        assert!(!p.allows_name("tenant4")); // shorter than the prefix
        assert!(!p.allows_name("other:tenant42:")); // prefix must be at the START
    }
}
