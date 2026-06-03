//! Router graph: DAG cycle checking, at-least-once per-source FIFO forwarding,
//! and `allow_cycle` hop-cap loop-breaking (DESIGN §8, API §6).
//!
//! Phase 2 forwarding is an in-process append into the dest topic, driven off the
//! committed source log via a per-router cursor.

use crate::error::{Error, Result};
use crate::types::Router;
use std::collections::HashMap;

/// The router registry plus the forwarding graph used for cycle detection.
#[derive(Debug, Default)]
pub struct RouterGraph {
    /// Router name → definition.
    routers: HashMap<String, Router>,
    /// Per-router forward cursor over its source topic (seq last forwarded). In the
    /// async/derived model this is the durable progress marker: the
    /// source seq each router has forwarded *through*. It advances ONLY by the
    /// count actually committed into the dest (no silent loss — the R2 fix), so a
    /// filtered/back-pressured/crashed forward is re-driven from the un-advanced
    /// cursor on the next pass.
    cursors: HashMap<String, u64>,
    /// Per-router count of records forwarded (`forwarded_total`).
    forwarded_total: HashMap<String, u64>,
    /// Per-router `dest_base`: the dest seq just BELOW this router's first
    /// forwarded record (captured when the router starts forwarding into a dest).
    /// Together with `forwarded_total` it pins the deterministic dest seq of the
    /// next forwarded record (`dest_base + forwarded_total + 1`), so a re-derived
    /// dest record always gets the SAME seq across a restart (a consumer cursor
    /// into the dest stays valid — §4 of the design). Defaulted to the dest head
    /// at router-create time so a router attached to a pre-populated dest does not
    /// collide with existing seqs.
    dest_base: HashMap<String, u64>,
}

impl RouterGraph {
    pub fn new() -> Self {
        RouterGraph::default()
    }

    pub fn get(&self, name: &str) -> Option<&Router> {
        self.routers.get(name)
    }

    /// Number of routers currently defined (resource-limit check; [`crate::limits`]).
    pub fn len(&self) -> usize {
        self.routers.len()
    }

    /// Whether the graph holds no routers.
    pub fn is_empty(&self) -> bool {
        self.routers.is_empty()
    }

    pub fn forwarded_total(&self, name: &str) -> u64 {
        self.forwarded_total.get(name).copied().unwrap_or(0)
    }

    /// All routers, for listing (caller paginates).
    pub fn iter(&self) -> impl Iterator<Item = &Router> {
        self.routers.values()
    }

    /// Insert/replace a router after a DAG cycle check. Returns whether the
    /// router was newly created. Rejects cycle-introducing routers with
    /// `409 router_cycle` unless `allow_cycle`.
    pub fn upsert(&mut self, router: Router) -> Result<bool> {
        // Cycle check considers the graph *as it would be* with this router's
        // edge applied. An idempotent re-PUT of an existing edge can't introduce
        // a *new* cycle, but recomputing is harmless and keeps the rule simple.
        // Temporarily ignore any existing edge under this name so re-PUTs of an
        // unchanged router don't false-positive on their own edge.
        if !router.allow_cycle {
            if let Some(cycle) =
                self.would_create_cycle_excluding(&router.source, &router.dest, Some(&router.name))
            {
                return Err(Error::new(
                    crate::types::ErrorCode::RouterCycle,
                    format!(
                        "router {:?} would create a cycle {}",
                        router.name,
                        cycle.join(" -> ")
                    ),
                )
                .with_detail(serde_json::json!({ "cycle": cycle })));
            }
        }

        let created = !self.routers.contains_key(&router.name);
        let name = router.name.clone();
        self.routers.insert(name.clone(), router);
        // A fresh router starts its forward cursor at the source's current head
        // is the caller's concern; the registry tracks only the running cursor.
        self.cursors.entry(name.clone()).or_insert(0);
        self.forwarded_total.entry(name.clone()).or_insert(0);
        self.dest_base.entry(name).or_insert(0);
        Ok(created)
    }

    /// Cap-aware [`Self::upsert`]: enforce `max_routers` (`0` ⇒ unlimited)
    /// **atomically with the insert** under the caller's single held graph lock
    /// (codex P2 #10). A *new* router is refused (`Throttled`) only when the live
    /// count is already at the cap; an idempotent re-PUT of an existing router is an
    /// update and always proceeds. Because the count read and the insert happen
    /// under one lock, a concurrent create race can never push the router count over
    /// the cap (the prior read-len-then-drop-lock-then-insert was a TOCTOU that
    /// fully bypassed the cap). Returns whether the router was newly created.
    pub fn upsert_capped(&mut self, router: Router, max_routers: u64) -> Result<bool> {
        let is_new = !self.routers.contains_key(&router.name);
        if is_new && max_routers != 0 && self.routers.len() as u64 >= max_routers {
            return Err(Error::new(
                crate::types::ErrorCode::Throttled,
                format!(
                    "router limit reached ({max_routers} routers); cannot create {:?}",
                    router.name
                ),
            )
            .with_retry_after(crate::limits::LIMIT_RETRY_AFTER_S)
            .with_detail(serde_json::json!({
                "limit": "max_routers",
                "max": max_routers,
            })));
        }
        self.upsert(router)
    }

    /// Remove a router by name. Returns whether it existed.
    pub fn remove(&mut self, name: &str) -> bool {
        let existed = self.routers.remove(name).is_some();
        self.cursors.remove(name);
        self.forwarded_total.remove(name);
        self.dest_base.remove(name);
        existed
    }

    /// Names of all routers referencing `topic_name` as source or dest, WITHOUT
    /// removing them (sorted). Used to pre-compute a topic-delete cascade so its WAL
    /// tombstones can be durably logged *before* the in-memory removal (codex P0:
    /// a delete must not become a false idempotent success on a WAL failure).
    pub fn routers_touching_topic(&self, topic_name: &str) -> Vec<String> {
        let mut names: Vec<String> = self
            .routers
            .values()
            .filter(|r| r.source == topic_name || r.dest == topic_name)
            .map(|r| r.name.clone())
            .collect();
        names.sort();
        names
    }

    /// Remove and return the names of all routers referencing `topic_name` as
    /// source or dest (topic-delete cascade, API §1.4).
    pub fn remove_touching_topic(&mut self, topic_name: &str) -> Vec<String> {
        let mut removed: Vec<String> = self
            .routers
            .values()
            .filter(|r| r.source == topic_name || r.dest == topic_name)
            .map(|r| r.name.clone())
            .collect();
        removed.sort();
        for name in &removed {
            self.routers.remove(name);
            self.cursors.remove(name);
            self.forwarded_total.remove(name);
            self.dest_base.remove(name);
        }
        removed
    }

    /// All routers whose source is `topic_name` (forwarding fan-out on append).
    /// Returns owned definitions so the caller can drop the graph lock before
    /// touching dest topics (avoids holding the router lock across an append).
    pub fn routers_for_source(&self, topic_name: &str) -> Vec<Router> {
        self.routers
            .values()
            .filter(|r| r.source == topic_name)
            .cloned()
            .collect()
    }

    /// Record that `count` records were forwarded by `router` up through source
    /// seq `src_head` (advances the per-router cursor + `forwarded_total`).
    pub fn note_forwarded(&mut self, router: &str, src_head: u64, count: u64) {
        self.cursors.insert(router.to_string(), src_head);
        *self.forwarded_total.entry(router.to_string()).or_insert(0) += count;
    }

    /// The per-router forward cursor (source seq forwarded through). `0` ⇒ nothing
    /// forwarded yet (the source is 1-based, so seq 0 never exists).
    pub fn cursor(&self, name: &str) -> u64 {
        self.cursors.get(name).copied().unwrap_or(0)
    }

    /// Set the per-router forward cursor explicitly (recovery clamp / seeding).
    pub fn set_cursor(&mut self, name: &str, cursor: u64) {
        self.cursors.insert(name.to_string(), cursor);
    }

    /// The per-router `dest_base` (dest seq just below the first forwarded record).
    pub fn dest_base(&self, name: &str) -> u64 {
        self.dest_base.get(name).copied().unwrap_or(0)
    }

    /// Seed `dest_base` for a router ONLY if it has not been set yet (i.e. nothing
    /// forwarded). Used at create-time so a router attached to a non-empty dest
    /// numbers its first forwarded record after the dest's current head, and the
    /// deterministic dest-seq scheme (`dest_base + forwarded_total`) starts from
    /// the right base. Idempotent re-PUTs / replays never clobber a running base.
    pub fn seed_dest_base(&mut self, name: &str, base: u64) {
        if self.forwarded_total.get(name).copied().unwrap_or(0) == 0 {
            self.dest_base.insert(name.to_string(), base);
        }
    }

    /// The deterministic dest seq the NEXT forwarded record of `router` will get:
    /// `dest_base + forwarded_total + 1`. Pure function of durable state, so it is
    /// stable across a restart (the at-least-once re-derivation contract, §4).
    pub fn next_dest_seq(&self, name: &str) -> u64 {
        self.dest_base(name) + self.forwarded_total(name) + 1
    }

    /// Snapshot every router with its forward cursor + total + dest_base (for a
    /// metadata snapshot). Returns `(router, cursor, forwarded_total, dest_base)`.
    pub fn snapshot_all(&self) -> Vec<(Router, u64, u64, u64)> {
        self.routers
            .values()
            .map(|r| {
                (
                    r.clone(),
                    self.cursors.get(&r.name).copied().unwrap_or(0),
                    self.forwarded_total.get(&r.name).copied().unwrap_or(0),
                    self.dest_base.get(&r.name).copied().unwrap_or(0),
                )
            })
            .collect()
    }

    /// Restore a router (with its cursor + total + dest_base) during snapshot load.
    /// Bypasses the cycle check (the router was already accepted live).
    pub fn restore(&mut self, router: Router, cursor: u64, forwarded_total: u64, dest_base: u64) {
        let name = router.name.clone();
        self.routers.insert(name.clone(), router);
        self.cursors.insert(name.clone(), cursor);
        self.forwarded_total.insert(name.clone(), forwarded_total);
        self.dest_base.insert(name, dest_base);
    }

    /// Whether adding `source -> dest` would create a directed cycle in the
    /// existing graph. Returns the offending cycle path (topic names) if so.
    pub fn would_create_cycle(&self, source: &str, dest: &str) -> Option<Vec<String>> {
        self.would_create_cycle_excluding(source, dest, None)
    }

    /// Cycle check ignoring the edge of `exclude` (used so an idempotent re-PUT
    /// of an existing router doesn't trip on its own edge). A directed cycle is
    /// introduced iff `dest` can already reach `source` over the other edges, in
    /// which case `source -> dest` closes the loop.
    fn would_create_cycle_excluding(
        &self,
        source: &str,
        dest: &str,
        exclude: Option<&str>,
    ) -> Option<Vec<String>> {
        // Self-loop is its own cycle (validate_router also rejects source==dest,
        // but be defensive).
        if source == dest {
            return Some(vec![source.to_string(), dest.to_string()]);
        }
        // Adjacency over topic names: src -> dst for every router except `exclude`.
        let mut adj: HashMap<&str, Vec<&str>> = HashMap::new();
        for r in self.routers.values() {
            if exclude == Some(r.name.as_str()) {
                continue;
            }
            adj.entry(r.source.as_str())
                .or_default()
                .push(r.dest.as_str());
        }
        // DFS from `dest`, seeking a path back to `source`.
        let mut stack: Vec<&str> = vec![dest];
        let mut path: Vec<String> = Vec::new();
        let mut visited: std::collections::HashSet<&str> = std::collections::HashSet::new();
        // Iterative DFS that records the discovery path so we can report it.
        if let Some(found) = dfs_to(&adj, dest, source, &mut visited, &mut path) {
            // `found` is dest..source; the full cycle is source -> dest .. source.
            let mut cycle = vec![source.to_string()];
            cycle.extend(found);
            return Some(cycle);
        }
        let _ = &mut stack;
        None
    }
}

/// Depth-first search for a path from `from` to `target` over `adj`, returning
/// the discovered node path `from..=target` (inclusive) if one exists.
fn dfs_to<'a>(
    adj: &HashMap<&'a str, Vec<&'a str>>,
    from: &'a str,
    target: &'a str,
    visited: &mut std::collections::HashSet<&'a str>,
    path: &mut Vec<String>,
) -> Option<Vec<String>> {
    path.push(from.to_string());
    if from == target {
        return Some(path.clone());
    }
    visited.insert(from);
    if let Some(neighbors) = adj.get(from) {
        for &n in neighbors {
            if visited.contains(n) {
                continue;
            }
            if let Some(found) = dfs_to(adj, n, target, visited, path) {
                return Some(found);
            }
        }
    }
    path.pop();
    None
}

/// Compute the default router name from a source/dest pair: `"<src>-><dst>"`.
pub fn default_router_name(source: &str, dest: &str) -> String {
    format!("{source}->{dest}")
}

/// Validate a router create request (source != dest, names valid).
pub fn validate_router(source: &str, dest: &str) -> Result<()> {
    if source == dest {
        return Err(Error::invalid_request("router source and dest must differ"));
    }
    if !crate::config::is_valid_name(source) || !crate::config::is_valid_name(dest) {
        return Err(Error::invalid_request("invalid source or dest topic name"));
    }
    Ok(())
}
