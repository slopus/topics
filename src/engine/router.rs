//! Router graph: DAG cycle checking, at-least-once per-source FIFO forwarding,
//! and `allow_cycle` hop-cap loop-breaking (DESIGN §8, API §6).
//!
//! Phase 2 forwarding is an in-process append into the dest box, driven off the
//! committed source log via a per-router cursor.

use crate::error::{Error, Result};
use crate::types::Router;
use std::collections::HashMap;

/// The router registry plus the forwarding graph used for cycle detection.
#[derive(Debug, Default)]
pub struct RouterGraph {
    /// Router name → definition.
    routers: HashMap<String, Router>,
    /// Per-router forward cursor over its source box (seq last forwarded).
    cursors: HashMap<String, u64>,
    /// Per-router count of records forwarded (`forwarded_total`).
    forwarded_total: HashMap<String, u64>,
}

impl RouterGraph {
    pub fn new() -> Self {
        RouterGraph::default()
    }

    pub fn get(&self, name: &str) -> Option<&Router> {
        self.routers.get(name)
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
            if let Some(cycle) = self.would_create_cycle_excluding(
                &router.source,
                &router.dest,
                Some(&router.name),
            ) {
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
        self.forwarded_total.entry(name).or_insert(0);
        Ok(created)
    }

    /// Remove a router by name. Returns whether it existed.
    pub fn remove(&mut self, name: &str) -> bool {
        let existed = self.routers.remove(name).is_some();
        self.cursors.remove(name);
        self.forwarded_total.remove(name);
        existed
    }

    /// Names of all routers referencing `box_name` as source or dest, WITHOUT
    /// removing them (sorted). Used to pre-compute a box-delete cascade so its WAL
    /// tombstones can be durably logged *before* the in-memory removal (codex P0:
    /// a delete must not become a false idempotent success on a WAL failure).
    pub fn routers_touching_box(&self, box_name: &str) -> Vec<String> {
        let mut names: Vec<String> = self
            .routers
            .values()
            .filter(|r| r.source == box_name || r.dest == box_name)
            .map(|r| r.name.clone())
            .collect();
        names.sort();
        names
    }

    /// Remove and return the names of all routers referencing `box_name` as
    /// source or dest (box-delete cascade, API §1.4).
    pub fn remove_touching_box(&mut self, box_name: &str) -> Vec<String> {
        let mut removed: Vec<String> = self
            .routers
            .values()
            .filter(|r| r.source == box_name || r.dest == box_name)
            .map(|r| r.name.clone())
            .collect();
        removed.sort();
        for name in &removed {
            self.routers.remove(name);
            self.cursors.remove(name);
            self.forwarded_total.remove(name);
        }
        removed
    }

    /// All routers whose source is `box_name` (forwarding fan-out on append).
    /// Returns owned definitions so the caller can drop the graph lock before
    /// touching dest boxes (avoids holding the router lock across an append).
    pub fn routers_for_source(&self, box_name: &str) -> Vec<Router> {
        self.routers
            .values()
            .filter(|r| r.source == box_name)
            .cloned()
            .collect()
    }

    /// Whether ANY router has `box_name` as its source — a cheap existence check
    /// for the write fast path. The common case (no routers) lets the writer skip
    /// snapshotting/cloning every record purely for forwarding (codex P0 #2): a
    /// no-router write never deep-clones its payloads. Short-circuits on the first
    /// match instead of collecting owned `Router`s.
    pub fn has_routers_for_source(&self, box_name: &str) -> bool {
        self.routers.values().any(|r| r.source == box_name)
    }

    /// Record that `count` records were forwarded by `router` up through source
    /// seq `src_head` (advances the per-router cursor + `forwarded_total`).
    pub fn note_forwarded(&mut self, router: &str, src_head: u64, count: u64) {
        self.cursors.insert(router.to_string(), src_head);
        *self.forwarded_total.entry(router.to_string()).or_insert(0) += count;
    }

    /// Snapshot every router with its forward cursor + total (for a metadata
    /// snapshot). Returns `(router, cursor, forwarded_total)` tuples.
    pub fn snapshot_all(&self) -> Vec<(Router, u64, u64)> {
        self.routers
            .values()
            .map(|r| {
                (
                    r.clone(),
                    self.cursors.get(&r.name).copied().unwrap_or(0),
                    self.forwarded_total.get(&r.name).copied().unwrap_or(0),
                )
            })
            .collect()
    }

    /// Restore a router (with its cursor + total) during snapshot load. Bypasses
    /// the cycle check (the router was already accepted live).
    pub fn restore(&mut self, router: Router, cursor: u64, forwarded_total: u64) {
        let name = router.name.clone();
        self.routers.insert(name.clone(), router);
        self.cursors.insert(name.clone(), cursor);
        self.forwarded_total.insert(name, forwarded_total);
    }

    /// Whether adding `source -> dest` would create a directed cycle in the
    /// existing graph. Returns the offending cycle path (box names) if so.
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
        // Adjacency over box names: src -> dst for every router except `exclude`.
        let mut adj: HashMap<&str, Vec<&str>> = HashMap::new();
        for r in self.routers.values() {
            if exclude == Some(r.name.as_str()) {
                continue;
            }
            adj.entry(r.source.as_str()).or_default().push(r.dest.as_str());
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
        return Err(Error::invalid_request("invalid source or dest box name"));
    }
    Ok(())
}
