//! Phase-5B Stage-1: dual-protocol (HTTP/1.1 keep-alive + HTTP/2 cleartext
//! "prior-knowledge") serve loop.
//!
//! The server now serves BOTH protocols on the same listener, auto-detecting
//! the connection (hyper-util `auto::Builder` sniffs the HTTP/2 preface). These
//! tests prove, over the REAL bound server (`common::Harness`, which serves via
//! `topics::serve::serve`, the exact production path):
//!
//! - an HTTP/2-prior-knowledge client (`reqwest .http2_prior_knowledge()`) gets
//!   a `200` from `/v0/health`, the negotiated version is HTTP/2, and it can
//!   write + diff over h2; and
//! - a plain HTTP/1.1 client still works (version HTTP/1.1), so every existing
//!   reqwest test keeps working unchanged.

mod common;

use std::time::Duration;

use common::Harness;
use reqwest::Version;
use serde_json::json;

/// A blocking reqwest client pinned to HTTP/2 prior knowledge (h2c, no TLS): it
/// opens with the HTTP/2 connection preface and never tries HTTP/1.
fn h2_client() -> reqwest::blocking::Client {
    reqwest::blocking::Client::builder()
        .http2_prior_knowledge()
        .timeout(Duration::from_secs(10))
        .build()
        .expect("build h2-prior-knowledge client")
}

/// A blocking reqwest client speaking ordinary HTTP/1.1 (the default).
fn h1_client() -> reqwest::blocking::Client {
    reqwest::blocking::Client::builder()
        .http1_only()
        .timeout(Duration::from_secs(10))
        .build()
        .expect("build h1 client")
}

#[test]
fn h2c_prior_knowledge_health_is_http2_and_200() {
    let h = Harness::start();
    let c = h2_client();

    let resp = c
        .get(format!("{}/v0/health", h.base_url()))
        .send()
        .expect("h2 health request");
    assert_eq!(resp.status().as_u16(), 200, "h2 /v0/health must be 200");
    assert_eq!(
        resp.version(),
        Version::HTTP_2,
        "the h2-prior-knowledge client must negotiate HTTP/2"
    );
    let body: serde_json::Value = resp.json().expect("health body json");
    assert_eq!(body["status"], "ok");
}

#[test]
fn h2c_client_can_write_and_diff() {
    let h = Harness::start();
    let c = h2_client();
    let base = h.base_url();

    // Write two records over h2 (auto-create -> 201).
    let w = c
        .post(format!("{base}/v0/topics/h2box"))
        .json(&json!({ "records": [{ "data": { "v": 1 } }, { "data": { "v": 2 } }] }))
        .send()
        .expect("h2 write request");
    assert_eq!(w.version(), Version::HTTP_2, "write served over HTTP/2");
    assert_eq!(w.status().as_u16(), 201, "first write auto-creates -> 201");
    let wbody: serde_json::Value = w.json().expect("write body");
    assert_eq!(wbody["seqs"], json!([1, 2]));
    assert_eq!(wbody["head_seq"], 2);

    // Diff over h2 returns both records.
    let d = c
        .post(format!("{base}/v0/topics/h2box/diff"))
        .json(&json!({ "from_seq": 0 }))
        .send()
        .expect("h2 diff request");
    assert_eq!(d.version(), Version::HTTP_2, "diff served over HTTP/2");
    assert_eq!(d.status().as_u16(), 200);
    let dbody: serde_json::Value = d.json().expect("diff body");
    let recs = dbody["records"].as_array().expect("records array");
    assert_eq!(recs.len(), 2, "diff returns both records over h2");
    assert_eq!(recs[0]["$seq"], 1);
    assert_eq!(recs[1]["$seq"], 2);
    assert_eq!(dbody["caught_up"], true);
}

#[test]
fn h1_client_still_works_and_is_http1() {
    let h = Harness::start();
    let c = h1_client();
    let base = h.base_url();

    // Health over h1.
    let resp = c
        .get(format!("{base}/v0/health"))
        .send()
        .expect("h1 health request");
    assert_eq!(resp.status().as_u16(), 200);
    assert_eq!(
        resp.version(),
        Version::HTTP_11,
        "a plain client must still be served over HTTP/1.1"
    );

    // Write + diff over h1 on the same dual-protocol listener.
    let w = c
        .post(format!("{base}/v0/topics/h1box"))
        .json(&json!({ "records": [{ "data": 1 }] }))
        .send()
        .expect("h1 write");
    assert_eq!(w.version(), Version::HTTP_11);
    assert_eq!(w.status().as_u16(), 201);

    let d = c
        .post(format!("{base}/v0/topics/h1box/diff"))
        .json(&json!({ "from_seq": 0 }))
        .send()
        .expect("h1 diff");
    assert_eq!(d.status().as_u16(), 200);
    let dbody: serde_json::Value = d.json().expect("diff body");
    assert_eq!(dbody["records"].as_array().map(|a| a.len()), Some(1));
}
