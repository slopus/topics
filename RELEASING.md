# Releasing

`topics` ships as a single container image published to the GitHub Container
Registry (GHCR) at **`ghcr.io/slopus/topics`**. Releases are tag-driven.

## Cutting a release

1. Make sure `main` is green (the `CI` workflow runs build + test + clippy on
   every push and PR).
2. Tag the commit with a `vX.Y.Z` version and push the tag:

   ```bash
   git tag v1.2.3
   git push origin v1.2.3
   ```

3. The **Publish container image** workflow (`.github/workflows/docker-publish.yml`)
   triggers on the `v*` tag, builds a multi-arch image (`linux/amd64` and
   `linux/arm64`), and pushes it to GHCR with these tags:

   - `ghcr.io/slopus/topics:1.2.3` (full version)
   - `ghcr.io/slopus/topics:1.2` (major.minor)
   - `ghcr.io/slopus/topics:latest`

   The build also attaches provenance + SBOM attestations.

You can also trigger the workflow manually from the Actions tab
(**workflow_dispatch**); a manual run is tagged with the short commit SHA
instead of a version.

## Running the published image

The image binds `0.0.0.0:4000` by default. Because the server **refuses to
start on a non-loopback bind with no API keys** (it would be an open,
unauthenticated event store), you must pass either `TOPICS_API_KEYS` (for any
real deployment) or, **for local/dev only**, `TOPICS_ALLOW_INSECURE_NO_AUTH=1`.

### Production-ish (with auth)

```bash
docker run --rm \
  -p 4000:4000 \
  -v topics-data:/data \
  -e TOPICS_API_KEYS=replace-with-a-real-secret \
  ghcr.io/slopus/topics:latest
```

- `-p 4000:4000` — maps the container's listen port to the host.
- `-v topics-data:/data` — a named volume holding the WAL, segments, and
  snapshots (`TOPICS_DATA_DIR` defaults to `/data` in the image). Reusing the
  volume across restarts preserves durable state.
- `-e TOPICS_API_KEYS=...` — comma-separated bearer keys. Clients then send
  `Authorization: Bearer <key>`.

Check health once it is up:

```bash
curl -fsS http://127.0.0.1:4000/v0/health   # -> 200
```

### Local / dev only (auth disabled)

```bash
docker run --rm \
  -p 4000:4000 \
  -v topics-data:/data \
  -e TOPICS_ALLOW_INSECURE_NO_AUTH=1 \
  ghcr.io/slopus/topics:latest
```

Never use `TOPICS_ALLOW_INSECURE_NO_AUTH=1` on a network-exposed deployment —
it leaves the event store open and unauthenticated.

## Permissions

The workflow uses the built-in `GITHUB_TOKEN` with `packages: write`, so no
extra secrets are required. The first publish creates the GHCR package under the
repository's owner; make it public (or grant pull access) in the package
settings if external users need to pull it.
