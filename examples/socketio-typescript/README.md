# Socket.IO TypeScript chat over topics

This example runs a Socket.IO chat server that uses a local `topics` instance as
the durable event backbone.

Topology:

- Socket.IO clients send chat messages to whichever Node server they reached.
- The server appends each message to `socketio.chat.ingress`.
- Every browser client has a stable `clientId`, sent in the Socket.IO handshake.
- For each Socket.IO client, the server creates a durable topic named like
  `socketio.chat.client.<clientId>`.
- For each Socket.IO client, the server creates a router from
  `socketio.chat.ingress` to that client's topic.
- The Socket.IO server tails that client's own topic and emits those records to
  that socket.

Because each browser client's subscription is represented by durable topics
state, round-robin load balancing does not change the feed. A server can stop
and restart; when the browser reconnects with the same `clientId`, the server
reuses the same client topic and router, then resumes from its saved cursor or
replays the durable client topic.

Routers start forwarding from the current ingress head when they are created, so
a brand-new browser client sees messages sent after it joins. A returning browser
client keeps its existing client topic and can replay messages routed there while
its Socket.IO server was offline.

## Run

Start `topics` locally first. This example expects:

```bash
docker run --rm -p 4000:4000 \
  -e TOPICS_ALLOW_INSECURE_NO_AUTH=1 \
  ghcr.io/slopus/topics:latest
```

If you already have topics listening on `127.0.0.1:4000` with no auth, use that.

Then run the example:

```bash
cd examples/socketio-typescript
npm install
npm start
```

`npm start` binds to a free localhost port and opens the chat UI in your browser.

To simulate multiple Socket.IO servers behind a round-robin load balancer, run
another copy in a second terminal:

```bash
cd examples/socketio-typescript
SERVER_ID=server-b npm start
```

For a restart test, stop a server and restart it with the same `SERVER_ID`:

```bash
SERVER_ID=server-a npm start
```

Useful environment variables:

| Name | Default | Purpose |
| --- | --- | --- |
| `TOPICS_URL` | `http://127.0.0.1:4000` | topics base URL. |
| `STREAM_PREFIX` | `socketio.chat` | Prefix for the ingress topic, client topics, and routers. |
| `SERVER_ID` | random per process | Stable id for restart cursor state. |
| `PORT` | `0` | Port to bind. `0` asks the OS for a free port. |
| `HOST` | `127.0.0.1` | Listen host. |
| `NO_OPEN` | unset | Set to `1` to suppress browser opening. |
