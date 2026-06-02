import crypto from "node:crypto";
import fs from "node:fs/promises";
import http from "node:http";
import os from "node:os";
import path from "node:path";
import { fileURLToPath } from "node:url";

import express from "express";
import open from "open";
import { Server, Socket } from "socket.io";

type ChatMessage = {
  id: string;
  room: string;
  user: string;
  text: string;
  sentAt: number;
  originServer: string;
  clientId: string;
};

type DeliveredMessage = ChatMessage & {
  seq: number;
  streamTs: number;
};

type ClientMessageInput = {
  id?: string;
  room?: string;
  user?: string;
  text?: string;
  clientId?: string;
};

type SendAck =
  | { ok: true; id: string; ingressSeqs?: number[] }
  | { ok: false; error: string };

type ServerToClientEvents = {
  "server:hello": (payload: ServerHello) => void;
  "chat:history": (payload: { messages: DeliveredMessage[] }) => void;
  "chat:message": (message: DeliveredMessage) => void;
  "stream:gap": (payload: Tombstone) => void;
  "chat:error": (payload: { message: string }) => void;
};

type ClientToServerEvents = {
  "chat:send": (
    payload: ClientMessageInput,
    ack?: (response: SendAck) => void,
  ) => void;
};

type SocketData = {
  session?: ClientSession;
};

type ChatSocket = Socket<ClientToServerEvents, ServerToClientEvents, never, SocketData>;

type ServerHello = {
  serverId: string;
  clientId: string;
  topicsUrl: string;
  topics: {
    ingress: string;
    client: string;
  };
  router: string;
  cursor: number;
};

type ServerInfo = {
  serverId: string;
  topicsUrl: string;
  ingressTopic: string;
};

type StreamsErrorBody = {
  error?: {
    code?: string;
    message?: string;
    detail?: unknown;
  };
};

type WriteResponse = {
  seqs?: number[];
};

type DiffRecord = {
  "$seq": number;
  "$ts": number;
  data: unknown;
};

type Tombstone = {
  gap_from: number;
  gap_to: number;
  reason: string;
  earliest_seq: number;
  head_seq: number;
};

type DiffResponse = {
  records: DiffRecord[];
  next_from_seq: number;
  head_seq: number;
  caught_up: boolean;
  tombstone: Tombstone | null;
};

type SavedState = {
  cursor: number;
  messages: DeliveredMessage[];
};

type ClientSession = {
  clientId: string;
  clientTopic: string;
  routerName: string;
  stateFile: string;
  cursor: number;
  messagesById: Map<string, DeliveredMessage>;
};

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const appRoot = path.resolve(__dirname, "..");
const publicDir = path.join(__dirname, "public");

const args = new Set(process.argv.slice(2));
const topicsUrl = trimTrailingSlash(process.env.TOPICS_URL ?? "http://127.0.0.1:4000");
const streamPrefix = process.env.STREAM_PREFIX ?? "socketio.chat";
const ingressTopic = `${streamPrefix}.ingress`;
const serverId =
  process.env.SERVER_ID ??
  `${os.hostname().replace(/[^A-Za-z0-9_.:-]/g, "-")}-${process.pid}-${crypto
    .randomUUID()
    .slice(0, 8)}`;
const host = process.env.HOST ?? "127.0.0.1";
const requestedPort = Number.parseInt(process.env.PORT ?? "0", 10);
const shouldOpenBrowser = args.has("--open") && process.env.NO_OPEN !== "1";
const maxHistory = 200;

let shuttingDown = false;

const app = express();
const httpServer = http.createServer(app);
const io = new Server<ClientToServerEvents, ServerToClientEvents, never, SocketData>(
  httpServer,
  {
    serveClient: true,
  },
);

app.use(express.static(publicDir));
app.get("/api/server", (_req, res) => {
  res.json(buildServerInfo());
});

io.on("connection", (socket) => {
  void handleConnection(socket);
});

async function main() {
  await ensureIngressTopic();

  httpServer.listen(requestedPort, host, async () => {
    const address = httpServer.address();
    if (!address || typeof address === "string") {
      throw new Error("server did not bind to a TCP address");
    }

    const url = `http://${host}:${address.port}`;
    console.log(`socketio-typescript server ${serverId}`);
    console.log(`web app: ${url}`);
    console.log(`topics: ${topicsUrl}`);
    console.log(`ingress topic: ${ingressTopic}`);
    console.log("each browser client gets its own durable topic and router from ingress");

    if (shouldOpenBrowser) {
      try {
        await open(url);
      } catch (error) {
        console.warn(`could not open browser: ${error instanceof Error ? error.message : error}`);
      }
    }
  });
}

async function handleConnection(socket: ChatSocket) {
  const abortController = new AbortController();

  try {
    const clientId = resolveClientId(socket);
    const session = await createClientSession(clientId);
    socket.data.session = session;

    socket.emit("server:hello", buildHello(session));
    socket.emit("chat:history", { messages: historySnapshot(session) });

    socket.on("chat:send", async (payload, ack) => {
      try {
        const message = normalizeClientMessage(payload, session.clientId);
        const write = await appendToIngress(message);
        ack?.({ ok: true, id: message.id, ingressSeqs: write.seqs });
      } catch (error) {
        const message = error instanceof Error ? error.message : String(error);
        socket.emit("chat:error", { message });
        ack?.({ ok: false, error: message });
      }
    });

    socket.on("disconnect", () => {
      abortController.abort();
      void saveClientState(session);
    });

    await tailClientTopic(socket, session, abortController.signal);
  } catch (error) {
    const message = error instanceof Error ? error.message : String(error);
    socket.emit("chat:error", { message });
    socket.disconnect(true);
  }
}

async function ensureIngressTopic() {
  await waitForStreams();
  await streamsJson(`/v0/topics/${encodeURIComponent(ingressTopic)}`, {
    method: "PUT",
    body: JSON.stringify(durableLogConfig()),
  });
}

async function createClientSession(clientId: string): Promise<ClientSession> {
  const clientSlug = cleanName(clientId) || `client-${crypto.randomUUID()}`;
  const clientTopic = `${streamPrefix}.client.${clientSlug}`;
  const routerName = `${streamPrefix}.route.${clientSlug}`;
  const stateFile = path.join(
    appRoot,
    ".state",
    `${safeFileName(serverId)}.${safeFileName(clientSlug)}.json`,
  );

  const session: ClientSession = {
    clientId,
    clientTopic,
    routerName,
    stateFile,
    cursor: 0,
    messagesById: new Map(),
  };

  await loadClientState(session);
  await ensureClientSubscription(session);
  return session;
}

async function ensureClientSubscription(session: ClientSession) {
  await streamsJson(`/v0/topics/${encodeURIComponent(session.clientTopic)}`, {
    method: "PUT",
    body: JSON.stringify(durableLogConfig()),
  });
  await streamsJson(`/v0/routers/${encodeURIComponent(session.routerName)}`, {
    method: "PUT",
    body: JSON.stringify({
      source: ingressTopic,
      dest: session.clientTopic,
      preserve_node: true,
      preserve_tag: true,
      create_dest: false,
    }),
  });
}

function durableLogConfig() {
  return {
    durable: true,
    durability: "fsync",
    cap_records: 0,
    cap_bytes: 0,
    ttl_ms: 0,
    discard: "reject",
    dedupe_node: false,
  };
}

async function waitForStreams() {
  const deadline = Date.now() + 15_000;
  let lastError: unknown;

  while (Date.now() < deadline) {
    try {
      await streamsJson("/v0/ready", { method: "GET" });
      return;
    } catch (error) {
      lastError = error;
      await sleep(500);
    }
  }

  const detail = lastError instanceof Error ? lastError.message : String(lastError);
  throw new Error(`topics is not ready at ${topicsUrl}: ${detail}`);
}

async function appendToIngress(message: ChatMessage): Promise<WriteResponse> {
  return streamsJson<WriteResponse>(`/v0/topics/${encodeURIComponent(ingressTopic)}`, {
    method: "POST",
    body: JSON.stringify({
      node: serverId,
      idempotency_key: message.id,
      records: [
        {
          tag: `room:${message.room}`,
          data: message,
          meta: {
            socketio_server: serverId,
            socketio_client: message.clientId,
          },
        },
      ],
    }),
  });
}

async function tailClientTopic(socket: ChatSocket, session: ClientSession, signal: AbortSignal) {
  while (!signal.aborted && !shuttingDown && socket.connected) {
    try {
      const previousCursor = session.cursor;
      const diff = await streamsJson<DiffResponse>(
        `/v0/topics/${encodeURIComponent(session.clientTopic)}/diff`,
        {
          method: "POST",
          signal,
          body: JSON.stringify({
            from_seq: session.cursor,
            limit: 100,
            wait_ms: 15_000,
            include_tags: true,
            include_meta: false,
          }),
        },
      );

      if (diff.tombstone) {
        session.cursor = Math.max(session.cursor, diff.tombstone.gap_to);
        socket.emit("stream:gap", diff.tombstone);
      }

      for (const record of diff.records) {
        const delivered = toDeliveredMessage(record);
        if (!session.messagesById.has(delivered.id)) {
          remember(session, delivered);
          socket.emit("chat:message", delivered);
        }
      }

      session.cursor = Math.max(session.cursor, diff.next_from_seq ?? diff.head_seq ?? session.cursor);
      if (session.cursor !== previousCursor || diff.records.length > 0 || diff.tombstone) {
        await saveClientState(session);
      }
    } catch (error) {
      if (!signal.aborted && !shuttingDown && socket.connected) {
        console.warn(
          `client ${session.clientId} tail retrying after error: ${
            error instanceof Error ? error.message : error
          }`,
        );
        await sleep(1_000);
      }
    }
  }
}

function normalizeClientMessage(payload: ClientMessageInput, connectionClientId: string): ChatMessage {
  const text = cleanText(payload.text, 1_000);
  if (!text) {
    throw new Error("message text is required");
  }

  const user = cleanText(payload.user, 48) || "guest";
  const room = cleanName(payload.room) || "lobby";
  const id = cleanText(payload.id, 160) || `${serverId}:${crypto.randomUUID()}`;

  return {
    id,
    room,
    user,
    text,
    sentAt: Date.now(),
    originServer: serverId,
    clientId: connectionClientId,
  };
}

function toDeliveredMessage(record: DiffRecord): DeliveredMessage {
  if (!isChatMessage(record.data)) {
    throw new Error(`record ${record["$seq"]} does not contain a chat message`);
  }

  return {
    ...record.data,
    seq: record["$seq"],
    streamTs: record["$ts"],
  };
}

function isChatMessage(value: unknown): value is ChatMessage {
  if (!value || typeof value !== "object") {
    return false;
  }
  const candidate = value as Partial<ChatMessage>;
  return (
    typeof candidate.id === "string" &&
    typeof candidate.room === "string" &&
    typeof candidate.user === "string" &&
    typeof candidate.text === "string" &&
    typeof candidate.sentAt === "number" &&
    typeof candidate.originServer === "string" &&
    typeof candidate.clientId === "string"
  );
}

function remember(session: ClientSession, message: DeliveredMessage) {
  session.messagesById.set(message.id, message);

  const sorted = historySnapshot(session);
  while (sorted.length > maxHistory) {
    const oldest = sorted.shift();
    if (oldest) {
      session.messagesById.delete(oldest.id);
    }
  }
}

function historySnapshot(session: ClientSession): DeliveredMessage[] {
  return [...session.messagesById.values()].sort((a, b) => a.seq - b.seq);
}

function buildServerInfo(): ServerInfo {
  return {
    serverId,
    topicsUrl,
    ingressTopic,
  };
}

function buildHello(session: ClientSession): ServerHello {
  return {
    serverId,
    clientId: session.clientId,
    topicsUrl,
    topics: {
      ingress: ingressTopic,
      client: session.clientTopic,
    },
    router: session.routerName,
    cursor: session.cursor,
  };
}

async function streamsJson<T = unknown>(pathName: string, init: RequestInit): Promise<T> {
  const url = new URL(pathName, `${topicsUrl}/`);
  const headers = new Headers(init.headers);

  if (init.body && !headers.has("content-type")) {
    headers.set("content-type", "application/json");
  }

  const timeoutSignal = AbortSignal.timeout(20_000);
  const signal = init.signal ? AbortSignal.any([init.signal, timeoutSignal]) : timeoutSignal;
  const response = await fetch(url, {
    ...init,
    headers,
    signal,
  });
  const text = await response.text();
  const body = text ? (JSON.parse(text) as unknown) : undefined;

  if (!response.ok) {
    const errorBody = body as StreamsErrorBody | undefined;
    const errorMessage =
      errorBody?.error?.message
        ? errorBody.error.message
        : `${response.status} ${response.statusText}`;
    throw new Error(`${url.pathname}: ${errorMessage}`);
  }

  return body as T;
}

async function loadClientState(session: ClientSession) {
  try {
    const saved = JSON.parse(await fs.readFile(session.stateFile, "utf8")) as Partial<SavedState>;
    session.cursor = Number.isSafeInteger(saved.cursor) ? Number(saved.cursor) : 0;
    for (const message of saved.messages ?? []) {
      if (isDeliveredMessage(message)) {
        session.messagesById.set(message.id, message);
      }
    }
  } catch (error) {
    if ((error as NodeJS.ErrnoException).code !== "ENOENT") {
      console.warn(`could not read state file: ${error instanceof Error ? error.message : error}`);
    }
  }
}

async function saveClientState(session: ClientSession) {
  await fs.mkdir(path.dirname(session.stateFile), { recursive: true });
  const state: SavedState = {
    cursor: session.cursor,
    messages: historySnapshot(session),
  };
  const tmpFile = `${session.stateFile}.${process.pid}.tmp`;
  await fs.writeFile(tmpFile, `${JSON.stringify(state, null, 2)}\n`);
  await fs.rename(tmpFile, session.stateFile);
}

function isDeliveredMessage(value: unknown): value is DeliveredMessage {
  return (
    isChatMessage(value) &&
    typeof (value as Partial<DeliveredMessage>).seq === "number" &&
    typeof (value as Partial<DeliveredMessage>).streamTs === "number"
  );
}

function resolveClientId(socket: ChatSocket): string {
  const auth = socket.handshake.auth as Record<string, unknown> | undefined;
  return cleanName(auth?.clientId) || cleanName(socket.id) || `client-${crypto.randomUUID()}`;
}

function cleanText(value: unknown, max: number): string {
  return typeof value === "string" ? value.trim().slice(0, max) : "";
}

function cleanName(value: unknown): string {
  const cleaned = cleanText(value, 96).replace(/[^A-Za-z0-9._:-]/g, "-");
  return cleaned.replace(/^-+|-+$/g, "");
}

function safeFileName(value: string): string {
  return value.replace(/[^A-Za-z0-9._:-]/g, "-");
}

function trimTrailingSlash(value: string): string {
  return value.replace(/\/+$/g, "");
}

function sleep(ms: number) {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

async function shutdown(signal: NodeJS.Signals) {
  if (shuttingDown) {
    return;
  }
  shuttingDown = true;
  console.log(`received ${signal}, shutting down`);
  for (const socket of await io.fetchSockets()) {
    const session = socket.data.session;
    if (session) {
      await saveClientState(session);
    }
  }
  io.close();
  httpServer.close(() => process.exit(0));
  setTimeout(() => process.exit(0), 1_000).unref();
}

process.on("SIGINT", (signal) => {
  void shutdown(signal);
});
process.on("SIGTERM", (signal) => {
  void shutdown(signal);
});

main().catch((error) => {
  console.error(error);
  process.exit(1);
});
