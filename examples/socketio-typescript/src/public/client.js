const clientId = getOrCreateClientId();
const socket = io({
  auth: {
    clientId,
  },
});

const els = {
  subtitle: document.querySelector("#subtitle"),
  socketStatus: document.querySelector("#socket-status"),
  streamStatus: document.querySelector("#stream-status"),
  serverId: document.querySelector("#server-id"),
  clientTopic: document.querySelector("#client-topic"),
  cursor: document.querySelector("#cursor"),
  messages: document.querySelector("#messages"),
  form: document.querySelector("#chat-form"),
  user: document.querySelector("#user"),
  message: document.querySelector("#message"),
};

const defaultName = `guest-${clientId.slice(-4)}`;
els.user.value = localStorage.getItem("topics-socketio-chat-user") || defaultName;
setFormEnabled(false);

const messagesById = new Map();
let connectedServerId = "";

socket.on("connect", () => {
  setStatus(els.socketStatus, "Socket.IO connected", "ok");
});

socket.on("disconnect", () => {
  setStatus(els.socketStatus, "Socket.IO disconnected", "warn");
});

socket.on("server:hello", (hello) => {
  connectedServerId = hello.serverId;
  els.serverId.textContent = hello.serverId;
  els.clientTopic.textContent = hello.topics.client;
  els.cursor.textContent = String(hello.cursor);
  els.subtitle.textContent = `Client ${hello.clientId} is subscribed to ${hello.topics.ingress}`;
  setStatus(els.streamStatus, "topics ready", "ok");
  setFormEnabled(true);
});

socket.on("chat:history", ({ messages }) => {
  for (const message of messages) {
    upsertMessage(message);
  }
  renderMessages();
});

socket.on("chat:message", (message) => {
  upsertMessage(message);
  renderMessages();
});

socket.on("stream:gap", (gap) => {
  addSystemMessage(
    `Stream gap ${gap.gap_from}-${gap.gap_to} (${gap.reason}). The cursor advanced to ${gap.gap_to}.`,
  );
  els.cursor.textContent = String(gap.gap_to);
});

socket.on("chat:error", ({ message }) => {
  addSystemMessage(message);
});

els.form.addEventListener("submit", async (event) => {
  event.preventDefault();

  const text = els.message.value.trim();
  if (!text) {
    return;
  }

  const user = els.user.value.trim() || defaultName;
  localStorage.setItem("topics-socketio-chat-user", user);
  els.message.value = "";
  setFormEnabled(false);

  const payload = {
    id: `${clientId}:${Date.now()}:${crypto.randomUUID()}`,
    room: "lobby",
    user,
    text,
    clientId,
  };

  socket.emit("chat:send", payload, (ack) => {
    setFormEnabled(true);
    els.message.focus();
    if (!ack?.ok) {
      addSystemMessage(ack?.error || "message was not accepted");
    }
  });
});

function upsertMessage(message) {
  messagesById.set(message.id, message);
  els.cursor.textContent = String(Math.max(Number(els.cursor.textContent || "0"), message.seq));
}

function renderMessages() {
  const messages = [...messagesById.values()].sort((a, b) => a.seq - b.seq);
  els.messages.replaceChildren();

  if (messages.length === 0) {
    const empty = document.createElement("div");
    empty.className = "empty";
    empty.textContent = "No messages yet";
    els.messages.append(empty);
    return;
  }

  for (const message of messages) {
    const item = document.createElement("article");
    item.className = `message ${message.clientId === clientId ? "own" : ""}`;
    item.dataset.messageId = message.id;

    const head = document.createElement("div");
    head.className = "message-head";

    const author = document.createElement("strong");
    author.textContent = message.user;

    const meta = document.createElement("span");
    meta.textContent = `seq ${message.seq} via ${message.originServer}`;

    const body = document.createElement("p");
    body.textContent = message.text;

    head.append(author, meta);
    item.append(head, body);
    els.messages.append(item);
  }

  els.messages.scrollTop = els.messages.scrollHeight;
}

function addSystemMessage(text) {
  const item = document.createElement("article");
  item.className = "message system";
  item.textContent = text;
  els.messages.append(item);
  els.messages.scrollTop = els.messages.scrollHeight;
}

function setStatus(element, text, state) {
  element.textContent = text;
  element.className = `status status-${state}`;
}

function setFormEnabled(enabled) {
  els.form.querySelector("button").disabled = !enabled;
  els.message.disabled = !enabled;
}

function getOrCreateClientId() {
  const key = "topics-socketio-chat-client-id";
  const existing = localStorage.getItem(key);
  if (existing) {
    return existing;
  }
  const next = crypto.randomUUID();
  localStorage.setItem(key, next);
  return next;
}
