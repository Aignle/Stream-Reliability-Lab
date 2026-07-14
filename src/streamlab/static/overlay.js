(() => {
  "use strict";

  const state = document.querySelector("#connection-state");
  const runLabel = document.querySelector("#run-id");
  const feed = document.querySelector("#effect-feed");
  const empty = document.querySelector("#empty-state");
  const template = document.querySelector("#effect-template");
  const params = new URLSearchParams(window.location.search);
  const sessionId = window.crypto.randomUUID();
  const renderedIds = new Set();
  let socket;
  let reconnectDelay = 250;
  let runId = params.get("run_id");

  const setState = (value, label) => {
    state.dataset.state = value;
    state.querySelector("strong").textContent = label;
  };

  const effectText = (event) => {
    const payload = event.payload || {};
    switch (event.event_type) {
      case "comment": return payload.text;
      case "follow": return "Followed the synthetic channel";
      case "gift": return `${payload.quantity} \u00d7 ${payload.gift_name}`;
      case "like": return `${payload.count} synthetic likes`;
      case "subscription": return `${payload.tier} \u00b7 ${payload.months} month${payload.months === 1 ? "" : "s"}`;
      case "command": return `Command: ${payload.name}`;
      default: return "Processed synthetic event";
    }
  };

  const acknowledgeRender = (eventId) => {
    if (!socket || socket.readyState !== WebSocket.OPEN) return;
    socket.send(JSON.stringify({
      kind: "render_ack",
      event_id: eventId,
      rendered_at: new Date().toISOString(),
    }));
  };

  const renderEffect = (event) => {
    if (renderedIds.has(event.event_id)) {
      acknowledgeRender(event.event_id);
      return;
    }
    const card = template.content.firstElementChild.cloneNode(true);
    card.dataset.eventId = event.event_id;
    card.dataset.eventType = event.event_type;
    card.querySelector(".effect-symbol").textContent = event.event_type.slice(0, 1).toUpperCase();
    card.querySelector(".effect-type").textContent = event.event_type.replace("_", " ");
    card.querySelector(".effect-id").textContent = event.event_id.slice(0, 8);
    card.querySelector(".effect-title").textContent = event.actor_id;
    card.querySelector(".effect-detail").textContent = effectText(event);
    empty?.remove();
    feed.prepend(card);
    renderedIds.add(event.event_id);
    acknowledgeRender(event.event_id);
  };

  const resolveRun = async () => {
    if (runId) return runId;
    const response = await fetch("/api/runs/latest", { cache: "no-store" });
    if (!response.ok) throw new Error("No run is available yet");
    const latest = await response.json();
    runId = latest.run_id;
    return runId;
  };

  const connect = async () => {
    try {
      const activeRun = await resolveRun();
      runLabel.textContent = activeRun;
      const protocol = window.location.protocol === "https:" ? "wss:" : "ws:";
      const url = `${protocol}//${window.location.host}/ws/overlay?run_id=${encodeURIComponent(activeRun)}&session_id=${encodeURIComponent(sessionId)}`;
      setState("connecting", "Connecting");
      socket = new WebSocket(url);
      socket.addEventListener("open", () => {
        reconnectDelay = 250;
        setState("connected", "Connected");
      });
      socket.addEventListener("message", (message) => {
        const payload = JSON.parse(message.data);
        if (payload.kind === "effect") renderEffect(payload.event);
      });
      socket.addEventListener("close", () => {
        setState("reconnecting", "Reconnecting");
        window.setTimeout(connect, reconnectDelay);
        reconnectDelay = Math.min(reconnectDelay * 2, 2000);
      });
      socket.addEventListener("error", () => socket.close());
    } catch (error) {
      setState("waiting", error.message || "Waiting for a run");
      window.setTimeout(connect, reconnectDelay);
      reconnectDelay = Math.min(reconnectDelay * 2, 2000);
    }
  };

  connect();
})();
