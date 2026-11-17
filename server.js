// Servidor de TAG: solo reenvía mensajes entre el anfitrión y los jugadores de cada sala.
const http = require('http');
const { WebSocketServer } = require('ws');

const srv = http.createServer((q, r) => {
  r.writeHead(200, { 'content-type': 'text/plain; charset=utf-8' });
  r.end('TAG server ok');
});
const wss = new WebSocketServer({ server: srv, maxPayload: 8192 });
const rooms = new Map(); // código -> { host, cl: Map(id -> ws) }
let nextId = 1;
const send = (w, o) => w && w.readyState === 1 && w.send(JSON.stringify(o));

wss.on('connection', ws => {
  ws.alive = true;
  ws.on('pong', () => (ws.alive = true));
  ws.on('message', raw => {
    let m;
    try { m = JSON.parse(raw); } catch (e) { return; }
    if (m.t === 'host' && !ws.room) {
      const c = String(m.code || '').toUpperCase().slice(0, 8);
      if (!c || rooms.has(c)) return send(ws, { t: 'taken' });
      ws.room = { c, host: true };
      rooms.set(c, { host: ws, cl: new Map() });
      send(ws, { t: 'ok' });
    } else if (m.t === 'join' && !ws.room) {
      const c = String(m.code || '').toUpperCase();
      const r = rooms.get(c);
      if (!r) return send(ws, { t: 'nf' });
      ws.room = { c, host: false, id: nextId++ };
      r.cl.set(ws.room.id, ws);
      send(ws, { t: 'ok' });
      send(r.host, { t: 'j', id: ws.room.id });
    } else if (m.t === 'd' && ws.room) {
      const r = rooms.get(ws.room.c);
      if (!r) return;
      if (ws.room.host) {
        if (m.to === '*') {
          const s = JSON.stringify({ t: 'd', d: m.d });
          r.cl.forEach(c => c.readyState === 1 && c.send(s));
        } else send(r.cl.get(m.to), { t: 'd', d: m.d });
      } else send(r.host, { t: 'd', id: ws.room.id, d: m.d });
    } else if (m.t === 'kick' && ws.room && ws.room.host) {
      const r = rooms.get(ws.room.c);
      const c = r && r.cl.get(m.id);
      if (c) c.close();
    }
  });
  ws.on('close', () => {
    if (!ws.room) return;
    const r = rooms.get(ws.room.c);
    if (!r) return;
    if (ws.room.host) {
      r.cl.forEach(c => { send(c, { t: 'hostgone' }); c.close(); });
      rooms.delete(ws.room.c);
    } else {
      r.cl.delete(ws.room.id);
      send(r.host, { t: 'x', id: ws.room.id });
    }
  });
});

// Mantiene vivas las conexiones y limpia las muertas
setInterval(() => {
  wss.clients.forEach(w => {
    if (!w.alive) return w.terminate();
    w.alive = false;
    w.ping();
  });
}, 25000);

srv.listen(process.env.PORT || 8080, () => console.log('TAG server listo'));
