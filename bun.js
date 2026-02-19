// server.js – Bun signaling server with reconnection priority
const port = process.env.PORT || 3000;

const DEFAULT_MAX_PEERS = 8;

const peers = new Map(); // peerId -> { ws, id, rooms: Set }
const rooms = new Map(); // roomName -> { name, peers: Map<peerId, { id, joinTime }> }

function generateId() { return crypto.randomUUID(); }

function findOrCreateRoom(baseName, maxPeers) {
  let index = 1;
  while (true) {
    const roomName = `${baseName}${index}`;
    if (!rooms.has(roomName)) {
      rooms.set(roomName, { name: roomName, peers: new Map() });
      return roomName;
    }
    if (rooms.get(roomName).peers.size < maxPeers) return roomName;
    index++;
  }
}

function getRoomHost(room) {
  let oldest = null;
  for (const p of room.peers.values()) {
    if (!oldest || p.joinTime < oldest.joinTime) oldest = p;
  }
  return oldest?.id ?? null;
}

function broadcastToRoom(room, msg, exclude = null) {
  const data = JSON.stringify(msg);
  for (const pid of room.peers.keys()) {
    if (pid === exclude) continue;
    peers.get(pid)?.ws.send(data);
  }
}

function peerLeft(pid, roomName) {
  const room = rooms.get(roomName);
  if (!room) return;
  const wasHost = getRoomHost(room) === pid;
  room.peers.delete(pid);
  if (room.peers.size === 0) { rooms.delete(roomName); return; }
  broadcastToRoom(room, { type: 'peerLeft', room: roomName, peerId: pid });
  if (wasHost) {
    const newHost = getRoomHost(room);
    broadcastToRoom(room, { type: 'hostChanged', room: roomName, newHostId: newHost });
  }
}

Bun.serve({
  port: port,
  fetch(req, server) {
    if (new URL(req.url).pathname === '/ws') {
      server.upgrade(req);
      return;
    }
    return new Response('Not found', { status: 404 });
  },
  websocket: {
    open(ws) {
      const id = generateId();
      ws.data = { id };
      peers.set(id, { ws, id, rooms: new Set() });
      ws.send(JSON.stringify({ type: 'connected', id }));
      console.log(`CONNECTED: ${id}`);
    },
    message(ws, raw) {
      const msg = JSON.parse(raw);
      const from = ws.data.id;

      switch (msg.type) {
        case 'join': {
          const { roomBase, maxPeers = DEFAULT_MAX_PEERS, previousRoom } = msg;
          let roomName;
          if (previousRoom) {
            // Rejoin previous room regardless of capacity
            roomName = previousRoom;
            if (!rooms.has(roomName)) {
              rooms.set(roomName, { name: roomName, peers: new Map() });
            }
          } else {
            roomName = findOrCreateRoom(roomBase, maxPeers);
          }
          const room = rooms.get(roomName);
          const joinTime = Date.now();
          room.peers.set(from, { id: from, joinTime });
          peers.get(from).rooms.add(roomName);
          const hostId = getRoomHost(room);
          const peerList = Array.from(room.peers.keys());
          ws.send(JSON.stringify({
            type: 'joined',
            room: roomName,
            yourId: from,
            peers: peerList,
            hostId,
          }));
          broadcastToRoom(room, { type: 'peerJoined', room: roomName, peerId: from }, from);
          break;
        }
        case 'leave': {
          peerLeft(from, msg.room);
          peers.get(from)?.rooms.delete(msg.room);
          break;
        }
        case 'offer': case 'answer': case 'candidate': {
          const target = peers.get(msg.target);
          if (target) {
            target.ws.send(JSON.stringify({
              type: msg.type,
              from,
              room: msg.room,
              sdp: msg.sdp,
              candidate: msg.candidate,
            }));
          }
          break;
        }
        default: console.warn('Unknown type', msg.type);
      }
    },
    close(ws) {
      const pid = ws.data.id;
      const p = peers.get(pid);
      if (p) {
        for (const r of p.rooms) peerLeft(pid, r);
        peers.delete(pid);
        console.log(`DISCONNECTED: ${pid}`);
      }
    }
  }
});

console.log('Server running on ws://localhost:3000/ws');
