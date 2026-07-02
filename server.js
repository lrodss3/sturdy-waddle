// server.js – Bun signaling server with persistent client ID and ghost cleanup
const ALLOWED_ORIGINS = ['*' ,'https://yourdomain.com', 'http://localhost:3000']; // change as needed

const PORT = process.env.PORT || 3000 || 15191;
const DEFAULT_MAX_PEERS = 8;
const HEARTBEAT_INTERVAL = 5_000;       // send ping every 5 seconds
const HEARTBEAT_TIMEOUT = 10_000;       // disconnect if no pong within 10 seconds
const CONSISTENCY_CHECK_INTERVAL = 60_000; // run ghost cleanup every minute

const DEBUG = process.env.DEBUG === 'true';
function log(...args) { if (DEBUG) console.log('[SERVER]', ...args); }

const peers = new Map(); // peerId -> { ws, id, rooms: Set, lastPong }
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
  if (room.peers.size === 0) {
    rooms.delete(roomName);
    return;
  }
  broadcastToRoom(room, { type: 'peerLeft', room: roomName, peerId: pid });
  if (wasHost) {
    const newHost = getRoomHost(room);
    broadcastToRoom(room, { type: 'hostChanged', room: roomName, newHostId: newHost });
  }
}

function removePeer(pid) {
  const peer = peers.get(pid);
  if (peer) {
    const roomsCopy = new Set(peer.rooms);
    for (const roomName of roomsCopy) {
      peerLeft(pid, roomName);
    }
    peers.delete(pid);
    log(`Removed peer ${pid}`);
  } else {
    for (const [roomName, room] of rooms) {
      if (room.peers.has(pid)) {
        peerLeft(pid, roomName);
        log(`Cleaned up ghost ${pid} from room ${roomName}`);
      }
    }
  }
}

function consistencyCheck() {
  log('Running ghost cleanup...');
  for (const [roomName, room] of rooms) {
    for (const pid of room.peers.keys()) {
      if (!peers.has(pid)) {
        log(`Ghost peer ${pid} found in room ${roomName}, removing.`);
        peerLeft(pid, roomName);
      }
    }
  }
}

// --- NEW HELPER: Find a room with a common peer ---
function findRoomWithCommonPeer(peer, baseName) {
  // Collect all existing rooms that match the base name pattern (e.g., "newroom1", "newroom2")
  const matchingRooms = new Map(); // roomName -> Map of peers
  for (const [roomName, room] of rooms) {
    if (roomName.startsWith(baseName) && /^\d+$/.test(roomName.slice(baseName.length))) {
      matchingRooms.set(roomName, room.peers);
    }
  }
  if (matchingRooms.size === 0) return null;

  // For each room the current peer is already in, check its peers
  for (const currentRoomName of peer.rooms) {
    const currentRoom = rooms.get(currentRoomName);
    if (!currentRoom) continue;
    for (const otherPeerId of currentRoom.peers.keys()) {
      // See if that other peer is in any of the matching rooms
      for (const [candRoomName, candRoomPeers] of matchingRooms) {
        if (candRoomPeers.has(otherPeerId)) {
          return candRoomName; // first match wins
        }
      }
    }
  }
  return null;
}

Bun.serve({
  port: PORT,
  fetch(req, server) {
    const url = new URL(req.url);
    if (url.pathname === '/ws') {
      // --- SIMPLE ORIGIN CHECK ---
      const origin = req.headers.get('origin');
      if (!ALLOWED_ORIGINS.includes('*') && (!origin || !ALLOWED_ORIGINS.includes(origin))) {
        return new Response('Forbidden', { status: 403 });
      }
      // --- END CHECK ---
      const clientId = url.searchParams.get('clientId');
      const success = server.upgrade(req, { data: { clientId } });
      if (success) return;
      return new Response('Upgrade failed', { status: 500 });
    }
    return new Response('Not found', { status: 404 });
  },
  websocket: {
    open(ws) {
      const clientId = ws.data?.clientId;
      let id = clientId;
      if (!id) id = generateId();

      if (peers.has(id)) {
        console.log(`Replacing existing peer ${id}`);
        const oldPeer = peers.get(id);
        try { oldPeer.ws.close(1000, 'Replaced by new connection'); } catch {}
        removePeer(id);
      }

      ws.data = { id };
      peers.set(id, { ws, id, rooms: new Set(), lastPong: Date.now() });
      ws.send(JSON.stringify({ type: 'connected', id }));
      console.log(`CONNECTED: ${id}${clientId ? ' (persistent)' : ''}`);
    },

    message(ws, raw) {
      const msg = JSON.parse(raw);
      const from = ws.data.id;
      const fromPeer = peers.get(from);
      if (!fromPeer) {
        ws.close(1011, 'Peer not found');
        return;
      }
      fromPeer.lastPong = Date.now();

      switch (msg.type) {
        case 'join': {
          const { roomBase, maxPeers = DEFAULT_MAX_PEERS, previousRoom } = msg;
          let roomName;

          // --- MODIFIED JOIN LOGIC ---
          if (!previousRoom) {
            // Joining a room by base name: look for a room that already contains a common peer.
            const candidateRoom = findRoomWithCommonPeer(fromPeer, roomBase);
            if (candidateRoom) {
              roomName = candidateRoom;               // exception: use that room (even if full)
            } else {
              roomName = findOrCreateRoom(roomBase, maxPeers); // normal logic
            }
          } else {
            // Joining a specific previous room (rejoin after page reload)
            roomName = previousRoom;
            if (!rooms.has(roomName)) {
              rooms.set(roomName, { name: roomName, peers: new Map() });
            }
          }
          // --- END OF MODIFICATION ---

          const room = rooms.get(roomName);
          const joinTime = Date.now();
          room.peers.set(from, { id: from, joinTime });
          fromPeer.rooms.add(roomName);
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
          fromPeer.rooms.delete(msg.room);
          break;
        }

        case 'offer':
        case 'answer':
        case 'candidate': {
          const target = peers.get(msg.target);
          if (target) {
            target.ws.send(JSON.stringify({
              type: msg.type,
              from,
              room: msg.room,
              sdp: msg.sdp,
              candidate: msg.candidate,
            }));
          } else {
            log(`Target ${msg.target} not found`);
          }
          break;
        }

        default:
          console.warn('Unknown message type:', msg.type);
      }
    },

    close(ws) {
      if (!ws.data?.id) return;
      removePeer(ws.data.id);
    },

    pong(ws) {
      const peer = peers.get(ws.data.id);
      if (peer) peer.lastPong = Date.now();
    }
  }
});

console.log(`Server running on ws://localhost:${PORT}/ws`);

setInterval(() => {
  const now = Date.now();
  for (const [pid, peer] of peers) {
    if (now - peer.lastPong > HEARTBEAT_TIMEOUT) {
      console.log(`HEARTBEAT TIMEOUT: ${pid}`);
      try { peer.ws.close(1008, 'Heartbeat timeout'); } catch {}
    } else {
      try { peer.ws.ping(); } catch (err) {
        console.log(`Ping failed for ${pid}`);
        try { peer.ws.close(1011, 'Ping failed'); } catch {}
      }
    }
  }
}, HEARTBEAT_INTERVAL);

setInterval(consistencyCheck, CONSISTENCY_CHECK_INTERVAL);
