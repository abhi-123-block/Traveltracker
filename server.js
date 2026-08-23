const express = require('express');
const http = require('http');
const { Server } = require('socket.io');
const cors = require('cors');
const sqlite3 = require('sqlite3').verbose();
const path = require('path');
const fs = require('fs');

// ─── App setup ────────────────────────────────────────────────
const app = express();
const server = http.createServer(app);
const io = new Server(server, {
  cors: { origin: "*", methods: ["GET", "POST"] }
});

app.use(cors());
app.use(express.json());
app.use(express.static('public'));

// ─── Database – ensure folder exists ──────────────────────────
const dbDir = path.join(__dirname, 'database');
if (!fs.existsSync(dbDir)) fs.mkdirSync(dbDir, { recursive: true });
const dbPath = path.join(dbDir, 'fitness_tracker.db');
const db = new sqlite3.Database(dbPath);

// ─── Create tables ─────────────────────────────────────────────
db.serialize(() => {
  db.run(`CREATE TABLE IF NOT EXISTS users (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    username TEXT UNIQUE NOT NULL,
    created_at DATETIME DEFAULT CURRENT_TIMESTAMP
  )`);

  db.run(`CREATE TABLE IF NOT EXISTS trips (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    name TEXT NOT NULL,
    destination TEXT,
    max_participants INTEGER DEFAULT 10,
    created_by INTEGER,
    created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (created_by) REFERENCES users (id)
  )`);

  db.run(`CREATE TABLE IF NOT EXISTS trip_members (
    user_id INTEGER,
    trip_id INTEGER,
    total_distance REAL DEFAULT 0,
    total_steps INTEGER DEFAULT 0,
    last_latitude REAL,
    last_longitude REAL,
    is_active BOOLEAN DEFAULT 0,
    last_update DATETIME DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (user_id, trip_id),
    FOREIGN KEY (user_id) REFERENCES users (id),
    FOREIGN KEY (trip_id) REFERENCES trips (id)
  )`);

  db.run(`CREATE TABLE IF NOT EXISTS locations (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    user_id INTEGER,
    trip_id INTEGER,
    latitude REAL,
    longitude REAL,
    timestamp DATETIME DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (user_id) REFERENCES users (id),
    FOREIGN KEY (trip_id) REFERENCES trips (id)
  )`);
});

// ─── Helpers ────────────────────────────────────────────────────
function haversineDistance(lat1, lon1, lat2, lon2) {
  const R = 6371;
  const dLat = (lat2 - lat1) * Math.PI / 180;
  const dLon = (lon2 - lon1) * Math.PI / 180;
  const a = Math.sin(dLat/2)**2 + Math.cos(lat1 * Math.PI/180) * Math.cos(lat2 * Math.PI/180) * Math.sin(dLon/2)**2;
  return 2 * R * Math.atan2(Math.sqrt(a), Math.sqrt(1-a));
}

function distanceToSteps(km) {
  return Math.round(km / 0.000762);
}

// ─── In‑memory state ──────────────────────────────────────────
const activeUsers = new Map(); // socketId → { userId, username, lat, lng, tripId, socketId }
const userPaths = new Map();  // socketId → [ {lat, lng}, … ]

// ─── Socket.IO events ──────────────────────────────────────────
io.on('connection', (socket) => {
  console.log(`🔌 Client connected: ${socket.id}`);

  socket.on('register', (data) => {
    const { username, latitude, longitude, tripId } = data;
    if (!username || latitude == null || longitude == null) {
      return socket.emit('error', { message: 'Invalid registration data' });
    }

    db.get('SELECT id FROM users WHERE username = ?', [username], (err, row) => {
      if (err) return socket.emit('error', { message: 'Database error' });
      if (!row) return socket.emit('error', { message: 'User not found' });

      const userId = row.id;
      if (tripId) {
        db.run('INSERT OR IGNORE INTO trip_members (user_id, trip_id, last_latitude, last_longitude, is_active) VALUES (?, ?, ?, ?, 1)',
          [userId, tripId, latitude, longitude]);
      }

      activeUsers.set(socket.id, { userId, username, latitude, longitude, tripId: tripId || null, socketId: socket.id });
      userPaths.set(socket.id, [{ lat: latitude, lng: longitude }]);

      db.run('INSERT INTO locations (user_id, trip_id, latitude, longitude) VALUES (?, ?, ?, ?)',
        [userId, tripId || null, latitude, longitude]);

      if (tripId) {
        db.run('UPDATE trip_members SET last_latitude = ?, last_longitude = ?, is_active = 1, last_update = CURRENT_TIMESTAMP WHERE user_id = ? AND trip_id = ?',
          [latitude, longitude, userId, tripId]);
      }

      socket.emit('registered', { userId, tripId });
      sendLeaderboard(socket, tripId);
      sendAllUsers(socket);
    });
  });

  socket.on('updateLocation', (data) => {
    const { latitude, longitude } = data;
    const user = activeUsers.get(socket.id);
    if (!user) return socket.emit('error', { message: 'Not registered' });

    const prevLat = user.latitude;
    const prevLon = user.longitude;
    const distance = haversineDistance(prevLat, prevLon, latitude, longitude);

    user.latitude = latitude;
    user.longitude = longitude;

    db.run('INSERT INTO locations (user_id, trip_id, latitude, longitude) VALUES (?, ?, ?, ?)',
      [user.userId, user.tripId, latitude, longitude]);

    if (user.tripId) {
      db.run(`UPDATE trip_members 
              SET total_distance = total_distance + ?,
                  total_steps = total_steps + ?,
                  last_latitude = ?,
                  last_longitude = ?,
                  last_update = CURRENT_TIMESTAMP,
                  is_active = 1
              WHERE user_id = ? AND trip_id = ?`,
        [distance, distanceToSteps(distance), latitude, longitude, user.userId, user.tripId]);
    }

    const path = userPaths.get(socket.id) || [];
    path.push({ lat: latitude, lng: longitude });
    userPaths.set(socket.id, path);

    io.emit('locationUpdate', {
      socketId: socket.id,
      username: user.username,
      latitude,
      longitude,
      distance,
      tripId: user.tripId
    });

    sendLeaderboard(null, user.tripId);
  });

  socket.on('getLeaderboard', (tripId) => {
    sendLeaderboard(socket, tripId);
  });

  socket.on('toggleTracking', ({ isTracking }) => {
    const user = activeUsers.get(socket.id);
    if (user && user.tripId) {
      db.run('UPDATE trip_members SET is_active = ? WHERE user_id = ? AND trip_id = ?',
        [isTracking ? 1 : 0, user.userId, user.tripId]);
      sendLeaderboard(null, user.tripId);
    }
  });

  socket.on('disconnect', () => {
    if (activeUsers.has(socket.id)) {
      const user = activeUsers.get(socket.id);
      console.log(`❌ User disconnected: ${user.username}`);
      if (user.tripId) {
        db.run('UPDATE trip_members SET is_active = 0 WHERE user_id = ? AND trip_id = ?',
          [user.userId, user.tripId]);
      }
      activeUsers.delete(socket.id);
      io.emit('userDisconnected', { socketId: socket.id, username: user.username });
      sendLeaderboard(null, user.tripId);
    }
  });
});

// ─── Helper broadcast functions ──────────────────────────────
function sendLeaderboard(targetSocket = null, tripId = null) {
  if (!tripId) return;
  db.all(`SELECT u.username, tm.total_distance, tm.total_steps, tm.is_active, tm.last_update
          FROM trip_members tm
          JOIN users u ON tm.user_id = u.id
          WHERE tm.trip_id = ?
          ORDER BY tm.total_distance DESC`, [tripId], (err, rows) => {
    if (err) return console.error(err);
    const data = rows.map(r => ({
      username: r.username,
      distance: parseFloat(r.total_distance.toFixed(2)),
      steps: r.total_steps,
      isActive: r.is_active === 1,
      lastUpdate: r.last_update
    }));
    if (targetSocket) targetSocket.emit('leaderboardUpdate', data);
    else io.emit('leaderboardUpdate', data);
  });
}

function sendAllUsers(targetSocket) {
  const users = Array.from(activeUsers.values()).map(u => ({
    socketId: u.socketId,
    username: u.username,
    latitude: u.latitude,
    longitude: u.longitude,
    tripId: u.tripId
  }));
  targetSocket.emit('allUsers', users);
  userPaths.forEach((path, socketId) => {
    targetSocket.emit('userPath', { socketId, path });
  });
}

// ─── REST API ──────────────────────────────────────────────────

// Register new user
app.post('/api/register', (req, res) => {
  const { username } = req.body;
  if (!username) return res.status(400).json({ error: 'Username required' });
  db.get('SELECT id FROM users WHERE username = ?', [username], (err, row) => {
    if (err) return res.status(500).json({ error: 'Database error' });
    if (row) return res.status(409).json({ error: 'Username already taken' });
    db.run('INSERT INTO users (username) VALUES (?)', [username], function(err) {
      if (err) return res.status(500).json({ error: 'Could not create user' });
      res.json({ userId: this.lastID, username });
    });
  });
});

// Login – check if user exists
app.post('/api/login', (req, res) => {
  const { username } = req.body;
  if (!username) return res.status(400).json({ error: 'Username required' });
  db.get('SELECT id FROM users WHERE username = ?', [username], (err, row) => {
    if (err) return res.status(500).json({ error: 'Database error' });
    if (!row) return res.status(404).json({ error: 'User not found' });
    res.json({ userId: row.id, username });
  });
});

// Get trips for a user
app.get('/api/users/:username/trips', (req, res) => {
  const { username } = req.params;
  db.get('SELECT id FROM users WHERE username = ?', [username], (err, row) => {
    if (err || !row) return res.status(404).json({ error: 'User not found' });
    db.all(`SELECT t.*, u.username as creator_name
            FROM trip_members tm
            JOIN trips t ON tm.trip_id = t.id
            JOIN users u ON t.created_by = u.id
            WHERE tm.user_id = ?
            ORDER BY t.created_at DESC`, [row.id], (err, trips) => {
      if (err) return res.status(500).json({ error: 'DB error' });
      res.json(trips);
    });
  });
});

// Create trip (accepts username, creates user if needed)
app.post('/api/trips', (req, res) => {
  const { name, destination, maxParticipants, username } = req.body;
  if (!name || !username) {
    return res.status(400).json({ error: 'Missing trip name or username' });
  }

  db.get('SELECT id FROM users WHERE username = ?', [username], (err, row) => {
    if (err) return res.status(500).json({ error: 'Database error' });
    if (!row) return res.status(404).json({ error: 'User not found' });

    const userId = row.id;
    db.run(
      'INSERT INTO trips (name, destination, max_participants, created_by) VALUES (?, ?, ?, ?)',
      [name, destination, maxParticipants || 10, userId],
      function(err) {
        if (err) return res.status(500).json({ error: 'Failed to create trip' });
        const tripId = this.lastID;
        db.run('INSERT OR IGNORE INTO trip_members (user_id, trip_id, is_active) VALUES (?, ?, 1)',
          [userId, tripId]);
        res.json({ tripId });
      }
    );
  });
});

// Join trip
app.post('/api/trips/join', (req, res) => {
  const { username, tripId } = req.body;
  if (!username || !tripId) return res.status(400).json({ error: 'Missing username or tripId' });

  db.get('SELECT id FROM users WHERE username = ?', [username], (err, row) => {
    if (err || !row) return res.status(404).json({ error: 'User not found' });
    const userId = row.id;
    db.run('INSERT OR IGNORE INTO trip_members (user_id, trip_id) VALUES (?, ?)',
      [userId, tripId], (err) => {
        if (err) return res.status(500).json({ error: 'Could not join trip' });
        res.json({ success: true });
      });
  });
});

// Get trip details (including admin info)
app.get('/api/trips/:tripId', (req, res) => {
  const { tripId } = req.params;
  db.get(`SELECT t.*, u.username as creator_name
          FROM trips t
          JOIN users u ON t.created_by = u.id
          WHERE t.id = ?`, [tripId], (err, row) => {
    if (err || !row) return res.status(404).json({ error: 'Trip not found' });
    res.json(row);
  });
});

// Get members of a trip
app.get('/api/trips/:tripId/members', (req, res) => {
  const { tripId } = req.params;
  db.all(`SELECT u.id, u.username, tm.total_distance, tm.total_steps, tm.is_active
          FROM trip_members tm
          JOIN users u ON tm.user_id = u.id
          WHERE tm.trip_id = ?`, [tripId], (err, rows) => {
    if (err) return res.status(500).json({ error: 'DB error' });
    res.json(rows);
  });
});

// Add member to trip (admin only)
app.post('/api/trips/:tripId/members', (req, res) => {
  const { tripId } = req.params;
  const { username, adminUsername } = req.body;
  if (!username || !adminUsername) {
    return res.status(400).json({ error: 'Missing username or adminUsername' });
  }

  // Check if admin is the creator
  db.get('SELECT created_by FROM trips WHERE id = ?', [tripId], (err, trip) => {
    if (err || !trip) return res.status(404).json({ error: 'Trip not found' });
    db.get('SELECT id FROM users WHERE username = ?', [adminUsername], (err, admin) => {
      if (err || !admin) return res.status(404).json({ error: 'Admin not found' });
      if (trip.created_by !== admin.id) {
        return res.status(403).json({ error: 'Only the trip creator can add members' });
      }
      // Find user to add
      db.get('SELECT id FROM users WHERE username = ?', [username], (err, user) => {
        if (err || !user) return res.status(404).json({ error: 'User not found' });
        db.run('INSERT OR IGNORE INTO trip_members (user_id, trip_id) VALUES (?, ?)',
          [user.id, tripId], (err) => {
            if (err) return res.status(500).json({ error: 'Could not add member' });
            res.json({ success: true });
          });
      });
    });
  });
});

// Remove member from trip (admin only)
app.delete('/api/trips/:tripId/members/:userId', (req, res) => {
  const { tripId, userId } = req.params;
  const { adminUsername } = req.body;
  if (!adminUsername) return res.status(400).json({ error: 'Missing adminUsername' });

  db.get('SELECT created_by FROM trips WHERE id = ?', [tripId], (err, trip) => {
    if (err || !trip) return res.status(404).json({ error: 'Trip not found' });
    db.get('SELECT id FROM users WHERE username = ?', [adminUsername], (err, admin) => {
      if (err || !admin) return res.status(404).json({ error: 'Admin not found' });
      if (trip.created_by !== admin.id) {
        return res.status(403).json({ error: 'Only the trip creator can remove members' });
      }
      db.run('DELETE FROM trip_members WHERE user_id = ? AND trip_id = ?', [userId, tripId], (err) => {
        if (err) return res.status(500).json({ error: 'Could not remove member' });
        res.json({ success: true });
      });
    });
  });
});

// Get path for a user in a trip
app.get('/api/trips/:tripId/user/:username/path', (req, res) => {
  const { tripId, username } = req.params;
  db.get('SELECT id FROM users WHERE username = ?', [username], (err, userRow) => {
    if (err || !userRow) return res.status(404).json({ error: 'User not found' });
    db.all('SELECT latitude, longitude, timestamp FROM locations WHERE user_id = ? AND trip_id = ? ORDER BY timestamp ASC',
      [userRow.id, tripId], (err, locs) => {
        if (err) return res.status(500).json({ error: 'DB error' });
        res.json(locs);
      });
  });
});

// ─── Serve frontend pages ──────────────────────────────────────
app.get('/', (req, res) => res.sendFile(path.join(__dirname, 'public', 'index.html')));
app.get('/register', (req, res) => res.sendFile(path.join(__dirname, 'public', 'register.html')));
app.get('/login', (req, res) => res.sendFile(path.join(__dirname, 'public', 'login.html')));
app.get('/dashboard', (req, res) => res.sendFile(path.join(__dirname, 'public', 'dashboard.html')));
app.get('/trip', (req, res) => res.sendFile(path.join(__dirname, 'public', 'trip.html')));

// ─── Start server ──────────────────────────────────────────────
const PORT = process.env.PORT || 3000;
server.listen(PORT, () => {
  console.log(`🚀 Server running at http://localhost:${PORT}`);
});