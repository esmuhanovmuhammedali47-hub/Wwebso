const WebSocket = require("ws");

const port = process.env.PORT || 10000;
const FIREBASE_BASE = "https://looprix-validation-default-rtdb.firebaseio.com";

const wss = new WebSocket.Server({ port });

const clients = new Set();

console.log("WebSocket server started on port", port);

// Cleanup Firebase logs
async function cleanupLogs() {
    try {
        const res = await fetch(`${FIREBASE_BASE}/logs.json`);
        const logs = await res.json();

        if (!logs) return;

        const now = Date.now();
        const maxAge = 30 * 1000;

        const deletions = [];

        for (const [key, log] of Object.entries(logs)) {
            if (!log?.time || now - log.time > maxAge) {
                deletions.push(
                    fetch(`${FIREBASE_BASE}/logs/${key}.json`, {
                        method: "DELETE",
                    }).then(() => {
                        console.log("Deleted:", key);
                    })
                );
            }
        }

        await Promise.allSettled(deletions);
    } catch (err) {
        console.error("Cleanup error:", err);
    }
}

setInterval(cleanupLogs, 30 * 1000);

// Broadcast helper
function broadcast(data) {
    const msg = JSON.stringify(data);

    for (const client of clients) {
        if (client.readyState === WebSocket.OPEN) {
            client.send(msg);
        }
    }
}

// WebSocket connection
wss.on("connection", (ws) => {
    console.log("Client connected");
    clients.add(ws);

    ws.isAlive = true;

    ws.on("pong", () => {
        ws.isAlive = true;
    });

    ws.on("message", async (message) => {
        try {
            const raw = message.toString();

            let data;
            try {
                data = JSON.parse(raw);
            } catch {
                data = { message: raw };
            }

            const payload = {
                ...data,
                time: Date.now(),
            };

            // save to Firebase
            await fetch(`${FIREBASE_BASE}/logs.json`, {
                method: "POST",
                headers: { "Content-Type": "application/json" },
                body: JSON.stringify(payload),
            });

            broadcast(data);

        } catch (err) {
            console.error("Message error:", err);
        }
    });

    ws.on("close", () => {
        clients.delete(ws);
        console.log("Client disconnected");
    });
});

// Heartbeat (detect dead clients)
setInterval(() => {
    for (const ws of clients) {
        if (!ws.isAlive) {
            clients.delete(ws);
            ws.terminate();
            continue;
        }

        ws.isAlive = false;
        ws.ping();
    }
}, 30000);