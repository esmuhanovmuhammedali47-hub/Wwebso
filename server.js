const WebSocket = require("ws");

const port = process.env.PORT || 10000;

const FIREBASE_BASE =
  "https://trictracaj-default-rtdb.firebaseio.com";

const FIREBASE_URL =
  `${FIREBASE_BASE}/logs.json`;

const wss = new WebSocket.Server({ port });

const clients = new Set();

console.log("WebSocket server started");

// AUTO CLEAN EVERY 5 MINUTES
setInterval(async () => {
    try {
        console.log("Cleaning old Firebase logs...");

        const res = await fetch(`${FIREBASE_BASE}/logs.json`);
        const logs = await res.json();

        if (!logs) return;

        const now = Date.now();
        const maxAge = 5 * 60 * 1000; // 5 minutes

        for (const key in logs) {
            const log = logs[key];

            if (!log.time || now - log.time > maxAge) {

                await fetch(`${FIREBASE_BASE}/logs/${key}.json`, {
                    method: "DELETE"
                });

                console.log("Deleted:", key);
            }
        }

    } catch (err) {
        console.log("Cleanup error:", err);
    }
}, 60 * 1000); // checks every minute

wss.on("connection", (ws) => {
    console.log("Client connected");

    clients.add(ws);

    ws.on("message", async (message) => {
        try {
            const text = message.toString();

            console.log("Received:", text);

            let data;

            try {
                data = JSON.parse(text);
            } catch {
                data = { message: text };
            }

            // save to firebase
            await fetch(FIREBASE_URL, {
                method: "POST",
                headers: {
                    "Content-Type": "application/json"
                },
                body: JSON.stringify({
                    ...data,
                    time: Date.now()
                })
            });

            // send to all websocket clients
            for (const client of clients) {
                if (client.readyState === WebSocket.OPEN) {
                    client.send(JSON.stringify(data));
                }
            }

        } catch (err) {
            console.log(err);
        }
    });

    ws.on("close", () => {
        clients.delete(ws);
        console.log("Client disconnected");
    });
});