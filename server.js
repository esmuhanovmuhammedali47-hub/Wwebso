const WebSocket = require("ws");

const port = process.env.PORT || 10000;

const FIREBASE_BASE =
  "https://trictracaj-default-rtdb.firebaseio.com";

const FIREBASE_URL =
  `${FIREBASE_BASE}/logs.json`;

const wss = new WebSocket.Server({ port });

const clients = new Set();

console.log("WebSocket server started");

setInterval(async () => {
    try {

        const res = await fetch(`${FIREBASE_BASE}/logs.json`);
        const logs = await res.json();

        if (!logs) return;

        const now = Date.now();
        const maxAge = 30 * 1000;

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
        console.log(err);
    }
}, 30 * 1000);

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