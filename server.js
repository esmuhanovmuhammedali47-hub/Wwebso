const WebSocket = require("ws");

const port = process.env.PORT || 10000;

const FIREBASE_BASE =
  "https://trictracaj-default-rtdb.firebaseio.com";

const FIREBASE_URL =
  `${FIREBASE_BASE}/logs.json`;

const wss = new WebSocket.Server({ port });

const clients = new Set();
const blacklist = new Set();

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

    // Sync current blacklist to newly connected client
    if (blacklist.size > 0) {
        ws.send(JSON.stringify({
            action: "sync_blacklist",
            blacklist: Array.from(blacklist)
        }));
    }

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

            // Handle blacklist logic
            if (data.action === "blacklist" && data.jobId) {
                blacklist.add(data.jobId);
                console.log(`Server ${data.jobId} blacklisted for 1 minute`);

                // Broadcast blacklist state to all clients
                for (const client of clients) {
                    if (client.readyState === WebSocket.OPEN) {
                        client.send(JSON.stringify({
                            action: "blacklist",
                            jobId: data.jobId
                        }));
                    }
                }

                // Remove from blacklist after 1 minute (60000 ms)
                setTimeout(() => {
                    blacklist.delete(data.jobId);
                    console.log(`Server ${data.jobId} removed from blacklist`);

                    for (const client of clients) {
                        if (client.readyState === WebSocket.OPEN) {
                            client.send(JSON.stringify({
                                action: "unblacklist",
                                jobId: data.jobId
                            }));
                        }
                    }
                }, 60000);

                return;
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