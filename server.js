const WebSocket = require("ws");

const port = process.env.PORT || 10000;

const FIREBASE_BASE =
  "https://trictracaj-default-rtdb.firebaseio.com";

const FIREBASE_URL =
  `${FIREBASE_BASE}/logs.json`;

const FIREBASE_BLACKLIST_URL =
  `${FIREBASE_BASE}/blacklist.json`;

const wss = new WebSocket.Server({ port });

const clients = new Set();
const blacklist = new Set();

console.log("WebSocket server started");

// AUTO CLEAN FIREBASE LOGS OLDER THAN 1 MINUTE
setInterval(async () => {
    try {
        console.log("Cleaning old Firebase logs...");

        const res = await fetch(FIREBASE_URL);
        const logs = await res.json();

        if (!logs) return;

        const now = Date.now();
        const maxAge = 1 * 60 * 1000; // 1 minute

        for (const key in logs) {
            const log = logs[key];

            if (!log.time || now - log.time > maxAge) {
                await fetch(`${FIREBASE_BASE}/logs/${key}.json`, {
                    method: "DELETE"
                });
                console.log("Deleted old log:", key);
            }
        }

    } catch (err) {
        console.log("Cleanup error:", err);
    }
}, 10 * 1000); // checks every 10 seconds

wss.on("connection", (ws) => {
    console.log("Client connected");

    clients.add(ws);

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

            if (data.action === "blacklist" && data.jobId) {
                if (blacklist.has(data.jobId)) return;

                blacklist.add(data.jobId);
                console.log(`Server ${data.jobId} blacklisted for 10 seconds`);

                for (const client of clients) {
                    if (client.readyState === WebSocket.OPEN) {
                        client.send(JSON.stringify({
                            action: "blacklist",
                            jobId: data.jobId
                        }));
                    }
                }

                setTimeout(async () => {
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

                    // DELETE THE BLACKLIST LOG FROM FIREBASE BY NEW PATH
                    try {
                        const res = await fetch(FIREBASE_BLACKLIST_URL);
                        const bList = await res.json();
                        if (bList) {
                            for (const key in bList) {
                                if (bList[key].jobId === data.jobId) {
                                    await fetch(`${FIREBASE_BASE}/blacklist/${key}.json`, {
                                        method: "DELETE"
                                    });
                                    console.log("Deleted blacklist log from Firebase path:", key);
                                }
                            }
                        }
                    } catch (err) {
                        console.log("Error deleting blacklist log:", err);
                    }
                }, 10000);

                // SAVE BLACKLIST TO FIREBASE BY NEW PATH
                await fetch(FIREBASE_BLACKLIST_URL, {
                    method: "POST",
                    headers: {
                        "Content-Type": "application/json"
                    },
                    body: JSON.stringify({
                        jobId: data.jobId,
                        time: Date.now()
                    })
                });

                return;
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
