const WebSocket = require("ws");

const port = process.env.PORT || 10000;

const FIREBASE_URL =
  "https://trictracaj-default-rtdb.firebaseio.com/logs.json";

const wss = new WebSocket.Server({ port });

const clients = new Set();

console.log("WebSocket server started");

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
