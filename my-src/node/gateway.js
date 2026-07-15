const dotenv = require("dotenv");
const ws = require("ws");
const http = require("http");
const cG = require("./build/Release/gateway.node");

const GATEWAY_CAPACITY = 20;
const MAX_DATA_CAPACITY = 256;
const WS_CAPACITY = MAX_DATA_CAPACITY * 10; // max 10 packets of data

dotenv.config();

const server = http.createServer((req, res) => {});

const wss = new ws.WebSocketServer({ server });

let tasks = new Array(GATEWAY_CAPACITY);

wss.on("connection", (ws) => {
	// tell c that a new connection has arrived
	const taskID = cG.createTask(
		(taskID) => {
			ws.resume();
		},
		(taskID) => {
			ws.close();
			tasks[taskID] = null;
		},
		(buffer) => {
			ws.send(buffer);

			if (ws.bufferedAmount > WS_CAPACITY) {
				const interval = setInterval(() => {
					if (bufferedAmount < WS_CAPACITY) {
						cG.onDrain(taskID);
						clearInterval(interval);
					}
				}, 10);
				return true;
			}

			return false;
		},
	);
	tasks[taskID] = ws;

	ws.on("message", (message) => {
		// tell c that a new message has arrived
		if (cG.onMessage(message, taskID)) {
			ws.pause();
		}
	});
});

server.listen(8000, "localhost", () => {
	console.log("server listening on port 8000");
});
