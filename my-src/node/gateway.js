const dotenv = require("dotenv");
const ws = require("ws");
const http = require("http");
const cG = require("./build/Release/gateway.node");

const GATEWAY_CAPACITY = 20;
const MAX_DATA_CAPACITY = 256;
const WS_CAPACITY = MAX_DATA_CAPACITY * 10; // max 10 packets of data

dotenv.config();

console.log("Server starting...", cG.ok);

const server = http.createServer((req, res) => {});

console.log("Server started");

const wss = new ws.WebSocketServer({ server });

console.log("wss created");

let tasks = new Array(GATEWAY_CAPACITY);

console.log("tasks created");

wss.on("connection", (ws) => {
	// tell c that a new connection has arrived
	const taskID = cG.createTask(
		(taskID) => {
			console.log("here1");
			ws.resume();
		},
		(taskID) => {
			console.log("here2");
			ws.close();
			tasks[taskID] = null;
		},
		(buffer) => {
			console.log("here3");
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
	console.log("connection");

	ws.on("message", (message) => {
		console.log("message");
		// tell c that a new message has arrived
		if (cG.onMessage(message, taskID)) {
			ws.pause();
		}
	});
});

console.log("here - 1");

server.listen(8000, "localhost", () => {
	console.log("server listening on port 8000");
});

console.log("here 1");
