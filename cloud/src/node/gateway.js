const dotenv = require("dotenv");
const ws = require("ws");
const http = require("http");
const cG = require("./build/Release/gateway.node");
const { exit, chdir } = require("process");
const { spawn, execSync } = require("child_process");

const GATEWAY_CAPACITY = 20;
const MAX_DATA_CAPACITY = 256;
const WS_CAPACITY = MAX_DATA_CAPACITY * 10; // max 10 packets of data

dotenv.config();

console.log("Server starting...", cG.ok);

const server = http.createServer((req, res) => {});

const wss = new ws.WebSocketServer({ server });

cG.init((path) => {
	console.log("init called");
	chdir("../../");
	// console.log(execSync("ls").toString());
	spawn(path, {
		stdio: "inherit",
	});
	const run = () => {
		console.log("wss size", wss.clients.size);
		if (wss.clients.size == 0) {
			wss.close();
			server.close();
			exit(0);
		}
	};
	run();
	setInterval(run, 1000);
});

let tasks = new Array(GATEWAY_CAPACITY);

wss.on("connection", (ws) => {
	// tell c that a new connection has arrived
	const taskID = cG.createTasks(
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
					if (ws.bufferedAmount < WS_CAPACITY) {
						cG.onDrain(taskID);
						clearInterval(interval);
					}
				}, 10);
				return true;
			}

			return false;
		},
		(ide_buffer) => {
			ws.send(JSON.stringify({ type: 2, data: Array.from(ide_buffer) }));
			if (ws.bufferedAmount > WS_CAPACITY) {
				const interval = setInterval(() => {
					if (ws.bufferedAmount < WS_CAPACITY) {
						cG.onIdeDrain();
						clearInterval(interval);
					}
				}, 10);
				return true;
			}
			return false;
		}
	);

	if (taskID < 0) {
		ws.close(404);
	} else {
		tasks[taskID] = ws;
		console.log("connection");
	}

	// type - 0 = terminal
	// type - 1 = command
	ws.on("message", (message) => {
		console.log("message:", message);
		try {
			const obj = JSON.parse(message);
			if (
				Object.hasOwn(obj, "type") &&
				Object.hasOwn(obj, "data") &&
				typeof obj.type == "number"
			) {
				if (obj.type == 1) {
					const buf = Buffer.from(obj.data);
					if (cG.onMessage(buf, taskID, obj.type)) {
						ws.pause();
					}
				} else if (obj.type == 2) {
					const buf = Buffer.from(obj.data);
					if (cG.onIdeMessage(buf, taskID)) {
						ws.pause();
					}
				} else {
					throw Error();
				}
			} else {
				throw Error();
			}
		} catch (error) {
			// tell c that a new message has arrived
			if (cG.onMessage(message, taskID, 0)) {
				ws.pause();
			}
		}
	});
});

server.listen(3000, "0.0.0.0", () => {
	console.log("server listening on port 3000");
});
