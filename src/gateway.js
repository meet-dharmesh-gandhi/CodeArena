const express = require("express");
const { WebSocketServer } = require("ws");
const dgram = require("dgram");
const net = require("net");
const http = require("http");

// --- CONSTANTS MAP (Matching constants.h) ---
const PACKET_ID = 0xcaf1;
const PACKET_TYPES = {
	GATEWAY_PACKET: 9,
	GATEWAY_REPLY_PACKET: 10,
	TASK_PACKET: 14,
	IO_PACKET: 17,
};
const NODE_TYPES = { GATEWAY: 0, ASSIGNER: 2 };
const IO_TYPES = { INPUT: 0, OUTPUT: 1 };
const PORTS = {
	HTTP: 3000,
	UDP_COM: 8005, // GATEWAY_COM_PORT
	TCP_TASK: 8010, // GATEWAY_TASK_PORT
};

// --- GLOBAL STATE ---
let currentTaskID = 1;
const activeTasks = new Map(); // Maps taskID -> { ws: WebSocket, assignerIP: string }
const activeAssignerConnections = new Map(); // Maps Assigner IP -> net.Socket

// --- UDP SOCKET (Monitor Communication) ---
const udpClient = dgram.createSocket("udp4");
udpClient.bind(PORTS.UDP_COM, () => {
	udpClient.setBroadcast(true);
	console.log(
		`[UDP] Gateway listening for Monitor replies on ${PORTS.UDP_COM}`,
	);
});

udpClient.on("message", (msg, rinfo) => {
	// Unpack generic_packet to check ID and Type
	const packet_ID = msg.readInt32LE(0);
	const packet_type = msg.readInt32LE(4);

	if (
		packet_ID === PACKET_ID &&
		packet_type === PACKET_TYPES.GATEWAY_REPLY_PACKET
	) {
		// Unpack gateway_reply_packet
		const node_type = msg.readInt32LE(8);
		const UID = msg.readInt32LE(12);

		// Extract IP from sockaddr_in (bytes 16-32)
		// struct sockaddr_in: sin_family (2), sin_port (2), sin_addr (4), sin_zero (8)
		const ipNum = msg.readUInt32BE(20); // sin_addr
		const assignerIP = [
			(ipNum >>> 24) & 255,
			(ipNum >>> 16) & 255,
			(ipNum >>> 8) & 255,
			ipNum & 255,
		].join(".");

		console.log(`[UDP] Monitor routed us to Assigner at ${assignerIP}`);

		// Find the pending task and assign it
		const pendingTask = Array.from(activeTasks.values()).find(
			(t) => !t.assignerIP,
		);
		if (pendingTask) {
			pendingTask.assignerIP = assignerIP;
			sendTaskPacketToAssigner(pendingTask.taskID, assignerIP);
		}
	}
});

function sendTaskPacketToAssigner(taskID, assignerIP) {
	// Pack struct task_packet (20 bytes)
	const buf = Buffer.alloc(20);
	buf.writeInt32LE(PACKET_ID, 0);
	buf.writeInt32LE(PACKET_TYPES.TASK_PACKET, 4);
	buf.writeInt32LE(NODE_TYPES.GATEWAY, 8);
	buf.writeInt32LE(1001, 12); // Gateway UID
	buf.writeInt32LE(taskID, 16);

	udpClient.send(buf, 0, buf.length, PORTS.UDP_COM, assignerIP, (err) => {
		if (err) console.error("[UDP] Error sending Task Packet:", err);
		else console.log(`[UDP] Task ${taskID} sent to Assigner ${assignerIP}`);
	});
}

// --- TCP SERVER (Assigner Communication) ---
const tcpServer = net.createServer((socket) => {
	console.log(`[TCP] Assigner connected from ${socket.remoteAddress}`);
	activeAssignerConnections.set(socket.remoteAddress, socket);

	socket.on("data", (data) => {
		// Assuming data arrives as a complete struct io_packet (280 bytes)
		// In production, you would buffer this to handle partial chunks
		if (data.length >= 280) {
			const packet_ID = data.readInt32LE(0);
			const packet_type = data.readInt32LE(4);

			if (
				packet_ID === PACKET_ID &&
				packet_type === PACKET_TYPES.IO_PACKET
			) {
				const task_ID = data.readInt32LE(16);
				const io_type = data.readInt32LE(20);

				// Read null-terminated string from data[256] array at offset 24
				const payloadEnd = data.indexOf(0x00, 24);
				const payload = data.toString(
					"utf8",
					24,
					payloadEnd !== -1 ? payloadEnd : 280,
				);

				if (io_type === IO_TYPES.OUTPUT) {
					const task = activeTasks.get(task_ID);
					if (task && task.ws) {
						task.ws.send(
							JSON.stringify({ type: "output", data: payload }),
						);
					}
				}
			}
		}
	});

	socket.on("close", () => {
		console.log(`[TCP] Assigner ${socket.remoteAddress} disconnected`);
		activeAssignerConnections.delete(socket.remoteAddress);
	});
});

tcpServer.listen(PORTS.TCP_TASK, () => {
	console.log(`[TCP] Gateway listening for Assigners on ${PORTS.TCP_TASK}`);
});

// --- HTTP & WEBSOCKET SERVER (Client Facing) ---
const app = express();
const server = http.createServer(app);
const wss = new WebSocketServer({ server });

wss.on("connection", (ws) => {
	const taskID = currentTaskID++;
	console.log(`[WS] Client connected. Assigned Task ID: ${taskID}`);

	// Register the task
	activeTasks.set(taskID, { ws: ws, assignerIP: null });

	// Step 1: Ask Monitor for an Assigner
	// Pack struct gateway_packet (12 bytes)
	const buf = Buffer.alloc(12);
	buf.writeInt32LE(PACKET_ID, 0);
	buf.writeInt32LE(PACKET_TYPES.GATEWAY_PACKET, 4);
	buf.writeInt32LE(0, 8); // redirected = 0

	// Broadcast to the local subnet (255.255.255.255)
	udpClient.send(buf, 0, buf.length, PORTS.UDP_COM, "255.255.255.255");

	ws.on("message", (message) => {
		// Client sends code to execute
		const task = activeTasks.get(taskID);
		if (task && task.assignerIP) {
			const assignerSocket = activeAssignerConnections.get(
				`::ffff:${task.assignerIP}`,
			);

			if (assignerSocket) {
				// Pack struct io_packet
				const ioBuf = Buffer.alloc(280);
				ioBuf.writeInt32LE(PACKET_ID, 0);
				ioBuf.writeInt32LE(PACKET_TYPES.IO_PACKET, 4);
				ioBuf.writeInt32LE(NODE_TYPES.GATEWAY, 8);
				ioBuf.writeInt32LE(1001, 12); // UID
				ioBuf.writeInt32LE(taskID, 16);
				ioBuf.writeInt32LE(IO_TYPES.INPUT, 20);

				// Write payload into the 256-byte data array
				ioBuf.write(message.toString(), 24, 256, "utf8");

				assignerSocket.write(ioBuf);
				console.log(`[TCP] Sent code for Task ${taskID} to Assigner`);
			}
		}
	});

	ws.on("close", () => {
		console.log(`[WS] Client for Task ${taskID} disconnected`);
		activeTasks.delete(taskID);
		// TODO: Send a cancel packet to the Assigner to kill the container
	});
});

app.use(express.static("public")); // Serve simple HTML frontend

server.listen(PORTS.HTTP, () => {
	console.log(`[HTTP] Web server running on port ${PORTS.HTTP}`);
});
