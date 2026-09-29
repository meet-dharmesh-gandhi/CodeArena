const WebSocket = require("ws");

const SESSION_IDLE_MS = Number(
	process.env.TERMINAL_SESSION_IDLE_MS || 10 * 60 * 1000,
);
const TESTING_GATEWAY_PORTS = Array.from(
	{ length: 11 },
	(_, index) => 3000 + index,
);

const sessions = new Map();

const toSessionId = ({ studentId, contestId, problemId }) =>
	// Date.now is needed to ensure multiple submissions allow the terminal to run
	`${studentId}:${contestId}:${problemId}:${Date.now()}`;

const isTestingMode = () =>
	process.env.MODE === "testing" || process.env.NODE_ENV === "testing";

const getGatewayCandidates = (gatewayUrl) => {
	const baseUrl = new URL(gatewayUrl);

	if (!isTestingMode()) {
		return [baseUrl.toString()];
	}

	return TESTING_GATEWAY_PORTS.map((port) => {
		const candidate = new URL(baseUrl.toString());
		candidate.port = String(port);
		return candidate.toString();
	});
};

const connectWebSocket = (gatewayUrl, timeoutMs = 4000) =>
	new Promise((resolve, reject) => {
		const ws = new WebSocket(gatewayUrl);
		let settled = false;

		const finish = (fn, value) => {
			if (settled) return;
			console.log("finish called");
			settled = true;
			clearTimeout(timeout);
			fn(value);
		};

		const timeout = setTimeout(() => {
			console.log("url timed out: ", gatewayUrl);
			try {
				ws.terminate();
			} catch (err) {}
			finish(
				reject,
				new Error(`Timed out while connecting to ${gatewayUrl}.`),
			);
		}, timeoutMs);

		ws.once("open", () => finish(resolve, ws));
		ws.once("error", (err) => {
			console.log("url errored: ", gatewayUrl, err);
			try {
				ws.terminate();
			} catch (closeErr) {}
			finish(reject, err);
		});
	});

const buildFilesFrame = (files) => {
	const normalizedFiles = Array.isArray(files) ? files : [];
	if (normalizedFiles.length > 255) {
		throw new Error(
			"Too many files for terminal session. Max supported is 255.",
		);
	}

	const encodedFiles = normalizedFiles.map((f) => {
		const name = String(f?.name || "main.txt");
		const content = String(f?.content || "");
		const nameBuf = Buffer.from(name, "utf8");
		const contentBuf = Buffer.from(content, "utf8");

		if (nameBuf.length > 255) {
			throw new Error(`Filename too long for terminal frame: ${name}`);
		}

		return { nameBuf, contentBuf };
	});

	let totalSize = 1;
	for (const f of encodedFiles) {
		totalSize += 1 + f.nameBuf.length + 4 + f.contentBuf.length;
	}

	const frame = Buffer.allocUnsafe(totalSize);
	let offset = 0;

	frame.writeUInt8(encodedFiles.length, offset);
	offset += 1;

	for (const f of encodedFiles) {
		frame.writeUInt8(f.nameBuf.length, offset);
		offset += 1;

		f.nameBuf.copy(frame, offset);
		offset += f.nameBuf.length;

		frame.writeUInt32LE(f.contentBuf.length, offset);
		offset += 4;

		f.contentBuf.copy(frame, offset);
		offset += f.contentBuf.length;
	}

	return frame;
};

const broadcast = (sessionId, event) => {
	const session = sessions.get(sessionId);
	if (!session) return;

	const payload = `data: ${JSON.stringify(event)}\n\n`;
	console.log("payload:", payload);
	for (const res of session.sseClients) {
		try {
			res.write(payload);
		} catch (err) {
			session.sseClients.delete(res);
		}
	}
};

const emitSystem = (sessionId, message) => {
	broadcast(sessionId, {
		type: "system",
		sessionId,
		payload: message,
		timestamp: Date.now(),
	});
};

const createSession = async ({ sessionId, gatewayUrl }) => {
	const existing = sessions.get(sessionId);

	if (existing && existing.ws.readyState === WebSocket.OPEN) {
		existing.lastActivity = Date.now();
		console.log("ws not open");
		return existing;
	}

	if (existing && existing.ws.readyState === WebSocket.CONNECTING) {
		console.log("ws connecting");
		return existing;
	}

	const candidates = getGatewayCandidates(gatewayUrl);
	let connectedWs = null;
	let connectedUrl = null;
	let lastError = null;

	for (const candidateUrl of candidates) {
		console.log("ws candidate:", candidateUrl);
		try {
			connectedWs = await connectWebSocket(candidateUrl);
			connectedUrl = candidateUrl;
			break;
		} catch (err) {
			lastError = err;
		}
	}

	if (!connectedWs) {
		const errorMessage = isTestingMode()
			? `Unable to connect to gateway websocket on ports 3000-3010. Last error: ${lastError?.message || "unknown"}`
			: `Unable to connect to gateway websocket at ${gatewayUrl}. Last error: ${lastError?.message || "unknown"}`;
		console.log(
			`Unable to connect to gateway websocket on ports 3000-3010. Last error: ${lastError?.message || "unknown"}`,
		);
		throw new Error(errorMessage);
	}

	console.log(`found gateway at: ${connectedUrl}`);

	const session = {
		id: sessionId,
		ws: connectedWs,
		gatewayUrl: connectedUrl,
		filesSent: false,
		sseClients: new Set(existing?.sseClients || []),
		lastActivity: Date.now(),
	};

	sessions.set(sessionId, session);

	connectedWs.on("open", () => {
		console.log("ws open");
		session.lastActivity = Date.now();
		emitSystem(
			sessionId,
			`Gateway connection established: ${connectedUrl}.`,
		);
	});

	connectedWs.on("message", (msg) => {
		session.lastActivity = Date.now();
		const payload = Buffer.isBuffer(msg)
			? msg.toString("utf8")
			: String(msg);
		broadcast(sessionId, {
			type: "output",
			sessionId,
			payload,
			timestamp: Date.now(),
		});
	});

	connectedWs.on("close", () => {
		emitSystem(sessionId, "Gateway connection closed.");
		session.filesSent = false;
	});

	connectedWs.on("error", (err) => {
		emitSystem(sessionId, `Gateway error: ${err.message}`);
	});

	return session;
};

const initTerminal = async ({ sessionId, gatewayUrl, files }) => {
	console.log("initing...");
	const session = await createSession({ sessionId, gatewayUrl });

	if (!session.filesSent) {
		const fileFrame = buildFilesFrame(files);
		const setupFrame = Buffer.alloc(fileFrame.length + 1);
		setupFrame.writeUInt8(2, 0); // SETUP_MODE
		fileFrame.copy(setupFrame, 1);
		
		// Send files via type: 1 to trigger SETUP_MODE in the ephemeral container
		session.ws.send(JSON.stringify({ type: 1, data: Array.from(setupFrame) }));
		
		setTimeout(() => {
			if (session.ws.readyState === WebSocket.OPEN) {
				// Send TERMINAL_MODE (3) as raw bytes to trigger type: 0
				session.ws.send(Buffer.from([3]));
				
				session.filesSent = true;
				session.lastActivity = Date.now();
				emitSystem(
					sessionId,
					`Sent ${Array.isArray(files) ? files.length : 0} file(s) and started terminal.`,
				);
			}
		}, 300);
	}

	return session;
};

const sendInput = ({ sessionId, input }) => {
	const session = sessions.get(sessionId);
	if (!session || session.ws.readyState !== WebSocket.OPEN) {
		throw new Error(
			"Terminal session is not connected. Initialize session first.",
		);
	}

	const payload = Buffer.from(String(input || ""), "utf8");
	if (payload.length === 0) return;

	session.ws.send(payload);
	session.lastActivity = Date.now();
};

const attachSseClient = ({ sessionId, res }) => {
	let session = sessions.get(sessionId);
	if (!session) {
		session = {
			id: sessionId,
			ws: { readyState: WebSocket.CLOSED },
			filesSent: false,
			sseClients: new Set(),
			lastActivity: Date.now(),
		};
		sessions.set(sessionId, session);
	}

	session.sseClients.add(res);
	session.lastActivity = Date.now();

	broadcast(sessionId, {
		type: "ready",
		sessionId,
		payload: "SSE stream connected.",
		timestamp: Date.now(),
	});

	return () => {
		const current = sessions.get(sessionId);
		if (!current) return;
		current.sseClients.delete(res);
		current.lastActivity = Date.now();
	};
};

const closeSession = ({ sessionId }) => {
	const session = sessions.get(sessionId);
	if (!session) return false;

	if (
		session.ws &&
		(session.ws.readyState === WebSocket.OPEN ||
			session.ws.readyState === WebSocket.CONNECTING)
	) {
		try {
			session.ws.close();
		} catch (err) {}
	}

	for (const res of session.sseClients) {
		try {
			res.end();
		} catch (err) {}
	}

	sessions.delete(sessionId);
	return true;
};

setInterval(() => {
	const now = Date.now();
	for (const [sessionId, session] of sessions.entries()) {
		if (now - session.lastActivity <= SESSION_IDLE_MS) continue;
		closeSession({ sessionId });
	}
}, 60 * 1000).unref();


const sendIdePacket = ({ sessionId, packet }) => {
	const session = sessions.get(sessionId);
	if (!session || session.ws.readyState !== WebSocket.OPEN) {
		throw new Error(
			"Terminal session is not connected. Initialize session first.",
		);
	}
	session.ws.send(JSON.stringify(packet));
	session.lastActivity = Date.now();
};

module.exports = {
	sendIdePacket,
	toSessionId,
	initTerminal,
	sendInput,
	attachSseClient,
	closeSession,
};
