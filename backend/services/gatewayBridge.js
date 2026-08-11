const WebSocket = require("ws");

const SESSION_IDLE_MS = Number(
	process.env.TERMINAL_SESSION_IDLE_MS || 10 * 60 * 1000,
);

const sessions = new Map();

const toSessionId = ({ studentId, contestId, problemId }) =>
	`${studentId}:${contestId}:${problemId}`;

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
		return existing;
	}

	if (existing && existing.ws.readyState === WebSocket.CONNECTING) {
		return existing;
	}

	const ws = new WebSocket(gatewayUrl);

	const session = {
		id: sessionId,
		ws,
		filesSent: false,
		sseClients: new Set(existing?.sseClients || []),
		lastActivity: Date.now(),
	};

	sessions.set(sessionId, session);

	ws.on("open", () => {
		session.lastActivity = Date.now();
		emitSystem(sessionId, "Gateway connection established.");
	});

	ws.on("message", (msg) => {
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

	ws.on("close", () => {
		emitSystem(sessionId, "Gateway connection closed.");
		session.filesSent = false;
	});

	ws.on("error", (err) => {
		emitSystem(sessionId, `Gateway error: ${err.message}`);
	});

	return new Promise((resolve, reject) => {
		const timeout = setTimeout(() => {
			reject(
				new Error("Timed out while connecting to gateway websocket."),
			);
		}, 8000);

		ws.once("open", () => {
			clearTimeout(timeout);
			resolve(session);
		});

		ws.once("error", (err) => {
			clearTimeout(timeout);
			reject(err);
		});
	});
};

const initTerminal = async ({ sessionId, gatewayUrl, files }) => {
	const session = await createSession({ sessionId, gatewayUrl });

	if (!session.filesSent) {
		const frame = buildFilesFrame(files);
		session.ws.send(frame);
		session.filesSent = true;
		session.lastActivity = Date.now();
		emitSystem(
			sessionId,
			`Sent ${Array.isArray(files) ? files.length : 0} file(s) to gateway.`,
		);
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

module.exports = {
	toSessionId,
	initTerminal,
	sendInput,
	attachSseClient,
	closeSession,
};
