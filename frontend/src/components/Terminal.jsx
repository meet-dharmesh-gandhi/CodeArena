import React, { useEffect, useRef } from "react";
import { Terminal as XTerm } from "xterm";
import { FitAddon } from "xterm-addon-fit";
import "xterm/css/xterm.css";

const Terminal = ({ onInput, outputEvent }) => {
	const terminalRef = useRef(null);
	const xtermRef = useRef(null);
	const fitAddonRef = useRef(null);
	const onInputRef = useRef(onInput);

	useEffect(() => {
		onInputRef.current = onInput;
	}, [onInput]);

	useEffect(() => {
		if (!terminalRef.current) return;

		const term = new XTerm({
			cursorBlink: true,
			theme: {
				background: "#050505",
				foreground: "#ffffff",
				cursor: "#a855f7",
				selectionBackground: "rgba(168, 85, 247, 0.3)",
				black: "#000000",
				red: "#ff5555",
				green: "#50fa7b",
				yellow: "#f1fa8c",
				blue: "#bd93f9",
				magenta: "#ff79c6",
				cyan: "#8be9fd",
				white: "#bfbfbf",
			},
			fontFamily: "'JetBrains Mono', 'Fira Code', monospace",
			fontSize: 13,
			lineHeight: 1.4,
		});

		const fitAddon = new FitAddon();
		term.loadAddon(fitAddon);
		term.open(terminalRef.current);
		fitAddon.fit();

		xtermRef.current = term;
		fitAddonRef.current = fitAddon;

		term.writeln(
			"\x1b[1;35mCodeArena Interactive Linux Environment\x1b[0m",
		);
		term.writeln("Terminal input is forwarded directly to backend.");
		term.writeln("Waiting for backend stream...");
		term.write("\r\n");

		term.onData((data) => {
			if (!onInputRef.current) return;
			if (data === "\r") {
				onInputRef.current("\r\n");
				return;
			}
			onInputRef.current(data);
		});

		const handleResize = () => {
			fitAddon.fit();
		};
		window.addEventListener("resize", handleResize);

		// Initial fit needs a slight delay sometimes to get accurate dimensions
		setTimeout(() => fitAddon.fit(), 100);

		return () => {
			window.removeEventListener("resize", handleResize);
			term.dispose();
		};
	}, []);

	useEffect(() => {
		if (!outputEvent || !xtermRef.current) return;

		const term = xtermRef.current;
		if (outputEvent.type === "output" && outputEvent.payload) {
			term.write(String(outputEvent.payload));
			return;
		}

		if (outputEvent.type === "error" && outputEvent.payload) {
			term.writeln(`\x1b[31m${outputEvent.payload}\x1b[0m`);
			return;
		}

		if (outputEvent.type === "system" && outputEvent.payload) {
			term.writeln(`\x1b[33m${outputEvent.payload}\x1b[0m`);
		}
	}, [outputEvent]);

	return (
		<div className="w-full h-full bg-[#050505] relative">
			<div
				ref={terminalRef}
				className="absolute inset-0 p-4 overflow-hidden"
			/>
		</div>
	);
};

export default Terminal;
