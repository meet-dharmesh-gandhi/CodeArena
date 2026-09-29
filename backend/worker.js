const { Worker } = require('bullmq');
const mongoose = require('mongoose');
const dotenv = require('dotenv');
const WebSocket = require('ws');
const Submission = require('./models/Submission');
const Problem = require('./models/Problem');
const { connection } = require('./queue');

dotenv.config();

// MongoDB Connection
mongoose.connect(process.env.MONGODB_URI)
  .then(() => console.log('Worker connected to MongoDB'))
  .catch(err => console.error('Worker MongoDB error:', err));

const GATEWAY_WS_URL = process.env.GATEWAY_WS_URL || "ws://localhost:3000";

const LANG_CONFIG = {
  c:      { ext: 'c',    compile: 'gcc *.c -o bin -lm',  run: './bin' },
  cpp:    { ext: 'cpp',  compile: 'g++ *.cpp -o bin',    run: './bin' },
  java:   { ext: 'java', compile: 'javac *.java',        run: 'java Main' },
  python: { ext: 'py',   compile: null,                  run: 'python3 main.py' },
  bash:   { ext: 'sh',   compile: null,                  run: 'bash main.sh' },
  shell:  { ext: 'sh',   compile: null,                  run: 'bash main.sh' },
};

const buildFilesFrame = (files) => {
	const normalizedFiles = Array.isArray(files) ? files : [];
	const encodedFiles = normalizedFiles.map((f) => {
		const nameBuf = Buffer.from(String(f?.name || "main.txt"), "utf8");
		const contentBuf = Buffer.from(String(f?.content || ""), "utf8");
		return { nameBuf, contentBuf };
	});

	let totalSize = 1;
	for (const f of encodedFiles) totalSize += 1 + f.nameBuf.length + 4 + f.contentBuf.length;

	const frame = Buffer.alloc(totalSize);
	let offset = 0;
	frame.writeUInt8(encodedFiles.length, offset); offset += 1;

	for (const f of encodedFiles) {
		frame.writeUInt8(f.nameBuf.length, offset); offset += 1;
		f.nameBuf.copy(frame, offset); offset += f.nameBuf.length;
		frame.writeUInt32LE(f.contentBuf.length, offset); offset += 4;
		f.contentBuf.copy(frame, offset); offset += f.contentBuf.length;
	}
	return frame;
};

const executeCodeArena = (files, testCases, language, isSubmit) => {
  return new Promise((resolve, reject) => {
    const ws = new WebSocket(GATEWAY_WS_URL);
    let resolved = false;

    const cleanup = () => {
        if (ws.readyState === WebSocket.OPEN) ws.close();
    };

    ws.on('open', () => {
        console.log("Connected to Gateway for execution");
        // 1. Send SETUP_MODE
        const fileFrame = buildFilesFrame(files);
        const setupFrame = Buffer.alloc(fileFrame.length + 1);
        setupFrame.writeUInt8(2, 0); // SETUP_MODE (2)
        fileFrame.copy(setupFrame, 1);
        
        ws.send(JSON.stringify({ type: 1, data: Array.from(setupFrame) }));

        // 2. Send TEST_MODE (0) or SUBMIT_MODE (1)
        setTimeout(() => {
            const cfg = LANG_CONFIG[language] || LANG_CONFIG['python'];
            const cmd = cfg.compile ? `${cfg.compile} && ${cfg.run}` : cfg.run;
            const cmdBuf = Buffer.from(cmd, 'utf8');

            // Calculate total size for execution packet
            let totalSize = 1 + 1 + 1 + 1 + 2 + cmdBuf.length + 2; // mode, time_limit, memory_limit, mem_unit, cmd_size, command, num_inputs
            for (const tc of testCases) {
                const inputBuf = Buffer.from(tc.input || '', 'utf8');
                totalSize += 2 + inputBuf.length; // 2 bytes length, input data
                if (isSubmit) {
                    const outputBuf = Buffer.from(tc.output || '', 'utf8');
                    totalSize += 2 + outputBuf.length; // 2 bytes length, output data
                }
            }

            const execFrame = Buffer.alloc(totalSize);
            let offset = 0;
            execFrame.writeUInt8(isSubmit ? 1 : 0, offset); offset += 1; // Mode
            execFrame.writeUInt8(10, offset); offset += 1; // 10s Time Limit
            execFrame.writeUInt8(250, offset); offset += 1; // 250MB Memory Limit
            execFrame.writeUInt8(0, offset); offset += 1; // Memory unit: MB (0)
            
            execFrame.writeUInt16LE(cmdBuf.length, offset); offset += 2;
            cmdBuf.copy(execFrame, offset); offset += cmdBuf.length;

            execFrame.writeUInt16LE(testCases.length, offset); offset += 2;

            for (const tc of testCases) {
                const inputBuf = Buffer.from(tc.input || '', 'utf8');
                execFrame.writeUInt16LE(inputBuf.length, offset); offset += 2;
                inputBuf.copy(execFrame, offset); offset += inputBuf.length;

                if (isSubmit) {
                    const outputBuf = Buffer.from(tc.output || '', 'utf8');
                    execFrame.writeUInt16LE(outputBuf.length, offset); offset += 2;
                    outputBuf.copy(execFrame, offset); offset += outputBuf.length;
                }
            }

            ws.send(JSON.stringify({ type: 1, data: Array.from(execFrame) }));
        }, 500); // Give setup a tiny moment to complete writing files
    });

    let rawBuffer = Buffer.alloc(0);

    ws.on('message', (msg) => {
        // Output from container.c
        const chunk = Buffer.isBuffer(msg) ? msg : Buffer.from(msg, 'utf8');
        rawBuffer = Buffer.concat([rawBuffer, chunk]);

        // Attempt to parse results based on mode
        try {
            if (!isSubmit) {
                // TEST_MODE
                if (rawBuffer.length >= 1) {
                    const numOutputs = rawBuffer.readUInt8(0);
                    let offset = 1;
                    const results = [];
                    for (let i = 0; i < numOutputs; i++) {
                        if (rawBuffer.length < offset + 1 + 1 + 2 + 1 + 2) return; // Wait for more data
                        const time_sec = rawBuffer.readUInt8(offset); offset += 1;
                        const time_ms = rawBuffer.readUInt8(offset); offset += 1;
                        const mem = rawBuffer.readUInt16LE(offset); offset += 2;
                        const mem_unit = rawBuffer.readUInt8(offset); offset += 1;
                        const logSize = rawBuffer.readUInt16LE(offset); offset += 2;

                        if (rawBuffer.length < offset + logSize) return; // Wait for more data
                        const logData = rawBuffer.subarray(offset, offset + logSize).toString('utf8');
                        offset += logSize;

                        results.push({
                            executionTime: (time_sec * 1000) + time_ms,
                            memoryUsed: mem,
                            output: logData
                        });
                    }
                    if (!resolved) { resolved = true; resolve(results); cleanup(); }
                }
            } else {
                // SUBMIT_MODE
                if (rawBuffer.length >= 1) {
                    const numOutputs = rawBuffer.readUInt8(0);
                    let offset = 1;
                    const results = [];
                    for (let i = 0; i < numOutputs; i++) {
                        if (rawBuffer.length < offset + 1 + 1 + 2 + 1 + 1) return; // Wait for more data
                        const time_sec = rawBuffer.readUInt8(offset); offset += 1;
                        const time_ms = rawBuffer.readUInt8(offset); offset += 1;
                        const mem = rawBuffer.readUInt16LE(offset); offset += 2;
                        const mem_unit = rawBuffer.readUInt8(offset); offset += 1;
                        const passed = rawBuffer.readUInt8(offset); offset += 1;

                        results.push({
                            executionTime: (time_sec * 1000) + time_ms,
                            memoryUsed: mem,
                            passed: passed !== 0
                        });
                    }
                    if (!resolved) { resolved = true; resolve(results); cleanup(); }
                }
            }
        } catch (e) {
            console.error("Error parsing binary output", e);
        }
    });

    ws.on('error', (err) => {
        if (!resolved) { resolved = true; reject(err); cleanup(); }
    });
    ws.on('close', () => {
        if (!resolved) { resolved = true; reject(new Error("Connection closed before completion")); }
    });
  });
};

const worker = new Worker('submission-queue', async (job) => {
  const { submissionId, problemId, files, code, mainFile, language, customInput, type, command } = job.data;
  
  const normalizedFiles = files || [{ name: `main.${LANG_CONFIG[language]?.ext || 'txt'}`, content: code }];
  const jobId = job.id;
  console.log(`Processing ${type} for job ${jobId}`);

  try {
    const problem = await Problem.findById(problemId);
    if (!problem) throw new Error('Problem not found');

    if (type === 'run' || type === 'terminal') {
        // TEST MODE
        let testCases = [];
        if (customInput) {
            testCases = [{ input: customInput }];
        } else {
            const sampleCases = problem.testCases.filter(tc => tc.isSample);
            testCases = sampleCases.length > 0 ? sampleCases : [{ input: '' }];
        }

        const results = await executeCodeArena(normalizedFiles, testCases, language, false);

        if (type === 'terminal') return { results };

        let finalStatus = 'Accepted';
        let totalExecutionTime = 0;
        let maxMemory = 0;
        
        const sampleCases = problem.testCases.filter(tc => tc.isSample);
        const mappedResults = testCases.map((tc, idx) => {
            const res = results[idx] || { executionTime: 0, memoryUsed: 0, output: '' };
            totalExecutionTime += res.executionTime;
            maxMemory = Math.max(maxMemory, res.memoryUsed);

            let passed = false;
            let status = 'Accepted';
            if (res.output.includes('Compilation Error')) { // Basic check, ideally from stderr but container groups them
                status = 'Compilation Error';
            } else if (res.output.includes('Runtime Error')) {
                status = 'Runtime Error';
            } else {
                if (sampleCases[idx]) {
                    passed = res.output.trim() === sampleCases[idx].output.trim();
                    status = passed ? 'Accepted' : 'Wrong Answer';
                } else {
                    passed = true;
                }
            }

            if (!passed && finalStatus === 'Accepted') finalStatus = status;

            return {
                input: tc.input,
                expectedOutput: sampleCases[idx]?.output || '',
                actualOutput: res.output,
                passed,
                status
            };
        });

        const totalPassed = mappedResults.filter(r => r.passed).length;
        const totalTestCases = mappedResults.length;
        const earnedPoints = totalTestCases > 0 ? (totalPassed / totalTestCases) * (problem.points || 100) : 0;

        const detailedResults = {
            sampleCases: mappedResults,
            status: finalStatus,
            points: earnedPoints,
            totalPassed,
            totalTestCases,
            executionTime: totalExecutionTime,
            memoryUsed: maxMemory,
            outputFile: mappedResults[0]?.actualOutput // For backwards compatibility
        };

        if (submissionId) {
            await Submission.findByIdAndUpdate(submissionId, {
                status: finalStatus,
                results: detailedResults,
                points: earnedPoints,
                executionTime: totalExecutionTime,
                memoryUsed: maxMemory
            });
        }

        return { results: detailedResults };
    } else {
        // SUBMIT MODE
        const allTestCases = problem.testCases;
        const sampleCases = allTestCases.filter(tc => tc.isSample);
        const hiddenCases = allTestCases.filter(tc => !tc.isSample);

        const results = await executeCodeArena(normalizedFiles, allTestCases, language, true);

        let finalStatus = 'Accepted';
        let totalExecutionTime = 0;
        let maxMemory = 0;
        let totalPassed = 0;

        const sampleResults = [];
        const hiddenResults = [];

        allTestCases.forEach((tc, idx) => {
            const res = results[idx] || { executionTime: 0, memoryUsed: 0, passed: false };
            totalExecutionTime += res.executionTime;
            maxMemory = Math.max(maxMemory, res.memoryUsed);
            
            if (res.passed) totalPassed++;
            else if (finalStatus === 'Accepted') finalStatus = 'Wrong Answer';

            const outputObj = {
                input: tc.input,
                expectedOutput: tc.output,
                passed: res.passed,
                status: res.passed ? 'Accepted' : 'Wrong Answer'
            };

            if (tc.isSample) sampleResults.push(outputObj);
            else hiddenResults.push({ passed: res.passed, status: outputObj.status });
        });

        const earnedPoints = allTestCases.length > 0 ? (totalPassed / allTestCases.length) * (problem.points || 100) : 0;

        const detailedResults = {
            sampleCases: sampleResults,
            hidden: { passed: hiddenResults.filter(r => r.passed).length, total: hiddenCases.length, results: hiddenResults },
            points: earnedPoints,
            totalTestCases: allTestCases.length,
            totalPassed,
            executionTime: totalExecutionTime,
            memoryUsed: maxMemory,
            status: finalStatus
        };

        await Submission.findByIdAndUpdate(submissionId, {
            status: finalStatus,
            results: detailedResults,
            points: earnedPoints,
            files: normalizedFiles,
            executionTime: totalExecutionTime,
            memoryUsed: maxMemory
        });

        return { submissionId, results: detailedResults };
    }
  } catch (error) {
    console.error(`Error processing job ${job.id}:`, error);
    if (submissionId) {
        await Submission.findByIdAndUpdate(submissionId, { status: 'Judge Error' });
    }
    throw error;
  }
}, { connection });

worker.on('completed', job => console.log(`Job ${job.id} completed`));
worker.on('failed', (job, err) => console.error(`Job ${job.id} failed:`, err));

console.log('Worker is running...');
