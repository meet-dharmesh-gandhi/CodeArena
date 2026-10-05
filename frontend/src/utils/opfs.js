export const getOPFSDirectory = async (problemId) => {
    const root = await navigator.storage.getDirectory();
    return await root.getDirectoryHandle(`problem_${problemId}`, { create: true });
};

export const saveFileToOPFS = async (problemId, filename, content) => {
    try {
        const dir = await getOPFSDirectory(problemId);
        const fileHandle = await dir.getFileHandle(filename, { create: true });
        const writable = await fileHandle.createWritable();
        await writable.write(content);
        await writable.close();
    } catch (e) {
        console.error("OPFS Save Error:", e);
    }
};

export const loadFilesFromOPFS = async (problemId) => {
    try {
        const dir = await getOPFSDirectory(problemId);
        const files = [];
        for await (const [name, handle] of dir.entries()) {
            if (handle.kind === 'file') {
                const file = await handle.getFile();
                const content = await file.text();
                files.push({ name, content });
            }
        }
        return files;
    } catch (e) {
        console.error("OPFS Load Error:", e);
        return [];
    }
};

export const deleteFileFromOPFS = async (problemId, filename) => {
    try {
        const dir = await getOPFSDirectory(problemId);
        await dir.removeEntry(filename);
    } catch (e) {
        console.error("OPFS Delete Error:", e);
    }
};
