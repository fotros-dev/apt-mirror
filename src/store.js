import { open, rename, unlink, mkdir, stat, readFile, readdir, rm, utimes } from 'node:fs/promises';
import { statSync, readFileSync } from 'node:fs';
import { createHash, randomUUID } from 'node:crypto';
import { dirname, join, resolve, sep } from 'node:path';
import { dataDir } from './config.js';

export const cacheDir = join(dataDir, 'cache');
export const storePaths = {
  snapshots: join(cacheDir, 'snapshots'),
  blobs: join(cacheDir, 'blobs', 'sha256'),
  pool: join(cacheDir, 'pool'),
  keys: join(cacheDir, 'keys'),
  negative: join(cacheDir, 'negative'),
  tmp: join(cacheDir, 'tmp'),
  known: join(cacheDir, 'known.json'),
  breakers: join(cacheDir, 'breakers.json'),
};

const activeTempFiles = new Set();

export function insideDataDir(path) {
  const resolved = resolve(path);
  if (resolved !== dataDir && !resolved.startsWith(dataDir + sep)) {
    throw new Error(`refusing to touch a path outside the data directory: ${resolved}`);
  }
  return resolved;
}

export async function ensureStoreLayout() {
  for (const directory of [storePaths.snapshots, storePaths.blobs, storePaths.pool, storePaths.keys, storePaths.negative, storePaths.tmp]) {
    await mkdir(directory, { recursive: true });
  }
}

export async function removeLeftoverTempFiles() {
  const names = await readdir(storePaths.tmp).catch(() => []);
  for (const name of names) await rm(join(storePaths.tmp, name), { force: true, recursive: true });
  return names.length;
}

async function writeFully(handle, chunk) {
  let offset = 0;
  while (offset < chunk.length) {
    const { bytesWritten } = await handle.write(chunk, offset, chunk.length - offset);
    offset += bytesWritten;
  }
}

export async function openAtomicFile(finalPath) {
  const target = insideDataDir(finalPath);
  const tempPath = join(storePaths.tmp, randomUUID());
  const handle = await open(tempPath, 'wx');
  activeTempFiles.add(tempPath);
  let closed = false;
  const closeHandle = async () => {
    if (closed) return;
    closed = true;
    await handle.close();
  };
  return {
    tempPath,
    async write(chunk) {
      await writeFully(handle, typeof chunk === 'string' ? Buffer.from(chunk) : chunk);
    },
    async commit() {
      await handle.sync();
      await closeHandle();
      await mkdir(dirname(target), { recursive: true });
      await rename(tempPath, target);
      activeTempFiles.delete(tempPath);
    },
    async abort() {
      await closeHandle().catch(() => {});
      await unlink(tempPath).catch(() => {});
      activeTempFiles.delete(tempPath);
    },
  };
}

export async function writeFileAtomic(finalPath, data) {
  const file = await openAtomicFile(finalPath);
  try {
    await file.write(data);
    await file.commit();
  } catch (error) {
    await file.abort();
    throw error;
  }
}

export async function writeJsonAtomic(finalPath, value) {
  await writeFileAtomic(finalPath, `${JSON.stringify(value, null, 1)}\n`);
}

export async function removeActiveTempFiles() {
  for (const tempPath of activeTempFiles) await unlink(tempPath).catch(() => {});
  activeTempFiles.clear();
}

export async function readJson(path) {
  try {
    return JSON.parse(await readFile(path, 'utf8'));
  } catch (error) {
    if (error.code === 'ENOENT') return null;
    throw error;
  }
}

const jsonCache = new Map();

export function readJsonCached(path) {
  let stats;
  try {
    stats = statSync(path);
  } catch (error) {
    if (error.code === 'ENOENT') {
      jsonCache.delete(path);
      return null;
    }
    throw error;
  }
  const cached = jsonCache.get(path);
  if (cached && cached.mtimeMs === stats.mtimeMs && cached.size === stats.size) return cached.value;
  const value = JSON.parse(readFileSync(path, 'utf8'));
  jsonCache.set(path, { mtimeMs: stats.mtimeMs, size: stats.size, value });
  return value;
}

export function forgetCachedJson(path) {
  jsonCache.delete(path);
}

export function sha256Hex(data) {
  return createHash('sha256').update(data).digest('hex');
}

export function blobPath(sha256) {
  if (!/^[0-9a-f]{64}$/.test(sha256)) throw new Error(`invalid sha256: ${sha256}`);
  return join(storePaths.blobs, sha256.slice(0, 2), sha256);
}

export async function fileStats(path) {
  try {
    return await stat(path);
  } catch (error) {
    if (error.code === 'ENOENT') return null;
    throw error;
  }
}

export async function hasBlob(sha256) {
  return (await fileStats(blobPath(sha256))) !== null;
}

export async function writeBlob(data) {
  const sha256 = sha256Hex(data);
  if (!(await hasBlob(sha256))) await writeFileAtomic(blobPath(sha256), data);
  return sha256;
}

export function poolFilePath(upstreamName, repoPath) {
  const relative = repoPath.startsWith('pool/') ? repoPath.slice('pool/'.length) : repoPath;
  return insideDataDir(join(storePaths.pool, upstreamName, relative));
}

export function keyFilePath(keyName) {
  return insideDataDir(join(storePaths.keys, keyName));
}

export function snapshotDirectory(upstreamName, suite) {
  return insideDataDir(join(storePaths.snapshots, upstreamName, suite));
}

function negativeEntryPath(cacheKey) {
  return join(storePaths.negative, sha256Hex(cacheKey));
}

export async function recordNegative(cacheKey, seconds) {
  await writeJsonAtomic(negativeEntryPath(cacheKey), { key: cacheKey, expiresAt: Date.now() + seconds * 1000 });
}

export async function hasActiveNegative(cacheKey) {
  const entry = await readJson(negativeEntryPath(cacheKey)).catch(() => null);
  return Boolean(entry && entry.key === cacheKey && entry.expiresAt > Date.now());
}

export async function touchIfOlderThan(path, stats, maxAgeMs) {
  if (Date.now() - stats.mtimeMs < maxAgeMs) return;
  const now = new Date();
  await utimes(path, now, now).catch(() => {});
}

export async function* walkFiles(directory) {
  let entries;
  try {
    entries = await readdir(directory, { withFileTypes: true });
  } catch (error) {
    if (error.code === 'ENOENT') return;
    throw error;
  }
  for (const entry of entries) {
    const path = join(directory, entry.name);
    if (entry.isDirectory()) yield* walkFiles(path);
    else if (entry.isFile()) yield path;
  }
}

export async function directorySize(directory) {
  let total = 0;
  let count = 0;
  for await (const path of walkFiles(directory)) {
    const stats = await fileStats(path);
    if (stats) {
      total += stats.size;
      count += 1;
    }
  }
  return { bytes: total, files: count };
}

export function createKnownPackages() {
  const namesByArchitecture = new Map();
  let dirty = false;

  const mergeFromDisk = async () => {
    const onDisk = (await readJson(storePaths.known)) ?? {};
    for (const [architecture, names] of Object.entries(onDisk)) {
      if (!Array.isArray(names)) continue;
      let set = namesByArchitecture.get(architecture);
      if (!set) {
        set = new Set();
        namesByArchitecture.set(architecture, set);
      }
      for (const name of names) set.add(name);
    }
  };

  return {
    reload: mergeFromDisk,
    add(name, architecture) {
      let set = namesByArchitecture.get(architecture);
      if (!set) {
        set = new Set();
        namesByArchitecture.set(architecture, set);
      }
      if (set.has(name)) return;
      set.add(name);
      dirty = true;
    },
    has(name, architecture) {
      return Boolean(namesByArchitecture.get(architecture)?.has(name) || namesByArchitecture.get('all')?.has(name));
    },
    namesFor(architecture) {
      return new Set([...(namesByArchitecture.get(architecture) ?? []), ...(namesByArchitecture.get('all') ?? [])]);
    },
    count() {
      let total = 0;
      for (const set of namesByArchitecture.values()) total += set.size;
      return total;
    },
    async flush() {
      if (!dirty) return;
      dirty = false;
      await mergeFromDisk();
      const serialized = {};
      for (const [architecture, set] of [...namesByArchitecture.entries()].sort(([a], [b]) => a.localeCompare(b))) {
        serialized[architecture] = [...set].sort();
      }
      try {
        await writeJsonAtomic(storePaths.known, serialized);
      } catch (error) {
        dirty = true;
        throw error;
      }
    },
  };
}
