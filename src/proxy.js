import { createReadStream } from 'node:fs';
import { pipeline } from 'node:stream/promises';
import { posix } from 'node:path';
import { log } from './log.js';
import { decidePoolAction, parseRangeHeader, isNotModified, parsePackageFileName } from './policy.js';
import { releaseHashNames } from './deb.js';
import {
  blobPath,
  fileStats,
  poolFilePath,
  keyFilePath,
  hasActiveNegative,
  touchIfOlderThan,
  writeFileAtomic,
} from './store.js';
import {
  breakerFor,
  downloadPoolFile,
  fetchMetadata,
  inFlightPromise,
  poolFlightKey,
  singleFlight,
  UpstreamNotFoundError,
  IntegrityError,
} from './upstream.js';
import {
  currentSnapshot,
  keptManifests,
  fetchIndexFile,
  lookupPoolHash,
  refreshInBackgroundIfStale,
  snapshotLastModifiedMs,
} from './snapshot.js';

const dayMs = 24 * 3600 * 1000;
const debContentType = 'application/vnd.debian.binary-package';
const octetStream = 'application/octet-stream';
const defaultRetryAfterSeconds = 30;

export function sendEmpty(ctx, status, headers = {}) {
  ctx.res.writeHead(status, { ...headers, 'content-length': 0 });
  ctx.res.end();
}

export function sendJson(ctx, status, value) {
  const body = Buffer.from(JSON.stringify(value));
  ctx.res.writeHead(status, { 'content-type': 'application/json', 'content-length': body.length });
  if (ctx.req.method === 'HEAD') return ctx.res.end();
  ctx.bytes += body.length;
  ctx.res.end(body);
}

function sendUnavailable(ctx, retryAfterSeconds) {
  ctx.cache = ctx.cache ?? 'MISS';
  sendEmpty(ctx, 503, { 'retry-after': String(retryAfterSeconds ?? defaultRetryAfterSeconds) });
}

function sendNotModified(ctx, lastModifiedMs, cache) {
  ctx.cache = cache;
  ctx.res.writeHead(304, { 'last-modified': new Date(lastModifiedMs).toUTCString(), 'x-cache': cache });
  ctx.res.end();
}

function isClientDisconnect(error) {
  return error?.code === 'ERR_STREAM_PREMATURE_CLOSE' || error?.code === 'ECONNRESET' || error?.code === 'EPIPE';
}

async function sendStoredFile(ctx, path, stats, { cache, contentType = octetStream, lastModifiedMs = null, immutable = false }) {
  const { req, res } = ctx;
  ctx.cache = cache;
  const size = stats.size;
  const headers = { 'content-type': contentType, 'x-cache': cache, 'accept-ranges': 'bytes' };
  if (lastModifiedMs !== null) headers['last-modified'] = new Date(lastModifiedMs).toUTCString();
  if (immutable) headers['cache-control'] = 'public, max-age=31536000, immutable';
  const range = parseRangeHeader(req.headers.range, size);
  if (range?.unsatisfiable) {
    res.writeHead(416, { ...headers, 'content-range': `bytes */${size}`, 'content-length': 0 });
    res.end();
    return;
  }
  let start = 0;
  let end = size - 1;
  let status = 200;
  if (range) {
    status = 206;
    start = range.start;
    end = range.end;
    headers['content-range'] = `bytes ${start}-${end}/${size}`;
  }
  headers['content-length'] = size === 0 ? 0 : end - start + 1;
  res.writeHead(status, headers);
  if (req.method === 'HEAD' || size === 0) {
    res.end();
    return;
  }
  const stream = createReadStream(path, { start, end });
  stream.on('data', (chunk) => {
    ctx.bytes += chunk.length;
  });
  try {
    await pipeline(stream, res);
  } catch (error) {
    if (!isClientDisconnect(error)) throw error;
  }
}

function snapshotCacheState(snapshot) {
  return snapshot.pointer.lastAttemptFailed ? 'STALE' : 'HIT';
}

function requireSnapshot(context, ctx, route) {
  refreshInBackgroundIfStale(context, route.upstream, route.suite);
  const snapshot = currentSnapshot(route.upstream, route.suite);
  if (!snapshot) sendUnavailable(ctx, defaultRetryAfterSeconds);
  return snapshot;
}

async function serveRelease(context, ctx, route) {
  const snapshot = requireSnapshot(context, ctx, route);
  if (!snapshot) return;
  const sha256 = snapshot.manifest.release[route.fileName];
  if (!sha256) return sendEmpty(ctx, 404);
  const path = blobPath(sha256);
  const stats = await fileStats(path);
  if (!stats) {
    log.error('blob of the live snapshot is missing', { upstream: route.upstream, suite: route.suite, file: route.fileName, sha256 });
    return sendUnavailable(ctx, defaultRetryAfterSeconds);
  }
  const cache = snapshotCacheState(snapshot);
  const lastModifiedMs = snapshotLastModifiedMs(snapshot.manifest);
  if (isNotModified(ctx.req.headers['if-modified-since'], lastModifiedMs)) return sendNotModified(ctx, lastModifiedMs, cache);
  await sendStoredFile(ctx, path, stats, { cache, lastModifiedMs });
}

function sendUpstreamFailure(ctx, error) {
  if (error instanceof UpstreamNotFoundError) return sendEmpty(ctx, 404);
  if (error instanceof IntegrityError) {
    log.error('upstream content failed verification', { reqId: ctx.reqId, error });
    return sendEmpty(ctx, 502);
  }
  return sendUnavailable(ctx, error.retryAfterSeconds);
}

async function serveSnapshotFile(context, ctx, manifest, path, { cache, immutable }) {
  const file = manifest.files[path];
  const blob = blobPath(file.sha256);
  let stats = await fileStats(blob);
  let cacheState = cache;
  if (!stats) {
    try {
      await fetchIndexFile(context.config, manifest, path);
    } catch (error) {
      return sendUpstreamFailure(ctx, error);
    }
    stats = await fileStats(blob);
    cacheState = 'MISS';
  }
  const lastModifiedMs = snapshotLastModifiedMs(manifest);
  if (!immutable && isNotModified(ctx.req.headers['if-modified-since'], lastModifiedMs)) {
    return sendNotModified(ctx, lastModifiedMs, cacheState);
  }
  await sendStoredFile(ctx, blob, stats, { cache: cacheState, lastModifiedMs, immutable });
}

async function serveIndex(context, ctx, route) {
  const snapshot = requireSnapshot(context, ctx, route);
  if (!snapshot) return;
  const file = snapshot.manifest.files[route.indexPath];
  if (!file || file.absent) return sendEmpty(ctx, 404);
  await serveSnapshotFile(context, ctx, snapshot.manifest, route.indexPath, { cache: snapshotCacheState(snapshot), immutable: false });
}

const byHashLookups = new WeakMap();

function byHashLookupFor(manifest) {
  let lookup = byHashLookups.get(manifest);
  if (lookup) return lookup;
  lookup = new Map();
  for (const [path, file] of Object.entries(manifest.files)) {
    const directory = posix.dirname(path);
    for (const [algorithm, field] of Object.entries(releaseHashNames)) {
      if (file[field]) lookup.set(`${directory}|${algorithm}|${file[field]}`, path);
    }
  }
  byHashLookups.set(manifest, lookup);
  return lookup;
}

async function serveByHash(context, ctx, route) {
  refreshInBackgroundIfStale(context, route.upstream, route.suite);
  const lookupKey = `${route.directory === '' ? '.' : route.directory}|${route.algorithm}|${route.hash}`;
  for (const manifest of keptManifests(route.upstream, route.suite)) {
    const path = byHashLookupFor(manifest).get(lookupKey);
    if (path) return serveSnapshotFile(context, ctx, manifest, path, { cache: 'HIT', immutable: true });
  }
  if (route.algorithm === 'SHA256') {
    const blob = blobPath(route.hash);
    const stats = await fileStats(blob);
    if (stats) return sendStoredFile(ctx, blob, stats, { cache: 'HIT', immutable: true });
  }
  sendEmpty(ctx, 404);
}

function rememberPackage(context, fileName) {
  const parsed = parsePackageFileName(fileName);
  if (parsed) context.known.add(parsed.name, parsed.architecture);
}

function waitForDrainOrClose(res) {
  return new Promise((resolve) => {
    const done = () => {
      res.off('drain', done);
      res.off('close', done);
      resolve();
    };
    res.once('drain', done);
    res.once('close', done);
  });
}

async function fetchAndStreamPool(context, ctx, route, path) {
  const { req, res } = ctx;
  const expected = lookupPoolHash(route.upstream, route.repoPath);
  const streamToClient = req.method === 'GET';
  let streaming = false;
  let heldChunk = null;
  const writeToClient = async (chunk) => {
    if (res.destroyed || res.writableEnded) return;
    ctx.bytes += chunk.length;
    if (!res.write(chunk)) await waitForDrainOrClose(res);
  };
  try {
    await downloadPoolFile({
      config: context.config,
      upstreamName: route.upstream,
      repoPath: route.repoPath,
      expected,
      onStart: ({ size }) => {
        if (!streamToClient || size === null) return;
        streaming = true;
        ctx.cache = 'MISS';
        res.writeHead(200, { 'content-type': debContentType, 'content-length': size, 'x-cache': 'MISS', 'accept-ranges': 'bytes' });
      },
      onChunk: async (chunk) => {
        if (!streaming) return;
        const previous = heldChunk;
        heldChunk = chunk;
        if (previous) await writeToClient(previous);
      },
    });
  } catch (error) {
    if (streaming) {
      log.warn('pool download failed mid-stream', { reqId: ctx.reqId, path: route.repoPath, error });
      res.destroy(error);
      return;
    }
    return sendUpstreamFailure(ctx, error);
  }
  rememberPackage(context, route.fileName);
  if (streaming) {
    if (heldChunk) await writeToClient(heldChunk);
    res.end();
    return;
  }
  const stats = await fileStats(path);
  if (!stats) return sendUnavailable(ctx, defaultRetryAfterSeconds);
  await sendStoredFile(ctx, path, stats, { cache: 'MISS', contentType: debContentType });
}

async function servePool(context, ctx, route) {
  const path = poolFilePath(route.upstream, route.repoPath);
  const flightKey = poolFlightKey(route.upstream, route.repoPath);
  let stats = await fileStats(path);
  if (!stats) {
    const running = inFlightPromise(flightKey);
    if (running) {
      const joinedError = await running.then(
        () => null,
        (error) => error,
      );
      stats = await fileStats(path);
      if (!stats && joinedError) return sendUpstreamFailure(ctx, joinedError);
    }
  }
  const breaker = breakerFor(route.upstream);
  const action = decidePoolAction({
    cached: stats !== null,
    negativeActive: stats === null && (await hasActiveNegative(flightKey)),
    breakerOpen: breaker.isOpen(),
  });
  if (action === 'serve') {
    rememberPackage(context, route.fileName);
    await touchIfOlderThan(path, stats, dayMs);
    return sendStoredFile(ctx, path, stats, { cache: 'HIT', contentType: debContentType, immutable: true });
  }
  if (action === 'not-found') return sendEmpty(ctx, 404);
  if (action === 'unavailable') return sendUnavailable(ctx, breaker.retryAfterSeconds());
  await fetchAndStreamPool(context, ctx, route, path);
}

export function refreshKey(context, keyName) {
  return singleFlight(`key:${keyName}`, async () => {
    const fetched = await fetchMetadata(`key:${keyName}`, context.config.keys[keyName], {
      timeoutMs: context.config.timeouts.metadataHeadersMs,
    });
    await writeFileAtomic(keyFilePath(keyName), fetched.body);
    log.info('key refreshed', { key: keyName, bytes: fetched.body.length });
  });
}

export async function refreshStaleKeys(context) {
  for (const keyName of Object.keys(context.config.keys)) {
    const stats = await fileStats(keyFilePath(keyName));
    if (stats && Date.now() - stats.mtimeMs < dayMs) continue;
    await refreshKey(context, keyName).catch((error) => log.warn('key refresh failed', { key: keyName, error }));
  }
}

async function serveKey(context, ctx, route) {
  const path = keyFilePath(route.keyName);
  let stats = await fileStats(path);
  let cache = 'HIT';
  if (stats && Date.now() - stats.mtimeMs >= dayMs) {
    cache = 'STALE';
    refreshKey(context, route.keyName).catch((error) => log.warn('key refresh failed', { key: route.keyName, error }));
  }
  if (!stats) {
    try {
      await refreshKey(context, route.keyName);
    } catch (error) {
      if (error instanceof UpstreamNotFoundError) return sendEmpty(ctx, 502);
      return sendUpstreamFailure(ctx, error);
    }
    stats = await fileStats(path);
    cache = 'MISS';
  }
  await sendStoredFile(ctx, path, stats, { cache, contentType: 'application/pgp-keys', lastModifiedMs: stats.mtimeMs });
}

export async function serveRoute(context, ctx, route) {
  if (route.kind === 'release') return serveRelease(context, ctx, route);
  if (route.kind === 'index') return serveIndex(context, ctx, route);
  if (route.kind === 'by-hash') return serveByHash(context, ctx, route);
  if (route.kind === 'pool') return servePool(context, ctx, route);
  if (route.kind === 'key') return serveKey(context, ctx, route);
  if (route.kind === 'ping') return sendJson(ctx, 200, {});
  return sendEmpty(ctx, 404);
}
