import { createHash } from 'node:crypto';
import { log } from './log.js';
import { openAtomicFile, poolFilePath, recordNegative, writeJsonAtomic, storePaths } from './store.js';

export class UpstreamUnavailableError extends Error {
  name = 'UpstreamUnavailableError';
  constructor(message, { cause, breakerOpen = false, retryAfterSeconds } = {}) {
    super(message, { cause });
    this.breakerOpen = breakerOpen;
    this.retryAfterSeconds = retryAfterSeconds;
  }
}

export class UpstreamNotFoundError extends Error {
  name = 'UpstreamNotFoundError';
}

export class IntegrityError extends Error {
  name = 'IntegrityError';
}

const bodyIdleTimeoutMs = 15_000;
const maxResumeAttempts = 3;
const breakers = new Map();
const inFlight = new Map();
const activeControllers = new Set();
let stopped = false;
let breakerSettings = { threshold: 5, openMs: 30000, maxOpenMs: 300000 };
let persistBreakerStates = false;

export function configureUpstreams(config, { persistBreakers: shouldPersist = false } = {}) {
  breakerSettings = config.breaker;
  persistBreakerStates = shouldPersist;
  persistBreakers();
}

class CircuitBreaker {
  constructor(name) {
    this.name = name;
    this.state = 'closed';
    this.consecutiveFailures = 0;
    this.openMs = breakerSettings.openMs;
    this.openUntil = 0;
    this.probeInFlight = false;
    this.lastFailure = null;
  }

  transition(nextState, reason) {
    if (this.state === nextState) return;
    const previous = this.state;
    this.state = nextState;
    log.warn('breaker transition', { breaker: this.name, from: previous, to: nextState, reason, openMs: nextState === 'open' ? this.openMs : undefined });
    persistBreakers();
  }

  refreshState() {
    if (this.state === 'open' && Date.now() >= this.openUntil) this.transition('half-open', 'open period elapsed');
  }

  isOpen() {
    this.refreshState();
    return this.state === 'open' || (this.state === 'half-open' && this.probeInFlight);
  }

  acquire() {
    this.refreshState();
    if (this.state === 'closed') return { probe: false };
    if (this.state === 'half-open' && !this.probeInFlight) {
      this.probeInFlight = true;
      return { probe: true };
    }
    return null;
  }

  retryAfterSeconds() {
    return Math.max(1, Math.ceil((this.openUntil - Date.now()) / 1000));
  }

  recordSuccess(permit) {
    if (permit.probe) this.probeInFlight = false;
    this.consecutiveFailures = 0;
    if (this.state !== 'closed') {
      this.openMs = breakerSettings.openMs;
      this.transition('closed', permit.probe ? 'probe succeeded' : 'request succeeded');
    }
  }

  recordFailure(permit, error) {
    if (permit.probe) this.probeInFlight = false;
    this.lastFailure = { at: new Date().toISOString(), message: error?.message ?? String(error) };
    if (this.state === 'half-open' && permit.probe) {
      this.openMs = Math.min(this.openMs * 2, breakerSettings.maxOpenMs);
      this.openUntil = Date.now() + this.openMs;
      this.transition('open', `probe failed: ${this.lastFailure.message}`);
      return;
    }
    if (this.state !== 'closed') return;
    this.consecutiveFailures += 1;
    if (this.consecutiveFailures >= breakerSettings.threshold) {
      this.openMs = breakerSettings.openMs;
      this.openUntil = Date.now() + this.openMs;
      this.transition('open', `${this.consecutiveFailures} consecutive failures: ${this.lastFailure.message}`);
    }
  }

  describe() {
    this.refreshState();
    return {
      state: this.state,
      consecutiveFailures: this.consecutiveFailures,
      openUntil: this.state === 'open' ? new Date(this.openUntil).toISOString() : null,
      lastFailure: this.lastFailure,
    };
  }
}

function persistBreakers() {
  if (!persistBreakerStates) return;
  const states = { updatedAt: new Date().toISOString(), pid: process.pid, breakers: describeBreakers() };
  writeJsonAtomic(storePaths.breakers, states).catch((error) => log.warn('could not persist breaker state', { error }));
}

export function breakerFor(name) {
  let breaker = breakers.get(name);
  if (!breaker) {
    breaker = new CircuitBreaker(name);
    breakers.set(name, breaker);
  }
  return breaker;
}

export function describeBreakers() {
  return Object.fromEntries([...breakers.entries()].map(([name, breaker]) => [name, breaker.describe()]));
}

export function singleFlight(key, task) {
  const existing = inFlight.get(key);
  if (existing) return existing;
  const promise = Promise.resolve()
    .then(task)
    .finally(() => inFlight.delete(key));
  inFlight.set(key, promise);
  return promise;
}

export function inFlightPromise(key) {
  return inFlight.get(key) ?? null;
}

export function abortAllUpstreamRequests(reason) {
  stopped = true;
  for (const controller of activeControllers) controller.abort(new Error(reason));
}

export function encodeRepoPath(path) {
  return path.split('/').map(encodeURIComponent).join('/');
}

function describeFetchFailure(error) {
  const root = error?.cause ?? error;
  const code = root?.code ? `${root.code} ` : '';
  return `${code}${root?.message ?? root}`.trim();
}

function isCountedFailureStatus(status) {
  return status >= 500 || status === 429;
}

function isNotFoundStatus(status) {
  return status === 404 || status === 410 || status === 403;
}

export async function upstreamRequest(breakerName, url, { timeoutMs, headers = {} }) {
  if (stopped) throw new UpstreamUnavailableError('upstream requests are stopped because the mirror is shutting down');
  const breaker = breakerFor(breakerName);
  const permit = breaker.acquire();
  if (!permit) {
    throw new UpstreamUnavailableError(`circuit open for upstream ${breakerName}`, {
      breakerOpen: true,
      retryAfterSeconds: breaker.retryAfterSeconds(),
    });
  }
  const controller = new AbortController();
  activeControllers.add(controller);
  const timer = setTimeout(() => controller.abort(new Error(`no response headers within ${timeoutMs}ms`)), timeoutMs);
  let response;
  try {
    response = await fetch(url, {
      headers: { 'accept-encoding': 'identity', 'user-agent': 'apt-mirror/1.0', ...headers },
      signal: controller.signal,
      redirect: 'follow',
    });
  } catch (error) {
    activeControllers.delete(controller);
    const cause = controller.signal.aborted ? controller.signal.reason : error;
    const failure = new UpstreamUnavailableError(`request to ${url} failed: ${describeFetchFailure(cause)}`, { cause });
    breaker.recordFailure(permit, failure);
    throw failure;
  } finally {
    clearTimeout(timer);
  }
  if (isCountedFailureStatus(response.status)) {
    activeControllers.delete(controller);
    await response.body?.cancel().catch(() => {});
    const error = new UpstreamUnavailableError(`upstream answered ${response.status} for ${url}`);
    breaker.recordFailure(permit, error);
    throw error;
  }
  breaker.recordSuccess(permit);
  const release = () => activeControllers.delete(controller);
  return { response, controller, breaker, permit, release };
}

async function consumeBody({ response, controller, breaker, permit, release }, onChunk) {
  const idleTimeoutMs = bodyIdleTimeoutMs;
  const reader = response.body.getReader();
  let idleTimer = null;
  const armIdleTimer = () => {
    clearTimeout(idleTimer);
    idleTimer = setTimeout(() => controller.abort(new Error(`no data received for ${idleTimeoutMs}ms`)), idleTimeoutMs);
  };
  try {
    while (true) {
      let next;
      armIdleTimer();
      try {
        next = await reader.read();
      } catch (error) {
        const cause = controller.signal.aborted ? controller.signal.reason : error;
        breaker.recordFailure(permit, cause);
        throw new UpstreamUnavailableError(`reading body from ${response.url} failed: ${cause?.message ?? cause}`, { cause });
      } finally {
        clearTimeout(idleTimer);
      }
      if (next.done) break;
      await onChunk(Buffer.from(next.value.buffer, next.value.byteOffset, next.value.byteLength));
    }
  } finally {
    clearTimeout(idleTimer);
    release();
    reader.releaseLock();
  }
}

async function discardBody(request) {
  request.release();
  await request.response.body?.cancel().catch(() => {});
}

export async function fetchMetadata(breakerName, url, { timeoutMs, headers = {} }) {
  const request = await upstreamRequest(breakerName, url, { timeoutMs, headers });
  const { response } = request;
  if (response.status === 304) {
    await discardBody(request);
    return { status: 304, headers: response.headers };
  }
  if (isNotFoundStatus(response.status)) {
    await discardBody(request);
    throw new UpstreamNotFoundError(`upstream answered ${response.status} for ${url}`);
  }
  if (response.status !== 200) {
    await discardBody(request);
    throw new UpstreamUnavailableError(`unexpected status ${response.status} for ${url}`);
  }
  const chunks = [];
  await consumeBody(request, async (chunk) => {
    chunks.push(Buffer.from(chunk));
  });
  return { status: 200, headers: response.headers, body: Buffer.concat(chunks) };
}

async function openResumeRequest(breakerName, url, timeoutMs, offset, totalSize) {
  const request = await upstreamRequest(breakerName, url, { timeoutMs, headers: { range: `bytes=${offset}-` } });
  const expectedRange = `bytes ${offset}-${totalSize - 1}/${totalSize}`;
  if (request.response.status !== 206 || request.response.headers.get('content-range') !== expectedRange) {
    await discardBody(request);
    throw new UpstreamUnavailableError(`upstream did not resume ${url} at byte ${offset} (status ${request.response.status})`);
  }
  return request;
}

function isResumable(error) {
  return error instanceof UpstreamUnavailableError && !error.breakerOpen;
}

export async function downloadVerified({ breakerName, url, destination, timeoutMs, expectedSize, expectedSha256, onStart, onChunk }) {
  let request = await upstreamRequest(breakerName, url, { timeoutMs });
  const { response } = request;
  if (isNotFoundStatus(response.status)) {
    await discardBody(request);
    throw new UpstreamNotFoundError(`upstream answered ${response.status} for ${url}`);
  }
  if (response.status !== 200) {
    await discardBody(request);
    throw new UpstreamUnavailableError(`unexpected status ${response.status} for ${url}`);
  }
  const lengthHeader = response.headers.get('content-length');
  const contentLength = lengthHeader === null ? null : Number(lengthHeader);
  if (expectedSize != null && contentLength !== null && contentLength !== expectedSize) {
    await discardBody(request);
    throw new IntegrityError(`size mismatch for ${url}: expected ${expectedSize}, upstream announced ${contentLength}`);
  }
  const announcedSize = expectedSize ?? contentLength;
  const file = await openAtomicFile(destination);
  const hash = createHash('sha256');
  let received = 0;
  const handleChunk = async (chunk) => {
    received += chunk.length;
    if (announcedSize !== null && received > announcedSize) {
      throw new IntegrityError(`received more than ${announcedSize} bytes from ${url}`);
    }
    hash.update(chunk);
    await file.write(chunk);
    if (onChunk) await onChunk(chunk);
  };
  try {
    if (onStart) await onStart({ size: announcedSize });
    for (let resumes = 0; ; resumes += 1) {
      try {
        await consumeBody(request, handleChunk);
        break;
      } catch (error) {
        const canResume = isResumable(error) && announcedSize !== null && received < announcedSize && resumes < maxResumeAttempts;
        if (!canResume) throw error;
        log.warn('resuming interrupted upstream download', { url, received, size: announcedSize, attempt: resumes + 1, reason: error.message });
        await request.response.body?.cancel().catch(() => {});
        request = await openResumeRequest(breakerName, url, timeoutMs, received, announcedSize);
      }
    }
    const sha256 = hash.digest('hex');
    if (announcedSize !== null && received !== announcedSize) {
      throw new IntegrityError(`size mismatch for ${url}: expected ${announcedSize}, received ${received}`);
    }
    if (expectedSha256 && sha256 !== expectedSha256) {
      throw new IntegrityError(`sha256 mismatch for ${url}: expected ${expectedSha256}, received ${sha256}`);
    }
    if (announcedSize === null) log.warn('stored download without a known size', { url, bytes: received });
    await file.commit();
    return { size: received, sha256 };
  } catch (error) {
    await file.abort();
    await request.response.body?.cancel().catch(() => {});
    request.release();
    throw error;
  }
}
export function poolFlightKey(upstreamName, repoPath) {
  return `pool:${upstreamName}/${repoPath}`;
}

export function downloadPoolFile({ config, upstreamName, repoPath, expected, onStart, onChunk }) {
  const key = poolFlightKey(upstreamName, repoPath);
  return singleFlight(key, async () => {
    try {
      return await downloadVerified({
        breakerName: upstreamName,
        url: `${config.upstreams[upstreamName].url}/${encodeRepoPath(repoPath)}`,
        destination: poolFilePath(upstreamName, repoPath),
        timeoutMs: config.timeouts.poolHeadersMs,
        expectedSize: expected?.size ?? null,
        expectedSha256: expected?.sha256 ?? null,
        onStart,
        onChunk,
      });
    } catch (error) {
      if (error instanceof UpstreamNotFoundError) await recordNegative(key, config.negativeSeconds).catch(() => {});
      throw error;
    }
  });
}

export async function mapWithConcurrency(items, limit, worker) {
  const results = new Array(items.length);
  let nextIndex = 0;
  const runners = Array.from({ length: Math.min(limit, items.length) }, async () => {
    while (nextIndex < items.length) {
      const index = nextIndex;
      nextIndex += 1;
      results[index] = await worker(items[index], index);
    }
  });
  await Promise.all(runners);
  return results;
}

export function isNetworkFailure(error) {
  return error instanceof UpstreamUnavailableError;
}
