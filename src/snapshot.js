import { createReadStream, existsSync } from 'node:fs';
import { readdir, unlink } from 'node:fs/promises';
import { join, posix } from 'node:path';
import { log } from './log.js';
import {
  parseRelease,
  isSelectedIndexFile,
  packagesIndexCandidates,
  parsePackagesStream,
  createUniverse,
  addToUniverse,
  computeClosure,
} from './deb.js';
import { familyOfSuite, suiteJoinsFamily } from './policy.js';
import {
  blobPath,
  hasBlob,
  writeBlob,
  writeJsonAtomic,
  readJsonCached,
  snapshotDirectory,
  sha256Hex,
  fileStats,
  poolFilePath,
} from './store.js';
import {
  fetchMetadata,
  downloadVerified,
  downloadPoolFile,
  singleFlight,
  inFlightPromise,
  mapWithConcurrency,
  encodeRepoPath,
  isNetworkFailure,
  UpstreamNotFoundError,
} from './upstream.js';

const keptSnapshotCount = 3;
const downloadConcurrency = 8;
const suiteRefreshConcurrency = 4;
const validUntilWarningMs = 48 * 3600 * 1000;

class ReleaseMidUpdateError extends Error {
  name = 'ReleaseMidUpdateError';
}

export function pointerPath(upstreamName, suite) {
  return join(snapshotDirectory(upstreamName, suite), 'current.json');
}

export function manifestPath(upstreamName, suite, id) {
  return join(snapshotDirectory(upstreamName, suite), `${id}.json`);
}

export function readPointer(upstreamName, suite) {
  return readJsonCached(pointerPath(upstreamName, suite));
}

export function readManifest(upstreamName, suite, id) {
  return readJsonCached(manifestPath(upstreamName, suite, id));
}

export function currentSnapshot(upstreamName, suite) {
  const pointer = readPointer(upstreamName, suite);
  if (!pointer?.snapshot) return null;
  const manifest = readManifest(upstreamName, suite, pointer.snapshot);
  if (!manifest) return null;
  return { pointer, manifest };
}

export function keptManifests(upstreamName, suite) {
  const pointer = readPointer(upstreamName, suite);
  if (!pointer) return [];
  return (pointer.history ?? [pointer.snapshot]).map((id) => readManifest(upstreamName, suite, id)).filter(Boolean);
}

export function snapshotLastModifiedMs(manifest) {
  for (const candidate of [manifest.lastModified, manifest.date]) {
    const parsed = Date.parse(candidate ?? '');
    if (Number.isFinite(parsed)) return parsed;
  }
  return Date.parse(manifest.fetchedAt);
}

export function configuredSuites(config, filter) {
  const targets = [];
  for (const [upstreamName, upstream] of Object.entries(config.upstreams)) {
    for (const suite of upstream.suites) {
      if (filter?.upstream && filter.upstream !== upstreamName) continue;
      if (filter?.suite && filter.suite !== suite) continue;
      targets.push({ upstreamName, suite });
    }
  }
  return targets;
}

function newPointer(upstreamName, suite) {
  return {
    upstream: upstreamName,
    suite,
    snapshot: null,
    history: [],
    promotedAt: null,
    checkedAt: null,
    lastAttemptAt: null,
    lastAttemptFailed: false,
    lastFailure: null,
    blocked: null,
  };
}

function compactTimestamp(isoString) {
  return isoString.replace(/[-:]/g, '').replace(/\.\d+Z$/, 'Z');
}

async function fetchOptionalMetadata(upstreamName, url, timeoutMs) {
  try {
    return (await fetchMetadata(upstreamName, url, { timeoutMs })).body;
  } catch (error) {
    if (error instanceof UpstreamNotFoundError) return null;
    throw error;
  }
}

async function fetchReleaseFiles(config, upstreamName, suite, current) {
  const base = `${config.upstreams[upstreamName].url}/dists/${encodeURIComponent(suite)}`;
  const timeoutMs = config.timeouts.metadataHeadersMs;
  const headers = {};
  if (current?.etag) headers['if-none-match'] = current.etag;
  if (current?.lastModified) headers['if-modified-since'] = current.lastModified;
  const inRelease = await fetchMetadata(upstreamName, `${base}/InRelease`, { timeoutMs, headers });
  if (inRelease.status === 304) return { unchanged: true };
  if (current && current.release.InRelease === sha256Hex(inRelease.body)) return { unchanged: true };
  const parsed = parseRelease(inRelease.body.toString('utf8'));
  if (!parsed.date) throw new Error(`InRelease of ${upstreamName}/${suite} has no Date field`);
  const release = await fetchOptionalMetadata(upstreamName, `${base}/Release`, timeoutMs);
  const releaseGpg = await fetchOptionalMetadata(upstreamName, `${base}/Release.gpg`, timeoutMs);
  if (release) {
    const releaseDate = parseRelease(release.toString('utf8')).date;
    if (releaseDate !== parsed.date) {
      throw new ReleaseMidUpdateError(`upstream is mid-update: InRelease Date "${parsed.date}" differs from Release Date "${releaseDate}"`);
    }
  }
  return {
    unchanged: false,
    inRelease: inRelease.body,
    release,
    releaseGpg,
    parsed,
    etag: inRelease.headers.get('etag'),
    lastModified: inRelease.headers.get('last-modified'),
  };
}

async function buildPendingManifest(config, upstreamName, suite, fetched, current) {
  const upstream = config.upstreams[upstreamName];
  const release = {
    InRelease: await writeBlob(fetched.inRelease),
    Release: fetched.release ? await writeBlob(fetched.release) : null,
    'Release.gpg': fetched.releaseGpg ? await writeBlob(fetched.releaseGpg) : null,
  };
  const selection = {
    components: upstream.components,
    architectures: upstream.architectures,
    releaseArchitectures: fetched.parsed.architectures,
  };
  const files = {};
  for (const [path, info] of Object.entries(fetched.parsed.files)) {
    if (!info.sha256) continue;
    const previous = current?.files?.[path];
    const knownAbsent = Boolean(previous?.absent && previous.sha256 === info.sha256);
    files[path] = { ...info, selected: isSelectedIndexFile(path, selection), stored: false, absent: knownAbsent };
  }
  await mapWithConcurrency(Object.values(files), 32, async (file) => {
    file.stored = await hasBlob(file.sha256);
  });
  const fetchedAt = new Date().toISOString();
  return {
    upstream: upstreamName,
    suite,
    id: `${compactTimestamp(fetchedAt)}-${release.InRelease.slice(0, 12)}`,
    fetchedAt,
    lastModified: fetched.lastModified,
    etag: fetched.etag,
    date: fetched.parsed.date,
    validUntil: fetched.parsed.validUntil,
    notAutomatic: fetched.parsed.notAutomatic,
    acquireByHash: fetched.parsed.acquireByHash,
    codename: fetched.parsed.codename,
    architectures: fetched.parsed.architectures,
    release,
    files,
  };
}

export function fetchIndexFile(config, manifest, path) {
  const file = manifest.files[path];
  return singleFlight(`blob:${file.sha256}`, async () => {
    if (await hasBlob(file.sha256)) return;
    const base = `${config.upstreams[manifest.upstream].url}/dists/${encodeURIComponent(manifest.suite)}`;
    const directory = posix.dirname(path);
    const directoryPrefix = directory === '.' ? '' : `${encodeRepoPath(directory)}/`;
    const urls = [];
    if (manifest.acquireByHash) urls.push(`${base}/${directoryPrefix}by-hash/SHA256/${file.sha256}`);
    urls.push(`${base}/${encodeRepoPath(path)}`);
    let notFound = null;
    for (const url of urls) {
      try {
        await downloadVerified({
          breakerName: manifest.upstream,
          url,
          destination: blobPath(file.sha256),
          timeoutMs: config.timeouts.metadataHeadersMs,
          expectedSize: file.size,
          expectedSha256: file.sha256,
        });
        return;
      } catch (error) {
        if (!(error instanceof UpstreamNotFoundError)) throw error;
        notFound = error;
      }
    }
    throw notFound;
  });
}

export function choosePackagesIndex(manifest, component, architecture) {
  const candidates = packagesIndexCandidates(component, architecture);
  let listedReadable = false;
  for (const [path, gzip] of [[candidates.plain, false], [candidates.gzip, true]]) {
    const file = manifest.files[path];
    if (!file || file.absent) continue;
    listedReadable = true;
    if (existsSync(blobPath(file.sha256))) return { path, sha256: file.sha256, gzip };
  }
  if (!listedReadable && manifest.files[candidates.xz]) return { xzOnly: true, path: candidates.xz };
  return null;
}

export async function loadFamilyUniverse(config, family, architecture, overrideManifest = null) {
  const universe = createUniverse();
  const xzOnly = [];
  for (const [upstreamName, upstream] of Object.entries(config.upstreams)) {
    if (!upstream.architectures.includes(architecture)) continue;
    for (const suite of upstream.suites) {
      if (!suiteJoinsFamily(suite, family)) continue;
      const useOverride = overrideManifest?.upstream === upstreamName && overrideManifest?.suite === suite;
      const manifest = useOverride ? overrideManifest : currentSnapshot(upstreamName, suite)?.manifest;
      if (!manifest) continue;
      const source = { upstream: upstreamName, suite, notAutomatic: manifest.notAutomatic };
      for (const component of upstream.components) {
        for (const indexArchitecture of [architecture, 'all']) {
          const index = choosePackagesIndex(manifest, component, indexArchitecture);
          if (!index) continue;
          if (index.xzOnly) {
            xzOnly.push({ upstream: upstreamName, suite, path: index.path });
            continue;
          }
          await parsePackagesStream(createReadStream(blobPath(index.sha256)), { gzip: index.gzip }, (record) => {
            if (record.architecture === architecture || record.architecture === 'all') addToUniverse(universe, record, source);
          });
        }
      }
    }
  }
  for (const entry of xzOnly) log.warn('only an xz Packages index is available, cannot parse it', entry);
  return { universe, xzOnly };
}

export function familiesForSuite(config, suite, architecture) {
  if (suite !== 'nodistro') return [familyOfSuite(suite)];
  const families = new Set();
  for (const upstream of Object.values(config.upstreams)) {
    if (!upstream.architectures.includes(architecture)) continue;
    for (const candidate of upstream.suites) if (candidate !== 'nodistro') families.add(familyOfSuite(candidate));
  }
  return [...families];
}

export async function downloadPackageRecords(config, records, { onFetched } = {}) {
  const outcome = { fetched: [], cached: [], network: [], hard: [], bytes: 0 };
  const missing = [];
  for (const record of records) {
    if (await fileStats(poolFilePath(record.upstream, record.filename))) outcome.cached.push(record);
    else missing.push(record);
  }
  await mapWithConcurrency(missing, downloadConcurrency, async (record) => {
    try {
      await downloadPoolFile({
        config,
        upstreamName: record.upstream,
        repoPath: record.filename,
        expected: { size: record.size, sha256: record.sha256 },
      });
      outcome.fetched.push(record);
      outcome.bytes += record.size;
      onFetched?.(record);
    } catch (error) {
      const failure = { package: record.name, version: record.version, upstream: record.upstream, filename: record.filename, reason: error.message };
      if (isNetworkFailure(error)) outcome.network.push(failure);
      else outcome.hard.push(failure);
    }
  });
  return outcome;
}

async function prefetchDelta(context, manifest, stats, problems) {
  const { config, known } = context;
  await known.reload();
  const upstream = config.upstreams[manifest.upstream];
  for (const architecture of upstream.architectures) {
    const knownNames = known.namesFor(architecture);
    if (knownNames.size === 0) continue;
    for (const family of familiesForSuite(config, manifest.suite, architecture)) {
      const { universe, xzOnly } = await loadFamilyUniverse(config, family, architecture, manifest);
      if (xzOnly.some((entry) => entry.upstream === manifest.upstream && entry.suite === manifest.suite)) {
        log.warn('skipping prefetch: suite only offers xz indices', { upstream: manifest.upstream, suite: manifest.suite, architecture });
        continue;
      }
      const roots = [...knownNames].filter((name) =>
        universe.packages.get(name)?.some((record) => record.upstream === manifest.upstream && record.suite === manifest.suite),
      );
      if (roots.length === 0) continue;
      const closure = computeClosure(universe, roots, (name) => known.has(name, architecture));
      const outcome = await downloadPackageRecords(config, closure);
      stats.debs += outcome.fetched.length;
      stats.bytes += outcome.bytes;
      problems.network.push(...outcome.network);
      problems.hard.push(...outcome.hard);
      log.info('prefetch delta', {
        upstream: manifest.upstream,
        suite: manifest.suite,
        architecture,
        family,
        roots: roots.length,
        closure: closure.length,
        fetched: outcome.fetched.length,
        failed: outcome.network.length + outcome.hard.length,
      });
      if (outcome.network.length > 0) return;
    }
  }
}

async function completeSnapshot(context, manifest, stats, { prefetch }) {
  const problems = { network: [], hard: [] };
  const wanted = Object.keys(manifest.files).filter((path) => manifest.files[path].selected && !manifest.files[path].absent);
  await mapWithConcurrency(wanted, downloadConcurrency, async (path) => {
    const file = manifest.files[path];
    if (await hasBlob(file.sha256)) {
      file.stored = true;
      return;
    }
    try {
      await fetchIndexFile(context.config, manifest, path);
      file.stored = true;
      stats.indexFiles += 1;
      stats.bytes += file.size;
    } catch (error) {
      if (error instanceof UpstreamNotFoundError) {
        file.absent = true;
        return;
      }
      const failure = { path, reason: error.message };
      if (isNetworkFailure(error)) problems.network.push(failure);
      else problems.hard.push(failure);
    }
  });
  if (problems.network.length > 0 || !prefetch) return problems;
  if (manifest.notAutomatic) return problems;
  await prefetchDelta(context, manifest, stats, problems);
  return problems;
}

export function isSnapshotComplete(manifest) {
  return Object.values(manifest.files).every((file) => !file.selected || file.absent || file.stored);
}

async function pruneOldManifests(upstreamName, suite, keepIds) {
  const directory = snapshotDirectory(upstreamName, suite);
  const names = await readdir(directory).catch(() => []);
  for (const name of names) {
    if (name === 'current.json' || !name.endsWith('.json')) continue;
    if (keepIds.includes(name.slice(0, -'.json'.length))) continue;
    await unlink(join(directory, name)).catch(() => {});
  }
}

async function promote(context, pointer, manifest) {
  await writeJsonAtomic(manifestPath(manifest.upstream, manifest.suite, manifest.id), manifest);
  pointer.history = [manifest.id, ...(pointer.history ?? []).filter((id) => id !== manifest.id)].slice(0, keptSnapshotCount);
  pointer.snapshot = manifest.id;
  pointer.promotedAt = new Date().toISOString();
  pointer.blocked = null;
  await writeJsonAtomic(pointerPath(manifest.upstream, manifest.suite), pointer);
  await pruneOldManifests(manifest.upstream, manifest.suite, pointer.history);
  log.info('snapshot promoted', { upstream: manifest.upstream, suite: manifest.suite, snapshot: manifest.id, date: manifest.date });
  context.onSnapshotUpdated?.(manifest);
}

function describeProblem(problems) {
  const first = problems.network[0] ?? problems.hard[0];
  return first ? `${first.path ?? first.filename}: ${first.reason}` : null;
}

async function refreshWithPointer(context, upstreamName, suite, pointer, current, stats, recheck) {
  const { config } = context;
  const nowIso = () => new Date().toISOString();
  const fetched = await fetchReleaseFiles(config, upstreamName, suite, current);

  if (fetched.unchanged) {
    pointer.checkedAt = nowIso();
    pointer.lastAttemptFailed = false;
    if (!current || (!recheck && isSnapshotComplete(current))) return { promoted: false, result: 'unchanged' };
    const problems = await completeSnapshot(context, current, stats, { prefetch: true });
    await writeJsonAtomic(manifestPath(upstreamName, suite, current.id), current);
    context.onSnapshotUpdated?.(current);
    if (problems.network.length > 0) {
      pointer.lastAttemptFailed = true;
      pointer.lastFailure = { at: nowIso(), message: describeProblem(problems) };
    }
    return {
      promoted: false,
      result: 'unchanged',
      completed: true,
      networkFailures: problems.network.length,
      hardFailures: problems.hard.length,
      failures: [...problems.network, ...problems.hard].slice(0, 20),
    };
  }

  const pending = await buildPendingManifest(config, upstreamName, suite, fetched, current);

  if (!current) {
    await promote(context, pointer, pending);
    const problems = await completeSnapshot(context, pending, stats, { prefetch: true });
    await writeJsonAtomic(manifestPath(upstreamName, suite, pending.id), pending);
    context.onSnapshotUpdated?.(pending);
    pointer.checkedAt = nowIso();
    pointer.lastAttemptFailed = problems.network.length > 0;
    if (pointer.lastAttemptFailed) pointer.lastFailure = { at: nowIso(), message: describeProblem(problems) };
    return { promoted: true, result: 'initial', networkFailures: problems.network.length, hardFailures: problems.hard.length };
  }

  const problems = await completeSnapshot(context, pending, stats, { prefetch: true });
  if (problems.network.length > 0) {
    pointer.lastAttemptFailed = true;
    pointer.lastFailure = { at: nowIso(), message: `network: ${describeProblem(problems)}` };
    return { promoted: false, result: 'deferred', reason: 'network failure or open breaker', failures: problems.network.slice(0, 20) };
  }

  pointer.checkedAt = nowIso();
  if (problems.hard.length > 0) {
    pointer.blocked = {
      since: pointer.blocked?.since ?? nowIso(),
      reason: `${problems.hard.length} files are missing upstream or fail verification`,
      missing: problems.hard.slice(0, 200),
    };
    const blockedMs = Date.now() - Date.parse(pointer.blocked.since);
    if (blockedMs < config.maxPromotionDelayHours * 3600 * 1000) {
      pointer.lastAttemptFailed = true;
      pointer.lastFailure = { at: nowIso(), message: `blocked: ${describeProblem(problems)}` };
      return { promoted: false, result: 'blocked', reason: pointer.blocked.reason, failures: problems.hard.slice(0, 20) };
    }
    for (const failure of problems.hard) {
      log.error('promoting snapshot although a file is missing', { upstream: upstreamName, suite, ...failure });
    }
  }

  await promote(context, pointer, pending);
  pointer.lastAttemptFailed = false;
  return { promoted: true, result: problems.hard.length > 0 ? 'forced' : 'promoted', hardFailures: problems.hard.length };
}

async function runRefresh(context, upstreamName, suite, { recheck = false } = {}) {
  const startedAt = Date.now();
  const stats = { indexFiles: 0, debs: 0, bytes: 0 };
  const existingPointer = readPointer(upstreamName, suite);
  const pointer = existingPointer ? structuredClone(existingPointer) : newPointer(upstreamName, suite);
  const existingManifest = pointer.snapshot ? readManifest(upstreamName, suite, pointer.snapshot) : null;
  const current = existingManifest ? structuredClone(existingManifest) : null;
  pointer.lastAttemptAt = new Date().toISOString();
  let outcome;
  try {
    outcome = await refreshWithPointer(context, upstreamName, suite, pointer, current, stats, recheck);
  } catch (error) {
    pointer.lastAttemptFailed = true;
    pointer.lastFailure = { at: new Date().toISOString(), message: error.message };
    outcome = { promoted: false, result: 'failed', reason: error.message };
    if (!isNetworkFailure(error) && !(error instanceof ReleaseMidUpdateError)) {
      log.error('refresh failed', { upstream: upstreamName, suite, error });
    }
  }
  await writeJsonAtomic(pointerPath(upstreamName, suite), pointer);
  const summary = {
    upstream: upstreamName,
    suite,
    ...outcome,
    indexFilesFetched: stats.indexFiles,
    debsFetched: stats.debs,
    bytes: stats.bytes,
    durationMs: Date.now() - startedAt,
  };
  const level = outcome.result === 'failed' || outcome.result === 'deferred' || outcome.result === 'blocked' ? 'warn' : 'info';
  log[level]('refresh', summary);
  return summary;
}

export function refreshSuite(context, upstreamName, suite, options = {}) {
  return singleFlight(`refresh:${upstreamName}/${suite}`, () => runRefresh(context, upstreamName, suite, options));
}

export function isRefreshRunning(upstreamName, suite) {
  return inFlightPromise(`refresh:${upstreamName}/${suite}`) !== null;
}

export async function refreshAll(context, { filter, recheck = false } = {}) {
  const targets = configuredSuites(context.config, filter);
  return mapWithConcurrency(targets, suiteRefreshConcurrency, ({ upstreamName, suite }) =>
    refreshSuite(context, upstreamName, suite, { recheck }),
  );
}

export function refreshInBackgroundIfStale(context, upstreamName, suite) {
  if (isRefreshRunning(upstreamName, suite)) return;
  const pointer = readPointer(upstreamName, suite);
  const now = Date.now();
  const checkedAt = Date.parse(pointer?.checkedAt ?? '') || 0;
  const attemptedAt = Date.parse(pointer?.lastAttemptAt ?? '') || 0;
  if (now - checkedAt < context.config.refreshMinutes * 60 * 1000) return;
  if (now - attemptedAt < 60 * 1000) return;
  refreshSuite(context, upstreamName, suite).catch((error) => log.error('background refresh crashed', { upstream: upstreamName, suite, error }));
}

export function warnAboutValidUntil(config) {
  for (const { upstreamName, suite } of configuredSuites(config)) {
    const manifest = currentSnapshot(upstreamName, suite)?.manifest;
    if (!manifest?.validUntil) continue;
    const remainingMs = Date.parse(manifest.validUntil) - Date.now();
    if (remainingMs < validUntilWarningMs) {
      log.warn('live snapshot is close to or past its Valid-Until', {
        upstream: upstreamName,
        suite,
        validUntil: manifest.validUntil,
        expired: remainingMs <= 0,
      });
    }
  }
}

const poolHashes = new Map();
const indexedSnapshots = new Set();

function isPackagesIndexListed(manifest, component, architecture) {
  return Object.values(packagesIndexCandidates(component, architecture)).some((path) => manifest.files[path]);
}

export async function indexPoolHashes(config, manifest) {
  const indexKey = `${manifest.upstream}/${manifest.suite}/${manifest.id}`;
  if (indexedSnapshots.has(indexKey)) return false;
  const upstream = config.upstreams[manifest.upstream];
  let complete = true;
  let entries = 0;
  for (const component of upstream.components) {
    for (const architecture of [...upstream.architectures, 'all']) {
      const index = choosePackagesIndex(manifest, component, architecture);
      if (!index) {
        if (isPackagesIndexListed(manifest, component, architecture)) complete = false;
        continue;
      }
      if (index.xzOnly) continue;
      await parsePackagesStream(createReadStream(blobPath(index.sha256)), { gzip: index.gzip }, (record) => {
        if (!record.sha256 || !Number.isFinite(record.size)) return;
        poolHashes.set(`${manifest.upstream}/${record.filename}`, { sha256: Buffer.from(record.sha256, 'hex'), size: record.size });
        entries += 1;
      });
    }
  }
  if (complete) indexedSnapshots.add(indexKey);
  log.debug('indexed pool hashes', { upstream: manifest.upstream, suite: manifest.suite, snapshot: manifest.id, entries, complete });
  return true;
}

export function syncPoolHashes(config) {
  return singleFlight('pool-hash-sync', async () => {
    let indexedAny = false;
    for (const { upstreamName, suite } of configuredSuites(config)) {
      const manifest = currentSnapshot(upstreamName, suite)?.manifest;
      if (!manifest) continue;
      try {
        if (await indexPoolHashes(config, manifest)) indexedAny = true;
      } catch (error) {
        log.warn('could not index pool hashes', { upstream: upstreamName, suite, error });
      }
    }
    if (indexedAny) log.info('pool hash index updated', { entries: poolHashes.size });
  });
}

export function lookupPoolHash(upstreamName, repoPath) {
  const entry = poolHashes.get(`${upstreamName}/${repoPath}`);
  return entry ? { sha256: entry.sha256.toString('hex'), size: entry.size } : null;
}
