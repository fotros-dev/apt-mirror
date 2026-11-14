import { createReadStream } from 'node:fs';
import { readdir, unlink } from 'node:fs/promises';
import { join, relative, sep } from 'node:path';
import { parsePackagesStream } from './deb.js';
import { storePaths, walkFiles, fileStats, poolFilePath, readJson, blobPath } from './store.js';
import { configuredSuites, currentSnapshot, choosePackagesIndex } from './snapshot.js';

const dayMs = 24 * 3600 * 1000;
const blobGraceMs = dayMs;

async function referencedPoolFiles(config) {
  const referenced = new Set();
  const uncertainUpstreams = new Set();
  for (const { upstreamName, suite } of configuredSuites(config)) {
    const manifest = currentSnapshot(upstreamName, suite)?.manifest;
    if (!manifest) continue;
    const upstream = config.upstreams[upstreamName];
    for (const component of upstream.components) {
      for (const architecture of [...upstream.architectures, 'all']) {
        const index = choosePackagesIndex(manifest, component, architecture);
        if (!index) continue;
        if (index.xzOnly) {
          uncertainUpstreams.add(upstreamName);
          continue;
        }
        await parsePackagesStream(createReadStream(blobPath(index.sha256)), { gzip: index.gzip }, (record) => {
          referenced.add(poolFilePath(upstreamName, record.filename));
        });
      }
    }
    for (const [path, file] of Object.entries(manifest.files)) {
      if (/\/binary-[^/]+\/Packages(\.gz|\.xz)?$/.test(path) && file.selected && !file.absent && !file.stored) {
        uncertainUpstreams.add(upstreamName);
      }
    }
  }
  return { referenced, uncertainUpstreams };
}

async function referencedBlobs() {
  const referenced = new Set();
  for await (const path of walkFiles(storePaths.snapshots)) {
    if (!path.endsWith('.json') || path.endsWith(`${sep}current.json`)) continue;
    const manifest = await readJson(path).catch(() => null);
    if (!manifest) continue;
    for (const sha256 of Object.values(manifest.release ?? {})) if (sha256) referenced.add(sha256);
    for (const file of Object.values(manifest.files ?? {})) referenced.add(file.sha256);
  }
  return referenced;
}

export async function collectGarbage(config, { olderThanDays, dryRun }) {
  const now = Date.now();
  const poolCutoff = now - olderThanDays * dayMs;
  const deletions = [];
  const report = { pool: { files: 0, bytes: 0 }, blobs: { files: 0, bytes: 0 }, negative: 0, tmp: 0, skippedUpstreams: [] };

  const { referenced: poolReferenced, uncertainUpstreams } = await referencedPoolFiles(config);
  report.skippedUpstreams = [...uncertainUpstreams];
  for await (const path of walkFiles(storePaths.pool)) {
    const upstreamName = relative(storePaths.pool, path).split(sep)[0];
    if (uncertainUpstreams.has(upstreamName) || poolReferenced.has(path)) continue;
    const stats = await fileStats(path);
    if (!stats || stats.mtimeMs >= poolCutoff) continue;
    deletions.push({ type: 'pool', path, size: stats.size });
    report.pool.files += 1;
    report.pool.bytes += stats.size;
  }

  const blobReferenced = await referencedBlobs();
  for await (const path of walkFiles(storePaths.blobs)) {
    const sha256 = path.slice(path.lastIndexOf(sep) + 1);
    if (blobReferenced.has(sha256)) continue;
    const stats = await fileStats(path);
    if (!stats || now - stats.mtimeMs < blobGraceMs) continue;
    deletions.push({ type: 'blob', path, size: stats.size });
    report.blobs.files += 1;
    report.blobs.bytes += stats.size;
  }

  for (const name of await readdir(storePaths.negative).catch(() => [])) {
    const path = join(storePaths.negative, name);
    const entry = await readJson(path).catch(() => null);
    if (!entry || entry.expiresAt < now) {
      deletions.push({ type: 'negative', path, size: 0 });
      report.negative += 1;
    }
  }

  for (const name of await readdir(storePaths.tmp).catch(() => [])) {
    const path = join(storePaths.tmp, name);
    const stats = await fileStats(path);
    if (stats && now - stats.mtimeMs > dayMs) {
      deletions.push({ type: 'tmp', path, size: stats.size });
      report.tmp += 1;
    }
  }

  if (!dryRun) {
    for (const deletion of deletions) await unlink(deletion.path).catch(() => {});
  }
  return { report, deletions };
}
