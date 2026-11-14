import { loadConfig, ConfigError } from './config.js';
import { log } from './log.js';
import {
  ensureStoreLayout,
  removeLeftoverTempFiles,
  removeActiveTempFiles,
  createKnownPackages,
  directorySize,
  readJson,
  storePaths,
  cacheDir,
} from './store.js';
import { configureUpstreams, abortAllUpstreamRequests, describeBreakers } from './upstream.js';
import {
  refreshAll,
  syncPoolHashes,
  indexPoolHashes,
  warnAboutValidUntil,
  configuredSuites,
  readPointer,
  currentSnapshot,
  isSnapshotComplete,
} from './snapshot.js';
import { refreshStaleKeys } from './proxy.js';
import { createMirrorServer } from './server.js';
import { warm, readExportFiles, machineFromNames } from './warm.js';
import { collectGarbage } from './gc.js';

const usage = `usage:
  node src/main.js serve
  node src/main.js refresh [<upstream>[/<suite>]] [--recheck]
  node src/main.js warm <export-file>... [--no-closure]
  node src/main.js warm --os ubuntu:noble --arch amd64 <package>... [--no-closure]
  node src/main.js gc [--older-than-days 90] [--dry-run]
  node src/main.js status
every command accepts --config <path>`;

const valueOptions = new Set(['--config', '--os', '--arch', '--older-than-days']);
const flagOptions = new Set(['--recheck', '--no-closure', '--dry-run', '--help']);

class UsageError extends Error {
  name = 'UsageError';
}

function parseArguments(argv) {
  const options = {};
  const positional = [];
  for (let index = 0; index < argv.length; index += 1) {
    const argument = argv[index];
    if (valueOptions.has(argument)) {
      if (index + 1 >= argv.length) throw new UsageError(`${argument} needs a value`);
      options[argument.slice(2)] = argv[index + 1];
      index += 1;
    } else if (flagOptions.has(argument)) {
      options[argument.slice(2)] = true;
    } else if (argument.startsWith('--')) {
      throw new UsageError(`unknown option ${argument}`);
    } else {
      positional.push(argument);
    }
  }
  return { options, positional };
}

async function prepare(options, { persistBreakers = false } = {}) {
  const config = loadConfig(options.config);
  await ensureStoreLayout();
  configureUpstreams(config, { persistBreakers });
  const known = createKnownPackages();
  await known.reload();
  const context = {
    config,
    known,
    onSnapshotUpdated: (manifest) => {
      indexPoolHashes(config, manifest).catch((error) => log.warn('could not index pool hashes', { error }));
    },
  };
  return context;
}

async function serve(options) {
  const removed = await removeLeftoverTempFiles();
  const context = await prepare(options, { persistBreakers: true });
  const { config, known } = context;
  if (removed > 0) log.warn('removed leftover temp files from an earlier run', { count: removed });

  const server = createMirrorServer(context);
  await new Promise((resolve, reject) => {
    server.once('error', reject);
    server.listen(config.listen.port, config.listen.host, resolve);
  });
  log.info('listening', { host: config.listen.host, port: server.address().port, config: config.path });

  let shuttingDown = false;
  let refreshTimer = null;
  const runRefreshCycle = async () => {
    try {
      await refreshAll(context);
      await refreshStaleKeys(context);
      warnAboutValidUntil(config);
      await known.flush();
      await syncPoolHashes(config);
    } catch (error) {
      log.error('refresh cycle failed', { error });
    }
    if (!shuttingDown) refreshTimer = setTimeout(runRefreshCycle, config.refreshMinutes * 60 * 1000);
  };
  syncPoolHashes(config).catch((error) => log.error('pool hash index failed', { error }));
  runRefreshCycle();

  const knownFlushTimer = setInterval(() => {
    known.flush().catch((error) => log.warn('could not flush known packages', { error }));
  }, 30_000);
  const poolHashTimer = setInterval(() => {
    syncPoolHashes(config).catch((error) => log.warn('pool hash sync failed', { error }));
  }, 60_000);

  const shutdown = (signal) => {
    if (shuttingDown) return;
    shuttingDown = true;
    log.info('shutting down', { signal });
    clearTimeout(refreshTimer);
    clearInterval(knownFlushTimer);
    clearInterval(poolHashTimer);
    const forceTimer = setTimeout(() => {
      log.warn('closing remaining connections');
      server.closeAllConnections();
      abortAllUpstreamRequests('shutdown deadline reached');
    }, 10_000);
    forceTimer.unref();
    server.close(async () => {
      abortAllUpstreamRequests('server stopped');
      await new Promise((resolve) => setTimeout(resolve, 250));
      await removeActiveTempFiles();
      await known.flush().catch((error) => log.warn('could not flush known packages', { error }));
      log.info('stopped');
      process.exit(0);
    });
    server.closeIdleConnections();
  };
  process.on('SIGTERM', () => shutdown('SIGTERM'));
  process.on('SIGINT', () => shutdown('SIGINT'));
}

async function refresh(options, positional) {
  if (positional.length > 1) throw new UsageError('refresh takes at most one <upstream>[/<suite>]');
  const context = await prepare(options);
  let filter;
  if (positional[0]) {
    const [upstream, ...suiteParts] = positional[0].split('/');
    filter = { upstream, suite: suiteParts.join('/') || undefined };
    if (!context.config.upstreams[upstream]) throw new UsageError(`unknown upstream ${upstream}`);
    if (filter.suite && !context.config.upstreams[upstream].suites.includes(filter.suite)) {
      throw new UsageError(`suite ${filter.suite} is not configured for ${upstream}`);
    }
  }
  const results = await refreshAll(context, { filter, recheck: Boolean(options.recheck) });
  await refreshStaleKeys(context);
  await context.known.flush();
  console.log('');
  for (const result of results) {
    const detail = result.reason ? ` (${result.reason})` : '';
    console.log(
      `${`${result.upstream}/${result.suite}`.padEnd(36)} ${result.result.padEnd(10)} promoted=${result.promoted} ` +
        `indices=${result.indexFilesFetched} debs=${result.debsFetched} bytes=${result.bytes} ${result.durationMs}ms${detail}`,
    );
  }
  return results.some((result) => result.result === 'failed') ? 1 : 0;
}

async function warmCommand(options, positional) {
  const context = await prepare(options);
  let machines;
  if (options.os || options.arch) {
    if (positional.length === 0) throw new UsageError('warm --os needs at least one package name');
    machines = [machineFromNames({ os: options.os, architecture: options.arch, names: positional })];
  } else {
    if (positional.length === 0) throw new UsageError('warm needs at least one export file');
    machines = await readExportFiles(positional);
  }
  const reports = await warm(context, machines, { closure: !options['no-closure'] });
  let failures = 0;
  console.log('');
  for (const report of reports) {
    const { outcome } = report;
    failures += outcome.network.length + outcome.hard.length;
    console.log(`${report.machine} (${report.family}/${report.architecture})`);
    console.log(`  fetched:        ${outcome.fetched.length}`);
    console.log(`  already cached: ${outcome.cached.length}`);
    console.log(`  failed:         ${outcome.network.length + outcome.hard.length}`);
    console.log(`  not found in any index: ${report.notFound.length}`);
    for (const name of report.notFound) console.log(`    ${name}`);
    if (report.foreign.length > 0) console.log(`  skipped foreign-architecture packages: ${report.foreign.join(', ')}`);
    for (const failure of [...outcome.network, ...outcome.hard]) console.log(`    failed ${failure.filename}: ${failure.reason}`);
  }
  return failures > 0 ? 1 : 0;
}

async function gcCommand(options) {
  const olderThanDays = Number(options['older-than-days'] ?? 90);
  if (!Number.isFinite(olderThanDays) || olderThanDays < 0) throw new UsageError('--older-than-days must be a non-negative number');
  const context = await prepare(options);
  const { report, deletions } = await collectGarbage(context.config, { olderThanDays, dryRun: Boolean(options['dry-run']) });
  if (options['dry-run']) {
    for (const deletion of deletions) console.log(`would delete ${deletion.type} ${deletion.path} (${deletion.size} bytes)`);
  }
  const verb = options['dry-run'] ? 'would delete' : 'deleted';
  console.log(`${verb}: ${report.pool.files} pool files (${formatBytes(report.pool.bytes)}), ` +
    `${report.blobs.files} blobs (${formatBytes(report.blobs.bytes)}), ${report.negative} negative entries, ${report.tmp} temp files`);
  console.log(`total: ${formatBytes(report.pool.bytes + report.blobs.bytes)}`);
  if (report.skippedUpstreams.length > 0) {
    console.log(`pool files of ${report.skippedUpstreams.join(', ')} were kept: their current indices are not fully stored`);
  }
  return 0;
}

function formatBytes(bytes) {
  const units = ['B', 'KiB', 'MiB', 'GiB', 'TiB'];
  let value = bytes;
  let unit = 0;
  while (value >= 1024 && unit < units.length - 1) {
    value /= 1024;
    unit += 1;
  }
  return `${value.toFixed(unit === 0 ? 0 : 1)} ${units[unit]}`;
}

function formatAge(isoString) {
  if (!isoString) return '-';
  const ms = Date.now() - Date.parse(isoString);
  if (!Number.isFinite(ms)) return '-';
  const minutes = Math.round(ms / 60000);
  if (minutes < 120) return `${minutes}m ago`;
  const hours = Math.round(minutes / 60);
  if (hours < 72) return `${hours}h ago`;
  return `${Math.round(hours / 24)}d ago`;
}

async function status(options) {
  const config = loadConfig(options.config);
  await ensureStoreLayout();
  const known = createKnownPackages();
  await known.reload();
  console.log('suites:');
  for (const { upstreamName, suite } of configuredSuites(config)) {
    const pointer = readPointer(upstreamName, suite);
    const snapshot = currentSnapshot(upstreamName, suite);
    const name = `${upstreamName}/${suite}`.padEnd(34);
    if (!snapshot) {
      console.log(`  ${name} no snapshot yet${pointer?.lastFailure ? `, last failure ${formatAge(pointer.lastFailure.at)}: ${pointer.lastFailure.message}` : ''}`);
      continue;
    }
    const { manifest } = snapshot;
    const parts = [
      `snapshot ${manifest.id}`,
      `fetched ${formatAge(manifest.fetchedAt)}`,
      `checked ${formatAge(pointer.checkedAt)}`,
      isSnapshotComplete(manifest) ? 'indices complete' : 'indices incomplete',
    ];
    if (manifest.validUntil) {
      const remainingHours = (Date.parse(manifest.validUntil) - Date.now()) / 3600000;
      parts.push(`valid-until ${manifest.validUntil}${remainingHours <= 0 ? ' EXPIRED' : remainingHours < 48 ? ' EXPIRES SOON' : ''}`);
    }
    console.log(`  ${name} ${parts.join(', ')}`);
    if (pointer.lastFailure && Date.parse(pointer.lastFailure.at) >= (Date.parse(pointer.checkedAt ?? '') || 0)) {
      console.log(`  ${''.padEnd(34)} last failure ${formatAge(pointer.lastFailure.at)}: ${pointer.lastFailure.message}${pointer.lastAttemptFailed ? ' (serving STALE)' : ''}`);
    }
    if (pointer.blocked) {
      console.log(`  ${''.padEnd(34)} promotion blocked since ${pointer.blocked.since}: ${pointer.blocked.reason}`);
    }
  }
  const persisted = await readJson(storePaths.breakers);
  const breakers = persisted?.breakers ?? describeBreakers();
  console.log('\nbreakers' + (persisted ? ` (as of ${persisted.updatedAt}, written by the running server)` : '') + ':');
  const entries = Object.entries(breakers);
  if (entries.length === 0) console.log('  all closed');
  for (const [name, breaker] of entries) {
    const openPeriodOver = breaker.state === 'open' && Date.parse(breaker.openUntil) <= Date.now();
    const state = openPeriodOver ? 'half-open (next request is the probe)' : breaker.state;
    const until = breaker.openUntil && !openPeriodOver ? ` until ${breaker.openUntil}` : '';
    const failure = breaker.lastFailure ? `, last failure ${breaker.lastFailure.at}: ${breaker.lastFailure.message}` : '';
    console.log(`  ${name.padEnd(34)} ${state}${until}${failure}`);
  }
  const size = await directorySize(cacheDir);
  console.log(`\ncache: ${formatBytes(size.bytes)} in ${size.files} files (${cacheDir})`);
  console.log(`known packages: ${known.count()}`);
  return 0;
}

async function main() {
  const [command, ...rest] = process.argv.slice(2);
  const { options, positional } = parseArguments(rest);
  if (!command || options.help || command === 'help') {
    console.log(usage);
    return command ? 0 : 1;
  }
  if (command === 'serve') {
    await serve(options);
    return null;
  }
  if (command === 'refresh') return refresh(options, positional);
  if (command === 'warm') return warmCommand(options, positional);
  if (command === 'gc') return gcCommand(options);
  if (command === 'status') return status(options);
  throw new UsageError(`unknown command ${command}`);
}

process.on('unhandledRejection', (error) => {
  log.error('unhandled rejection', { error });
});

main().then(
  (exitCode) => {
    if (exitCode !== null) process.exit(exitCode);
  },
  (error) => {
    if (error instanceof ConfigError || error instanceof UsageError) {
      console.error(`error: ${error.message}`);
      if (error instanceof UsageError) console.error(usage);
      process.exit(2);
    }
    log.error('fatal', { error });
    process.exit(1);
  },
);
