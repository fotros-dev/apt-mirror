import { readFile } from 'node:fs/promises';
import { basename } from 'node:path';
import { log } from './log.js';
import { candidateOf, findExactVersion, computeClosure } from './deb.js';
import { familyOfSuite } from './policy.js';
import { loadFamilyUniverse, downloadPackageRecords, configuredSuites } from './snapshot.js';

export function parseExportFile(text, source) {
  const lines = text.split(/\r?\n/);
  const header = /^#os\s+([a-z0-9._-]+):([A-Za-z0-9._-]+)\s+([a-z0-9-]+)\s*$/.exec(lines[0] ?? '');
  if (!header) throw new Error(`${source}: first line must look like "#os ubuntu:noble amd64"`);
  const entries = [];
  for (const line of lines.slice(1)) {
    const trimmed = line.trim();
    if (!trimmed || trimmed.startsWith('#')) continue;
    const [name, version, architecture] = trimmed.split(/\s+/);
    entries.push({ name, version: version || null, architecture: architecture || header[3] });
  }
  return { source, osId: header[1], codename: header[2], architecture: header[3], entries };
}

export async function readExportFiles(paths) {
  const machines = [];
  for (const path of paths) machines.push(parseExportFile(await readFile(path, 'utf8'), basename(path)));
  return machines;
}

export function machineFromNames({ os, architecture, names }) {
  const match = /^([a-z0-9._-]+):([A-Za-z0-9._-]+)$/.exec(os ?? '');
  if (!match) throw new Error('--os must look like ubuntu:noble');
  if (!architecture) throw new Error('--arch is required together with --os');
  return {
    source: 'command line',
    osId: match[1],
    codename: match[2],
    architecture,
    entries: names.map((name) => ({ name, version: null, architecture })),
  };
}

function configuredFamilies(config, architecture) {
  const families = new Set();
  for (const { upstreamName, suite } of configuredSuites(config)) {
    if (suite === 'nodistro') continue;
    if (config.upstreams[upstreamName].architectures.includes(architecture)) families.add(familyOfSuite(suite));
  }
  return families;
}

async function warmMachine(context, machine, { closure }) {
  const { config, known } = context;
  const architecture = machine.architecture;
  if (!configuredFamilies(config, architecture).has(machine.codename)) {
    throw new Error(`${machine.source}: no configured upstream serves ${machine.codename} for ${architecture}`);
  }
  const { universe } = await loadFamilyUniverse(config, machine.codename, architecture);
  if (universe.packages.size === 0) {
    throw new Error(`${machine.source}: no Packages indices are stored for ${machine.codename}/${architecture}; run refresh first`);
  }
  const wanted = new Map();
  const want = (record) => wanted.set(`${record.upstream}/${record.filename}`, record);
  const notFound = [];
  const foreign = [];
  const resolvedNames = [];
  for (const entry of machine.entries) {
    if (entry.architecture !== architecture && entry.architecture !== 'all') {
      foreign.push(`${entry.name}:${entry.architecture}`);
      continue;
    }
    known.add(entry.name, entry.architecture);
    const exact = entry.version ? findExactVersion(universe, entry.name, entry.version) : null;
    const candidate = candidateOf(universe, entry.name);
    if (exact) want(exact);
    if (candidate) want(candidate);
    if (!exact && !candidate) {
      notFound.push(entry.version ? `${entry.name} ${entry.version}` : entry.name);
      continue;
    }
    resolvedNames.push(entry.name);
  }
  if (closure) {
    for (const record of computeClosure(universe, resolvedNames, (name) => known.has(name, architecture))) want(record);
  }
  const records = [...wanted.values()];
  for (const record of records) known.add(record.name, record.architecture);
  const outcome = await downloadPackageRecords(config, records);
  return { machine: machine.source, family: machine.codename, architecture, outcome, notFound, foreign };
}

export async function warm(context, machines, { closure = true } = {}) {
  await context.known.reload();
  const reports = [];
  for (const machine of machines) {
    log.info('warming', { machine: machine.source, family: machine.codename, architecture: machine.architecture, entries: machine.entries.length });
    reports.push(await warmMachine(context, machine, { closure }));
    await context.known.flush();
  }
  return reports;
}
