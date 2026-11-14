import { createGunzip } from 'node:zlib';
import { createInterface } from 'node:readline';

const releaseHashFields = [
  ['md5sum', 'md5'],
  ['sha1', 'sha1'],
  ['sha256', 'sha256'],
  ['sha512', 'sha512'],
];

export const releaseHashNames = { MD5Sum: 'md5', SHA1: 'sha1', SHA256: 'sha256', SHA512: 'sha512' };

const commonArchitectures = [
  'all', 'amd64', 'arm64', 'armel', 'armhf', 'i386', 'mips64el', 'mipsel', 'ppc64el', 'riscv64',
  's390x', 'loong64', 'powerpc', 'ppc64', 'sparc64', 'hurd-i386', 'hurd-amd64', 'x32', 'alpha', 'hppa',
  'ia64', 'm68k', 'sh4',
];

const keptPackageFields = new Set([
  'package', 'version', 'architecture', 'filename', 'size', 'sha256', 'pre-depends', 'depends', 'recommends', 'provides',
]);

export function stripPgpSignature(text) {
  const normalized = text.replace(/\r\n/g, '\n');
  if (!normalized.startsWith('-----BEGIN PGP SIGNED MESSAGE-----')) return normalized;
  const lines = normalized.split('\n');
  let index = 1;
  while (index < lines.length && lines[index].trim() !== '') index += 1;
  const body = [];
  for (index += 1; index < lines.length; index += 1) {
    const line = lines[index];
    if (line.startsWith('-----BEGIN PGP SIGNATURE-----')) break;
    body.push(line.startsWith('- ') ? line.slice(2) : line);
  }
  return body.join('\n');
}

export function parseControlFields(text) {
  const fields = {};
  let currentField = null;
  for (const line of text.split('\n')) {
    if (line === '') {
      if (Object.keys(fields).length > 0) break;
      continue;
    }
    if (line[0] === ' ' || line[0] === '\t') {
      if (currentField) fields[currentField] += `\n${line.trim()}`;
      continue;
    }
    const colon = line.indexOf(':');
    if (colon === -1) continue;
    currentField = line.slice(0, colon).toLowerCase();
    fields[currentField] = line.slice(colon + 1).trim();
  }
  return fields;
}

export function parseRelease(text) {
  const fields = parseControlFields(stripPgpSignature(text));
  const files = {};
  for (const [fieldName, hashName] of releaseHashFields) {
    const block = fields[fieldName];
    if (!block) continue;
    for (const line of block.split('\n')) {
      const parts = line.trim().split(/\s+/);
      if (parts.length !== 3) continue;
      const [hash, size, path] = parts;
      files[path] ??= { size: Number(size) };
      files[path][hashName] = hash.toLowerCase();
    }
  }
  const words = (value) => (value ?? '').split(/\s+/).filter(Boolean);
  return {
    date: fields.date ?? null,
    validUntil: fields['valid-until'] ?? null,
    notAutomatic: fields.notautomatic === 'yes',
    acquireByHash: fields['acquire-by-hash'] === 'yes',
    suite: fields.suite ?? null,
    codename: fields.codename ?? null,
    architectures: words(fields.architectures),
    components: words(fields.components),
    files,
  };
}

function architectureOfIndexPath(segments, knownArchitectures) {
  for (const segment of segments.slice(0, -1)) {
    if (segment.startsWith('binary-')) return segment.slice('binary-'.length);
  }
  const stem = segments.at(-1).split('.')[0];
  for (const architecture of knownArchitectures) {
    if (stem.endsWith(`-${architecture}`)) return architecture;
  }
  return null;
}

export function isSelectedIndexFile(path, { components, architectures, releaseArchitectures }) {
  const component = components.find((name) => path.startsWith(`${name}/`));
  if (!component) return false;
  const segments = path.slice(component.length + 1).split('/');
  if (segments[0] === 'source' || segments[0] === 'debian-installer') return false;
  if (segments.some((segment) => segment.startsWith('installer-') || segment.startsWith('Contents-'))) return false;
  const knownArchitectures = new Set([...commonArchitectures, ...releaseArchitectures, ...architectures]);
  const architecture = architectureOfIndexPath(segments, [...knownArchitectures].sort((a, b) => b.length - a.length));
  return architecture === null || architecture === 'all' || architectures.includes(architecture);
}

export function packagesIndexCandidates(component, architecture) {
  const base = `${component}/binary-${architecture}/Packages`;
  return { plain: base, gzip: `${base}.gz`, xz: `${base}.xz` };
}

export async function parsePackagesStream(byteStream, { gzip }, onRecord) {
  let input = byteStream;
  if (gzip) {
    input = createGunzip();
    byteStream.on('error', (error) => input.destroy(error));
    byteStream.pipe(input);
  }
  const lines = createInterface({ input, crlfDelay: Infinity });
  let record = {};
  let currentField = null;
  const flush = () => {
    if (record.package && record.version && record.filename) {
      onRecord({
        name: record.package,
        version: record.version,
        architecture: record.architecture ?? 'all',
        filename: record.filename,
        size: Number(record.size),
        sha256: record.sha256?.toLowerCase() ?? null,
        preDepends: record['pre-depends'] ?? '',
        depends: record.depends ?? '',
        recommends: record.recommends ?? '',
        provides: record.provides ?? '',
      });
    }
    record = {};
    currentField = null;
  };
  for await (const line of lines) {
    if (line === '') {
      flush();
      continue;
    }
    if (line[0] === ' ' || line[0] === '\t') {
      if (currentField) record[currentField] += ` ${line.trim()}`;
      continue;
    }
    const colon = line.indexOf(':');
    if (colon === -1) {
      currentField = null;
      continue;
    }
    const field = line.slice(0, colon).toLowerCase();
    if (keptPackageFields.has(field)) {
      record[field] = line.slice(colon + 1).trim();
      currentField = field;
    } else {
      currentField = null;
    }
  }
  flush();
}

function packageNameOf(relation) {
  const name = relation.trim().split(/[\s(<>=[]/)[0];
  const colon = name.indexOf(':');
  return colon === -1 ? name : name.slice(0, colon);
}

export function parseDependencyGroups(field) {
  if (!field) return [];
  const groups = [];
  for (const group of field.split(',')) {
    const alternatives = group.split('|').map(packageNameOf).filter(Boolean);
    if (alternatives.length > 0) groups.push(alternatives);
  }
  return groups;
}

export function parseProvidedNames(field) {
  if (!field) return [];
  return field.split(',').map(packageNameOf).filter(Boolean);
}

function isDigit(character) {
  return character !== undefined && character >= '0' && character <= '9';
}

function isLetter(character) {
  return character !== undefined && ((character >= 'a' && character <= 'z') || (character >= 'A' && character <= 'Z'));
}

function characterOrder(character) {
  if (character === undefined || isDigit(character)) return 0;
  if (isLetter(character)) return character.charCodeAt(0);
  if (character === '~') return -1;
  return character.charCodeAt(0) + 256;
}

function compareVersionPart(left, right) {
  let i = 0;
  let j = 0;
  while (i < left.length || j < right.length) {
    let firstDifference = 0;
    while ((i < left.length && !isDigit(left[i])) || (j < right.length && !isDigit(right[j]))) {
      const leftOrder = characterOrder(left[i]);
      const rightOrder = characterOrder(right[j]);
      if (leftOrder !== rightOrder) return leftOrder - rightOrder;
      i += 1;
      j += 1;
    }
    while (left[i] === '0') i += 1;
    while (right[j] === '0') j += 1;
    while (isDigit(left[i]) && isDigit(right[j])) {
      if (!firstDifference) firstDifference = left.charCodeAt(i) - right.charCodeAt(j);
      i += 1;
      j += 1;
    }
    if (isDigit(left[i])) return 1;
    if (isDigit(right[j])) return -1;
    if (firstDifference) return firstDifference;
  }
  return 0;
}

export function splitVersion(version) {
  let rest = version.trim();
  let epoch = 0;
  const colon = rest.indexOf(':');
  if (colon !== -1) {
    epoch = Number(rest.slice(0, colon)) || 0;
    rest = rest.slice(colon + 1);
  }
  const dash = rest.lastIndexOf('-');
  if (dash === -1) return { epoch, upstream: rest, revision: '' };
  return { epoch, upstream: rest.slice(0, dash), revision: rest.slice(dash + 1) };
}

export function compareVersions(leftVersion, rightVersion) {
  const left = splitVersion(leftVersion);
  const right = splitVersion(rightVersion);
  if (left.epoch !== right.epoch) return left.epoch > right.epoch ? 1 : -1;
  const upstreamOrder = compareVersionPart(left.upstream, right.upstream);
  if (upstreamOrder !== 0) return Math.sign(upstreamOrder);
  return Math.sign(compareVersionPart(left.revision, right.revision));
}

export function createUniverse() {
  return { packages: new Map(), providers: new Map() };
}

export function addToUniverse(universe, record, source) {
  let versions = universe.packages.get(record.name);
  if (!versions) {
    versions = [];
    universe.packages.set(record.name, versions);
  }
  const duplicate = versions.some(
    (existing) => existing.version === record.version && existing.filename === record.filename && existing.upstream === source.upstream,
  );
  if (duplicate) return;
  versions.push({ ...record, upstream: source.upstream, suite: source.suite, notAutomatic: source.notAutomatic });
  for (const provided of parseProvidedNames(record.provides)) {
    let providers = universe.providers.get(provided);
    if (!providers) {
      providers = [];
      universe.providers.set(provided, providers);
    }
    if (!providers.includes(record.name)) providers.push(record.name);
  }
}

export function candidateOf(universe, name) {
  const versions = universe.packages.get(name);
  if (!versions || versions.length === 0) return null;
  const automatic = versions.filter((entry) => !entry.notAutomatic);
  const eligible = automatic.length > 0 ? automatic : versions;
  return eligible.reduce((best, entry) => (compareVersions(entry.version, best.version) > 0 ? entry : best));
}

export function findExactVersion(universe, name, version) {
  const versions = universe.packages.get(name);
  return versions?.find((entry) => entry.version === version) ?? null;
}

export function computeClosure(universe, rootNames, isKnown) {
  const chosen = new Map();
  const resolveName = (name) => {
    if (universe.packages.has(name)) return name;
    const providers = universe.providers.get(name);
    if (!providers || providers.length === 0) return null;
    return providers.find((provider) => isKnown(provider) || chosen.has(provider)) ?? providers[0];
  };
  const queue = [...rootNames];
  while (queue.length > 0) {
    const concreteName = resolveName(queue.pop());
    if (!concreteName || chosen.has(concreteName)) continue;
    const record = candidateOf(universe, concreteName);
    if (!record) continue;
    chosen.set(concreteName, record);
    const groups = [
      ...parseDependencyGroups(record.preDepends),
      ...parseDependencyGroups(record.depends),
      ...parseDependencyGroups(record.recommends),
    ];
    for (const alternatives of groups) {
      const available = alternatives.filter((alternative) => resolveName(alternative) !== null);
      if (available.length === 0) continue;
      const preferred = available.find((alternative) => isKnown(alternative) || chosen.has(resolveName(alternative)));
      queue.push(preferred ?? available[0]);
    }
  }
  return [...chosen.values()];
}
