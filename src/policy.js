const allowedSegment = /^[A-Za-z0-9._~+:@%_-]+$/;
const releaseFileNames = new Set(['InRelease', 'Release', 'Release.gpg']);
const hashLengths = { SHA256: 64, SHA512: 128, SHA1: 40, MD5Sum: 32 };
const packageFileName = /^([A-Za-z0-9][A-Za-z0-9+.~-]*)_([A-Za-z0-9.+~:-]+)_([A-Za-z0-9-]+)\.(deb|udeb|ddeb)$/;

function rejection(status, reason) {
  return { ok: false, status, reason };
}

export function parseRequestPath(rawUrl) {
  const queryStart = rawUrl.indexOf('?');
  const rawPath = queryStart === -1 ? rawUrl : rawUrl.slice(0, queryStart);
  if (!rawPath.startsWith('/')) return rejection(400, 'path must start with a slash');
  let decoded;
  try {
    decoded = decodeURIComponent(rawPath);
  } catch {
    return rejection(400, 'malformed percent-encoding');
  }
  if (decoded.includes('\0')) return rejection(400, 'NUL in path');
  if (decoded.includes('\\')) return rejection(400, 'backslash in path');
  if (decoded === '/') return rejection(404, 'root path');
  const segments = decoded.slice(1).split('/');
  for (const segment of segments) {
    if (segment === '') return rejection(400, 'empty path segment');
    if (segment.startsWith('.')) return rejection(400, 'path segment starts with a dot');
    if (!allowedSegment.test(segment)) return rejection(400, 'path segment contains a forbidden character');
  }
  return { ok: true, segments };
}

export function parsePackageFileName(fileName) {
  const match = packageFileName.exec(fileName);
  if (!match) return null;
  return { name: match[1], version: match[2], architecture: match[3] };
}

function isPoolFilePath(segments) {
  const fileName = segments.at(-1);
  if (!parsePackageFileName(fileName)) return false;
  return segments.every((segment) => !segment.includes('%'));
}

function classifyDistsFile(fileSegments, repoPathSegments) {
  if (fileSegments.length === 1 && releaseFileNames.has(fileSegments[0])) {
    return { kind: 'release', fileName: fileSegments[0] };
  }
  const poolIndex = fileSegments.indexOf('pool');
  if (poolIndex !== -1 && isPoolFilePath(fileSegments)) {
    return { kind: 'pool', repoPath: repoPathSegments.join('/'), fileName: fileSegments.at(-1) };
  }
  const count = fileSegments.length;
  if (count >= 3 && fileSegments[count - 3] === 'by-hash') {
    const algorithm = fileSegments[count - 2];
    const hash = fileSegments[count - 1].toLowerCase();
    const expectedLength = hashLengths[algorithm];
    if (!expectedLength || hash.length !== expectedLength || !/^[0-9a-f]+$/.test(hash)) return null;
    return { kind: 'by-hash', directory: fileSegments.slice(0, count - 3).join('/'), algorithm, hash };
  }
  return { kind: 'index', indexPath: fileSegments.join('/') };
}

export function routeRequest(segments, config) {
  const notFound = { kind: 'not-found' };
  const [first, second] = segments;
  if (segments.length === 2 && first === '-' && second === 'ping') return { kind: 'ping' };
  if (first === 'keys') {
    if (segments.length === 2 && Object.hasOwn(config.keys, second)) return { kind: 'key', keyName: second };
    return notFound;
  }
  if (!Object.hasOwn(config.upstreams, first)) return notFound;
  const upstream = config.upstreams[first];
  const repoPathSegments = segments.slice(1);

  if (second === 'dists') {
    const suite = segments[2];
    const fileSegments = segments.slice(3);
    if (!suite || !upstream.suites.includes(suite) || fileSegments.length === 0) return { ...notFound, upstream: first };
    const classified = classifyDistsFile(fileSegments, repoPathSegments);
    if (!classified) return { ...notFound, upstream: first };
    return { ...classified, upstream: first, suite };
  }

  if (second === 'pool') {
    if (segments.length < 4 || !isPoolFilePath(segments)) return { ...notFound, upstream: first };
    return { kind: 'pool', upstream: first, repoPath: repoPathSegments.join('/'), fileName: segments.at(-1) };
  }

  return notFound;
}

export function decidePoolAction({ cached, negativeActive, breakerOpen }) {
  if (cached) return 'serve';
  if (negativeActive) return 'not-found';
  if (breakerOpen) return 'unavailable';
  return 'fetch';
}

export function parseRangeHeader(header, size) {
  if (!header) return null;
  const match = /^bytes=(\d+)-(\d*)$/.exec(header.trim());
  if (!match) return null;
  const start = Number(match[1]);
  const end = match[2] === '' ? size - 1 : Math.min(Number(match[2]), size - 1);
  if (match[2] !== '' && Number(match[2]) < start) return null;
  if (start >= size) return { unsatisfiable: true };
  return { start, end };
}

export function isNotModified(ifModifiedSinceHeader, lastModifiedMs) {
  if (!ifModifiedSinceHeader || !Number.isFinite(lastModifiedMs)) return false;
  const since = Date.parse(ifModifiedSinceHeader);
  if (!Number.isFinite(since)) return false;
  return Math.floor(lastModifiedMs / 1000) <= Math.floor(since / 1000);
}

export function familyOfSuite(suite) {
  const dash = suite.indexOf('-');
  return dash === -1 ? suite : suite.slice(0, dash);
}

export function suiteJoinsFamily(suite, family) {
  return suite === 'nodistro' || familyOfSuite(suite) === family;
}
