import { readFileSync, existsSync, copyFileSync } from 'node:fs';
import { dirname, join, resolve } from 'node:path';
import { fileURLToPath } from 'node:url';

export const projectDir = resolve(dirname(fileURLToPath(import.meta.url)), '..');
export const dataDir = join(projectDir, 'data');

const defaultConfigPath = join(projectDir, 'config.json');
const exampleConfigPath = join(projectDir, 'config.example.json');

const upstreamNamePattern = /^[a-z0-9-]+$/;
const suitePattern = /^[A-Za-z0-9_~+-][A-Za-z0-9._~+-]*$/;
const componentPattern = /^[A-Za-z0-9_~+-][A-Za-z0-9._~+-]*(\/[A-Za-z0-9_~+-][A-Za-z0-9._~+-]*)*$/;
const architecturePattern = /^[a-z0-9-]+$/;
const keyNamePattern = /^[A-Za-z0-9_~+-][A-Za-z0-9._~+-]*$/;
const reservedUpstreamNames = new Set(['keys', '-']);

export class ConfigError extends Error {
  name = 'ConfigError';
}

export function loadConfig(configPath) {
  const path = configPath ? resolve(configPath) : defaultConfigPath;
  if (!configPath && !existsSync(path)) copyFileSync(exampleConfigPath, path);
  let raw;
  try {
    raw = JSON.parse(readFileSync(path, 'utf8'));
  } catch (error) {
    throw new ConfigError(`cannot read config file ${path}: ${error.message}`);
  }
  return { ...validateConfig(raw), path };
}

function fail(message) {
  throw new ConfigError(message);
}

function requireObject(value, where) {
  if (value === null || typeof value !== 'object' || Array.isArray(value)) fail(`${where} must be an object`);
  return value;
}

function rejectUnknownFields(value, allowedFields, where) {
  for (const field of Object.keys(value)) {
    if (!allowedFields.includes(field)) fail(`unknown field "${field}" in ${where}`);
  }
}

function requirePositiveInteger(value, where) {
  if (!Number.isInteger(value) || value <= 0) fail(`${where} must be a positive integer`);
  return value;
}

function requireString(value, where) {
  if (typeof value !== 'string' || value.length === 0) fail(`${where} must be a non-empty string`);
  return value;
}

function requireHttpUrl(value, where) {
  requireString(value, where);
  const trimmed = value.replace(/\/+$/, '');
  let parsed;
  try {
    parsed = new URL(trimmed);
  } catch {
    fail(`${where} is not a valid URL: ${value}`);
  }
  if (parsed.protocol !== 'http:' && parsed.protocol !== 'https:') fail(`${where} must use http or https`);
  if (parsed.search || parsed.hash) fail(`${where} must not contain a query or fragment`);
  return trimmed;
}

function requireNonEmptyList(value, pattern, where) {
  if (!Array.isArray(value) || value.length === 0) fail(`${where} must be a non-empty array`);
  const seen = new Set();
  for (const item of value) {
    if (typeof item !== 'string' || !pattern.test(item)) fail(`${where} contains an invalid entry: ${JSON.stringify(item)}`);
    if (seen.has(item)) fail(`${where} contains "${item}" twice`);
    seen.add(item);
  }
  return [...value];
}

function validateUpstream(name, value) {
  const where = `upstreams.${name}`;
  if (!upstreamNamePattern.test(name)) fail(`upstream name "${name}" must match [a-z0-9-]+`);
  if (reservedUpstreamNames.has(name)) fail(`upstream name "${name}" is reserved`);
  requireObject(value, where);
  rejectUnknownFields(value, ['url', 'suites', 'components', 'architectures'], where);
  return {
    url: requireHttpUrl(value.url, `${where}.url`),
    suites: requireNonEmptyList(value.suites, suitePattern, `${where}.suites`),
    components: requireNonEmptyList(value.components, componentPattern, `${where}.components`),
    architectures: requireNonEmptyList(value.architectures, architecturePattern, `${where}.architectures`),
  };
}

export function validateConfig(raw) {
  requireObject(raw, 'config');
  rejectUnknownFields(
    raw,
    ['listen', 'refreshMinutes', 'negativeSeconds', 'maxPromotionDelayHours', 'breaker', 'timeouts', 'upstreams', 'keys'],
    'config',
  );

  const listen = requireObject(raw.listen ?? { host: '0.0.0.0', port: 3142 }, 'listen');
  rejectUnknownFields(listen, ['host', 'port'], 'listen');
  requireString(listen.host, 'listen.host');
  if (!Number.isInteger(listen.port) || listen.port < 0 || listen.port > 65535) fail('listen.port must be an integer between 0 and 65535');

  const breaker = requireObject(raw.breaker ?? { threshold: 5, openMs: 30000, maxOpenMs: 300000 }, 'breaker');
  rejectUnknownFields(breaker, ['threshold', 'openMs', 'maxOpenMs'], 'breaker');
  requirePositiveInteger(breaker.threshold, 'breaker.threshold');
  requirePositiveInteger(breaker.openMs, 'breaker.openMs');
  requirePositiveInteger(breaker.maxOpenMs, 'breaker.maxOpenMs');
  if (breaker.maxOpenMs < breaker.openMs) fail('breaker.maxOpenMs must be at least breaker.openMs');

  const timeouts = requireObject(raw.timeouts ?? { metadataHeadersMs: 10000, poolHeadersMs: 30000 }, 'timeouts');
  rejectUnknownFields(timeouts, ['metadataHeadersMs', 'poolHeadersMs'], 'timeouts');
  requirePositiveInteger(timeouts.metadataHeadersMs, 'timeouts.metadataHeadersMs');
  requirePositiveInteger(timeouts.poolHeadersMs, 'timeouts.poolHeadersMs');

  const refreshMinutes = requirePositiveInteger(raw.refreshMinutes ?? 60, 'refreshMinutes');
  const negativeSeconds = requirePositiveInteger(raw.negativeSeconds ?? 60, 'negativeSeconds');
  const maxPromotionDelayHours = requirePositiveInteger(raw.maxPromotionDelayHours ?? 24, 'maxPromotionDelayHours');

  const rawUpstreams = requireObject(raw.upstreams, 'upstreams');
  if (Object.keys(rawUpstreams).length === 0) fail('upstreams must define at least one upstream');
  const upstreams = {};
  for (const [name, value] of Object.entries(rawUpstreams)) upstreams[name] = validateUpstream(name, value);

  const rawKeys = requireObject(raw.keys ?? {}, 'keys');
  const keys = {};
  for (const [name, url] of Object.entries(rawKeys)) {
    if (!keyNamePattern.test(name)) fail(`key name "${name}" contains invalid characters`);
    keys[name] = requireHttpUrl(url, `keys.${name}`);
  }

  return {
    listen: { host: listen.host, port: listen.port },
    refreshMinutes,
    negativeSeconds,
    maxPromotionDelayHours,
    breaker: { ...breaker },
    timeouts: { ...timeouts },
    upstreams,
    keys,
  };
}
