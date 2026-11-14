import { createServer } from 'node:http';
import { randomBytes } from 'node:crypto';
import { performance } from 'node:perf_hooks';
import { log } from './log.js';
import { parseRequestPath, routeRequest } from './policy.js';
import { serveRoute, sendEmpty } from './proxy.js';

function logRequest(ctx, aborted) {
  if (ctx.logged) return;
  ctx.logged = true;
  log.info('request', {
    reqId: ctx.reqId,
    method: ctx.req.method,
    path: ctx.req.url,
    upstream: ctx.upstream,
    kind: ctx.kind,
    cache: ctx.cache,
    status: ctx.res.statusCode,
    durationMs: Math.round(performance.now() - ctx.startedAt),
    bytes: ctx.bytes,
    aborted: aborted || undefined,
  });
}

async function routeAndServe(context, ctx) {
  const { req, res } = ctx;
  if (req.method !== 'GET' && req.method !== 'HEAD') {
    ctx.kind = 'rejected';
    return sendEmpty(ctx, 405, { allow: 'GET, HEAD' });
  }
  const parsed = parseRequestPath(req.url);
  if (!parsed.ok) {
    ctx.kind = 'rejected';
    log.debug('rejected request path', { reqId: ctx.reqId, reason: parsed.reason });
    return sendEmpty(ctx, parsed.status);
  }
  const route = routeRequest(parsed.segments, context.config);
  ctx.kind = route.kind;
  ctx.upstream = route.upstream ?? null;
  res.setHeader('x-request-id', ctx.reqId);
  await serveRoute(context, ctx, route);
}

async function handleRequest(context, req, res) {
  const ctx = {
    req,
    res,
    reqId: randomBytes(4).toString('hex'),
    startedAt: performance.now(),
    kind: null,
    upstream: null,
    cache: null,
    bytes: 0,
    logged: false,
  };
  res.on('finish', () => logRequest(ctx, false));
  res.on('close', () => logRequest(ctx, !res.writableFinished));
  try {
    await routeAndServe(context, ctx);
  } catch (error) {
    log.error('request handler failed', { reqId: ctx.reqId, path: req.url, error });
    if (!res.headersSent) {
      sendEmpty(ctx, 500);
    } else {
      res.destroy();
    }
  }
}

export function createMirrorServer(context) {
  const server = createServer((req, res) => {
    handleRequest(context, req, res);
  });
  server.headersTimeout = 10_000;
  server.requestTimeout = 0;
  server.keepAliveTimeout = 30_000;
  server.on('clientError', (error, socket) => {
    if (socket.writable && !socket.destroyed) socket.end('HTTP/1.1 400 Bad Request\r\nContent-Length: 0\r\nConnection: close\r\n\r\n');
    else socket.destroy();
    log.debug('client error', { error });
  });
  return server;
}
