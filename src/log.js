const levelRank = { debug: 10, info: 20, warn: 30, error: 40 };
const minimumRank = levelRank[process.env.LOG_LEVEL] ?? levelRank.info;

function expandError(error) {
  const expanded = { name: error.name, message: error.message, stack: error.stack };
  if (error.code) expanded.code = error.code;
  if (error.cause !== undefined) expanded.cause = error.cause instanceof Error ? expandError(error.cause) : String(error.cause);
  return expanded;
}

function write(level, msg, fields = {}) {
  if (levelRank[level] < minimumRank) return;
  const entry = { ts: new Date().toISOString(), level, msg };
  for (const [key, value] of Object.entries(fields)) {
    entry[key] = value instanceof Error ? expandError(value) : value;
  }
  process.stdout.write(`${JSON.stringify(entry)}\n`);
}

export const log = {
  debug: (msg, fields) => write('debug', msg, fields),
  info: (msg, fields) => write('info', msg, fields),
  warn: (msg, fields) => write('warn', msg, fields),
  error: (msg, fields) => write('error', msg, fields),
};
