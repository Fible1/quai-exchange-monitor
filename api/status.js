// api/status.js — read-only state for the dashboard page.

const REDIS_URL =
  process.env.UPSTASH_REDIS_REST_URL || process.env.KV_REST_API_URL;
const REDIS_TOKEN =
  process.env.UPSTASH_REDIS_REST_TOKEN || process.env.KV_REST_API_TOKEN;

function redisEnvError() {
  if (!REDIS_URL || !REDIS_TOKEN) {
    return "Redis env vars not set. Add UPSTASH_REDIS_REST_URL and UPSTASH_REDIS_REST_TOKEN, then redeploy.";
  }
  if (!/^https:\/\//.test(REDIS_URL)) {
    return `UPSTASH_REDIS_REST_URL must start with https:// (currently: "${REDIS_URL}"). Use the REST endpoint, not the redis:// TCP string.`;
  }
  if (/:\d+$/.test(REDIS_URL) || REDIS_URL.includes("@")) {
    return `UPSTASH_REDIS_REST_URL looks like a TCP connection string ("${REDIS_URL}"). Use https://<name>.upstash.io with no port.`;
  }
  return null;
}

async function redisCall(path, opts = {}) {
  const r = await fetch(`${REDIS_URL.replace(/\/$/, "")}${path}`, {
    ...opts,
    headers: { Authorization: `Bearer ${REDIS_TOKEN}`, ...(opts.headers || {}) },
  });
  const text = await r.text();
  let j;
  try {
    j = JSON.parse(text);
  } catch (_) {
    throw new Error(
      `Redis endpoint returned non-JSON (HTTP ${r.status}) — check UPSTASH_REDIS_REST_URL. Body starts: ${text.slice(0, 120).replace(/\s+/g, " ")}`
    );
  }
  if (j.error) throw new Error(`Redis error: ${j.error}`);
  return j;
}


module.exports = async (req, res) => {
  try {
  const envErr = redisEnvError();
  if (envErr) return res.status(500).json({ error: envErr });
  const j = await redisCall("/get/quai_monitor_state");
  res.setHeader("Cache-Control", "no-store");
  const state = j.result ? JSON.parse(j.result) : { exchanges: {}, alerts: [] };
  delete state.history; // internal working data, not needed by the page
  state.appVersion = "v12-avg-bar";
  return res.status(200).json(state);
  } catch (e) {
    console.error(e);
    return res
      .status(500)
      .json({ error: String((e && e.message) || e) });
  }
};
