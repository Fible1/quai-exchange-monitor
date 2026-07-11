// api/history.js — public, read-only. Feeds the price-vs-flows chart.
// Returns { history: [{t, b, p?}], price: [[ms, close]] }
// history = our own balance samples; price = MEXC hourly kline backfill.

const REDIS_URL =
  process.env.UPSTASH_REDIS_REST_URL || process.env.KV_REST_API_URL;
const REDIS_TOKEN =
  process.env.UPSTASH_REDIS_REST_TOKEN || process.env.KV_REST_API_TOKEN;

function redisEnvError() {
  if (!REDIS_URL || !REDIS_TOKEN) {
    return "Redis env vars not set. Add UPSTASH_REDIS_REST_URL and UPSTASH_REDIS_REST_TOKEN, then redeploy.";
  }
  if (!/^https:\/\//.test(REDIS_URL)) {
    return `UPSTASH_REDIS_REST_URL must start with https:// (currently: "${REDIS_URL}").`;
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
      `Redis endpoint returned non-JSON (HTTP ${r.status}). Body starts: ${text.slice(0, 120).replace(/\s+/g, " ")}`
    );
  }
  if (j.error) throw new Error(`Redis error: ${j.error}`);
  return j;
}

async function mexcHourly() {
  try {
    const ctl = new AbortController();
    const timer = setTimeout(() => ctl.abort(), 10000);
    const r = await fetch(
      "https://api.mexc.com/api/v3/klines?symbol=QUAIUSDT&interval=60m&limit=720",
      { signal: ctl.signal }
    ).finally(() => clearTimeout(timer));
    const j = await r.json();
    if (!Array.isArray(j)) return [];
    // kline: [openTime(ms), open, high, low, close, volume, closeTime, ...]
    return j.map((c) => [c[0], parseFloat(c[4])]);
  } catch (_) {
    return [];
  }
}

module.exports = async (req, res) => {
  try {
    const envErr = redisEnvError();
    if (envErr) return res.status(500).json({ error: envErr });

    const j = await redisCall("/get/quai_monitor_state");
    const state = j.result ? JSON.parse(j.result) : {};
    const history = Array.isArray(state.history) ? state.history : [];
    const price = await mexcHourly();

    res.setHeader("Cache-Control", "public, max-age=120");
    return res.status(200).json({ history, price });
  } catch (e) {
    console.error(e);
    return res.status(500).json({ error: String((e && e.message) || e) });
  }
};
