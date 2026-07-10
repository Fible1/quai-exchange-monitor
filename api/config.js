// api/config.js — read or update the alert threshold (% of balance).
// GET  → { thresholdPercent }
// POST → body { thresholdPercent, secret }  (secret = CRON_SECRET)

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

const CONFIG_KEY = "quai_monitor_config";
const DEFAULT_THRESHOLD_PCT = 1;

async function redisGet(key) {
  const j = await redisCall(`/get/${key}`);
  return j.result ? JSON.parse(j.result) : null;
}

async function redisSet(key, value) {
  await redisCall(`/set/${key}`, { method: "POST", body: JSON.stringify(value) });
}

module.exports = async (req, res) => {
  try {
  const envErr = redisEnvError();
  if (envErr) return res.status(500).json({ error: envErr });

  if (req.method === "GET") {
    const config = (await redisGet(CONFIG_KEY)) || {};
    res.setHeader("Cache-Control", "no-store");
    const out = {
      thresholdPercent:
        Number(config.thresholdPercent) > 0
          ? Number(config.thresholdPercent)
          : DEFAULT_THRESHOLD_PCT,
      whaleTxPercent:
        Number(config.whaleTxPercent) > 0
          ? Number(config.whaleTxPercent)
          : 0.5,
    };
    // The private watchlist is only returned when the secret is supplied.
    const qsSecret = (req.query && req.query.secret) || "";
    if (process.env.CRON_SECRET && qsSecret === process.env.CRON_SECRET) {
      out.watchedAddresses = Array.isArray(config.watchedAddresses)
        ? config.watchedAddresses
        : [];
    }
    return res.status(200).json(out);
  }

  if (req.method === "POST") {
    const body = req.body || {};
    if (!process.env.CRON_SECRET || body.secret !== process.env.CRON_SECRET) {
      return res.status(401).json({ error: "wrong secret" });
    }
    // Partial updates: only the fields present in the body are changed.
    const existing = (await redisGet(CONFIG_KEY)) || {};
    const next = { ...existing, updated: new Date().toISOString() };

    if (body.thresholdPercent !== undefined) {
      const pct = Number(body.thresholdPercent);
      if (!Number.isFinite(pct) || pct <= 0 || pct > 100) {
        return res
          .status(400)
          .json({ error: "thresholdPercent must be between 0 and 100" });
      }
      next.thresholdPercent = pct;
    }
    if (body.whaleTxPercent !== undefined) {
      const wp = Number(body.whaleTxPercent);
      if (!Number.isFinite(wp) || wp <= 0 || wp > 10) {
        return res
          .status(400)
          .json({ error: "whaleTxPercent must be between 0 and 10" });
      }
      next.whaleTxPercent = wp;
    }
    if (body.watchedAddresses !== undefined) {
      if (!Array.isArray(body.watchedAddresses) || body.watchedAddresses.length > 20) {
        return res
          .status(400)
          .json({ error: "watchedAddresses must be an array of at most 20 entries" });
      }
      const cleaned = [];
      for (const w of body.watchedAddresses) {
        const addr = w && typeof w.addr === "string" ? w.addr.trim() : "";
        if (!/^0x[0-9a-fA-F]{40}$/.test(addr)) {
          return res
            .status(400)
            .json({ error: `invalid address: "${addr.slice(0, 50)}" — must be 0x + 40 hex chars` });
        }
        const label =
          w && typeof w.label === "string" ? w.label.trim().slice(0, 40) : "";
        cleaned.push({ addr, label });
      }
      next.watchedAddresses = cleaned;
    }
    await redisSet(CONFIG_KEY, next);
    return res.status(200).json({
      ok: true,
      thresholdPercent: next.thresholdPercent,
      whaleTxPercent: next.whaleTxPercent,
    });
  }

  return res.status(405).json({ error: "method not allowed" });
  } catch (e) {
    console.error(e);
    return res
      .status(500)
      .json({ error: String((e && e.message) || e) });
  }
};
