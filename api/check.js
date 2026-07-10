// api/check.js — the 5-minute sampler, triggered by Vercel Cron (Pro plan).
// Vercel automatically sends: Authorization: Bearer <CRON_SECRET>
// Manual trigger also works: /api/check?secret=<CRON_SECRET>
//
// Metric: cumulative net balance change over the last 24 hours.
// Alerts are edge-triggered: one push when the 24h change crosses the
// threshold, re-armed when it falls back below 80% of the threshold.

const WATCHED = {
  "0x006243e4eE6C2CF6F993036f27f0A88f265Ddb4a": "MEXC",
  "0x007e2F1a4709B812F339f22E18032118FBcc8987": "Gate.io",
  "0x004cd3a9997a5170A9234CEddD1ec5DCE7Db23c3": "Kraken cold",
};

const DEFAULT_THRESHOLD_PCT = 1; // alert if 24h net change exceeds 1%
const WEI = 10n ** 18n;
const STATE_KEY = "quai_monitor_state";
const CONFIG_KEY = "quai_monitor_config";
const EXPLORER = "https://quaiscan.io";

// Rolling history: hourly samples for the recent ~8 days, thinned to
// one sample per ~6h beyond that, kept ~30.5 days total.
const SAMPLE_EVERY_MS = 55 * 60 * 1000;
const KEEP_MS = 30.5 * 24 * 3600 * 1000;
const DENSE_MS = 8 * 24 * 3600 * 1000;      // full hourly resolution window
const THIN_EVERY_MS = 5.5 * 3600 * 1000;    // resolution beyond that
const DAY_MS = 24 * 3600 * 1000;
const WEEK_MS = 7 * 24 * 3600 * 1000;
const WEEK_MIN_MS = 6 * 24 * 3600 * 1000;
const MONTH_MS = 30 * 24 * 3600 * 1000;
const MONTH_MIN_MS = 26 * 24 * 3600 * 1000;
const REARM_FRACTION = 0.8; // hysteresis: re-arm below 80% of threshold

// --- Upstash Redis (REST) ---------------------------------------------
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

async function redisGet(key) {
  const j = await redisCall(`/get/${key}`);
  return j.result ? JSON.parse(j.result) : null;
}

async function redisSet(key, value) {
  await redisCall(`/set/${key}`, { method: "POST", body: JSON.stringify(value) });
}

// --- Balances via official Quai JSON-RPC --------------------------------
const RPC_URL = process.env.QUAI_RPC_URL || "https://rpc.quai.network/cyprus1/";

function weiToQuai(weiStr) {
  const wei = BigInt(weiStr);
  return Number((wei * 100n) / WEI) / 100;
}

async function rpcGetBalance(address, id) {
  const r = await fetch(RPC_URL, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({
      jsonrpc: "2.0",
      method: "quai_getBalance",
      params: [address, "latest"],
      id,
    }),
  });
  const text = await r.text();
  let j;
  try {
    j = JSON.parse(text);
  } catch (_) {
    throw new Error(
      `Non-JSON from RPC ${RPC_URL} — HTTP ${r.status}, body starts: ${text.slice(0, 120).replace(/\s+/g, " ")}`
    );
  }
  if (j.error) throw new Error(`RPC error for ${address}: ${JSON.stringify(j.error)}`);
  if (typeof j.result !== "string") {
    throw new Error(`RPC gave no balance for ${address}: ${text.slice(0, 120)}`);
  }
  return weiToQuai(BigInt(j.result).toString());
}

async function fetchBalances() {
  const out = {};
  let id = 1;
  for (const a of Object.keys(WATCHED)) {
    out[a] = await rpcGetBalance(a, id++);
  }
  return out;
}

// --- History reference lookups ------------------------------------------
// Sample nearest to (now - periodMs); if history is younger than the
// period, fall back to the oldest sample (partial window).
function dayReference(history, addr, nowMs) {
  const candidates = history.filter((s) => s.b[addr] !== undefined);
  if (!candidates.length) return null;
  const target = nowMs - DAY_MS;
  let best = candidates[0];
  let bestDist = Math.abs(new Date(best.t).getTime() - target);
  for (const s of candidates) {
    const dist = Math.abs(new Date(s.t).getTime() - target);
    if (dist < bestDist) {
      bestDist = dist;
      best = s;
    }
  }
  const tMs = new Date(best.t).getTime();
  return { bal: best.b[addr], spanMs: nowMs - tMs, partial: nowMs - tMs < DAY_MS * 0.85 };
}

// Sample nearest to (now - periodMs); null until history is at least minMs old.
function periodReference(history, addr, nowMs, periodMs, minMs) {
  const candidates = history.filter((s) => s.b[addr] !== undefined);
  if (!candidates.length) return null;
  const oldest = new Date(candidates[0].t).getTime();
  if (nowMs - oldest < minMs) return null;
  const target = nowMs - periodMs;
  let best = candidates[0];
  let bestDist = Math.abs(new Date(best.t).getTime() - target);
  for (const s of candidates) {
    const dist = Math.abs(new Date(s.t).getTime() - target);
    if (dist < bestDist) {
      bestDist = dist;
      best = s;
    }
  }
  return { bal: best.b[addr] };
}

// --- Push via ntfy.sh ---------------------------------------------------
async function sendPush(title, body) {
  const topic = process.env.NTFY_TOPIC;
  if (!topic) return;
  await fetch(`https://ntfy.sh/${topic}`, {
    method: "POST",
    headers: {
      Title: title,
      Priority: "high",
      Tags: "rotating_light,whale",
    },
    body,
  });
}

// --- Handler ------------------------------------------------------------
module.exports = async (req, res) => {
  try {
    const secret = process.env.CRON_SECRET;
    const auth = req.headers["authorization"] || "";
    const qsSecret = (req.query && req.query.secret) || "";
    if (!secret || (auth !== `Bearer ${secret}` && qsSecret !== secret)) {
      return res.status(401).json({ error: "unauthorized" });
    }
    const envErr = redisEnvError();
    if (envErr) return res.status(500).json({ error: envErr });

    const now = new Date().toISOString();
    const nowMs = Date.now();
    const config = (await redisGet(CONFIG_KEY)) || {};
    const thresholdPct =
      Number(config.thresholdPercent) > 0
        ? Number(config.thresholdPercent)
        : DEFAULT_THRESHOLD_PCT;

    const balances = await fetchBalances();
    const state = (await redisGet(STATE_KEY)) || { exchanges: {}, alerts: [] };
    if (!Array.isArray(state.history)) state.history = [];

    // Append hourly sample, prune old ones
    const lastSample = state.history.length
      ? new Date(state.history[state.history.length - 1].t).getTime()
      : 0;
    if (nowMs - lastSample >= SAMPLE_EVERY_MS || !state.history.length) {
      state.history.push({ t: now, b: balances });
    }
    // Prune beyond 30.5 days, and thin resolution for samples older than
    // the dense window (keep ~6h spacing) to cap storage size.
    const kept = [];
    let lastOldKept = 0;
    for (const s of state.history) {
      const age = nowMs - new Date(s.t).getTime();
      if (age > KEEP_MS) continue;
      if (age <= DENSE_MS) {
        kept.push(s);
      } else {
        const tMs = new Date(s.t).getTime();
        if (tMs - lastOldKept >= THIN_EVERY_MS) {
          kept.push(s);
          lastOldKept = tMs;
        }
      }
    }
    state.history = kept;

    const fired = [];

    for (const [addr, bal] of Object.entries(balances)) {
      const name = WATCHED[addr];
      const prev = state.exchanges[addr] || {};

      const dayRef = dayReference(state.history, addr, nowMs);
      const weekRef = periodReference(state.history, addr, nowMs, WEEK_MS, WEEK_MIN_MS);
      const monthRef = periodReference(state.history, addr, nowMs, MONTH_MS, MONTH_MIN_MS);

      const dayDelta = dayRef ? bal - dayRef.bal : null;
      const dayPct =
        dayRef && dayRef.bal > 0 ? (dayDelta / dayRef.bal) * 100 : null;
      const weekDelta = weekRef ? bal - weekRef.bal : null;
      const weekPct =
        weekRef && weekRef.bal > 0 ? (weekDelta / weekRef.bal) * 100 : null;
      const monthDelta = monthRef ? bal - monthRef.bal : null;
      const monthPct =
        monthRef && monthRef.bal > 0 ? (monthDelta / monthRef.bal) * 100 : null;

      // Edge-triggered alert on the 24h (or partial-window) change
      let alertActive = Boolean(prev.alertActive);
      if (dayPct !== null) {
        const magnitude = Math.abs(dayPct);
        if (!alertActive && magnitude > thresholdPct) {
          alertActive = true;
          const dir = dayDelta > 0 ? "INFLOW" : "OUTFLOW";
          const windowLabel = dayRef.partial
            ? `${Math.round(dayRef.spanMs / 3600000)}h`
            : "24h";
          const alert = {
            time: now,
            exchange: name,
            address: addr,
            direction: dir,
            window: windowLabel,
            delta: dayDelta,
            deltaPct: dayPct,
            threshold: thresholdPct,
            from: dayRef.bal,
            to: bal,
          };
          fired.push(alert);
          state.alerts.unshift(alert);
        } else if (alertActive && magnitude < thresholdPct * REARM_FRACTION) {
          alertActive = false; // change subsided; re-arm
        }
      }

      state.exchanges[addr] = {
        name,
        balance: bal,
        dayDelta,
        dayPct,
        daySpanH: dayRef ? Math.round(dayRef.spanMs / 3600000) : null,
        dayPartial: dayRef ? dayRef.partial : true,
        weekDelta,
        weekPct,
        monthDelta,
        monthPct,
        alertActive,
        checked: now,
      };
    }

    state.alerts = state.alerts.slice(0, 20);
    state.lastCheck = now;
    state.thresholdPercent = thresholdPct;
    await redisSet(STATE_KEY, state);

    for (const a of fired) {
      await sendPush(
        `${a.exchange} ${a.window} ${a.direction}: ${Math.abs(a.deltaPct).toFixed(2)}%`,
        `${a.delta > 0 ? "+" : "−"}${Math.round(Math.abs(a.delta)).toLocaleString("en-US")} QUAI over ${a.window}\n` +
          `${a.from.toLocaleString("en-US")} → ${a.to.toLocaleString("en-US")}\n` +
          `${EXPLORER}/address/${a.address}`
      );
    }

    return res.status(200).json({
      ok: true,
      checked: now,
      thresholdPercent: thresholdPct,
      alertsFired: fired.length,
      balances,
    });
  } catch (e) {
    console.error(e);
    return res.status(500).json({ error: String((e && e.message) || e) });
  }
};
