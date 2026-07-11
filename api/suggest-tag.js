// api/suggest-tag.js — public endpoint: visitors suggest a label for an
// address seen in the whale feed. Suggestions land in a pending queue for
// admin approval; nothing goes live without it.
// Guards: address validation, label sanitization, queue cap, dedupe.

const REDIS_URL =
  process.env.UPSTASH_REDIS_REST_URL || process.env.KV_REST_API_URL;
const REDIS_TOKEN =
  process.env.UPSTASH_REDIS_REST_TOKEN || process.env.KV_REST_API_TOKEN;
const SUBS_KEY = "quai_monitor_tag_submissions";
const MAX_PENDING = 50;

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
    throw new Error(`Redis endpoint returned non-JSON (HTTP ${r.status})`);
  }
  if (j.error) throw new Error(`Redis error: ${j.error}`);
  return j;
}

module.exports = async (req, res) => {
  try {
    if (req.method !== "POST") {
      return res.status(405).json({ error: "method not allowed" });
    }
    if (!REDIS_URL || !REDIS_TOKEN) {
      return res.status(500).json({ error: "storage not configured" });
    }

    const body = req.body || {};
    const addr = typeof body.addr === "string" ? body.addr.trim() : "";
    if (!/^0x[0-9a-fA-F]{40}$/.test(addr)) {
      return res.status(400).json({ error: "invalid address" });
    }
    // Sanitize: strip HTML-significant characters, collapse whitespace.
    let label = typeof body.label === "string" ? body.label : "";
    label = label.replace(/[<>&"'`]/g, "").replace(/\s+/g, " ").trim().slice(0, 40);
    if (label.length < 3) {
      return res.status(400).json({ error: "label must be 3-40 characters" });
    }

    const j = await redisCall(`/get/${SUBS_KEY}`);
    const pending = j.result ? JSON.parse(j.result) : [];

    if (pending.length >= MAX_PENDING) {
      return res.status(429).json({ error: "suggestion queue is full — try again later" });
    }
    const addrL = addr.toLowerCase();
    if (pending.some((p) => p.addr === addrL && p.label.toLowerCase() === label.toLowerCase())) {
      return res.status(200).json({ ok: true, note: "already suggested" });
    }

    pending.push({
      id: Date.now().toString(36) + Math.random().toString(36).slice(2, 7),
      addr: addrL,
      label,
      t: new Date().toISOString(),
    });
    await redisCall(`/set/${SUBS_KEY}`, {
      method: "POST",
      body: JSON.stringify(pending),
    });

    return res.status(200).json({ ok: true });
  } catch (e) {
    console.error(e);
    return res.status(500).json({ error: String((e && e.message) || e) });
  }
};
