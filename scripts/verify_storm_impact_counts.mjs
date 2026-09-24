#!/usr/bin/env node

const SUPABASE_URL = process.env.SUPABASE_URL;
const SUPABASE_ACCESS_TOKEN = process.env.SUPABASE_ACCESS_TOKEN;
const SERPER_API_KEY = process.env.SERPER_API_KEY;

if (!SUPABASE_URL || !SUPABASE_ACCESS_TOKEN || !SERPER_API_KEY) {
  console.error("Missing SUPABASE_URL, SUPABASE_ACCESS_TOKEN, or SERPER_API_KEY");
  process.exit(1);
}

const args = Object.fromEntries(process.argv.slice(2).map((arg) => {
  const [key, ...rest] = arg.replace(/^--/, "").split("=");
  return [key, rest.length ? rest.join("=") : "true"];
}));
const force = args.force === "true";
const unverifiedOnly = args.unverified !== "false";
const limit = Math.min(2000, Math.max(1, Number(args.limit) || 100));
const offset = Math.max(0, Number(args.offset) || 0);
const days = Math.max(0, Number(args.days) || 0);
const projectRef = new URL(SUPABASE_URL).hostname.split(".")[0];

function validDate(value) {
  return /^\d{4}-\d{2}-\d{2}$/.test(String(value || "")) &&
    !Number.isNaN(Date.parse(`${value}T00:00:00Z`));
}

function sleep(ms) {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

async function dbQuery(query) {
  const response = await fetch(
    `https://api.supabase.com/v1/projects/${projectRef}/database/query`,
    {
      method: "POST",
      headers: {
        Authorization: `Bearer ${SUPABASE_ACCESS_TOKEN}`,
        "Content-Type": "application/json",
      },
      body: JSON.stringify({ query }),
    },
  );
  const text = await response.text();
  if (!response.ok) {
    throw new Error(`Supabase query ${response.status}: ${text.slice(0, 700)}`);
  }
  if (!text.trim()) return [];
  const parsed = JSON.parse(text);
  return Array.isArray(parsed) ? parsed : (parsed?.data || parsed?.result || []);
}

function sqlText(value) {
  if (value == null) return "null";
  return `'${String(value).replace(/'/g, "''")}'`;
}

function sqlJson(value) {
  return `${sqlText(JSON.stringify(value ?? {}))}::jsonb`;
}

async function getDates() {
  const exact = String(args.date || "").slice(0, 10);
  if (exact) {
    if (!validDate(exact)) throw new Error(`Invalid --date=${args.date}`);
    return [exact];
  }

  let cutoff = "";
  if (days > 0) {
    const d = new Date();
    d.setUTCDate(d.getUTCDate() - Math.ceil(days));
    cutoff = d.toISOString().slice(0, 10);
  }
  const checkedJoin = unverifiedOnly && !force
    ? "left join public.storm_google_impact_verification v on v.event_date = d.event_date"
    : "";
  const filters = ["d.event_date is not null"];
  if (cutoff) filters.push(`d.event_date >= ${sqlText(cutoff)}::date`);
  if (unverifiedOnly && !force) filters.push("v.event_date is null");
  const rows = await dbQuery(`
    with dates as (
      select event_date from public.hail_lsr_raw
      union
      select event_date from public.storm_polygons
    )
    select d.event_date::text as event_date
    from dates d
    ${checkedJoin}
    where ${filters.join(" and ")}
    order by d.event_date desc
    limit ${limit} offset ${offset};
  `);
  return rows.map((row) => String(row.event_date || "").slice(0, 10)).filter(validDate);
}

function plainText(value) {
  return String(value || "")
    .replace(/<script[\s\S]*?<\/script>/gi, " ")
    .replace(/<style[\s\S]*?<\/style>/gi, " ")
    .replace(/<[^>]+>/g, " ")
    .replace(/&nbsp;|&#160;/gi, " ")
    .replace(/&amp;/gi, "&")
    .replace(/&#39;|&apos;/gi, "'")
    .replace(/&quot;/gi, '"')
    .replace(/\s+/g, " ")
    .trim();
}

function extractCount(value) {
  const text = plainText(value);
  const patterns = [
    /there\s+(?:were|are)\s+([\d,]+)\s+(?:total\s+)?(?:properties|homes|households|housing units)\s+(?:that\s+)?(?:were|are|have been)?\s*(?:impacted|affected|damaged)/i,
    /(?:a\s+total\s+of\s+)?([\d,]+)\s+(?:total\s+)?(?:properties|homes|households|housing units)\s+(?:were|are|have been|that were)?\s*(?:impacted|affected|damaged)/i,
    /(?:impacted|affected|damaged)\s+(?:approximately\s+|about\s+|an estimated\s+)?([\d,]+)\s+(?:properties|homes|households|housing units)/i,
    /(?:properties|homes|households|housing units)\s+(?:impacted|affected|damaged)\s*[:\-]?\s*([\d,]+)/i,
  ];
  for (const pattern of patterns) {
    const match = text.match(pattern);
    if (!match) continue;
    const count = Number(match[1].replace(/,/g, ""));
    if (Number.isInteger(count) && count > 0 && count <= 50000000) {
      return { count, evidence: match[0].slice(0, 260) };
    }
  }
  return null;
}

function host(value) {
  try { return new URL(value).hostname.toLowerCase().replace(/^www\./, ""); }
  catch { return ""; }
}

function confidence(url) {
  const h = host(url);
  if (h === "hailstrike.com" || h.endsWith(".hailstrike.com")) return 0.98;
  if (h === "hailtrace.com" || h.endsWith(".hailtrace.com")) return 0.96;
  if (h === "interactivehailmaps.com" || h.endsWith(".interactivehailmaps.com")) return 0.92;
  if (h.endsWith(".gov")) return 0.92;
  return 0.84;
}

function candidatesFrom(body) {
  const out = [];
  function add(result, rank, kind, extraText) {
    if (!result) return;
    const url = result.link || result.url || result.sourceUrl || result.source?.link || "";
    if (!String(url).startsWith("https://")) return;
    out.push({
      rank,
      kind,
      url,
      title: String(result.title || result.source?.name || "").slice(0, 300),
      snippet: [
        result.snippet, result.answer, result.description, result.text, extraText,
      ].filter(Boolean).join(" | ").slice(0, 5000),
    });
  }
  (body.organic || []).forEach((r, i) => add(r, i + 1, "organic"));
  (body.news || []).forEach((r, i) => add(r, i + 1, "news"));
  add(body.answerBox, 0, "answer_box");
  const ai = body.aiOverview || body.ai_overview;
  if (ai) {
    const aiText = ai.text || ai.snippet || ai.answer || "";
    (ai.sources || []).forEach((r, i) => add(r, i + 1, "ai_overview", aiText));
  }
  return out;
}

async function googleSearch(query) {
  const response = await fetch("https://google.serper.dev/search", {
    method: "POST",
    headers: {
      "X-API-KEY": SERPER_API_KEY,
      "Content-Type": "application/json",
    },
    body: JSON.stringify({ q: query, gl: "us", hl: "en", num: 10 }),
  });
  const body = await response.json().catch(() => ({}));
  if (!response.ok) {
    throw new Error(`Google search ${response.status}: ${JSON.stringify(body).slice(0, 500)}`);
  }
  return body;
}

async function verifyDate(date) {
  const humanDate = new Intl.DateTimeFormat("en-US", {
    timeZone: "UTC", month: "long", day: "numeric", year: "numeric",
  }).format(new Date(`${date}T12:00:00Z`));
  const slashDate = `${date.slice(5, 7)}/${date.slice(8, 10)}/${date.slice(0, 4)}`;
  const queries = [
    `how many homes were affected by the ${humanDate} hailstorm`,
    `"${humanDate}" hailstorm properties impacted HailStrike`,
  ];
  const citations = [];

  for (const query of queries) {
    const body = await googleSearch(query);
    const candidates = candidatesFrom(body);
    candidates.forEach((c) => citations.push({ url: c.url, title: c.title }));

    for (const candidate of candidates) {
      let searchable = `${candidate.title} | ${candidate.snippet}`;
      let match = extractCount(searchable);
      if (!match && /hailstrike|hailtrace|interactivehailmaps/i.test(host(candidate.url))) {
        try {
          const page = await fetch(candidate.url, {
            headers: { "User-Agent": "HailMoneyMap/1.0 (impact-count-verification)" },
            redirect: "follow",
            signal: AbortSignal.timeout(7000),
          });
          if (page.ok) {
            searchable += " | " + (await page.text()).slice(0, 800000);
            match = extractCount(searchable);
          }
        } catch {}
      }
      if (!match) continue;
      const lower = plainText(searchable).toLowerCase();
      const dateSeen = lower.includes(date.toLowerCase()) ||
        lower.includes(humanDate.toLowerCase()) || lower.includes(slashDate);
      if (!dateSeen) continue;
      return {
        status: "verified",
        impacted_properties: match.count,
        source_url: candidate.url,
        source_title: candidate.title || null,
        source_provider: host(candidate.url) || null,
        query_text: query,
        search_rank: candidate.rank,
        confidence: confidence(candidate.url),
        google_citations: citations,
        raw: { evidence: match.evidence, result_kind: candidate.kind },
      };
    }
    await sleep(120);
  }

  return {
    status: "not_found",
    impacted_properties: null,
    source_url: null,
    source_title: null,
    source_provider: null,
    query_text: queries.join(" || "),
    search_rank: null,
    confidence: null,
    google_citations: citations,
    raw: { reason: "No explicit dated property-impact count found in Google results" },
  };
}

async function saveResult(date, result) {
  const now = new Date().toISOString();
  const query = `
    insert into public.storm_google_impact_verification (
      event_date,status,impacted_properties,source_url,source_title,source_provider,
      query_text,search_rank,confidence,google_citations,raw,verified_at,updated_at
    ) values (
      ${sqlText(date)}::date,${sqlText(result.status)},${result.impacted_properties ?? "null"},
      ${sqlText(result.source_url)},${sqlText(result.source_title)},${sqlText(result.source_provider)},
      ${sqlText(result.query_text)},${result.search_rank ?? "null"},${result.confidence ?? "null"},
      ${sqlJson(result.google_citations || [])},${sqlJson(result.raw || {})},
      ${sqlText(now)}::timestamptz,${sqlText(now)}::timestamptz
    )
    on conflict (event_date) do update set
      status=excluded.status, impacted_properties=excluded.impacted_properties,
      source_url=excluded.source_url, source_title=excluded.source_title,
      source_provider=excluded.source_provider, query_text=excluded.query_text,

      search_rank=excluded.search_rank, confidence=excluded.confidence,
      google_citations=excluded.google_citations, raw=excluded.raw,
      verified_at=excluded.verified_at, updated_at=excluded.updated_at
    where public.storm_google_impact_verification.status <> 'verified'
       or excluded.status = 'verified';
  `;
  await dbQuery(query);
}

async function main() {
  const dates = await getDates();
  console.log(`Verifying Google property-impact counts for ${dates.length} storm date(s)`);
  let verified = 0;
  let notFound = 0;
  let failures = 0;
  for (const [index, date] of dates.entries()) {
    try {
      const result = await verifyDate(date);
      await saveResult(date, result);
      if (result.status === "verified") {
        verified += 1;
        console.log(`[${index + 1}/${dates.length}] ${date}: ${result.impacted_properties} properties (${result.source_provider})`);
      } else {
        notFound += 1;
        console.log(`[${index + 1}/${dates.length}] ${date}: no explicit Google-verified count`);
      }
    } catch (error) {
      failures += 1;
      console.error(`[${index + 1}/${dates.length}] ${date}: FAILED ${error.message || error}`);
    }
    if (index + 1 < dates.length) await sleep(180);
  }
  console.log(`Done. verified=${verified}, not_found=${notFound}, failed=${failures}`);
  if (failures) process.exitCode = 1;
}

main().catch((error) => {
  console.error(error);
  process.exit(1);
});
