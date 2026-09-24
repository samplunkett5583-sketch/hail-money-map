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
const debug = args.debug === "true";
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
  if (h === "hailpoint.com" || h.endsWith(".hailpoint.com")) return 0.90;
  if (h === "weather.gov" || h.endsWith(".weather.gov") || h.endsWith(".noaa.gov")) return 0.92;
  return 0.84;
}

function trustedImpactSource(url) {
  const h = host(url);
  return h === "hailstrike.com" || h.endsWith(".hailstrike.com") ||
    h === "hailtrace.com" || h.endsWith(".hailtrace.com") ||
    h === "interactivehailmaps.com" || h.endsWith(".interactivehailmaps.com") ||
    h === "hailpoint.com" || h.endsWith(".hailpoint.com") ||
    h === "weather.gov" || h.endsWith(".weather.gov") || h.endsWith(".noaa.gov");
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

let hailStrikeSitemapPromise = null;

function shiftDate(date, daysToAdd) {
  const value = new Date(`${date}T12:00:00Z`);
  value.setUTCDate(value.getUTCDate() + daysToAdd);
  return value.toISOString().slice(0, 10);
}

async function getHailStrikeSitemap() {
  if (hailStrikeSitemapPromise) return hailStrikeSitemapPromise;
  hailStrikeSitemapPromise = fetch(
    "https://hailstrike.com/sitemaps/sitemap-search-aniswaths.xml",
    { headers: { "User-Agent": "HailMoneyMap/1.0 (impact-count-verification)" } },
  ).then(async (response) => {
    if (!response.ok) throw new Error(`HailStrike sitemap HTTP ${response.status}`);
    const xml = await response.text();
    const byDate = {};
    const re = /<url>\s*<loc>(https:\/\/hailstrike\.com\/hail-map\/\d+)<\/loc>\s*<lastmod>(\d{4}-\d{2}-\d{2})<\/lastmod>\s*<\/url>/g;
    for (const match of xml.matchAll(re)) {
      (byDate[match[2]] ||= []).push(match[1]);
    }
    return byDate;
  });
  return hailStrikeSitemapPromise;
}

async function getStormProfile(date) {
  const rows = await dbQuery(`
    with storm_points as (
      select state, hail_in::double precision as hail
      from public.hail_lsr_raw
      where event_date = ${sqlText(date)}::date
      union all
      select state, greatest(coalesce(band_min,0), coalesce(band_max,0))::double precision as hail
      from public.storm_polygons
      where event_date = ${sqlText(date)}::date
        and coalesce(storm_type,'hail') not in ('wind','tornado')
    )
    select coalesce(array_agg(distinct state) filter (where state is not null and state <> ''), '{}') as states,
           coalesce(max(hail),0) as max_hail
    from storm_points;
  `);
  const row = rows[0] || {};
  let states = row.states || [];
  if (typeof states === "string") {
    states = states.replace(/^\{|\}$/g, "").split(",").map((v) => v.replace(/"/g, "").trim()).filter(Boolean);
  }
  return {
    states: Array.isArray(states) ? states.map((v) => String(v).toUpperCase()) : [],
    maxHail: Number(row.max_hail) || 0,
  };
}

function parseHailStrikePage(html, url, expectedDate) {
  const title = String(html.match(/<title>([^<]+)<\/title>/i)?.[1] || "").trim();
  const dateText = title.match(/Hail Map For\s+(.+?)\s+\(/i)?.[1] || "";
  const parsedDate = dateText ? new Date(`${dateText} 12:00:00 UTC`) : null;
  const eventDate = parsedDate && !Number.isNaN(parsedDate.getTime())
    ? parsedDate.toISOString().slice(0, 10)
    : "";
  if (eventDate !== expectedDate) return null;

  const text = plainText(html);
  const countMatch = extractCount(text);
  if (!countMatch) return null;

  const maxHail = Number(text.match(/Maximum\s+(\d+(?:\.\d+)?)"/i)?.[1]) || 0;
  const spotters = Number(text.match(/Spotters\s+(\d+)/i)?.[1]) || 0;
  const metaStates = String(
    html.match(/<meta\s+name=["']description["']\s+content=["'][^"']*States affected:\s*([^"']*)["']/i)?.[1] || "",
  );
  const states = [...new Set(
    metaStates.split(/[,\s]+/).map((v) => v.trim().toUpperCase()).filter((v) => /^[A-Z]{2}$/.test(v)),
  )];

  return {
    url,
    title,
    eventDate,
    impactedProperties: countMatch.count,
    evidence: countMatch.evidence,
    maxHail,
    spotters,
    states,
    groundVerified: /GROUND VERIFIED/i.test(text),
  };
}

function hailStrikeCandidateScore(candidate, profile) {
  let score = 0;
  const sourceStates = new Set(candidate.states || []);
  const stormStates = new Set(profile.states || []);
  if (sourceStates.size && stormStates.size) {
    let overlap = 0;
    stormStates.forEach((state) => { if (sourceStates.has(state)) overlap += 1; });
    const union = new Set([...sourceStates, ...stormStates]).size || 1;
    score += (overlap / union) * 100;
    if (!overlap) score -= 100;
  }
  if (candidate.maxHail > 0 && profile.maxHail > 0) {
    score -= Math.abs(candidate.maxHail - profile.maxHail) * 20;
  }
  if (candidate.groundVerified) score += 3;
  score += Math.min(2, candidate.spotters / 10);
  return score;
}

async function findHailStrikeImpact(date, googleCitations) {
  const [sitemap, profile] = await Promise.all([
    getHailStrikeSitemap(),
    getStormProfile(date),
  ]);
  const candidateDates = [date, shiftDate(date, 1), shiftDate(date, -1)];
  const urls = [...new Set(candidateDates.flatMap((d) => sitemap[d] || []))].slice(0, 24);
  if (!urls.length) return null;

  const settled = await Promise.allSettled(urls.map(async (url) => {
    const response = await fetch(url, {
      headers: { "User-Agent": "HailMoneyMap/1.0 (impact-count-verification)" },
      redirect: "follow",
      signal: AbortSignal.timeout(8000),
    });
    if (!response.ok) return null;
    return parseHailStrikePage(await response.text(), url, date);
  }));
  const candidates = settled
    .filter((item) => item.status === "fulfilled" && item.value)
    .map((item) => item.value);
  if (!candidates.length) return null;

  candidates.sort((a, b) =>
    hailStrikeCandidateScore(b, profile) - hailStrikeCandidateScore(a, profile) ||
    b.spotters - a.spotters ||
    a.url.localeCompare(b.url)
  );
  const selected = candidates[0];
  if (debug) {
    console.log("[debug] HailStrike match", {
      profile,
      selected,
      candidates: candidates.map((c) => ({
        url: c.url, properties: c.impactedProperties, maxHail: c.maxHail,
        states: c.states, score: hailStrikeCandidateScore(c, profile),
      })),
    });
  }
  return {
    status: "verified",
    impacted_properties: selected.impactedProperties,
    source_url: selected.url,
    source_title: selected.title,
    source_provider: "hailstrike.com",
    query_text: `Google search + HailStrike source match for ${date}`,
    search_rank: null,
    confidence: selected.groundVerified ? 0.98 : 0.94,
    google_citations: [
      ...(googleCitations || []),
      { url: selected.url, title: selected.title },
    ],
    raw: {
      verification_method: "hailstrike-source-match",
      evidence: selected.evidence,
      storm_profile: profile,
      selected_event: {
        max_hail: selected.maxHail,
        states: selected.states,
        spotters: selected.spotters,
        ground_verified: selected.groundVerified,
      },
    },
  };
}

async function verifyDate(date) {
  const humanDate = new Intl.DateTimeFormat("en-US", {
    timeZone: "UTC", month: "long", day: "numeric", year: "numeric",
  }).format(new Date(`${date}T12:00:00Z`));
  const slashDate = `${date.slice(5, 7)}/${date.slice(8, 10)}/${date.slice(0, 4)}`;
  const queries = [
    `how many homes were affected by the ${humanDate} hailstorm`,
  ];
  const citations = [];

  for (const query of queries) {
    const body = await googleSearch(query);
    const candidates = candidatesFrom(body);
    if (debug) {
      console.log("[debug]", query, candidates.slice(0, 10).map((c) => ({
        title: c.title, url: c.url, snippet: c.snippet.slice(0, 260),
      })));
    }
    candidates.forEach((c) => citations.push({ url: c.url, title: c.title }));

    for (const candidate of candidates) {
      if (!trustedImpactSource(candidate.url)) continue;
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

  try {
    const hailStrike = await findHailStrikeImpact(date, citations);
    if (hailStrike) return hailStrike;
  } catch (error) {
    console.warn(`[${date}] HailStrike source match failed: ${error.message || error}`);
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
