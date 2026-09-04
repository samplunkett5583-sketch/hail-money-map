const fs = require("fs");
const path = require("path");
const { chromium } = require("C:/Users/samue/.cache/codex-runtimes/codex-primary-runtime/dependencies/node/node_modules/playwright");

const APP_URL = "http://127.0.0.1:53295/";
const CHROME = "C:/Program Files/Google/Chrome/Application/chrome.exe";
let activeBrowser;

function readFirebaseApiKey() {
  const html = fs.readFileSync(path.resolve(__dirname, "..", "public", "index.html"), "utf8");
  const match = html.match(/firebaseConfig\s*=\s*\{[\s\S]*?apiKey:\s*['\"]([^'\"]+)/);
  if (!match) throw new Error("Firebase web configuration was not found.");
  return match[1];
}

function readTestLogin() {
  const source = fs.readFileSync(path.resolve(__dirname, "..", "functions", "index.js"), "utf8");
  const match = source.match(/"(admin@hailmoney\.test)"\s*:\s*\{\s*password:\s*"([^"]+)"/);
  if (!match) throw new Error("The existing Hail Money browser test login was not found.");
  return { email: match[1], password: match[2] };
}

async function createEmployeeSession() {
  const login = readTestLogin();
  const apiKey = readFirebaseApiKey();
  let response = await fetch(`https://identitytoolkit.googleapis.com/v1/accounts:signInWithPassword?key=${encodeURIComponent(apiKey)}`, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({ ...login, returnSecureToken: true })
  });
  let data = await response.json();
  if (!response.ok || !data.idToken) {
    response = await fetch(`https://identitytoolkit.googleapis.com/v1/accounts:signUp?key=${encodeURIComponent(apiKey)}`, {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ ...login, returnSecureToken: true })
    });
    data = await response.json();
  }
  if (!response.ok || !data.idToken) {
    const customResponse = await fetch("http://127.0.0.1:5015/hailmoneymap/us-central1/employeeTestLogin", {
      method: "POST",
      headers: { "Content-Type": "application/json", Origin: "http://127.0.0.1:53295" },
      body: JSON.stringify(login)
    });
    const custom = await customResponse.json();
    if (!customResponse.ok || !custom.token) throw new Error(`Firebase test session could not be provisioned (HTTP ${customResponse.status}).`);
    response = await fetch(`https://identitytoolkit.googleapis.com/v1/accounts:signInWithCustomToken?key=${encodeURIComponent(apiKey)}`, {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ token: custom.token, returnSecureToken: true })
    });
    data = await response.json();
  }
  if (!response.ok || !data.idToken) throw new Error(`Firebase test sign-in failed (HTTP ${response.status}).`);
  return data.idToken;
}

async function main() {
  const firebaseToken = await createEmployeeSession();
  const browser = await chromium.launch({ executablePath: CHROME, headless: true });
  activeBrowser = browser;
  const context = await browser.newContext({ viewport: { width: 1280, height: 900 } });
  const page = await context.newPage();
  const pageErrors = [];
  page.on("pageerror", (error) => pageErrors.push(error.message));

  let accountRequests = 0;
  await page.route("**/abcAccounts", async (route) => {
    accountRequests += 1;
    if (accountRequests === 1) {
      await route.fulfill({
        status: 503,
        contentType: "application/json",
        body: JSON.stringify({ error: "Temporary test failure." })
      });
      return;
    }
    await route.continue();
  });

  await page.goto(APP_URL, { waitUntil: "domcontentloaded", timeout: 30000 });
  await page.waitForSelector("#est-source-ai", { state: "attached", timeout: 20000 });
  await page.evaluate((token) => {
    const user = { getIdToken: async () => token };
    window.auth = {
      currentUser: user,
      signInAnonymously: async () => ({ user })
    };
  }, firebaseToken);

  await page.evaluate(() => {
    document.querySelectorAll(".page").forEach((item) => item.classList.remove("active"));
    const estimates = document.getElementById("page-estimates");
    if (estimates) estimates.classList.add("active");
  });

  const initial = await page.evaluate(() => ({
    integrationConfigured: window.HAIL_MONEY_ABC_SUPPLY_ENABLED !== false,
    connectDisabled: document.getElementById("est-abc-connect").disabled,
    retryHidden: document.getElementById("est-abc-retry").hidden
  }));

  await page.locator('[data-est-property-type="residential"]').click({ force: true });
  await page.locator('[data-est-scope="roof"]').click({ force: true });
  await page.locator("#est-source-ai").click({ force: true });
  await page.locator("#est-ai-street").fill("1 Main Street");
  await page.locator("#est-ai-city").fill("Madison");
  await page.locator("#est-ai-state").selectOption("WI");
  await page.locator("#est-ai-zip").fill("53703");
  await page.locator("#est-ai-zip").dispatchEvent("input");

  await page.waitForFunction(() => !document.getElementById("est-abc-retry").hidden, null, { timeout: 15000 });
  const temporaryFailure = await page.evaluate(() => ({
    retryVisible: !document.getElementById("est-abc-retry").hidden,
    startEnabled: !document.getElementById("est-ai-generate").disabled
  }));

  await page.locator("#est-abc-retry").click({ force: true });
  await page.waitForFunction(() => {
    const status = document.getElementById("est-abc-status").textContent || "";
    return !/checking/i.test(status) && !document.getElementById("est-abc-connect").disabled;
  }, null, { timeout: 30000 });
  const retryResult = await page.evaluate(() => ({
    status: document.getElementById("est-abc-status").textContent,
    connectEnabled: !document.getElementById("est-abc-connect").disabled,
    startEnabled: !document.getElementById("est-ai-generate").disabled
  }));
  if (!retryResult.connectEnabled) {
    throw new Error(`Connect remained disabled after Retry: ${retryResult.status}`);
  }

  const popupPromise = context.waitForEvent("page", { timeout: 30000 });
  await page.locator("#est-abc-connect").click();
  const popup = await popupPromise;
  await popup.waitForURL((url) => url.hostname === "sandbox.auth.partners.abcsupply.com", { timeout: 30000 });
  const authorizationUrl = new URL(popup.url());
  const connectResult = {
    realAbcAuthorization: authorizationUrl.hostname === "sandbox.auth.partners.abcsupply.com" && authorizationUrl.pathname.endsWith("/authorize"),
    hasClientId: authorizationUrl.searchParams.has("client_id"),
    hasRegisteredRedirect: authorizationUrl.searchParams.get("redirect_uri") === "https://hailmoneymap.web.app/abc-oauth-callback.html",
    hasState: authorizationUrl.searchParams.has("state"),
    hasPricingScope: String(authorizationUrl.searchParams.get("scope") || "").split(" ").includes("pricing.read")
  };
  await popup.close();

  const hadConnectedAccount = await page.evaluate(() => {
    const account = document.getElementById("est-abc-account");
    const branch = document.getElementById("est-abc-branch");
    return !!(account && account.value && branch && branch.value);
  });
  await page.locator("#est-ai-generate").click({ force: true });
  await page.waitForFunction(() => document.getElementById("page-est-ai-processing").classList.contains("active"), null, { timeout: 10000 });
  const startResult = await page.evaluate(() => ({
    processingOpened: document.getElementById("page-est-ai-processing").classList.contains("active")
  }));
  startResult.startedWithoutConnectedAccount = startResult.processingOpened && !hadConnectedAccount;

  await browser.close();
  activeBrowser = null;
  console.log(JSON.stringify({
    initial,
    temporaryFailure,
    retry: { requestCount: accountRequests, ...retryResult },
    connect: connectResult,
    start: startResult,
    pageErrorCount: pageErrors.length
  }));
}

main().catch(async (error) => {
  if (activeBrowser) await activeBrowser.close().catch(() => {});
  console.log(JSON.stringify({ error: error.message }));
  process.exitCode = 1;
});
