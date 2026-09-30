const crypto = require("crypto");
const path = require("path");
const auth = require("../node_modules/firebase-tools/lib/auth");
const { requireAuth } = require("../node_modules/firebase-tools/lib/requireAuth");
const hostingApi = require("../node_modules/firebase-tools/lib/hosting/api");
const { Uploader } = require("../node_modules/firebase-tools/lib/deploy/hosting/uploader");

const PROJECT_ID = "hailmoneymap";
const SITE_ID = "hailmoneymap";
const CALLBACK_PATH = "abc-oauth-callback.html";
const CALLBACK_URL = `https://${SITE_ID}.web.app/${CALLBACK_PATH}`;
const INDEX_URL = `https://${SITE_ID}.web.app/index.html`;

function sha256(value) {
  return crypto.createHash("sha256").update(value).digest("hex");
}

async function main() {
  const account = auth.getGlobalDefaultAccount();
  if (!account) throw new Error("Firebase CLI is not signed in.");
  await requireAuth({ project: PROJECT_ID, user: account.user, tokens: account.tokens });

  const live = await hostingApi.getChannel("-", SITE_ID, "live");
  const sourceVersion = live && live.release && live.release.version && live.release.version.name;
  if (!sourceVersion) throw new Error("The live Firebase Hosting version could not be identified.");

  const beforeIndex = Buffer.from(await (await fetch(INDEX_URL, { cache: "no-store" })).arrayBuffer());
  const beforeIndexHash = sha256(beforeIndex);
  const recentCreatedVersion = (await hostingApi.listVersions(SITE_ID))
    .filter((version) => version.status === "CREATED" && Date.now() - Date.parse(version.createTime) < 30 * 60 * 1000)
    .sort((left, right) => Date.parse(right.createTime) - Date.parse(left.createTime))[0];
  const cloneOperation = recentCreatedVersion
    ? null
    : await hostingApi.cloneVersion(SITE_ID, sourceVersion, false);
  const clonedVersion = recentCreatedVersion && recentCreatedVersion.name ||
    cloneOperation && cloneOperation.response && cloneOperation.response.name ||
    cloneOperation && /\/versions\//.test(String(cloneOperation.name || "")) && cloneOperation.name;
  if (!clonedVersion) throw new Error("Firebase Hosting did not return the cloned version.");

  const uploader = new Uploader({
    version: clonedVersion,
    cwd: path.resolve(__dirname, ".."),
    projectRoot: path.resolve(__dirname, ".."),
    public: path.resolve(__dirname, "..", "public"),
    files: [CALLBACK_PATH]
  });
  await uploader.start();

  const versionId = clonedVersion.split("/").pop();
  await hostingApi.updateVersion(SITE_ID, versionId, { status: "FINALIZED" });
  await hostingApi.createRelease(SITE_ID, "live", clonedVersion, {
    message: "Add ABC Supply OAuth callback only"
  });

  await new Promise((resolve) => setTimeout(resolve, 2500));
  const callbackResponse = await fetch(CALLBACK_URL, { cache: "no-store" });
  const callbackBody = await callbackResponse.text();
  const afterIndex = Buffer.from(await (await fetch(INDEX_URL, { cache: "no-store" })).arrayBuffer());
  const verified = callbackResponse.ok &&
    callbackBody.includes("abcOAuthCallback") &&
    callbackBody.includes("hailmoney:abc-oauth") &&
    sha256(afterIndex) === beforeIndexHash;

  if (!verified) {
    await hostingApi.createRelease(SITE_ID, "live", sourceVersion, {
      message: "Automatic rollback: ABC callback verification failed"
    });
    throw new Error("Callback verification failed; the previous live version was restored.");
  }

  console.log(JSON.stringify({
    released: true,
    callbackStatus: callbackResponse.status,
    callbackVerified: true,
    existingIndexPreserved: true
  }));
}

main().catch((error) => {
  console.log(JSON.stringify({
    released: false,
    error: error && error.message ? error.message : "ABC callback deployment failed."
  }));
  process.exitCode = 1;
});
