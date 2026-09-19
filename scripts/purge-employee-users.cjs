const admin = require('../functions/node_modules/firebase-admin');

admin.initializeApp();
const db = admin.firestore();
const auth = admin.auth();

async function deleteAllAuthUsers() {
  let nextPageToken;
  let deleted = 0;
  do {
    const result = await auth.listUsers(1000, nextPageToken);
    const uids = result.users.map((user) => user.uid);
    if (uids.length) {
      const outcome = await auth.deleteUsers(uids);
      deleted += outcome.successCount;
      if (outcome.failureCount) {
        throw new Error(`Failed to delete ${outcome.failureCount} Firebase Auth users.`);
      }
    }
    nextPageToken = result.pageToken;
  } while (nextPageToken);
  return deleted;
}

async function deleteCollection(collectionName) {
  let deleted = 0;
  while (true) {
    const snap = await db.collection(collectionName).limit(400).get();
    if (snap.empty) break;
    const batch = db.batch();
    snap.docs.forEach((doc) => batch.delete(doc.ref));
    await batch.commit();
    deleted += snap.size;
  }
  return deleted;
}

(async () => {
  const authDeleted = await deleteAllAuthUsers();
  const profilesDeleted = await deleteCollection('hmEmployees');
  console.log(JSON.stringify({ authDeleted, profilesDeleted }));
})().catch((error) => {
  console.error(error);
  process.exit(1);
});
