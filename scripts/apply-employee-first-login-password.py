from pathlib import Path
import re

PUBLIC = Path('public/index.html')
FUNCTIONS = Path('functions/index.js')

public = PUBLIC.read_text(encoding='utf-8')
functions = FUNCTIONS.read_text(encoding='utf-8')

new_login = '''    async function hmLoginVerifiedEmployee(email, password) {
      var credential;
      try {
        credential = await window.auth.signInWithEmailAndPassword(email, password);
      } catch (loginError) {
        var code = String(loginError && loginError.code || '');
        var canBootstrapAdmin = String(email || '').trim().toLowerCase() === 'admin@hailmoney.test' &&
          (code === 'auth/invalid-login-credentials' || code === 'auth/user-not-found');
        if (!canBootstrapAdmin) throw loginError;
        credential = await window.auth.createUserWithEmailAndPassword(email, password);
        await hmEmployeeRequest('bootstrapFirstAdmin', {}, true);
        await credential.user.getIdToken(true);
      }
      var tokenResult = await credential.user.getIdTokenResult(true);
      if (tokenResult.claims.employee !== true) { await window.auth.signOut(); throw new Error('This account is not an approved Hail Money employee.'); }
      if (tokenResult.claims.mustChangePassword === true) {
        var newPassword = window.prompt('This is your first login. Create a new password with at least 8 characters.');
        if (!newPassword || newPassword.length < 8) { await window.auth.signOut(); throw new Error('You must create a new password with at least 8 characters before continuing.'); }
        var confirmPassword = window.prompt('Confirm your new password.');
        if (newPassword !== confirmPassword) { await window.auth.signOut(); throw new Error('The new passwords did not match. Please log in again and try again.'); }
        await hmEmployeeRequest('changeOwnPassword', { password: newPassword }, true);
        await credential.user.getIdToken(true);
        tokenResult = await credential.user.getIdTokenResult(true);
      }
      await hmCompleteVerifiedLogin({ email: credential.user.email || email, displayName: credential.user.displayName || email, role: tokenResult.claims.hmRole || 'Sales Rep' });
    }'''

login_pattern = r'    async function hmLoginVerifiedEmployee\(email, password\) \{.*?\n    \}(?=\n    async function hmProvisionEmployeeAccount)'
if "bootstrapFirstAdmin" not in public:
    public, count = re.subn(login_pattern, new_login, public, count=1, flags=re.S)
    if count != 1:
        raise SystemExit('Expected login function not found')

functions = re.sub(
    r'const HM_TEST_EMPLOYEES = \{.*?\};\n\n(?=function safeEmployeeProfile)',
    '',
    functions,
    count=1,
    flags=re.S,
)

legacy_pattern = r'exports\.employeeTestLogin = onRequest\(\{ cors: false, region: "us-central1" \}, async \(req, res\) => \{.*?\n\}\);\n\n(?=exports\.provisionEmployee)'
legacy_replacement = '''exports.employeeTestLogin = onRequest({ cors: false, region: "us-central1" }, async (req, res) => {
  permitCors(req, res);
  if (req.method === "OPTIONS") return res.status(204).send("");
  return res.status(410).json({ error: "Legacy test login is disabled." });
});

'''
if 'Legacy test login is disabled.' not in functions:
    functions, count = re.subn(legacy_pattern, legacy_replacement, functions, count=1, flags=re.S)
    if count != 1:
        raise SystemExit('Legacy employeeTestLogin block not found')

old_claims = '''    await admin.auth().setCustomUserClaims(userRecord.uid, {
      role: "authenticated",
      employee: true,
      hmRole
    });'''
new_claims = '''    const existingClaims = userRecord.customClaims || {};
    await admin.auth().setCustomUserClaims(userRecord.uid, {
      ...existingClaims,
      role: "authenticated",
      employee: true,
      hmRole,
      mustChangePassword: password ? true : existingClaims.mustChangePassword === true
    });'''
if old_claims in functions:
    functions = functions.replace(old_claims, new_claims, 1)
elif 'mustChangePassword: password ? true' not in functions:
    raise SystemExit('Provision claims block not found')

marker = 'function responseOutputText(response) {'
bootstrap_fn = '''exports.bootstrapFirstAdmin = onRequest({ cors: false, region: "us-central1", invoker: "public" }, async (req, res) => {
  permitCors(req, res);
  if (req.method === "OPTIONS") return res.status(204).send("");
  if (req.method !== "POST") return res.status(405).json({ error: "POST required." });
  try {
    const caller = await requireFirebaseUser(req);
    const email = String(caller.email || "").trim().toLowerCase();
    if (email !== "admin@hailmoney.test") return res.status(403).json({ error: "Initial administrator account is not authorized." });
    const existingEmployees = await db.collection("hmEmployees").limit(1).get();
    if (!existingEmployees.empty) return res.status(409).json({ error: "The initial administrator has already been created." });
    const authUsers = await admin.auth().listUsers(2);
    if (authUsers.users.length !== 1 || authUsers.users[0].uid !== caller.uid) {
      return res.status(409).json({ error: "Initial administrator bootstrap is no longer available." });
    }
    const current = await admin.auth().getUser(caller.uid);
    await admin.auth().setCustomUserClaims(caller.uid, {
      ...(current.customClaims || {}),
      role: "authenticated",
      employee: true,
      hmRole: "Admin",
      mustChangePassword: true
    });
    await admin.auth().updateUser(caller.uid, { displayName: "Admin", emailVerified: true });
    await db.collection("hmEmployees").doc(caller.uid).set({
      uid: caller.uid,
      email,
      displayName: "Admin",
      role: "Admin",
      active: true,
      mustChangePassword: true,
      updatedAt: FieldValue.serverTimestamp()
    }, { merge: true });
    return res.status(200).json({ ok: true });
  } catch (error) {
    const status = Number(error && error.statusCode) || 500;
    logger.error("Initial admin bootstrap failed", { message: error && error.message });
    return res.status(status).json({ error: error && error.message || "Initial administrator could not be created." });
  }
});

'''
change_fn = '''exports.changeOwnPassword = onRequest({ cors: false, region: "us-central1", invoker: "public" }, async (req, res) => {
  permitCors(req, res);
  if (req.method === "OPTIONS") return res.status(204).send("");
  if (req.method !== "POST") return res.status(405).json({ error: "POST required." });
  try {
    const caller = await requireFirebaseUser(req);
    const password = String(req.body && req.body.password || "");
    if (password.length < 8) return res.status(400).json({ error: "Password must contain at least 8 characters." });
    const current = await admin.auth().getUser(caller.uid);
    await admin.auth().updateUser(caller.uid, { password });
    await admin.auth().setCustomUserClaims(caller.uid, { ...(current.customClaims || {}), mustChangePassword: false });
    await db.collection("hmEmployees").doc(caller.uid).set({
      mustChangePassword: false,
      passwordChangedAt: FieldValue.serverTimestamp()
    }, { merge: true });
    return res.status(200).json({ ok: true });
  } catch (error) {
    const status = Number(error && error.statusCode) || 500;
    logger.error("Employee password change failed", { message: error && error.message });
    return res.status(status).json({ error: error && error.message || "Password could not be changed." });
  }
});

'''
if 'exports.bootstrapFirstAdmin = onRequest' not in functions:
    if marker not in functions:
        raise SystemExit('Function insertion marker not found')
    functions = functions.replace(marker, bootstrap_fn + marker, 1)
if 'exports.changeOwnPassword = onRequest' not in functions:
    if marker not in functions:
        raise SystemExit('Function insertion marker not found')
    functions = functions.replace(marker, change_fn + marker, 1)

PUBLIC.write_text(public, encoding='utf-8')
FUNCTIONS.write_text(functions, encoding='utf-8')
print('Employee first-login password flow patched successfully.')
