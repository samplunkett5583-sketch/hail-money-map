from pathlib import Path
import re

PUBLIC = Path('public/index.html')
FUNCTIONS = Path('functions/index.js')

public = PUBLIC.read_text(encoding='utf-8')
functions = FUNCTIONS.read_text(encoding='utf-8')

old_login = '''    async function hmLoginVerifiedEmployee(email, password) {\n      var testUser = CRM_TEST_USERS.find(function (user) { return user.email === email; }) || null;\n      if (testUser) {\n        /* Route through the trusted Cloud Function so the employee/hmRole/\n           hmOrganizationId custom claims are always issued server-side\n           before we ever sign in â€” signing straight into Firebase Auth\n           could leave the session with no organization claim. */\n        var loginResult = await hmEmployeeRequest('employeeTestLogin', { email: email, password: password }, false);\n        if (!loginResult || !loginResult.token) throw new Error('Employee login could not be completed.');\n        var testCredential = await window.auth.signInWithCustomToken(loginResult.token);\n        await testCredential.user.getIdToken(true);\n        var testProfile = loginResult.profile || {};\n        await hmCompleteVerifiedLogin({\n          email: testProfile.email || testUser.email,\n          displayName: testProfile.displayName || testUser.displayName,\n          role: testProfile.role || testUser.role\n        });\n        return;\n      }\n      var credential = await window.auth.signInWithEmailAndPassword(email, password);\n      var tokenResult = await credential.user.getIdTokenResult(true);\n      if (tokenResult.claims.employee !== true) { await window.auth.signOut(); throw new Error('This account is not an approved Hail Money employee.'); }\n      await hmCompleteVerifiedLogin({ email: credential.user.email || email, displayName: credential.user.displayName || email, role: tokenResult.claims.hmRole || 'Sales Rep' });\n    }'''

new_login = '''    async function hmLoginVerifiedEmployee(email, password) {\n      var credential = await window.auth.signInWithEmailAndPassword(email, password);\n      var tokenResult = await credential.user.getIdTokenResult(true);\n      if (tokenResult.claims.employee !== true) { await window.auth.signOut(); throw new Error('This account is not an approved Hail Money employee.'); }\n      if (tokenResult.claims.mustChangePassword === true) {\n        var newPassword = window.prompt('This is your first login. Create a new password with at least 8 characters.');\n        if (!newPassword || newPassword.length < 8) { await window.auth.signOut(); throw new Error('You must create a new password with at least 8 characters before continuing.'); }\n        var confirmPassword = window.prompt('Confirm your new password.');\n        if (newPassword !== confirmPassword) { await window.auth.signOut(); throw new Error('The new passwords did not match. Please log in again and try again.'); }\n        await hmEmployeeRequest('changeOwnPassword', { password: newPassword }, true);\n        await credential.user.getIdToken(true);\n        tokenResult = await credential.user.getIdTokenResult(true);\n      }\n      await hmCompleteVerifiedLogin({ email: credential.user.email || email, displayName: credential.user.displayName || email, role: tokenResult.claims.hmRole || 'Sales Rep' });\n    }'''

if old_login not in public:
    raise SystemExit('Expected login block not found')
public = public.replace(old_login, new_login, 1)

# Disable the legacy hard-coded test-account login endpoint.
functions = re.sub(
    r'const HM_TEST_EMPLOYEES = \{.*?exports\.provisionEmployee =',
    '''exports.employeeTestLogin = onRequest({ cors: false, region: "us-central1" }, async (req, res) => {\n  permitCors(req, res);\n  if (req.method === "OPTIONS") return res.status(204).send("");\n  return res.status(410).json({ error: "Legacy test login is disabled." });\n});\n\nexports.provisionEmployee =''',
    functions,
    count=1,
    flags=re.S,
)

old_claims = '''    await admin.auth().setCustomUserClaims(userRecord.uid, {\n      role: "authenticated",\n      employee: true,\n      hmRole\n    });'''
new_claims = '''    const existingClaims = userRecord.customClaims || {};\n    await admin.auth().setCustomUserClaims(userRecord.uid, {\n      ...existingClaims,\n      role: "authenticated",\n      employee: true,\n      hmRole,\n      mustChangePassword: password ? true : existingClaims.mustChangePassword === true\n    });'''
if old_claims not in functions:
    raise SystemExit('Provision claims block not found')
functions = functions.replace(old_claims, new_claims, 1)

marker = '''function responseOutputText(response) {'''
change_fn = '''exports.changeOwnPassword = onRequest({ cors: false, region: "us-central1", invoker: "public" }, async (req, res) => {\n  permitCors(req, res);\n  if (req.method === "OPTIONS") return res.status(204).send("");\n  if (req.method !== "POST") return res.status(405).json({ error: "POST required." });\n  try {\n    const caller = await requireFirebaseUser(req);\n    const password = String(req.body && req.body.password || "");\n    if (password.length < 8) return res.status(400).json({ error: "Password must contain at least 8 characters." });\n    const current = await admin.auth().getUser(caller.uid);\n    await admin.auth().updateUser(caller.uid, { password });\n    await admin.auth().setCustomUserClaims(caller.uid, { ...(current.customClaims || {}), mustChangePassword: false });\n    await db.collection("hmEmployees").doc(caller.uid).set({\n      mustChangePassword: false,\n      passwordChangedAt: FieldValue.serverTimestamp()\n    }, { merge: true });\n    return res.status(200).json({ ok: true });\n  } catch (error) {\n    const status = Number(error && error.statusCode) || 500;\n    logger.error("Employee password change failed", { message: error && error.message });\n    return res.status(status).json({ error: error && error.message || "Password could not be changed." });\n  }\n});\n\n'''
if change_fn not in functions:
    if marker not in functions:
        raise SystemExit('Function insertion marker not found')
    functions = functions.replace(marker, change_fn + marker, 1)

PUBLIC.write_text(public, encoding='utf-8')
FUNCTIONS.write_text(functions, encoding='utf-8')
print('Employee first-login password flow patched successfully.')
