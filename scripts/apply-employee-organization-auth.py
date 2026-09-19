from pathlib import Path

p = Path('functions/index.js')
s = p.read_text(encoding='utf-8')

if 'const HM_PRIMARY_ORGANIZATION_ID = "yopro";' not in s:
    marker = '};\n\nfunction safeEmployeeProfile(userRecord, fallback) {'
    if marker not in s:
        raise SystemExit('Employee seed marker not found')
    s = s.replace(marker, '};\nconst HM_PRIMARY_ORGANIZATION_ID = "yopro";\n\nfunction safeEmployeeProfile(userRecord, fallback) {', 1)

old_test_claims = '''    await admin.auth().setCustomUserClaims(userRecord.uid, {
      role: "authenticated",
      employee: true,
      hmRole: seed.role
    });
    userRecord = await admin.auth().getUser(userRecord.uid);'''
new_test_claims = '''    await admin.auth().setCustomUserClaims(userRecord.uid, {
      role: "authenticated",
      employee: true,
      hmRole: seed.role,
      hmOrganizationId: HM_PRIMARY_ORGANIZATION_ID
    });
    await db.collection("hmEmployees").doc(userRecord.uid).set({
      email,
      displayName: seed.displayName,
      role: seed.role,
      organizationId: HM_PRIMARY_ORGANIZATION_ID,
      active: true,
      updatedAt: FieldValue.serverTimestamp()
    }, { merge: true });
    userRecord = await admin.auth().getUser(userRecord.uid);'''
if old_test_claims in s:
    s = s.replace(old_test_claims, new_test_claims, 1)
elif 'hmOrganizationId: HM_PRIMARY_ORGANIZATION_ID' not in s:
    raise SystemExit('Employee test login claim block not found')

org_line = '    const organizationId = String(caller.hmOrganizationId || HM_PRIMARY_ORGANIZATION_ID).trim().toLowerCase();\n'
if org_line not in s:
    role_marker = '    const allowedRoles = ["Admin", "Manager", "Sales Rep", "Production", "Office", "Canvasser"];\n'
    if role_marker not in s:
        raise SystemExit('Provision role marker not found')
    s = s.replace(role_marker, role_marker + org_line, 1)

old_provision_claims = '''    await admin.auth().setCustomUserClaims(userRecord.uid, {
      role: "authenticated",
      employee: true,
      hmRole
    });'''
new_provision_claims = '''    await admin.auth().setCustomUserClaims(userRecord.uid, {
      role: "authenticated",
      employee: true,
      hmRole,
      hmOrganizationId: organizationId
    });'''
if old_provision_claims in s:
    s = s.replace(old_provision_claims, new_provision_claims, 1)
elif 'hmOrganizationId: organizationId' not in s:
    raise SystemExit('Provision claim block not found')

old_doc = '''      displayName,
      role: hmRole,
      active: body.active !== false,'''
new_doc = '''      displayName,
      role: hmRole,
      organizationId,
      active: body.active !== false,'''
if old_doc in s:
    s = s.replace(old_doc, new_doc, 1)
elif '      organizationId,\n      active: body.active !== false,' not in s:
    raise SystemExit('Provision employee Firestore block not found')

p.write_text(s, encoding='utf-8')
print('Employee organization claims restored without changing account passwords.')
