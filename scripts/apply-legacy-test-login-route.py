from pathlib import Path

p = Path('public/index.html')
s = p.read_text(encoding='utf-8')

old = '''      if (testUser) {
        var testCredential = await window.auth.signInWithEmailAndPassword(email, password);
        await testCredential.user.getIdToken(true);
        await hmCompleteVerifiedLogin({ email: testUser.email, displayName: testUser.displayName, role: testUser.role });
        return;
      }'''
new = '''      if (testUser) {
        var loginResult = await hmEmployeeRequest('employeeTestLogin', { email: email, password: password }, false);
        if (!loginResult || !loginResult.token) throw new Error('Employee login could not be completed.');
        var testCredential = await window.auth.signInWithCustomToken(loginResult.token);
        await testCredential.user.getIdToken(true);
        var testProfile = loginResult.profile || {};
        await hmCompleteVerifiedLogin({
          email: testProfile.email || testUser.email,
          displayName: testProfile.displayName || testUser.displayName,
          role: testProfile.role || testUser.role
        });
        return;
      }'''

if old in s:
    s = s.replace(old, new, 1)
elif "hmEmployeeRequest('employeeTestLogin'" not in s:
    raise SystemExit('Legacy test login block not found')

p.write_text(s, encoding='utf-8')
print('Server-backed test login restored without changing passwords.')
