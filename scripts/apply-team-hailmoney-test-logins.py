from pathlib import Path

p = Path('public/index.html')
s = p.read_text(encoding='utf-8')

helper = '''    function crmBuildHailMoneyTestEmail(name) {
      var firstName = String(name || '').trim().split(/\\s+/)[0] || '';
      var username = firstName.toLowerCase().replace(/[^a-z0-9]/g, '');
      return username ? username + '@hailmoney.test' : '';
    }

'''
marker = '    function crmGetSettingsTeamModalValidationState() {'
if 'function crmBuildHailMoneyTestEmail(name)' not in s:
    if marker not in s:
        raise SystemExit('Team modal validation marker not found')
    s = s.replace(marker, helper + marker, 1)

old_fields = '''      document.getElementById('settings-team-member-name').value = member.name || '';
      document.getElementById('settings-team-member-email').value = member.email || '';'''
new_fields = '''      var memberNameInput = document.getElementById('settings-team-member-name');
      var memberEmailInput = document.getElementById('settings-team-member-email');
      memberNameInput.value = member.name || '';
      memberEmailInput.value = isCreate ? crmBuildHailMoneyTestEmail(member.name || '') : (member.email || '');
      memberEmailInput.readOnly = !!isCreate;
      if (!memberNameInput.dataset.hailMoneyTestEmailBound) {
        memberNameInput.dataset.hailMoneyTestEmailBound = '1';
        memberNameInput.addEventListener('input', function () {
          var modeEl = document.getElementById('settings-team-member-mode');
          var emailEl = document.getElementById('settings-team-member-email');
          if (modeEl && modeEl.value === 'create' && emailEl) {
            emailEl.value = crmBuildHailMoneyTestEmail(memberNameInput.value || '');
            crmUpdateSettingsTeamModalSaveState();
          }
        });
      }'''
if old_fields in s:
    s = s.replace(old_fields, new_fields, 1)
elif 'memberNameInput.dataset.hailMoneyTestEmailBound' not in s:
    raise SystemExit('Team modal name/email fields not found')

old_email = "      var email = String(document.getElementById('settings-team-member-email').value || '').trim();"
new_email = """      var emailEl = document.getElementById('settings-team-member-email');
      var email = String(emailEl.value || '').trim();
      if (mode === 'create') {
        email = crmBuildHailMoneyTestEmail(name);
        emailEl.value = email;
      }"""
if old_email in s:
    s = s.replace(old_email, new_email, 1)
elif "email = crmBuildHailMoneyTestEmail(name);" not in s:
    raise SystemExit('Team save email line not found')

p.write_text(s, encoding='utf-8')
print('New team members now use firstname@hailmoney.test logins.')
