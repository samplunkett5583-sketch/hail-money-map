from pathlib import Path

path = Path('public/index.html')
text = path.read_text(encoding='utf-8')
marker = 'HAIL MONEY FORGOT PASSWORD V8'
if marker in text:
    print('Forgot password reset already applied')
    raise SystemExit(0)

anchor = '''            <button type="button" id="login-submit-btn">Log In / Create Account</button>'''
if anchor not in text:
    raise SystemExit('Could not find native login submit button')

native = '''            <button type="button" id="login-submit-btn">Log In / Create Account</button>
            <button type="button" id="forgotPasswordBtn" style="display:block;width:100%;margin:9px 0 0;padding:8px 6px;border:0;background:transparent;color:#17172c;font-weight:800;text-decoration:underline;cursor:pointer;">Forgot password?</button>'''
text = text.replace(anchor, native, 1)

password_action = '''      if (action === 'password') {
        crmOpenLoginResetModal(crmGetCurrentTeamMember());
        return;
      }'''
password_action_replacement = '''      if (action === 'password') {
        var passwordMember = crmGetCurrentTeamMember();
        if (!passwordMember) {
          var targetEmail = String(prompt('Enter the Hail Money account email whose password you want to change:', '') || '').trim().toLowerCase();
          if (!targetEmail) return;
          var passwordMembers = crmGetTeamMembers();
          passwordMember = passwordMembers.find(function (member) {
            return String(member && member.email || '').trim().toLowerCase() === targetEmail;
          }) || null;
          if (!passwordMember) {
            alert('No Hail Money team member was found for ' + targetEmail + '.');
            return;
          }
        }
        crmOpenLoginResetModal(passwordMember);
        return;
      }'''
if password_action not in text:
    raise SystemExit('Could not find profile Change Password action')
text = text.replace(password_action, password_action_replacement, 1)

js = r'''
<script>
/* HAIL MONEY FORGOT PASSWORD V8 */
(function(){
  'use strict';
  var cooldownUntil = 0;

  function ready(fn){
    if (document.readyState === 'loading') document.addEventListener('DOMContentLoaded', fn, {once:true});
    else fn();
  }

  function loginEmailInput(){
    return document.getElementById('login-email') || document.querySelector('#page-login input[type="email"]');
  }

  function stampNow(){
    try { return new Date().toLocaleTimeString([], {hour:'numeric', minute:'2-digit'}); }
    catch (_) { return new Date().toLocaleTimeString(); }
  }

  function openReset(){
    var existing = document.getElementById('hmForgotOverlay');
    if (existing) existing.remove();
    var currentEmail = loginEmailInput();
    var overlay = document.createElement('div');
    overlay.id = 'hmForgotOverlay';
    overlay.style.cssText = 'position:fixed;inset:0;z-index:2147483647;background:rgba(0,0,0,.72);display:flex;align-items:center;justify-content:center;padding:16px;';
    overlay.innerHTML = '<div style="width:min(420px,100%);background:#171717;border:1px solid #c99d3e;border-radius:14px;padding:20px;color:#fff;box-shadow:0 24px 70px rgba(0,0,0,.5)"><h2 style="margin:0 0 8px">Reset password</h2><p style="margin:0 0 14px;color:#cfcfcf">Enter the exact email you use to sign in to Hail Money.</p><input id="hmForgotEmail" type="email" autocomplete="email" style="width:100%;box-sizing:border-box;padding:12px;border-radius:8px;border:1px solid #555;background:#0f0f0f;color:#fff"/><div id="hmForgotStatus" style="min-height:42px;margin-top:10px;font-size:13px;line-height:1.35"></div><div style="display:flex;gap:10px;justify-content:flex-end;margin-top:14px"><button id="hmForgotCancel" type="button" style="padding:10px 14px">Close</button><button id="hmForgotSend" type="button" style="padding:10px 14px;background:#c99d3e;border:0;border-radius:8px;font-weight:700">Send fresh reset email</button></div></div>';
    document.body.appendChild(overlay);
    var email = document.getElementById('hmForgotEmail');
    email.value = currentEmail && currentEmail.value ? String(currentEmail.value).trim().toLowerCase() : '';
    email.focus();
    document.getElementById('hmForgotCancel').onclick = function(){ overlay.remove(); };
    document.getElementById('hmForgotSend').onclick = async function(){
      var status = document.getElementById('hmForgotStatus');
      var address = String(email.value || '').trim().toLowerCase();
      var send = this;
      if (!address) { status.textContent = 'Enter your email address.'; status.style.color = '#ff8b8b'; return; }
      if (!window.auth || typeof window.auth.sendPasswordResetEmail !== 'function') {
        status.textContent = 'Firebase Authentication is not available yet. Refresh the page and try again.';
        status.style.color = '#ff8b8b';
        return;
      }
      var now = Date.now();
      if (now < cooldownUntil) {
        var secs = Math.max(1, Math.ceil((cooldownUntil - now) / 1000));
        status.textContent = 'A fresh reset was already sent. Wait ' + secs + ' seconds before requesting another one, and use the newest email.';
        status.style.color = '#ffd66b';
        return;
      }
      send.disabled = true;
      send.textContent = 'Sending...';
      try {
        await window.auth.sendPasswordResetEmail(address);
        cooldownUntil = Date.now() + 90000;
        var sentAt = stampNow();
        status.textContent = 'Fresh reset sent to ' + address + ' at ' + sentAt + '. Open the newest Hail Money reset email received after ' + sentAt + '. Do not use an older reset email.';
        status.style.color = '#9fe0ad';
        send.textContent = 'Fresh reset sent';
        setTimeout(function(){ send.disabled = false; send.textContent = 'Send another fresh reset'; }, 90000);
      } catch (error) {
        var code = String(error && error.code || '');
        if (code.indexOf('invalid-email') !== -1) status.textContent = 'That email address is not valid.';
        else if (code.indexOf('too-many-requests') !== -1) status.textContent = 'Firebase temporarily blocked repeated reset requests. Wait a few minutes before sending one more.';
        else status.textContent = ((error && error.message) || 'Could not send password reset email.') + (code ? ' [' + code + ']' : '');
        status.style.color = '#ff8b8b';
        send.disabled = false;
        send.textContent = 'Send fresh reset email';
      }
    };
  }

  ready(function(){
    var btn = document.getElementById('forgotPasswordBtn');
    if (btn) btn.addEventListener('click', openReset);
  });
})();
</script>
'''

if '</body>' not in text:
    raise SystemExit('Could not find closing body tag')
text = text.replace('</body>', js + '\n</body>', 1)
path.write_text(text, encoding='utf-8')
print('Added native forgot-password control and fixed admin password target selection')
