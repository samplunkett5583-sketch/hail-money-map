from pathlib import Path

path = Path('public/index.html')
text = path.read_text(encoding='utf-8')
marker = 'HAIL MONEY FORGOT PASSWORD V6'
if marker in text:
    print('Forgot password reset already applied')
    raise SystemExit(0)

login_button = '<button type="button" id="login-submit-btn">Log In / Create Account</button>'
forgot_button = '''<button type="button" id="login-submit-btn">Log In / Create Account</button>\n            <button type="button" id="forgotPasswordBtn" style="display:block;width:100%;margin:8px auto 0;padding:7px 10px;border:0;background:transparent;color:#17172c;font-size:12px;font-weight:800;text-decoration:underline;cursor:pointer;">Forgot password?</button>'''
if login_button not in text:
    raise SystemExit('Could not find login submit button')
text = text.replace(login_button, forgot_button, 1)

js = r'''
<script>
/* HAIL MONEY FORGOT PASSWORD V6 */
(function(){
  'use strict';
  function ready(fn){
    if (document.readyState === 'loading') document.addEventListener('DOMContentLoaded', fn, {once:true});
    else fn();
  }
  function openReset(){
    var existing = document.getElementById('hmForgotOverlay');
    if (existing) existing.remove();
    var loginEmail = document.getElementById('login-email');
    var overlay = document.createElement('div');
    overlay.id = 'hmForgotOverlay';
    overlay.style.cssText = 'position:fixed;inset:0;z-index:2147483647;background:rgba(0,0,0,.72);display:flex;align-items:center;justify-content:center;padding:16px;';
    overlay.innerHTML = '<div style="width:min(420px,100%);background:#171717;border:1px solid #c99d3e;border-radius:14px;padding:20px;color:#fff;box-shadow:0 24px 70px rgba(0,0,0,.5)"><h2 style="margin:0 0 8px">Reset password</h2><p style="margin:0 0 14px;color:#cfcfcf">Enter the exact email you use to sign in to Hail Money.</p><input id="hmForgotEmail" type="email" autocomplete="email" style="width:100%;box-sizing:border-box;padding:12px;border-radius:8px;border:1px solid #555;background:#0f0f0f;color:#fff"/><div id="hmForgotStatus" style="min-height:20px;margin-top:10px;font-size:13px;line-height:1.35"></div><div style="display:flex;gap:10px;justify-content:flex-end;margin-top:14px"><button id="hmForgotCancel" type="button" style="padding:10px 14px">Cancel</button><button id="hmForgotSend" type="button" style="padding:10px 14px;background:#c99d3e;border:0;border-radius:8px;font-weight:700">Send reset email</button></div></div>';
    document.body.appendChild(overlay);
    var email = document.getElementById('hmForgotEmail');
    email.value = loginEmail && loginEmail.value ? String(loginEmail.value).trim().toLowerCase() : '';
    email.focus();
    document.getElementById('hmForgotCancel').onclick = function(){ overlay.remove(); };
    document.getElementById('hmForgotSend').onclick = async function(){
      var status = document.getElementById('hmForgotStatus');
      var address = String(email.value || '').trim().toLowerCase();
      if (!address) { status.textContent = 'Enter your email address.'; status.style.color = '#ff8b8b'; return; }
      if (!window.auth || typeof window.auth.sendPasswordResetEmail !== 'function') {
        status.textContent = 'Firebase Authentication is not available yet. Refresh the page and try again.';
        status.style.color = '#ff8b8b';
        return;
      }
      var send = this; send.disabled = true; send.textContent = 'Sending...';
      try {
        await window.auth.sendPasswordResetEmail(address);
        status.textContent = 'Reset request sent for ' + address + '. Check inbox and spam/junk.';
        status.style.color = '#9fe0ad';
      } catch (error) {
        var code = String(error && error.code || '');
        if (code.indexOf('invalid-email') !== -1) status.textContent = 'That email address is not valid.';
        else if (code.indexOf('user-not-found') !== -1) status.textContent = 'No Firebase login account exists for ' + address + '.';
        else if (code.indexOf('too-many-requests') !== -1) status.textContent = 'Firebase blocked repeated reset attempts. Wait a few minutes and try once more.';
        else status.textContent = ((error && error.message) || 'Could not send password reset email.') + (code ? ' [' + code + ']' : '');
        status.style.color = '#ff8b8b';
      } finally {
        send.disabled = false; send.textContent = 'Send reset email';
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
print('Added native forgot-password button directly inside the login card')
