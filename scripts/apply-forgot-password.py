from pathlib import Path

path = Path('public/index.html')
text = path.read_text(encoding='utf-8')
marker = 'HAIL MONEY FORGOT PASSWORD V4'
if marker in text:
    print('Forgot password reset already applied')
    raise SystemExit(0)

js = r'''
<script>
/* HAIL MONEY FORGOT PASSWORD V4 */
(function(){
  'use strict';

  function ready(fn){
    if (document.readyState === 'loading') document.addEventListener('DOMContentLoaded', fn, {once:true});
    else fn();
  }

  function loginEmailInput(){
    return document.getElementById('login-email') ||
      document.getElementById('authEmail') ||
      document.querySelector('input[type="email"]');
  }

  function loginAreaVisible(){
    var email = loginEmailInput();
    if (!email) return false;
    var r = email.getBoundingClientRect();
    var s = getComputedStyle(email);
    return r.width > 0 && r.height > 0 && s.display !== 'none' && s.visibility !== 'hidden';
  }

  function openReset(){
    var existing = document.getElementById('hmForgotOverlay');
    if (existing) existing.remove();
    var currentEmail = loginEmailInput();
    var overlay = document.createElement('div');
    overlay.id = 'hmForgotOverlay';
    overlay.style.cssText = 'position:fixed;inset:0;z-index:2147483647;background:rgba(0,0,0,.72);display:flex;align-items:center;justify-content:center;padding:16px;';
    overlay.innerHTML = '<div style="width:min(420px,100%);background:#171717;border:1px solid #c99d3e;border-radius:14px;padding:20px;color:#fff;box-shadow:0 24px 70px rgba(0,0,0,.5)"><h2 style="margin:0 0 8px">Reset password</h2><p style="margin:0 0 14px;color:#cfcfcf">Enter the exact email you use to sign in to Hail Money.</p><input id="hmForgotEmail" type="email" autocomplete="email" style="width:100%;box-sizing:border-box;padding:12px;border-radius:8px;border:1px solid #555;background:#0f0f0f;color:#fff"/><div id="hmForgotStatus" style="min-height:20px;margin-top:10px;font-size:13px;line-height:1.35"></div><div style="display:flex;gap:10px;justify-content:flex-end;margin-top:14px"><button id="hmForgotCancel" type="button" style="padding:10px 14px">Cancel</button><button id="hmForgotSend" type="button" style="padding:10px 14px;background:#c99d3e;border:0;border-radius:8px;font-weight:700">Send reset email</button></div></div>';
    document.body.appendChild(overlay);
    var email = document.getElementById('hmForgotEmail');
    email.value = currentEmail && currentEmail.value ? String(currentEmail.value).trim().toLowerCase() : '';
    email.focus();
    document.getElementById('hmForgotCancel').onclick = function(){ overlay.remove(); };
    document.getElementById('hmForgotSend').onclick = async function(){
      var status = document.getElementById('hmForgotStatus');
      var address = String(email.value || '').trim().toLowerCase();
      if (!address) { status.textContent = 'Enter your email address.'; status.style.color = '#ff8b8b'; return; }
      if (!window.auth || typeof window.auth.sendPasswordResetEmail !== 'function') {
        status.textContent = 'Firebase Authentication is not available on this page. Refresh and try again.';
        status.style.color = '#ff8b8b';
        return;
      }
      var send = this; send.disabled = true; send.textContent = 'Sending...';
      try {
        await window.auth.sendPasswordResetEmail(address);
        status.textContent = 'Firebase accepted the reset request for ' + address + '. Check that inbox and spam/junk.';
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

  function install(){
    var dock = document.getElementById('forgotPasswordBtn');
    if (!dock) {
      dock = document.createElement('button');
      dock.type = 'button';
      dock.id = 'forgotPasswordBtn';
      dock.textContent = 'Forgot password?';
      dock.style.cssText = 'position:fixed;left:50%;bottom:18px;transform:translateX(-50%);z-index:2147483646;appearance:none;background:#111;border:1px solid #c99d3e;border-radius:999px;padding:10px 16px;color:#c99d3e;font:inherit;font-weight:700;cursor:pointer;box-shadow:0 8px 28px rgba(0,0,0,.4);';
      dock.onclick = openReset;
      document.body.appendChild(dock);
    }
    dock.style.display = loginAreaVisible() ? 'block' : 'none';
  }

  ready(function(){
    install();
    setInterval(install, 500);
    new MutationObserver(install).observe(document.documentElement, {subtree:true, childList:true, attributes:true, attributeFilter:['style','class','hidden']});
  });
})();
</script>
'''

if '</body>' not in text:
    raise SystemExit('Could not find closing body tag')
text = text.replace('</body>', js + '\n</body>', 1)
path.write_text(text, encoding='utf-8')
print('Added corrected forgot-password reset control using the real login email field')
