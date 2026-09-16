from pathlib import Path

path = Path('public/index.html')
text = path.read_text(encoding='utf-8')
marker = 'HAIL MONEY FORGOT PASSWORD V3'
if marker in text:
    print('Forgot password reset already applied')
    raise SystemExit(0)

js = r'''
<script>
/* HAIL MONEY FORGOT PASSWORD V3 */
(function(){
  'use strict';

  function ready(fn){
    if (document.readyState === 'loading') document.addEventListener('DOMContentLoaded', fn, {once:true});
    else fn();
  }

  function findEmail(){
    var screen = document.getElementById('authScreen') || document;
    return document.getElementById('authEmail') || screen.querySelector('input[type="email"]') || screen.querySelector('input[name*="email" i]') || document.querySelector('input[type="email"]');
  }

  function authScreenVisible(){
    var screen = document.getElementById('authScreen');
    if (!screen) return true;
    var s = getComputedStyle(screen);
    return s.display !== 'none' && s.visibility !== 'hidden' && !screen.hidden;
  }

  function openReset(){
    var existing = document.getElementById('hmForgotOverlay');
    if (existing) existing.remove();
    var currentEmail = findEmail();
    var overlay = document.createElement('div');
    overlay.id = 'hmForgotOverlay';
    overlay.style.cssText = 'position:fixed;inset:0;z-index:2147483647;background:rgba(0,0,0,.72);display:flex;align-items:center;justify-content:center;padding:16px;';
    overlay.innerHTML = '<div style="width:min(420px,100%);background:#171717;border:1px solid #c99d3e;border-radius:14px;padding:20px;color:#fff;box-shadow:0 24px 70px rgba(0,0,0,.5)"><h2 style="margin:0 0 8px">Reset password</h2><p style="margin:0 0 14px;color:#cfcfcf">Enter the email for your Hail Money account.</p><input id="hmForgotEmail" type="email" autocomplete="email" style="width:100%;box-sizing:border-box;padding:12px;border-radius:8px;border:1px solid #555;background:#0f0f0f;color:#fff"/><div id="hmForgotStatus" style="min-height:20px;margin-top:10px;font-size:13px"></div><div style="display:flex;gap:10px;justify-content:flex-end;margin-top:14px"><button id="hmForgotCancel" type="button" style="padding:10px 14px">Cancel</button><button id="hmForgotSend" type="button" style="padding:10px 14px;background:#c99d3e;border:0;border-radius:8px;font-weight:700">Send reset email</button></div></div>';
    document.body.appendChild(overlay);
    var email = document.getElementById('hmForgotEmail');
    email.value = currentEmail && currentEmail.value ? currentEmail.value : '';
    email.focus();
    document.getElementById('hmForgotCancel').onclick = function(){ overlay.remove(); };
    document.getElementById('hmForgotSend').onclick = async function(){
      var status = document.getElementById('hmForgotStatus');
      var address = String(email.value || '').trim();
      if (!address) { status.textContent = 'Enter your email address.'; status.style.color = '#ff8b8b'; return; }
      if (!window.auth || typeof window.auth.sendPasswordResetEmail !== 'function') { status.textContent = 'Password reset is temporarily unavailable.'; status.style.color = '#ff8b8b'; return; }
      var send = this; send.disabled = true; send.textContent = 'Sending...';
      try {
        await window.auth.sendPasswordResetEmail(address);
        status.textContent = 'Reset email sent. Check your inbox and spam folder.';
        status.style.color = '#9fe0ad';
      } catch (error) {
        var code = String(error && error.code || '');
        if (code.indexOf('invalid-email') !== -1) status.textContent = 'Enter a valid email address.';
        else if (code.indexOf('too-many-requests') !== -1) status.textContent = 'Too many attempts. Wait a few minutes and try again.';
        else if (code.indexOf('user-not-found') !== -1) status.textContent = 'If that email is registered, a reset email has been sent.';
        else status.textContent = (error && error.message) || 'Could not send password reset email.';
        status.style.color = code.indexOf('user-not-found') !== -1 ? '#9fe0ad' : '#ff8b8b';
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
    dock.style.display = authScreenVisible() ? 'block' : 'none';
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
print('Added always-visible forgot-password control to login screen')
