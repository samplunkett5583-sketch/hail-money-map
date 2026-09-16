from pathlib import Path

path = Path('public/index.html')
text = path.read_text(encoding='utf-8')
marker = 'HAIL MONEY FORGOT PASSWORD V2'
if marker in text:
    print('Forgot password reset already applied')
    raise SystemExit(0)

js = r'''
<script>
/* HAIL MONEY FORGOT PASSWORD V2 */
(function(){
  'use strict';

  function ready(fn){
    if (document.readyState === 'loading') document.addEventListener('DOMContentLoaded', fn, {once:true});
    else fn();
  }

  function findLoginInputs(){
    var screen = document.getElementById('authScreen') || document;
    var email = document.getElementById('authEmail') || screen.querySelector('input[type="email"]') || screen.querySelector('input[name*="email" i]');
    var password = document.getElementById('authPassword') || screen.querySelector('input[type="password"]') || screen.querySelector('input[name*="password" i]');
    return { screen: screen, email: email, password: password };
  }

  function showStatus(message, isError){
    var status = document.getElementById('authStatus');
    if (status) {
      status.textContent = message;
      status.style.color = isError ? '#ff8b8b' : '#9fe0ad';
      return;
    }
    var inputs = findLoginInputs();
    if (!inputs.password || !inputs.password.parentNode) return;
    status = document.createElement('div');
    status.id = 'hmForgotPasswordStatus';
    status.style.marginTop = '8px';
    status.style.fontSize = '13px';
    status.style.color = isError ? '#ff8b8b' : '#9fe0ad';
    status.textContent = message;
    inputs.password.parentNode.insertBefore(status, inputs.password.nextSibling);
  }

  function install(){
    if (document.getElementById('forgotPasswordBtn')) return true;
    var inputs = findLoginInputs();
    if (!inputs.password || !inputs.password.parentNode) return false;

    var btn = document.createElement('button');
    btn.type = 'button';
    btn.id = 'forgotPasswordBtn';
    btn.textContent = 'Forgot password?';
    btn.style.cssText = 'appearance:none;background:none;border:0;padding:8px 0 2px;color:#c99d3e;font:inherit;font-weight:700;text-decoration:underline;cursor:pointer;max-width:100%;display:block;';
    inputs.password.parentNode.insertBefore(btn, inputs.password.nextSibling);

    btn.addEventListener('click', async function(){
      var current = findLoginInputs();
      var email = String(current.email && current.email.value || '').trim();
      if (!email) {
        showStatus('Enter your email address first.', true);
        if (current.email) current.email.focus();
        return;
      }
      if (!window.auth || typeof window.auth.sendPasswordResetEmail !== 'function') {
        showStatus('Password reset is temporarily unavailable. Please try again in a moment.', true);
        return;
      }

      btn.disabled = true;
      var original = btn.textContent;
      btn.textContent = 'Sending reset email...';
      try {
        await window.auth.sendPasswordResetEmail(email);
        showStatus('Password reset email sent. Check your inbox and spam folder.', false);
      } catch (error) {
        var code = String(error && error.code || '');
        if (code.indexOf('invalid-email') !== -1) showStatus('Enter a valid email address.', true);
        else if (code.indexOf('too-many-requests') !== -1) showStatus('Too many reset attempts. Wait a few minutes and try again.', true);
        else if (code.indexOf('user-not-found') !== -1) showStatus('If that email is registered, a reset email has been sent.', false);
        else showStatus((error && error.message) || 'Could not send password reset email.', true);
      } finally {
        btn.disabled = false;
        btn.textContent = original;
      }
    });
    return true;
  }

  ready(function(){
    if (install()) return;
    var attempts = 0;
    var timer = setInterval(function(){
      attempts += 1;
      if (install() || attempts >= 40) clearInterval(timer);
    }, 250);
    new MutationObserver(function(){ install(); }).observe(document.documentElement, {subtree:true, childList:true});
  });
})();
</script>
'''

if '</body>' not in text:
    raise SystemExit('Could not find closing body tag')
text = text.replace('</body>', js + '\n</body>', 1)
path.write_text(text, encoding='utf-8')
print('Added runtime forgot-password reset link to login screen')
