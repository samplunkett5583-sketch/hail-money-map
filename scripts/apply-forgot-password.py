from pathlib import Path
import re

path = Path('public/index.html')
text = path.read_text(encoding='utf-8')
marker = 'HAIL MONEY FORGOT PASSWORD V1'
if marker in text:
    print('Forgot password reset already applied')
    raise SystemExit(0)

# Add a visible reset link immediately after the password field without depending on
# the surrounding login-card markup, which has changed several times.
password_pattern = re.compile(r'(<input\b[^>]*\bid=["\']authPassword["\'][^>]*>)', re.I)
match = password_pattern.search(text)
if not match:
    raise SystemExit('Could not find authPassword input')

button = r'''\1
        <button type="button" id="forgotPasswordBtn" style="appearance:none;background:none;border:0;padding:8px 0 2px;color:#c99d3e;font:inherit;font-weight:700;text-decoration:underline;cursor:pointer;max-width:100%;">Forgot password?</button>'''
text = password_pattern.sub(button, text, count=1)

js = r'''
<script>
/* HAIL MONEY FORGOT PASSWORD V1 */
(function(){
  'use strict';
  function ready(fn){
    if (document.readyState === 'loading') document.addEventListener('DOMContentLoaded', fn, {once:true});
    else fn();
  }
  ready(function(){
    var btn = document.getElementById('forgotPasswordBtn');
    var emailInput = document.getElementById('authEmail');
    var status = document.getElementById('authStatus');
    if (!btn || !emailInput) return;

    function show(message, isError){
      if (typeof window.setAuthStatus === 'function') {
        window.setAuthStatus(message, !!isError);
        return;
      }
      if (status) {
        status.textContent = message;
        status.style.color = isError ? '#ff8b8b' : '#9fe0ad';
      }
    }

    btn.addEventListener('click', async function(){
      var email = String(emailInput.value || '').trim();
      if (!email) {
        show('Enter your email address first.', true);
        emailInput.focus();
        return;
      }
      if (!window.auth || typeof window.auth.sendPasswordResetEmail !== 'function') {
        show('Password reset is temporarily unavailable. Please try again in a moment.', true);
        return;
      }
      btn.disabled = true;
      var original = btn.textContent;
      btn.textContent = 'Sending reset email...';
      try {
        await window.auth.sendPasswordResetEmail(email);
        show('Password reset email sent. Check your inbox and spam folder.', false);
      } catch (error) {
        var code = String(error && error.code || '');
        var message = (error && error.message) || 'Could not send password reset email.';
        if (code.indexOf('invalid-email') !== -1) message = 'Enter a valid email address.';
        else if (code.indexOf('too-many-requests') !== -1) message = 'Too many reset attempts. Wait a few minutes and try again.';
        // Do not reveal whether an account exists for the supplied email.
        else if (code.indexOf('user-not-found') !== -1) message = 'If that email is registered, a reset email has been sent.';
        show(message, true);
      } finally {
        btn.disabled = false;
        btn.textContent = original;
      }
    });
  });
})();
</script>
'''

if '</body>' not in text:
    raise SystemExit('Could not find closing body tag')
text = text.replace('</body>', js + '\n</body>', 1)
path.write_text(text, encoding='utf-8')
print('Added forgot-password reset link to login screen')
