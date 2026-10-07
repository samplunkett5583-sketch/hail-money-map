(function () {
  'use strict';

  function setupPasswordForm() {
    var overlay = document.getElementById('login-reset-modal-overlay');
    var save = document.getElementById('login-reset-save-btn');
    var next = document.getElementById('login-reset-password');
    var confirm = document.getElementById('login-reset-password-confirm');
    var message = document.getElementById('login-reset-modal-msg');
    if (!overlay || !save || !next || !confirm || !message) return;

    var field = document.createElement('div');
    field.className = 'field full';
    var label = document.createElement('label');
    label.textContent = 'Current Password';
    label.htmlFor = 'hm-current-password';
    var current = document.createElement('input');
    current.id = 'hm-current-password';
    current.type = 'password';
    current.autocomplete = 'current-password';
    field.appendChild(label);
    field.appendChild(current);
    next.parentNode.parentNode.insertBefore(field, next.parentNode);

    var title = document.getElementById('login-reset-modal-title');
    if (title) title.textContent = 'Change Password';

    function fail(text) {
      message.textContent = text;
      message.style.display = 'block';
    }
    function close() {
      overlay.style.display = 'none';
      current.value = '';
      next.value = '';
      confirm.value = '';
      message.style.display = 'none';
    }

    var cancel = document.createElement('button');
    cancel.type = 'button';
    cancel.className = 'btn';
    cancel.textContent = 'Cancel';
    save.parentNode.insertBefore(cancel, save);
    cancel.addEventListener('click', close);

    document.addEventListener('click', function (event) {
      var action = event.target && event.target.closest && event.target.closest('[data-profile-action="password"]');
      if (action) {
        current.value = '';
        setTimeout(function () { if (overlay.style.display !== 'none') current.focus(); }, 25);
      }
    }, true);

    document.addEventListener('click', async function (event) {
      var target = event.target && event.target.closest && event.target.closest('#login-reset-save-btn');
      if (!target) return;
      event.preventDefault();
      event.stopPropagation();
      event.stopImmediatePropagation();
      message.style.display = 'none';
      var oldPass = current.value;
      var newPass = next.value;
      var confirmPass = confirm.value;
      if (!oldPass) return fail('Enter your current password.');
      if (newPass.length < 8) return fail('New password must have at least 8 characters.');
      if (newPass !== confirmPass) return fail('New passwords do not match.');
      if (oldPass === newPass) return fail('Use a different new password.');
      save.disabled = true;
      save.textContent = 'Saving…';
      try {
        await window.hmUpdateMyPassword(oldPass, newPass);
        close();
        alert('Your password was changed successfully.');
      } catch (error) {
        var code = error && error.code || '';
        if (code === 'auth/wrong-password' || code === 'auth/invalid-credential') fail('Current password is incorrect.');
        else if (code === 'auth/too-many-requests') fail('Too many attempts. Try again later.');
        else fail(error && error.message ? error.message : 'Unable to update password.');
      } finally {
        save.disabled = false;
        save.textContent = 'Save Password';
      }
    }, true);

    current.addEventListener('keydown', function (event) {
      if (event.key === 'Enter') { event.preventDefault(); save.click(); }
    });
  }

  window.hmUpdateMyPassword = async function (currentPassword, newPassword) {
    var user = window.auth && window.auth.currentUser;
    if (!user || !user.email) throw new Error('Sign in again before changing your password.');
    var credential = firebase.auth.EmailAuthProvider.credential(user.email, currentPassword);
    await user.reauthenticateWithCredential(credential);
    await user.updatePassword(newPassword);
  };

  if (document.readyState === 'loading') document.addEventListener('DOMContentLoaded', setupPasswordForm);
  else setupPasswordForm();
}());
