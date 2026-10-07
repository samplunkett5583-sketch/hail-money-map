(function () {
  'use strict';
  window.hmUpdateMyPassword = async function (currentPassword, newPassword) {
    var user = window.auth && window.auth.currentUser;
    if (!user || !user.email) throw new Error('Sign in again to change your password.');
    var credential = firebase.auth.EmailAuthProvider.credential(user.email, currentPassword);
    await user.reauthenticateWithCredential(credential);
    await user.updatePassword(newPassword);
    return true;
  };
}());
