document.getElementById("loginForm").addEventListener("submit", function (e) {
  e.preventDefault(); // Stop the page refreshing

  const email = document.getElementById("authEmail").value;
  const password = document.getElementById("authPassword").value;

  console.log("Attempt login with:", email);

  // Temporary fake login check — replace later with real backend
  if (email === "sam@company.com" && password === "password123") {
    document.getElementById("authStatus").innerText = "Login successful!";
  } else {
    document.getElementById("authStatus").innerText = "Invalid login!";
  }
});
