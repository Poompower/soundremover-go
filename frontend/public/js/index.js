const API_BASE = "http://localhost:8080"; // gateway

function setAlert(el, msg) {
  el.textContent = msg;
  el.classList.remove("d-none");
}
function clearAlert(el) {
  el.textContent = "";
  el.classList.add("d-none");
}

function getToken() {
  return localStorage.getItem("token") || "";
}
function setToken(t) {
  localStorage.setItem("token", t);
}
function clearToken() {
  localStorage.removeItem("token");
  localStorage.removeItem("user");
  localStorage.removeItem("sv_token");
  localStorage.removeItem("sv_user");
}

function setUser(u) {
  localStorage.setItem("user", JSON.stringify(u));
}
function getUser() {
  try { return JSON.parse(localStorage.getItem("user") || "null"); }
  catch { return null; }
}

function openTab(which) {
  const loginTab = document.getElementById("login-tab");
  const regTab = document.getElementById("register-tab");
  if (which === "register") regTab?.click();
  else loginTab?.click();
}

function updateNavbar() {
  const token = getToken();
  const user = getUser();
  const navHome = document.getElementById("navHome");
  const logoutBtn = document.getElementById("logoutBtn");
  const dashboardBtn = document.getElementById("dashboardBtn");
  const heroActions = document.getElementById("heroActions");

  if (token && user) {
    // auth
    logoutBtn.classList.remove("d-none");
    dashboardBtn?.classList.remove("d-none");
    navHome.href = user.is_staff ? "/admin_dashboard" : "/dashboard.html";

    heroActions.innerHTML = `
      <a class="btn btn-primary rounded-3 px-4"
         href="${user.is_staff ? "/admin_dashboard" : "/dashboard.html"}">
        Go to Dashboard
      </a>
    `;
  } else {
    logoutBtn.classList.add("d-none");
    dashboardBtn?.classList.add("d-none");
    navHome.href = "/";
    heroActions.innerHTML = `
      <button type="button" class="btn-space" data-bs-toggle="modal" data-bs-target="#authModal"
        onclick="openTab('login')" id="authBtn">
        <strong>Sign In/Sign Up</strong>
        <div id="container-stars"><div id="stars"></div></div>
        <div id="glow"><div class="circle"></div><div class="circle"></div></div>
      </button>
    `;
  }
}

document.addEventListener("DOMContentLoaded", () => {
  updateNavbar();

  const loginForm = document.getElementById("loginForm");
  const registerForm = document.getElementById("registerForm");
  const loginAlert = document.getElementById("loginAlert");
  const registerAlert = document.getElementById("registerAlert");
  const logoutBtn = document.getElementById("logoutBtn");

  logoutBtn.addEventListener("click", () => {
    clearToken();
    updateNavbar();
  });

  loginForm.addEventListener("submit", async (e) => {
    e.preventDefault();
    clearAlert(loginAlert);

    const fd = new FormData(loginForm);
    const payload = {
      username: fd.get("username"),
      password: fd.get("password"),
    };

    try {
      const res = await fetch(`${API_BASE}/api/login`, {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify(payload),
      });
      const data = await res.json().catch(() => ({}));

      if (!res.ok) {
        setAlert(loginAlert, data.error || "Login failed");
        return;
      }

      setToken(data.token);
      setUser(data.user);
      localStorage.setItem("sv_token", data.token);
      localStorage.setItem("sv_user", JSON.stringify(data.user));

      // ปิด modal
      const modalEl = document.getElementById("authModal");
      const modal = bootstrap.Modal.getInstance(modalEl) || new bootstrap.Modal(modalEl);
      modal.hide();

      const next = new URLSearchParams(window.location.search).get("next");
      if (next && next.startsWith("/")) {
        window.location.href = next;
        return;
      }
      updateNavbar();
    } catch (err) {
      setAlert(loginAlert, "Gateway ไม่ตอบ (เช็ค docker/port)");
    }
  });

  registerForm.addEventListener("submit", async (e) => {
    e.preventDefault();
    clearAlert(registerAlert);

    const fd = new FormData(registerForm);
    const payload = {
      first_name: fd.get("first_name"),
      last_name: fd.get("last_name"),
      email: fd.get("email"),
      username: fd.get("username"),
      password: fd.get("password"),
    };

    if (!payload.password || payload.password.length < 8) {
      setAlert(registerAlert, "รหัสผ่านต้องยาวอย่างน้อย 8 ตัว");
      return;
    }

    try {
      const res = await fetch(`${API_BASE}/api/register`, {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify(payload),
      });
      const data = await res.json().catch(() => ({}));

      if (!res.ok) {
        setAlert(registerAlert, data.error || "Register failed");
        return;
      }

      // หลังสมัครเสร็จ สลับไปหน้า login ให้เลย
      openTab("login");
      setAlert(loginAlert, "สมัครสำเร็จ! ลอง Sign In ได้เลย");
      clearAlert(registerAlert);
    } catch (err) {
      setAlert(registerAlert, "Gateway ไม่ตอบ (เช็ค docker/port)");
    }
  });
});
