const API = "/api";
const TOKEN_KEY = "token";
const USER_KEY = "user";

const state = {
  token: localStorage.getItem(TOKEN_KEY) || localStorage.getItem("sv_token") || null,
  user: JSON.parse(localStorage.getItem(USER_KEY) || localStorage.getItem("sv_user") || "null"),
  currentPage: "discover",
  prevPage: "discover",
  currentAlbum: null,
  queue: [],
  queueIndex: -1,
  audio: new Audio(),
  playing: false,
};

async function api(method, path, body) {
  const opts = { method, headers: { "Content-Type": "application/json" } };
  if (state.token) opts.headers["Authorization"] = "Bearer " + state.token;
  if (body) opts.body = JSON.stringify(body);
  const res = await fetch(API + path, opts);
  const data = await res.json().catch(() => ({}));
  if (!res.ok) throw new Error(data.error || "Request failed");
  return data;
}

function switchTab(tab) {
  document.querySelectorAll(".auth-tab").forEach((t, i) =>
    t.classList.toggle("active", (i === 0 && tab === "login") || (i === 1 && tab === "register"))
  );
  document.getElementById("login-form").style.display = tab === "login" ? "" : "none";
  document.getElementById("register-form").style.display = tab === "register" ? "" : "none";
  document.getElementById("auth-error").style.display = "none";
}

async function login() {
  const email = document.getElementById("login-email").value;
  const password = document.getElementById("login-password").value;
  try {
    const data = await api("POST", "/auth/login", { email, password });
    setAuth(data.token, data.user);
    initApp();
  } catch (e) {
    showAuthError(e.message);
  }
}

async function register() {
  const username = document.getElementById("reg-username").value;
  const email = document.getElementById("reg-email").value;
  const password = document.getElementById("reg-password").value;
  try {
    await api("POST", "/auth/register", { email, username, password });
    showToast("Account created! Please sign in.", "success");
    switchTab("login");
  } catch (e) {
    showAuthError(e.message);
  }
}

function setAuth(token, user) {
  state.token = token;
  state.user = user;
  localStorage.setItem(TOKEN_KEY, token);
  localStorage.setItem(USER_KEY, JSON.stringify(user));
  // backward compatibility with old template keys
  localStorage.setItem("sv_token", token);
  localStorage.setItem("sv_user", JSON.stringify(user));
}

function logout() {
  localStorage.removeItem(TOKEN_KEY);
  localStorage.removeItem(USER_KEY);
  localStorage.removeItem("sv_token");
  localStorage.removeItem("sv_user");
  location.href = "/";
}

function showAuthError(msg) {
  const el = document.getElementById("auth-error");
  el.textContent = msg;
  el.style.display = "block";
}

function initApp() {
  document.getElementById("auth-overlay").classList.add("hidden");
  document.getElementById("app").style.display = "flex";
  updateSidebar();
  loadDiscover();
  loadProfile();
}

function updateSidebar() {
  if (!state.user) return;
  const initial = (state.user.display_name || state.user.username || "U")[0].toUpperCase();
  document.getElementById("user-avatar-sidebar").textContent = initial;
  document.getElementById("sidebar-username").textContent = state.user.display_name || state.user.username;
  document.getElementById("sidebar-email").textContent = state.user.email || "-";
}

function showPage(page) {
  state.prevPage = state.currentPage;
  state.currentPage = page;
  document.querySelectorAll(".page").forEach((p) => p.classList.remove("active"));
  document.getElementById("page-" + page).classList.add("active");
  document.querySelectorAll(".nav-item").forEach((n) => n.classList.remove("active"));
  const nav = document.getElementById("nav-" + page);
  if (nav) nav.classList.add("active");
  if (page === "discover") loadDiscover();
  if (page === "library") loadLibrary();
}

function goBack() {
  showPage(state.prevPage === "album" ? "discover" : state.prevPage);
}

async function loadDiscover() {
  const el = document.getElementById("discover-content");
  el.innerHTML = '<div class="loading"><div class="spinner"></div>Loading...</div>';
  try {
    const albums = await api("GET", "/albums");
    renderAlbumGrid(el, albums || []);
  } catch (e) {
    el.innerHTML = `<div class="empty-state"><div class="empty-state-icon">🌐</div><h3>${e.message}</h3></div>`;
  }
}

async function loadLibrary() {
  const el = document.getElementById("library-content");
  el.innerHTML = '<div class="loading"><div class="spinner"></div>Loading...</div>';
  try {
    const albums = await api("GET", "/my-albums");
    renderAlbumGrid(el, albums || []);
  } catch (e) {
    el.innerHTML = '<div class="empty-state"><div class="empty-state-icon">📁</div><h3>No albums yet</h3></div>';
  }
}

function renderAlbumGrid(container, albums) {
  if (!albums.length) {
    container.innerHTML = '<div class="empty-state"><div class="empty-state-icon">🎵</div><h3>No albums here yet</h3></div>';
    return;
  }
  const colors = ["cover-1", "cover-2", "cover-3", "cover-4", "cover-5"];
  const emojis = ["🎵", "🎸", "🎹", "🎺", "🥁", "🎻", "🎤", "🎧"];
  container.innerHTML = `<div class="albums-grid">${albums
    .map(
      (a, i) => `
    <div class="album-card" onclick="openAlbum(${a.id})">
      <div class="album-cover">
        ${
          a.cover_url
            ? `<img src="${esc(a.cover_url)}" onerror="this.parentElement.innerHTML='<div class=\\'album-cover-placeholder ${
                colors[i % 5]
              }\\'>${emojis[i % 8]}</div>'">`
            : `<div class="album-cover-placeholder ${colors[i % 5]}">${emojis[i % 8]}</div>`
        }
        <div class="album-cover-overlay">
          <button class="play-btn-large" onclick="event.stopPropagation(); playAlbum(${a.id})">
            <svg width="22" height="22" fill="currentColor" viewBox="0 0 24 24"><polygon points="5,3 19,12 5,21" fill="#000"/></svg>
          </button>
        </div>
      </div>
      <div class="album-info">
        <div class="album-title">${esc(a.title)}</div>
        <div class="album-meta">
          ${a.year ? `<span>${a.year}</span>` : ""}
          <span class="badge badge-${a.visibility}">${a.visibility}</span>
        </div>
      </div>
    </div>
  `
    )
    .join("")}</div>`;
}

async function openAlbum(id) {
  state.prevPage = state.currentPage;
  showPage("album");
  const el = document.getElementById("album-detail-content");
  el.innerHTML = '<div class="loading"><div class="spinner"></div>Loading...</div>';
  try {
    const album = await api("GET", `/albums/${id}`);
    state.currentAlbum = album;
    renderAlbumDetail(el, album);
  } catch (e) {
    el.innerHTML = `<div class="empty-state"><div class="empty-state-icon">⚠️</div><h3>${e.message}</h3></div>`;
  }
}

function renderAlbumDetail(container, album) {
  const isOwner = state.user && album.owner_id === state.user.id;
  const emojis = ["🎵", "🎸", "🎹", "🎺", "🥁", "🎻"];
  const emoji = emojis[album.id % emojis.length];
  const totalDuration = (album.tracks || []).reduce((s, t) => s + t.duration, 0);

  container.innerHTML = `
    <div class="album-detail-header">
      <div class="album-detail-cover">
        ${
          album.cover_url
            ? `<img src="${esc(album.cover_url)}" onerror="this.parentElement.innerHTML='<div class=\\'album-detail-cover-placeholder\\'>${emoji}</div>'">`
            : `<div class="album-detail-cover-placeholder">${emoji}</div>`
        }
      </div>
      <div class="album-detail-info">
        <div class="album-detail-type">Album &nbsp;•&nbsp; <span class="badge badge-${album.visibility}">${album.visibility}</span></div>
        <div class="album-detail-title">${esc(album.title)}</div>
        <div class="album-detail-meta">
          ${album.genre ? esc(album.genre) + " &nbsp;•&nbsp; " : ""}
          ${album.year ? album.year + " &nbsp;•&nbsp; " : ""}
          ${(album.tracks || []).length} songs, ${fmtTime(totalDuration)}
        </div>
        ${album.description ? `<div style="margin-top:12px;color:var(--muted);font-size:14px;">${esc(album.description)}</div>` : ""}
      </div>
    </div>

    <div class="album-actions">
      <button class="btn-play" onclick="playAlbum(${album.id})">
        <svg width="24" height="24" fill="currentColor" viewBox="0 0 24 24"><polygon points="5,3 19,12 5,21" fill="#000"/></svg>
      </button>
      ${
        isOwner
          ? `
        <button class="btn-visibility" onclick="toggleVisibility(${album.id}, '${album.visibility}')">
          ${album.visibility === "public" ? "🔒 Make Private" : "🌐 Make Public"}
        </button>
        <button class="btn-create" onclick="openAddTrackModal(${album.id})" style="font-size:13px;padding:8px 16px;">
          + Add Track
        </button>
      `
          : ""
      }
    </div>

    ${
      (album.tracks || []).length
        ? `
      <div class="track-list-header">
        <span>#</span><span>Title</span>
        <span style="text-align:right">Plays</span>
        <span style="text-align:right">Duration</span>
        <span></span>
      </div>
      ${(album.tracks || [])
        .sort((a, b) => a.track_num - b.track_num)
        .map(
          (t, i) => `
        <div class="track-row" onclick="playTrackAt(${i})">
          <div class="track-num">${t.track_num || i + 1}</div>
          <div>
            <div class="track-name">${esc(t.title)}</div>
            ${t.artists ? `<div class="track-artist">${esc(t.artists)}</div>` : ""}
          </div>
          <div class="track-plays">${fmtPlays(t.plays || 0)}</div>
          <div class="track-duration">${fmtTime(t.duration || 0)}</div>
          <div class="track-actions">
            ${isOwner ? `<button class="btn-delete-track" onclick="event.stopPropagation(); deleteTrack(${album.id}, ${t.id})">🗑</button>` : ""}
          </div>
        </div>
      `
        )
        .join("")}
    `
        : `
      <div class="empty-state" style="padding:40px;">
        <div class="empty-state-icon">🎵</div>
        <h3>No tracks yet</h3>
        ${isOwner ? "<p>Add your first track above</p>" : ""}
      </div>
    `
    }
  `;
}

function openCreateAlbumModal() {
  document.getElementById("create-album-modal").classList.remove("hidden");
}

async function createAlbum() {
  const title = document.getElementById("new-album-title").value.trim();
  if (!title) return showToast("Title is required", "error");
  try {
    await api("POST", "/albums", {
      title,
      description: document.getElementById("new-album-desc").value,
      genre: document.getElementById("new-album-genre").value,
      year: parseInt(document.getElementById("new-album-year").value, 10) || 0,
      cover_url: document.getElementById("new-album-cover").value,
    });
    closeModal("create-album-modal");
    showToast("Album created!", "success");
    loadLibrary();
  } catch (e) {
    showToast(e.message, "error");
  }
}

async function deleteAlbumById(id) {
  if (!confirm("Delete this album?")) return;
  try {
    await api("DELETE", `/albums/${id}`);
    showToast("Album deleted", "success");
    loadLibrary();
  } catch (e) {
    showToast(e.message, "error");
  }
}

async function toggleVisibility(albumID, current) {
  const newVis = current === "public" ? "private" : "public";
  try {
    await api("PATCH", `/albums/${albumID}/visibility`, { visibility: newVis });
    showToast(`Album is now ${newVis}`, "success");
    openAlbum(albumID);
  } catch (e) {
    showToast(e.message, "error");
  }
}

let addTrackAlbumID = null;

function openAddTrackModal(albumID) {
  addTrackAlbumID = albumID;
  document.getElementById("add-track-modal").classList.remove("hidden");
}

async function addTrack() {
  const title = document.getElementById("new-track-title").value.trim();
  if (!title) return showToast("Title is required", "error");
  try {
    await api("POST", `/albums/${addTrackAlbumID}/tracks`, {
      title,
      artists: document.getElementById("new-track-artists").value,
      duration: parseInt(document.getElementById("new-track-duration").value, 10) || 0,
      track_num: parseInt(document.getElementById("new-track-num").value, 10) || 1,
      audio_url: document.getElementById("new-track-audio").value,
    });
    closeModal("add-track-modal");
    showToast("Track added!", "success");
    openAlbum(addTrackAlbumID);
  } catch (e) {
    showToast(e.message, "error");
  }
}

async function deleteTrack(albumID, trackID) {
  if (!confirm("Remove this track?")) return;
  try {
    await api("DELETE", `/albums/${albumID}/tracks/${trackID}`);
    showToast("Track removed", "success");
    openAlbum(albumID);
  } catch (e) {
    showToast(e.message, "error");
  }
}

async function loadProfile() {
  if (!state.user) return;
  try {
    const u = await api("GET", "/users/me");
    state.user = u;
    const initial = (u.display_name || u.username || "U")[0].toUpperCase();
    document.getElementById("profile-avatar-big").textContent = initial;
    document.getElementById("profile-username").textContent = u.username || "-";
    document.getElementById("profile-email").textContent = u.email || "-";
    document.getElementById("profile-displayname").value = u.display_name || "";
    updateSidebar();
  } catch (_) {}
}

async function updateProfile() {
  try {
    const u = await api("PUT", "/users/me", {
      display_name: document.getElementById("profile-displayname").value,
    });
    state.user = u;
    localStorage.setItem(USER_KEY, JSON.stringify(u));
    localStorage.setItem("sv_user", JSON.stringify(u));
    showToast("Profile updated!", "success");
    updateSidebar();
  } catch (e) {
    showToast(e.message, "error");
  }
}

async function playAlbum(albumID) {
  try {
    const album = await api("GET", `/albums/${albumID}`);
    const tracks = (album.tracks || []).sort((a, b) => a.track_num - b.track_num);
    if (!tracks.length) return showToast("No tracks", "error");
    state.queue = tracks;
    state.currentAlbum = album;
    playQueueIndex(0);
  } catch (e) {
    showToast(e.message, "error");
  }
}

function playTrackAt(index) {
  if (!state.currentAlbum) return;
  const tracks = (state.currentAlbum.tracks || []).sort((a, b) => a.track_num - b.track_num);
  state.queue = tracks;
  playQueueIndex(index);
}

let simInterval = null;
let simCurrent = 0;
let simTotal = 0;

function playQueueIndex(i) {
  if (i < 0 || i >= state.queue.length) return;
  state.queueIndex = i;
  const track = state.queue[i];

  document.getElementById("player-track-name").textContent = track.title;
  document.getElementById("player-track-artist").textContent = track.artists || (state.currentAlbum ? state.currentAlbum.title : "");
  document.getElementById("total-time").textContent = fmtTime(track.duration || 0);

  api("POST", `/albums/${track.album_id}/tracks/${track.id}/play`).catch(() => {});

  if (track.audio_url) {
    state.audio.src = track.audio_url;
    state.audio.play().catch(() => {});
  } else {
    simulatePlayback(track.duration || 180);
  }
  state.playing = true;
  updatePlayButton();
}

function simulatePlayback(duration) {
  clearInterval(simInterval);
  simCurrent = 0;
  simTotal = duration;
  simInterval = setInterval(() => {
    if (!state.playing) return;
    simCurrent++;
    if (simCurrent >= simTotal) {
      clearInterval(simInterval);
      nextTrack();
      return;
    }
    document.getElementById("progress-fill").style.width = (simCurrent / simTotal) * 100 + "%";
    document.getElementById("current-time").textContent = fmtTime(simCurrent);
  }, 1000);
}

function togglePlay() {
  state.playing = !state.playing;
  if (state.audio.src) {
    if (state.playing) state.audio.play();
    else state.audio.pause();
  }
  updatePlayButton();
}

function updatePlayButton() {
  document.getElementById("play-icon").style.display = state.playing ? "none" : "";
  document.getElementById("pause-icon").style.display = state.playing ? "" : "none";
}

function nextTrack() {
  playQueueIndex(state.queueIndex + 1);
}

function prevTrack() {
  playQueueIndex(state.queueIndex - 1);
}

function seekTo(e) {
  const pct = (e.clientX - e.currentTarget.getBoundingClientRect().left) / e.currentTarget.offsetWidth;
  if (state.audio.duration) {
    state.audio.currentTime = pct * state.audio.duration;
  } else if (simTotal) {
    simCurrent = Math.floor(pct * simTotal);
    document.getElementById("progress-fill").style.width = pct * 100 + "%";
    document.getElementById("current-time").textContent = fmtTime(simCurrent);
  }
}

state.audio.addEventListener("timeupdate", () => {
  if (!state.audio.duration) return;
  const pct = (state.audio.currentTime / state.audio.duration) * 100;
  document.getElementById("progress-fill").style.width = pct + "%";
  document.getElementById("current-time").textContent = fmtTime(Math.floor(state.audio.currentTime));
  document.getElementById("total-time").textContent = fmtTime(Math.floor(state.audio.duration));
});
state.audio.addEventListener("ended", nextTrack);

function esc(str) {
  if (!str) return "";
  return str.replace(/&/g, "&amp;").replace(/</g, "&lt;").replace(/>/g, "&gt;").replace(/"/g, "&quot;");
}

function fmtTime(s) {
  return `${Math.floor(s / 60)}:${String(s % 60).padStart(2, "0")}`;
}

function fmtPlays(n) {
  if (n >= 1e6) return (n / 1e6).toFixed(1) + "M";
  if (n >= 1e3) return Math.floor(n / 1e3) + "K";
  return String(n);
}

function closeModal(id) {
  document.getElementById(id).classList.add("hidden");
}

let toastTimer;
function showToast(msg, type = "") {
  const el = document.getElementById("toast");
  el.textContent = msg;
  el.className = "show " + type;
  clearTimeout(toastTimer);
  toastTimer = setTimeout(() => {
    el.className = "";
  }, 3000);
}

if (!state.token) {
  const next = encodeURIComponent("/library.html");
  location.href = `/?next=${next}`;
} else {
  // Migrate to shared keys so home and library stay in sync.
  localStorage.setItem(TOKEN_KEY, state.token);
  if (state.user) localStorage.setItem(USER_KEY, JSON.stringify(state.user));
  initApp();
}
