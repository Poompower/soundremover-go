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
  separateJobID: null,
  separatePollTimer: null,
  separateInitDone: false,
  libraryItems: [],
  currentLibraryItem: null,
  syncMixer: null,
};

function getAuthToken() {
  const token = state.token || localStorage.getItem(TOKEN_KEY) || localStorage.getItem("sv_token") || "";
  if (token && !state.token) state.token = token;
  return token;
}

async function api(method, path, body) {
  const opts = { method, headers: { "Content-Type": "application/json" } };
  const token = getAuthToken();
  if (token) opts.headers["Authorization"] = "Bearer " + token;
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
  if (state.separatePollTimer) {
    clearInterval(state.separatePollTimer);
    state.separatePollTimer = null;
  }
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
  initSeparatePage();
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
  if (state.currentPage === "library-track" && page !== "library-track") {
    cleanupSyncMixer();
  }
  state.prevPage = state.currentPage;
  state.currentPage = page;
  document.querySelectorAll(".page").forEach((p) => p.classList.remove("active"));
  document.getElementById("page-" + page).classList.add("active");
  document.querySelectorAll(".nav-item").forEach((n) => n.classList.remove("active"));
  const nav = document.getElementById("nav-" + page);
  if (nav) nav.classList.add("active");
  if (page === "discover") loadDiscover();
  if (page === "library") loadLibrary();
  if (page === "separate") initSeparatePage();
}

function goBack() {
  if (state.currentPage === "library-track") {
    cleanupSyncMixer();
  }
  showPage(state.prevPage === "album" || state.prevPage === "library-track" ? "discover" : state.prevPage);
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
    const data = await api("GET", "/jobs?status=SUCCEEDED&limit=60");
    state.libraryItems = (data && data.jobs) || [];
    renderLibraryCards(el, state.libraryItems);
  } catch (e) {
    el.innerHTML = `<div class="empty-state"><div class="empty-state-icon">📁</div><h3>${esc(e.message || "No songs yet")}</h3></div>`;
  }
}

function renderLibraryCards(container, items) {
  if (!items.length) {
    container.innerHTML = '<div class="empty-state"><div class="empty-state-icon">🎼</div><h3>No separated songs yet</h3></div>';
    return;
  }
  container.innerHTML = `<div class="track-cards-grid">${items
    .map((item) => {
      const sourceName = item.source_filename || `job_${item.id}`;
      const displayName = cleanSongTitle(sourceName);
      const stems = (item.stems || []).slice(0, 4);
      return `
        <div class="song-card" onclick="openLibraryTrack(${item.id})">
          <div class="song-card-title">${esc(displayName)}</div>
          <div class="song-card-meta">Job #${item.id} • ${esc(item.model || "2stems")}</div>
          <div class="song-card-badges">
            ${stems.map((s) => `<span class="stem-badge">${esc(s)}</span>`).join("")}
          </div>
        </div>
      `;
    })
    .join("")}</div>`;
}

function cleanSongTitle(filename) {
  const base = (filename || "").split(/[\\/]/).pop() || "";
  return base.replace(/\.[^.]+$/, "") || "Untitled";
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

if (state.token) {
  // Migrate to shared keys so home and dashboard stay in sync.
  localStorage.setItem(TOKEN_KEY, state.token);
  if (state.user) localStorage.setItem(USER_KEY, JSON.stringify(state.user));
  initApp();
} else {
  const next = encodeURIComponent("/dashboard.html");
  window.location.href = `/?next=${next}`;
}

function initSeparatePage() {
  if (state.separateInitDone) return;

  const ytmp3Form = document.getElementById("ytmp3-form");
  const separateForm = document.getElementById("separate-form");
  const localFileInput = document.getElementById("separate-local-file");
  const useYtmp3FileBtn = document.getElementById("use-ytmp3-filename-btn");

  if (!ytmp3Form || !separateForm || !localFileInput || !useYtmp3FileBtn) return;

  ytmp3Form.addEventListener("submit", handleYTMP3Submit);
  separateForm.addEventListener("submit", handleSeparateSubmit);

  localFileInput.addEventListener("change", () => {
    const chosen = localFileInput.files && localFileInput.files[0] ? localFileInput.files[0].name : "";
    if (!chosen) return;
    document.getElementById("separate-filename").value = chosen;
  });

  useYtmp3FileBtn.addEventListener("click", () => {
    const filename = useYtmp3FileBtn.getAttribute("data-filename") || "";
    if (!filename) return;
    document.getElementById("separate-filename").value = filename;
    showToast("Applied downloaded filename", "success");
  });

  document.getElementById("download-vocals-btn").addEventListener("click", () => downloadJobStem("vocals"));
  document.getElementById("download-accompaniment-btn").addEventListener("click", () => downloadJobStem("accompaniment"));
  document.getElementById("download-original-btn").addEventListener("click", () => downloadJobStem("original"));

  state.separateInitDone = true;
}

function setSeparateResult(payload) {
  const target = document.getElementById("separate-result");
  if (!target) return;
  if (typeof payload === "string") {
    target.textContent = payload;
    return;
  }
  target.textContent = JSON.stringify(payload, null, 2);
}

function setDownloadButtonsVisible(visible) {
  const ids = ["download-vocals-btn", "download-accompaniment-btn", "download-original-btn"];
  ids.forEach((id) => {
    const el = document.getElementById(id);
    if (!el) return;
    el.style.display = visible ? "inline-flex" : "none";
  });
}

async function handleYTMP3Submit(e) {
  e.preventDefault();
  const resultEl = document.getElementById("ytmp3-result");
  const useBtn = document.getElementById("use-ytmp3-filename-btn");
  const url = (document.getElementById("ytmp3-url").value || "").trim();
  const filename = (document.getElementById("ytmp3-filename").value || "").trim();

  if (!url) {
    resultEl.textContent = "Please enter YouTube URL";
    return;
  }

  resultEl.textContent = "Downloading...";
  useBtn.style.display = "none";
  try {
    const data = await api("POST", "/ytmp3", { url, filename });
    const generated = (data && data.filename) || "";
    resultEl.textContent = generated
      ? `Downloaded: ${generated}`
      : "Downloaded successfully";
    if (generated) {
      useBtn.setAttribute("data-filename", generated);
      useBtn.style.display = "inline-flex";
    }
    showToast("YTMP3 download completed", "success");
  } catch (err) {
    resultEl.textContent = err.message || "YTMP3 failed";
    showToast(err.message || "YTMP3 failed", "error");
  }
}

function buildStemURL(jobID, stem) {
  const q = new URLSearchParams();
  q.set("stem", stem);
  const token = getAuthToken();
  if (token) q.set("token", token);
  return `${API}/jobs/${jobID}/stream?${q.toString()}`;
}

async function openLibraryTrack(jobID) {
  const item = (state.libraryItems || []).find((x) => x.id === jobID);
  if (!item) {
    showToast("Song not found", "error");
    return;
  }
  cleanupSyncMixer();
  state.prevPage = state.currentPage;
  showPage("library-track");
  const el = document.getElementById("library-track-detail-content");
  const sourceName = item.source_filename || `job_${item.id}`;
  const displayName = cleanSongTitle(sourceName);
  const tracks = [{ key: "original", label: "Original File", audio: new Audio(buildStemURL(item.id, "original")) }];
  (item.stems || []).forEach((stem) => {
    tracks.push({
      key: stem,
      label: stem,
      audio: new Audio(buildStemURL(item.id, stem)),
    });
  });
  tracks.forEach((t) => {
    t.audio.preload = "metadata";
    t.enabled = true;
    t.volume = 1;
    t.audio.volume = 1;
  });

  state.currentLibraryItem = item;
  state.syncMixer = {
    jobID: item.id,
    tracks,
    isPlaying: false,
    syncTimer: null,
    isSeeking: false,
  };

  el.innerHTML = `
    <div class="sync-panel">
      <div class="sync-header">
        <div class="sync-title">${esc(displayName)}</div>
        <span class="badge badge-public">JOB #${item.id}</span>
      </div>
      <div class="sync-subtitle">${esc(sourceName)}</div>
      <div class="sync-main-controls">
        <button class="sync-btn" id="sync-play-btn" onclick="toggleSyncPlay()">Play</button>
        <button class="sync-btn" onclick="stopSyncPlayback()">Stop</button>
      </div>
      <div class="mix-progress">
        <span class="time-label" id="sync-current-time">0:00</span>
        <div class="progress-bar" onclick="seekSyncTracks(event)">
          <div class="progress-fill" id="sync-progress-fill" style="width:0%"></div>
        </div>
        <span class="time-label" id="sync-total-time">0:00</span>
      </div>
      <div class="mix-track-list">
        ${tracks
          .map(
            (track, idx) => `
          <div class="mix-track-row">
            <div class="mix-track-label">${esc(track.label)}</div>
            <input class="mix-track-volume" type="range" min="0" max="100" value="100" oninput="setSyncTrackVolume(${idx}, this.value)">
            <button class="mix-track-toggle active" id="sync-toggle-${idx}" onclick="toggleSyncTrack(${idx})">ON</button>
          </div>
        `
          )
          .join("")}
      </div>
    </div>
  `;

  const ref = getSyncReferenceTrack();
  if (ref) {
    ref.audio.addEventListener("loadedmetadata", syncDurationLabel, { once: true });
    ref.audio.addEventListener("timeupdate", updateSyncProgressFromReference);
  }
}

function getSyncReferenceTrack() {
  const mixer = state.syncMixer;
  if (!mixer || !mixer.tracks || !mixer.tracks.length) return null;
  return mixer.tracks.find((t) => t.key === "original") || mixer.tracks[0];
}

function syncDurationLabel() {
  const ref = getSyncReferenceTrack();
  if (!ref || !isFinite(ref.audio.duration)) return;
  const total = document.getElementById("sync-total-time");
  if (total) total.textContent = fmtTime(Math.floor(ref.audio.duration));
}

function updateSyncProgressFromReference() {
  const mixer = state.syncMixer;
  const ref = getSyncReferenceTrack();
  if (!mixer || !ref || mixer.isSeeking) return;
  if (!isFinite(ref.audio.duration) || ref.audio.duration <= 0) return;
  const pct = (ref.audio.currentTime / ref.audio.duration) * 100;
  const fill = document.getElementById("sync-progress-fill");
  const current = document.getElementById("sync-current-time");
  if (fill) fill.style.width = `${pct}%`;
  if (current) current.textContent = fmtTime(Math.floor(ref.audio.currentTime));
}

function applySyncAlign() {
  const mixer = state.syncMixer;
  const ref = getSyncReferenceTrack();
  if (!mixer || !ref) return;
  const baseTime = ref.audio.currentTime || 0;
  mixer.tracks.forEach((track) => {
    if (!track.enabled || track === ref) return;
    const drift = Math.abs((track.audio.currentTime || 0) - baseTime);
    if (drift > 0.08) {
      track.audio.currentTime = baseTime;
    }
  });
}

function toggleSyncPlay() {
  const mixer = state.syncMixer;
  if (!mixer) return;
  if (mixer.isPlaying) {
    mixer.isPlaying = false;
    mixer.tracks.forEach((t) => t.audio.pause());
    if (mixer.syncTimer) {
      clearInterval(mixer.syncTimer);
      mixer.syncTimer = null;
    }
    const btn = document.getElementById("sync-play-btn");
    if (btn) btn.textContent = "Play";
    return;
  }

  const ref = getSyncReferenceTrack();
  if (!ref) return;
  const baseTime = ref.audio.currentTime || 0;
  mixer.tracks.forEach((track) => {
    if (!track.enabled) {
      track.audio.pause();
      return;
    }
    track.audio.currentTime = baseTime;
  });
  const playPromises = mixer.tracks
    .filter((track) => track.enabled)
    .map((track) =>
      track.audio
        .play()
        .then(() => true)
        .catch(() => false)
    );

  Promise.all(playPromises).then((results) => {
    const hasPlayingTrack = results.some(Boolean);
    const btn = document.getElementById("sync-play-btn");
    if (!hasPlayingTrack) {
      mixer.isPlaying = false;
      if (btn) btn.textContent = "Play";
      showToast("Cannot play track (check auth/stream URL)", "error");
      return;
    }
    mixer.isPlaying = true;
    if (btn) btn.textContent = "Pause";
    if (mixer.syncTimer) clearInterval(mixer.syncTimer);
    mixer.syncTimer = setInterval(applySyncAlign, 300);
  });
}

function stopSyncPlayback() {
  const mixer = state.syncMixer;
  if (!mixer) return;
  mixer.isPlaying = false;
  mixer.tracks.forEach((track) => {
    track.audio.pause();
    track.audio.currentTime = 0;
  });
  if (mixer.syncTimer) {
    clearInterval(mixer.syncTimer);
    mixer.syncTimer = null;
  }
  const btn = document.getElementById("sync-play-btn");
  if (btn) btn.textContent = "Play";
  const fill = document.getElementById("sync-progress-fill");
  const current = document.getElementById("sync-current-time");
  if (fill) fill.style.width = "0%";
  if (current) current.textContent = "0:00";
}

function seekSyncTracks(e) {
  const mixer = state.syncMixer;
  const ref = getSyncReferenceTrack();
  if (!mixer || !ref || !isFinite(ref.audio.duration) || ref.audio.duration <= 0) return;
  const rect = e.currentTarget.getBoundingClientRect();
  const pct = Math.max(0, Math.min(1, (e.clientX - rect.left) / rect.width));
  const nextTime = pct * ref.audio.duration;
  mixer.isSeeking = true;
  mixer.tracks.forEach((track) => {
    track.audio.currentTime = nextTime;
  });
  mixer.isSeeking = false;
  updateSyncProgressFromReference();
}

function setSyncTrackVolume(index, value) {
  const mixer = state.syncMixer;
  if (!mixer || !mixer.tracks[index]) return;
  const vol = Math.max(0, Math.min(1, Number(value) / 100));
  mixer.tracks[index].volume = vol;
  mixer.tracks[index].audio.volume = vol;
}

function toggleSyncTrack(index) {
  const mixer = state.syncMixer;
  if (!mixer || !mixer.tracks[index]) return;
  const track = mixer.tracks[index];
  track.enabled = !track.enabled;
  const btn = document.getElementById(`sync-toggle-${index}`);
  if (btn) {
    btn.classList.toggle("active", track.enabled);
    btn.textContent = track.enabled ? "ON" : "OFF";
  }
  if (!track.enabled) {
    track.audio.pause();
  } else if (mixer.isPlaying) {
    const ref = getSyncReferenceTrack();
    const targetTime = ref ? ref.audio.currentTime : 0;
    track.audio.currentTime = targetTime;
    track.audio.play().catch(() => {});
  }
}

function cleanupSyncMixer() {
  const mixer = state.syncMixer;
  if (!mixer) return;
  if (mixer.syncTimer) {
    clearInterval(mixer.syncTimer);
  }
  mixer.tracks.forEach((track) => {
    track.audio.pause();
    track.audio.src = "";
  });
  state.syncMixer = null;
  state.currentLibraryItem = null;
}

async function handleSeparateSubmit(e) {
  e.preventDefault();
  const localFileEl = document.getElementById("separate-local-file");
  let filename = (document.getElementById("separate-filename").value || "").trim();
  const model = document.getElementById("separate-model").value || "2stems";

  const localFile = localFileEl && localFileEl.files && localFileEl.files[0] ? localFileEl.files[0] : null;
  if (!filename && !localFile) {
    setSeparateResult("Please enter filename in audio_data/input");
    return;
  }

  if (state.separatePollTimer) {
    clearInterval(state.separatePollTimer);
    state.separatePollTimer = null;
  }
  setDownloadButtonsVisible(false);
  setSeparateResult(localFile ? "Uploading local file..." : "Creating file record...");

  try {
    if (localFile) {
      const uploaded = await uploadLocalInputFile(localFile);
      filename = uploaded.filename || filename || localFile.name;
      document.getElementById("separate-filename").value = filename;
    }
    if (!filename) throw new Error("filename is required");

    setSeparateResult("Creating file record...");
    const fileRes = await api("POST", "/files", { filename });
    const fileID = fileRes && fileRes.file ? fileRes.file.id : 0;
    if (!fileID) throw new Error("Cannot create file record");

    const jobRes = await api("POST", "/jobs", { file_id: fileID, model });
    const jobID = jobRes && jobRes.job ? jobRes.job.id : 0;
    if (!jobID) throw new Error("Cannot create job");

    state.separateJobID = jobID;
    setSeparateResult(jobRes);
    showToast(`Job #${jobID} queued`, "success");
    startPollingJob(jobID);
  } catch (err) {
    setSeparateResult(err.message || "Create job failed");
    showToast(err.message || "Create job failed", "error");
  }
}

async function uploadLocalInputFile(file) {
  const fd = new FormData();
  fd.append("file", file);
  const res = await fetch(`${API}/input/upload`, {
    method: "POST",
    headers: {
      Authorization: "Bearer " + getAuthToken(),
    },
    body: fd,
  });
  const data = await res.json().catch(() => ({}));
  if (!res.ok) throw new Error(data.error || "upload failed");
  return data;
}

function startPollingJob(jobID) {
  const poll = async () => {
    try {
      const res = await api("GET", `/jobs/${jobID}`);
      setSeparateResult(res);
      const status = (res && res.job && res.job.status) || "";
      if (status === "SUCCEEDED") {
        if (state.separatePollTimer) {
          clearInterval(state.separatePollTimer);
          state.separatePollTimer = null;
        }
        setDownloadButtonsVisible(true);
        showToast("Separation finished", "success");
      }
      if (status === "FAILED") {
        if (state.separatePollTimer) {
          clearInterval(state.separatePollTimer);
          state.separatePollTimer = null;
        }
        showToast(res.job.error_msg || "Separation failed", "error");
      }
    } catch (err) {
      if (state.separatePollTimer) {
        clearInterval(state.separatePollTimer);
        state.separatePollTimer = null;
      }
      showToast(err.message || "Poll failed", "error");
    }
  };

  poll();
  state.separatePollTimer = setInterval(poll, 3000);
}

async function downloadJobStem(stem) {
  if (!state.separateJobID) {
    showToast("No recent job to download", "error");
    return;
  }
  const url = `${API}/jobs/${state.separateJobID}/download?stem=${encodeURIComponent(stem)}`;
  try {
    const token = getAuthToken();
    const res = await fetch(url, {
      headers: {
        Authorization: "Bearer " + token,
      },
    });
    if (!res.ok) {
      let msg = `Download failed (${res.status})`;
      try {
        const j = await res.json();
        if (j.error) msg = j.error;
      } catch (_) {}
      throw new Error(msg);
    }

    const blob = await res.blob();
    let filename = `job_${state.separateJobID}_${stem}.wav`;
    const cd = res.headers.get("Content-Disposition") || "";
    const m = cd.match(/filename="?([^"]+)"?/i);
    if (m && m[1]) filename = m[1];

    const a = document.createElement("a");
    const href = URL.createObjectURL(blob);
    a.href = href;
    a.download = filename;
    document.body.appendChild(a);
    a.click();
    a.remove();
    URL.revokeObjectURL(href);
  } catch (err) {
    showToast(err.message || "Download failed", "error");
  }
}
