const form = document.getElementById("connect-form");
const driverInput = document.getElementById("driver-id");
const disconnectBtn = document.getElementById("disconnect-btn");
const clearBtn = document.getElementById("clear-btn");
const statusBadge = document.getElementById("status-badge");
const eventList = document.getElementById("event-list");
const emptyState = document.getElementById("empty-state");
const eventCount = document.getElementById("event-count");
const latestUpdate = document.getElementById("latest-update");

let source = null;
let eventsSeen = 0;
let map = null;
let marker = null;
let currentLatLng = null;
let animationFrame = null;
let hasPlacedMarker = false;
let mapReady = false;
let markerLabel = "";

const initialDriverID = new URLSearchParams(window.location.search).get("driver_id");
if (initialDriverID) {
  driverInput.value = initialDriverID;
}

function initMap() {
  if (map) {
    return;
  }

  const mapElement = document.getElementById("map");
  if (!window.L) {
    mapElement.classList.add("map-unavailable");
    mapElement.textContent = "Map library did not load. The live feed still works, but the browser could not load Leaflet or map tiles.";
    latestUpdate.textContent = "Map unavailable. Check internet access to the Leaflet/OpenStreetMap CDN, then refresh.";
    return;
  }

  map = L.map("map", {
    zoomControl: true,
    attributionControl: true,
  }).setView([9.03, 38.74], 13);

  L.tileLayer("https://{s}.tile.openstreetmap.org/{z}/{x}/{y}.png", {
    maxZoom: 19,
    attribution: "&copy; OpenStreetMap contributors",
  }).addTo(map);

  setTimeout(() => {
    map.invalidateSize();
  }, 0);
  mapReady = true;
}

function getMarkerSize() {
  if (!mapReady || !map) {
    return 28;
  }

  const zoom = map.getZoom();
  return Math.max(20, Math.min(36, 20 + (zoom - 11) * 2));
}

function buildMarkerIcon() {
  const size = getMarkerSize();
  return L.divIcon({
    className: "driver-icon-wrapper",
    html: `
      <div class="driver-marker" aria-hidden="true">
        <span role="img" aria-label="Car">🚗</span>
      </div>
    `,
    iconSize: [size, size],
    iconAnchor: [size / 2, size / 2],
  });
}

function refreshMarkerIcon() {
  if (!marker || !mapReady) {
    return;
  }

  marker.setIcon(buildMarkerIcon());
}

function setStatus(state, label) {
  statusBadge.className = `status-badge ${state}`;
  statusBadge.textContent = label;
}

function updateCount() {
  eventCount.textContent = `${eventsSeen} event${eventsSeen === 1 ? "" : "s"}`;
}

function showEmptyState() {
  emptyState.hidden = eventsSeen > 0;
}

function prettyPayload(payload) {
  if (typeof payload === "string") {
    return payload;
  }
  return JSON.stringify(payload, null, 2);
}

function addEvent(label, payload) {
  eventsSeen += 1;
  updateCount();
  showEmptyState();

  const item = document.createElement("li");
  item.className = "event-card";

  const meta = document.createElement("div");
  meta.className = "event-meta";

  const type = document.createElement("span");
  type.textContent = label;

  const time = document.createElement("span");
  time.textContent = new Date().toLocaleTimeString();

  meta.append(type, time);

  const body = document.createElement("pre");
  body.className = "event-body";
  body.textContent = prettyPayload(payload);

  item.append(meta, body);
  eventList.prepend(item);
}

function setLatestUpdate(payload) {
  if (!payload || typeof payload !== "object") {
    latestUpdate.textContent = "Connected. Waiting for coordinate data.";
    return;
  }

  const details = payload.driver_data || payload;
  const lat = Number(details.latitude ?? details.lat);
  const lng = Number(details.longitude ?? details.lng);

  if (!Number.isFinite(lat) || !Number.isFinite(lng)) {
    latestUpdate.textContent = "Connected. Waiting for coordinate data.";
    return;
  }

  const updatedAt = details.timestamp || new Date().toISOString();
  const companyID = details.company_id || "unknown";
  latestUpdate.textContent =
    `Driver ${details.worker_id || driverInput.value.trim()} at ${lat.toFixed(5)}, ` +
    `${lng.toFixed(5)} | company ${companyID} | updated ${updatedAt}`;
}

function stopMarkerAnimation() {
  if (animationFrame) {
    cancelAnimationFrame(animationFrame);
    animationFrame = null;
  }
}

function resetMarker() {
  stopMarkerAnimation();

  if (marker && map) {
    map.removeLayer(marker);
  }

  marker = null;
  currentLatLng = null;
  hasPlacedMarker = false;
}

function ensureMarker(lat, lng, label) {
  initMap();
  if (!mapReady) {
    return null;
  }

  if (marker) {
    return marker;
  }
  marker = L.marker([lat, lng], { icon: buildMarkerIcon() }).addTo(map);
  marker.bindPopup(label);
  marker.bindTooltip(label, {
    permanent: true,
    direction: "top",
    offset: [0, -16],
    className: "driver-tooltip",
  });
  currentLatLng = null;
  hasPlacedMarker = false;
  return marker;
}

function maybeFollowMarker(target) {
  if (!mapReady || !map) {
    return;
  }

  if (!map.getBounds().pad(-0.35).contains(target)) {
    map.panTo(target, { animate: true, duration: 0.6 });
  }
}

function animateMarkerTo(lat, lng, popupText) {
  const target = L.latLng(lat, lng);
  const activeMarker = ensureMarker(lat, lng, popupText);
  if (!activeMarker) {
    return;
  }
  markerLabel = popupText;
  activeMarker.setPopupContent(popupText);
  activeMarker.setTooltipContent(popupText);

  if (!currentLatLng || !hasPlacedMarker) {
    currentLatLng = target;
    activeMarker.setLatLng(target);
    map.setView(target, 15);
    hasPlacedMarker = true;
    activeMarker.openTooltip();
    refreshMarkerIcon();
    return;
  }

  stopMarkerAnimation();

  const start = currentLatLng;
  const startTime = performance.now();
  const distance = start.distanceTo(target);
  const duration = Math.max(220, Math.min(700, distance * 0.18));

  function step(now) {
    const progress = Math.min((now - startTime) / duration, 1);
    const eased = progress < 0.5
      ? 2 * progress * progress
      : 1 - Math.pow(-2 * progress + 2, 2) / 2;
    const nextLat = start.lat + (target.lat - start.lat) * eased;
    const nextLng = start.lng + (target.lng - start.lng) * eased;
    const nextPosition = L.latLng(nextLat, nextLng);

    activeMarker.setLatLng(nextPosition);

    if (progress < 1) {
      animationFrame = requestAnimationFrame(step);
      return;
    }

    currentLatLng = target;
    animationFrame = null;
  }

  animationFrame = requestAnimationFrame(step);
  maybeFollowMarker(target);
}

function handleMapUpdate(payload) {
  if (!payload || typeof payload !== "object") {
    return;
  }

  const details = payload.driver_data || payload;
  const lat = Number(details.latitude ?? details.lat);
  const lng = Number(details.longitude ?? details.lng);

  if (!Number.isFinite(lat) || !Number.isFinite(lng)) {
    return;
  }

  const label = `${details.worker_id || driverInput.value.trim()} | ${lat.toFixed(5)}, ${lng.toFixed(5)}`;
  setLatestUpdate(payload);
  animateMarkerTo(lat, lng, label);
}

function clearEvents() {
  eventsSeen = 0;
  eventList.innerHTML = "";
  updateCount();
  showEmptyState();
  latestUpdate.textContent = "Waiting for the first coordinate update.";
  resetMarker();
}

function disconnect() {
  if (source) {
    source.close();
    source = null;
  }
  resetMarker();
  setStatus("idle", "Disconnected");
}

function parseMessage(raw) {
  try {
    return JSON.parse(raw);
  } catch {
    return raw;
  }
}

function connect(driverID) {
  disconnect();
  setStatus("connecting", "Connecting");

  const url = `/sse?driver_id=${encodeURIComponent(driverID)}`;
  const nextURL = new URL(window.location.href);
  nextURL.searchParams.set("driver_id", driverID);
  window.history.replaceState({}, "", nextURL);

  source = new EventSource(url);

  source.onopen = () => {
    setStatus("connected", "Connected");
    addEvent("connection", "SSE connection opened");
  };

  source.onmessage = (event) => {
    const payload = parseMessage(event.data);
    addEvent("message", payload);
    handleMapUpdate(payload);
  };

  source.onerror = () => {
    setStatus("error", "Connection issue");
    addEvent("error", "The SSE stream closed or failed. The browser may retry automatically.");
  };
}

form.addEventListener("submit", (event) => {
  event.preventDefault();
  const driverID = driverInput.value.trim();
  if (!driverID) {
    setStatus("error", "Driver ID required");
    return;
  }
  connect(driverID);
});

disconnectBtn.addEventListener("click", () => {
  disconnect();
});

clearBtn.addEventListener("click", () => {
  clearEvents();
});

initMap();
if (mapReady && map) {
  map.on("zoomend", () => {
    refreshMarkerIcon();
    if (marker && markerLabel) {
      marker.setTooltipContent(markerLabel);
    }
  });
}
updateCount();
showEmptyState();
