const form = document.getElementById("connect-form");
const companyInput = document.getElementById("company-id");
const driverInput = document.getElementById("driver-id");
const watchSelectedBtn = document.getElementById("watch-selected-btn");
const backNearbyBtn = document.getElementById("back-nearby-btn");
const locateBtn = document.getElementById("locate-btn");
const disconnectBtn = document.getElementById("disconnect-btn");
const clearBtn = document.getElementById("clear-btn");
const statusBadge = document.getElementById("status-badge");
const eventList = document.getElementById("event-list");
const emptyState = document.getElementById("empty-state");
const eventCount = document.getElementById("event-count");
const latestUpdate = document.getElementById("latest-update");
const locationNote = document.getElementById("location-note");

const BBOX_RADIUS_LAT = 0.018;
const BBOX_RADIUS_LNG = 0.018;
const MAX_RENDERED_EVENTS = 200;

let source = null;
let eventsSeen = 0;
let map = null;
let clientMarker = null;
let markersByDriver = new Map();
let driverStates = new Map();
let driverAnimations = new Map();
let mapReady = false;
let currentPosition = null;
let currentBBox = null;
let lastNearbyBBox = null;
let lastCompanyID = "";
let selectedDriverID = "";
let selectedDriverIDs = new Set();
let followMode = "nearby";

const initialParams = new URLSearchParams(window.location.search);
const initialCompanyID = initialParams.get("company_id");
const initialDriverIDs = initialParams.getAll("driver_id");
if (initialCompanyID) {
  companyInput.value = initialCompanyID;
}
if (initialDriverIDs.length > 0) {
  driverInput.value = initialDriverIDs[0];
  selectedDriverID = initialDriverIDs[0];
  selectedDriverIDs = new Set(initialDriverIDs);
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

function buildMarkerIcon(driverID) {
  const size = getMarkerSize();
  const classes = ["driver-marker"];
  if (driverID === selectedDriverID) {
    classes.push("selected");
  } else if (selectedDriverIDs.has(driverID)) {
    classes.push("chosen");
  }

  return L.divIcon({
    className: "driver-icon-wrapper",
    html: `
      <div class="${classes.join(" ")}" aria-hidden="true">
        <span role="img" aria-label="Car">🚗</span>
      </div>
    `,
    iconSize: [size, size],
    iconAnchor: [size / 2, size / 2],
  });
}

function buildClientIcon() {
  return L.divIcon({
    className: "client-icon-wrapper",
    html: `
      <div class="client-marker" aria-hidden="true">
        <span role="img" aria-label="Client">📍</span>
      </div>
    `,
    iconSize: [24, 24],
    iconAnchor: [12, 12],
  });
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

  while (eventList.children.length > MAX_RENDERED_EVENTS) {
    eventList.lastElementChild?.remove();
  }
}

function updateLocationNote(message) {
  locationNote.textContent = message;
}

function buildBBox(lat, lng) {
  return {
    minLat: lat - BBOX_RADIUS_LAT,
    minLng: lng - BBOX_RADIUS_LNG,
    maxLat: lat + BBOX_RADIUS_LAT,
    maxLng: lng + BBOX_RADIUS_LNG,
  };
}

function parseMessage(raw) {
  try {
    return JSON.parse(raw);
  } catch {
    return raw;
  }
}

function detailsFromPayload(payload) {
  if (!payload || typeof payload !== "object") {
    return null;
  }
  return payload.driver_data || payload;
}

function driverIdFromDetails(details) {
  return details?.worker_id || details?.id || "";
}

function defaultWaitingMessage() {
  switch (followMode) {
    case "single":
      return "Connected. Waiting for driver location updates.";
    case "selected":
      return "Connected. Waiting for selected driver updates.";
    default:
      return "Connected. Waiting for nearby driver data.";
  }
}

function setLatestUpdate(details) {
  if (!details) {
    latestUpdate.textContent = defaultWaitingMessage();
    return;
  }

  const lat = Number(details.latitude ?? details.lat);
  const lng = Number(details.longitude ?? details.lng);
  if (!Number.isFinite(lat) || !Number.isFinite(lng)) {
    latestUpdate.textContent = defaultWaitingMessage();
    return;
  }

  const updatedAt = details.timestamp || details.updated_at || new Date().toISOString();
  const companyID = details.company_id || "unknown";
  latestUpdate.textContent =
    `Driver ${driverIdFromDetails(details)} at ${lat.toFixed(5)}, ${lng.toFixed(5)} | company ${companyID} | updated ${updatedAt}`;
}

function driverLabel(details) {
  const lat = Number(details.latitude ?? details.lat);
  const lng = Number(details.longitude ?? details.lng);
  const companyID = details.company_id || "unknown";
  return `${driverIdFromDetails(details)} | ${lat.toFixed(5)}, ${lng.toFixed(5)} | ${companyID}`;
}

function stopMarkerAnimation(driverID) {
  const frame = driverAnimations.get(driverID);
  if (frame) {
    cancelAnimationFrame(frame);
    driverAnimations.delete(driverID);
  }
}

function stopAllMarkerAnimations() {
  for (const driverID of driverAnimations.keys()) {
    stopMarkerAnimation(driverID);
  }
}

function clearDriverMarkers() {
  stopAllMarkerAnimations();
  if (map) {
    for (const entry of markersByDriver.values()) {
      map.removeLayer(entry.marker);
    }
  }
  markersByDriver = new Map();
  driverStates = new Map();
}

function ensureClientMarker() {
  initMap();
  if (!mapReady || !currentPosition) {
    return;
  }

  const point = [currentPosition.lat, currentPosition.lng];
  if (!clientMarker) {
    clientMarker = L.marker(point, { icon: buildClientIcon(), zIndexOffset: 600 }).addTo(map);
    clientMarker.bindTooltip("You are here", {
      permanent: true,
      direction: "bottom",
      offset: [0, 18],
      className: "driver-tooltip",
    });
    return;
  }

  clientMarker.setLatLng(point);
}

function refreshControls() {
  watchSelectedBtn.disabled = selectedDriverIDs.size === 0 || !lastCompanyID;
  backNearbyBtn.disabled = !lastNearbyBBox || !lastCompanyID || followMode === "nearby";
}

function toggleDriverSelection(driverID) {
  if (!driverID) {
    return;
  }

  if (selectedDriverIDs.has(driverID)) {
    selectedDriverIDs.delete(driverID);
  } else {
    selectedDriverIDs.add(driverID);
  }

  if (followMode === "single" && selectedDriverID !== driverID) {
    selectedDriverID = driverID;
  }

  refreshAllMarkerIcons();
  refreshControls();
}

function ensureMarker(details) {
  initMap();
  if (!mapReady) {
    return null;
  }

  const driverID = driverIdFromDetails(details);
  const lat = Number(details.latitude ?? details.lat);
  const lng = Number(details.longitude ?? details.lng);
  if (!driverID || !Number.isFinite(lat) || !Number.isFinite(lng)) {
    return null;
  }

  let entry = markersByDriver.get(driverID);
  if (!entry) {
    const marker = L.marker([lat, lng], {
      icon: buildMarkerIcon(driverID),
      zIndexOffset: driverID === selectedDriverID ? 500 : 0,
    }).addTo(map);
    marker.on("click", () => {
      if (followMode === "nearby") {
        toggleDriverSelection(driverID);
      } else if (followMode === "selected") {
        toggleDriverSelection(driverID);
      }
    });
    marker.on("dblclick", () => {
      followSingleDriver(driverID);
    });
    entry = { marker };
    markersByDriver.set(driverID, entry);
  }

  const label = driverLabel(details);
  if (!entry.popupBound) {
    entry.marker.bindPopup(label);
    entry.marker.bindTooltip(label, {
      permanent: driverID === selectedDriverID,
      direction: "top",
      offset: [0, -16],
      className: "driver-tooltip",
    });
    entry.popupBound = true;
  } else {
    entry.marker.setPopupContent(label);
    entry.marker.setTooltipContent(label);
  }

  entry.marker.setIcon(buildMarkerIcon(driverID));
  entry.marker.setZIndexOffset(driverID === selectedDriverID ? 500 : 0);
  if (driverID === selectedDriverID) {
    entry.marker.openTooltip();
  } else {
    entry.marker.closeTooltip();
  }

  return entry.marker;
}

function animateMarkerTo(driverID, lat, lng) {
  const entry = markersByDriver.get(driverID);
  if (!entry) {
    return;
  }

  const marker = entry.marker;
  const target = L.latLng(lat, lng);
  const current = driverStates.get(driverID);
  if (!current) {
    marker.setLatLng(target);
    driverStates.set(driverID, target);
    return;
  }

  stopMarkerAnimation(driverID);

  const start = current;
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
    marker.setLatLng([nextLat, nextLng]);

    if (progress < 1) {
      driverAnimations.set(driverID, requestAnimationFrame(step));
      return;
    }

    driverStates.set(driverID, target);
    driverAnimations.delete(driverID);
  }

  driverAnimations.set(driverID, requestAnimationFrame(step));
}

function maybeFollowSelectedDriver(driverID) {
  if (!mapReady || !map || driverID !== selectedDriverID) {
    return;
  }

  const target = driverStates.get(driverID);
  if (!target) {
    return;
  }

  if (!map.getBounds().pad(-0.35).contains(target)) {
    map.panTo(target, { animate: true, duration: 0.6 });
  }
}

function updateDriver(details, { focus = false } = {}) {
  const driverID = driverIdFromDetails(details);
  const lat = Number(details.latitude ?? details.lat);
  const lng = Number(details.longitude ?? details.lng);

  if (!driverID || !Number.isFinite(lat) || !Number.isFinite(lng)) {
    return;
  }

  const marker = ensureMarker(details);
  if (!marker) {
    return;
  }

  if (!driverStates.has(driverID)) {
    marker.setLatLng([lat, lng]);
    driverStates.set(driverID, L.latLng(lat, lng));
  } else {
    animateMarkerTo(driverID, lat, lng);
  }

  setLatestUpdate(details);
  if (focus || driverID === selectedDriverID) {
    maybeFollowSelectedDriver(driverID);
  }
}

function removeDriver(driverID) {
  stopMarkerAnimation(driverID);
  const entry = markersByDriver.get(driverID);
  if (entry && map) {
    map.removeLayer(entry.marker);
  }
  markersByDriver.delete(driverID);
  driverStates.delete(driverID);
  selectedDriverIDs.delete(driverID);

  if (selectedDriverID === driverID) {
    selectedDriverID = "";
    driverInput.value = "";
  }

  refreshControls();
}

function handleSnapshot(payload) {
  clearDriverMarkers();
  ensureClientMarker();

  const drivers = Array.isArray(payload.paginated) ? payload.paginated : [];
  for (const driver of drivers) {
    updateDriver(driver, { focus: driverIdFromDetails(driver) === selectedDriverID });
  }

  if (drivers.length === 0) {
    if (followMode === "single") {
      latestUpdate.textContent = "The selected driver is not currently available for this company.";
    } else if (followMode === "selected") {
      latestUpdate.textContent = "None of the selected drivers are currently available.";
    } else {
      latestUpdate.textContent = "No nearby drivers found in the current area for this company.";
    }
    return;
  }

  fitMapToVisiblePoints();
  refreshControls();
}

function fitMapToVisiblePoints() {
  if (!mapReady || !map) {
    return;
  }

  const points = [];
  if (currentPosition) {
    points.push([currentPosition.lat, currentPosition.lng]);
  }
  for (const point of driverStates.values()) {
    points.push([point.lat, point.lng]);
  }

  if (points.length === 0) {
    return;
  }
  if (points.length === 1) {
    map.setView(points[0], 15);
    return;
  }

  map.fitBounds(points, { padding: [40, 40] });
}

function clearEvents() {
  eventsSeen = 0;
  eventList.innerHTML = "";
  updateCount();
  showEmptyState();
  latestUpdate.textContent = "Waiting for the first coordinate update.";
  clearDriverMarkers();
  ensureClientMarker();
}

function disconnect() {
  if (source) {
    source.close();
    source = null;
  }
  stopAllMarkerAnimations();
  setStatus("idle", "Disconnected");
}

function updateURL(companyID, bbox, driverIDs) {
  const nextURL = new URL(window.location.href);
  nextURL.searchParams.set("company_id", companyID);
  if (bbox) {
    nextURL.searchParams.set("min_lat", bbox.minLat.toFixed(6));
    nextURL.searchParams.set("min_lng", bbox.minLng.toFixed(6));
    nextURL.searchParams.set("max_lat", bbox.maxLat.toFixed(6));
    nextURL.searchParams.set("max_lng", bbox.maxLng.toFixed(6));
  } else {
    nextURL.searchParams.delete("min_lat");
    nextURL.searchParams.delete("min_lng");
    nextURL.searchParams.delete("max_lat");
    nextURL.searchParams.delete("max_lng");
  }

  nextURL.searchParams.delete("driver_id");
  for (const driverID of driverIDs) {
    nextURL.searchParams.append("driver_id", driverID);
  }
  window.history.replaceState({}, "", nextURL);
}

function buildSSEURL(companyID, bbox, driverIDs) {
  const params = new URLSearchParams({
    company_id: companyID,
  });
  if (bbox) {
    params.set("min_lat", bbox.minLat.toFixed(6));
    params.set("min_lng", bbox.minLng.toFixed(6));
    params.set("max_lat", bbox.maxLat.toFixed(6));
    params.set("max_lng", bbox.maxLng.toFixed(6));
  }
  for (const driverID of driverIDs) {
    params.append("driver_id", driverID);
  }
  return `/sse?${params.toString()}`;
}

function connectStream({ companyID, bbox = null, driverIDs = [], mode = "nearby" }) {
  disconnect();
  clearEvents();
  lastCompanyID = companyID;
  followMode = mode;
  if (bbox) {
    currentBBox = bbox;
    lastNearbyBBox = bbox;
  }

  if (mode === "single") {
    selectedDriverID = driverIDs[0] || "";
    selectedDriverIDs = selectedDriverID ? new Set([selectedDriverID]) : new Set();
    driverInput.value = selectedDriverID;
    clearDriverMarkers();
    ensureClientMarker();
  } else if (mode === "selected") {
    selectedDriverID = "";
    selectedDriverIDs = new Set(driverIDs);
    driverInput.value = "";
    clearDriverMarkers();
    ensureClientMarker();
  } else {
    selectedDriverID = "";
    driverInput.value = "";
  }

  setStatus("connecting", "Connecting");
  updateURL(companyID, bbox, driverIDs);
  refreshControls();

  source = new EventSource(buildSSEURL(companyID, bbox, driverIDs));

  source.onopen = () => {
    if (mode === "single") {
      setStatus("connected", "Following driver");
      addEvent("connection", `Following driver ${driverIDs[0]}`);
    } else if (mode === "selected") {
      setStatus("connected", "Watching selected");
      addEvent("connection", `Watching ${driverIDs.length} selected drivers`);
    } else {
      setStatus("connected", "Connected nearby");
      addEvent("connection", "Tracking nearby drivers");
    }
  };

  source.onmessage = (event) => {
    const payload = parseMessage(event.data);
    const command = payload && typeof payload === "object" ? payload.command : "";
    addEvent(command || "message", payload);

    if (!payload || typeof payload !== "object") {
      return;
    }

    if (command === "snapshot") {
      handleSnapshot(payload);
      return;
    }

    if (command === "track") {
      updateDriver(detailsFromPayload(payload), { focus: true });
      return;
    }

    if (command === "driver-left") {
      const details = detailsFromPayload(payload);
      const driverIDToRemove = driverIdFromDetails(details);
      if (!driverIDToRemove) {
        return;
      }
      removeDriver(driverIDToRemove);
      latestUpdate.textContent = `Driver ${driverIDToRemove} left the current area.`;
      return;
    }

    if (command === "connected" && payload.message) {
      latestUpdate.textContent = payload.message;
    }
  };

  source.onerror = () => {
    setStatus("error", "Connection issue");
    addEvent("error", "The SSE stream closed or failed. The browser may retry automatically.");
  };
}

function requestCurrentLocation() {
  return new Promise((resolve, reject) => {
    if (!navigator.geolocation) {
      reject(new Error("Geolocation is not supported in this browser."));
      return;
    }

    navigator.geolocation.getCurrentPosition(
      (position) => {
        currentPosition = {
          lat: position.coords.latitude,
          lng: position.coords.longitude,
        };
        currentBBox = buildBBox(currentPosition.lat, currentPosition.lng);
        lastNearbyBBox = currentBBox;
        updateLocationNote(
          `Using client location ${currentPosition.lat.toFixed(5)}, ${currentPosition.lng.toFixed(5)} to find nearby drivers.`
        );
        ensureClientMarker();
        fitMapToVisiblePoints();
        refreshControls();
        resolve(currentPosition);
      },
      (error) => {
        reject(new Error(error.message || "Failed to get client location."));
      },
      {
        enableHighAccuracy: true,
        timeout: 10000,
        maximumAge: 15000,
      }
    );
  });
}

async function ensureLocation() {
  if (currentPosition) {
    return currentPosition;
  }
  setStatus("connecting", "Getting location");
  return requestCurrentLocation();
}

async function connectFromForm() {
  const companyID = companyInput.value.trim();
  const requestedDriverID = driverInput.value.trim();
  if (!companyID) {
    setStatus("error", "Company ID required");
    return;
  }

  try {
    const position = await ensureLocation();
    const bbox = buildBBox(position.lat, position.lng);
    if (requestedDriverID) {
      followSingleDriver(requestedDriverID, companyID);
      return;
    }
    connectStream({ companyID, bbox, driverIDs: [], mode: "nearby" });
  } catch (error) {
    setStatus("error", "Location unavailable");
    updateLocationNote(error.message);
    latestUpdate.textContent = "Grant browser location access to load nearby drivers.";
  }
}

function followSingleDriver(driverID, companyID = lastCompanyID) {
  if (!driverID || !companyID) {
    return;
  }
  selectedDriverID = driverID;
  connectStream({ companyID, bbox: null, driverIDs: [driverID], mode: "single" });
}

function watchSelectedDrivers() {
  if (!lastCompanyID || selectedDriverIDs.size === 0) {
    return;
  }

  connectStream({
    companyID: lastCompanyID,
    bbox: null,
    driverIDs: Array.from(selectedDriverIDs),
    mode: "selected",
  });
}

function returnToNearby() {
  if (!lastCompanyID || !lastNearbyBBox) {
    return;
  }

  connectStream({
    companyID: lastCompanyID,
    bbox: lastNearbyBBox,
    driverIDs: [],
    mode: "nearby",
  });
}

function refreshAllMarkerIcons() {
  for (const [driverID, entry] of markersByDriver.entries()) {
    entry.marker.setIcon(buildMarkerIcon(driverID));
    entry.marker.setZIndexOffset(driverID === selectedDriverID ? 500 : 0);
    if (entry.marker.getTooltip()) {
      if (driverID === selectedDriverID) {
        entry.marker.openTooltip();
      } else {
        entry.marker.closeTooltip();
      }
    }
  }
  refreshControls();
}

form.addEventListener("submit", async (event) => {
  event.preventDefault();
  await connectFromForm();
});

watchSelectedBtn.addEventListener("click", () => {
  watchSelectedDrivers();
});

backNearbyBtn.addEventListener("click", () => {
  returnToNearby();
});

locateBtn.addEventListener("click", async () => {
  try {
    await requestCurrentLocation();
    setStatus("idle", "Location ready");
  } catch (error) {
    setStatus("error", "Location unavailable");
    updateLocationNote(error.message);
  }
});

disconnectBtn.addEventListener("click", () => {
  disconnect();
});

clearBtn.addEventListener("click", () => {
  selectedDriverID = "";
  selectedDriverIDs = new Set();
  driverInput.value = "";
  followMode = "nearby";
  clearEvents();
  refreshAllMarkerIcons();
});

initMap();
if (mapReady && map) {
  map.on("zoomend", () => {
    refreshAllMarkerIcons();
  });
}

updateCount();
showEmptyState();
ensureClientMarker();
refreshControls();
requestCurrentLocation()
  .then(() => {
    setStatus("idle", "Location ready");
  })
  .catch((error) => {
    updateLocationNote(error.message);
  });
