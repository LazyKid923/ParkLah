import { useCallback, useEffect, useMemo, useRef, useState } from "react";
import { CircleParking, LocateFixed } from "lucide-react";
import { Map, MapControls, MapMarker, MarkerContent, MarkerPopup, useMap } from "@/components/ui/map";

const DEFAULT_CENTER = [103.8198, 1.3521];
const MAP_STYLES = {
  light: "https://tiles.openfreemap.org/styles/bright",
  dark: "https://tiles.openfreemap.org/styles/fiord",
};

const USER_ACCURACY_SOURCE = "user-accuracy-source";
const USER_ACCURACY_FILL = "user-accuracy-fill";
const USER_ACCURACY_LINE = "user-accuracy-line";

function parseTimeInputToMinutes(value) {
  const match = /^(\d{2}):(\d{2})$/.exec(String(value || "").trim());
  if (!match) {
    return null;
  }
  const hh = Number(match[1]);
  const mm = Number(match[2]);
  if (!Number.isInteger(hh) || !Number.isInteger(mm) || hh < 0 || hh > 23 || mm < 0 || mm > 59) {
    return null;
  }
  return hh * 60 + mm;
}

function minutesToTimeInput(totalMinutes) {
  const mins = ((Math.trunc(totalMinutes) % 1440) + 1440) % 1440;
  const hh = String(Math.floor(mins / 60)).padStart(2, "0");
  const mm = String(mins % 60).padStart(2, "0");
  return `${hh}:${mm}`;
}

function formatDurationLabel(totalMinutes) {
  const mins = Math.max(1, Math.round(totalMinutes));
  const hours = Math.floor(mins / 60);
  const rem = mins % 60;
  if (hours > 0 && rem > 0) {
    return `${hours}h ${rem}m`;
  }
  if (hours > 0) {
    return `${hours}h`;
  }
  return `${rem}m`;
}

function formatMinutesAsClock(totalMinutes) {
  const mins = ((Math.trunc(totalMinutes) % 1440) + 1440) % 1440;
  const hh24 = Math.floor(mins / 60);
  const mm = mins % 60;
  const suffix = hh24 < 12 ? "am" : "pm";
  const hh12 = hh24 % 12 || 12;
  if (mm === 0) {
    return `${hh12}${suffix}`;
  }
  return `${hh12}:${String(mm).padStart(2, "0")}${suffix}`;
}

function formatTimeInputLabel(value) {
  const mins = parseTimeInputToMinutes(value);
  if (!Number.isFinite(mins)) {
    return value;
  }
  return formatMinutesAsClock(mins);
}

function computeStayDurationMinutes(fromTime, toTime) {
  const from = parseTimeInputToMinutes(fromTime);
  const to = parseTimeInputToMinutes(toTime);
  if (!Number.isFinite(from) || !Number.isFinite(to)) {
    return 60;
  }
  let diff = to - from;
  if (diff <= 0) {
    diff += 24 * 60;
  }
  return diff;
}

function getDefaultStayWindow() {
  const now = new Date();
  const mins = now.getHours() * 60 + now.getMinutes();
  const rounded = Math.ceil(mins / 15) * 15;
  return {
    from: minutesToTimeInput(rounded),
    to: minutesToTimeInput(rounded + 120),
  };
}

function getParkingPriceLabel(carpark, stayMinutes, stayDurationLabel) {
  const estimate = Number(carpark.price_now_estimate);
  if (Number.isFinite(estimate) && estimate >= 0) {
    const estimateMinutes = Number(carpark.price_now_estimate_minutes);
    if (Number.isFinite(estimateMinutes) && estimateMinutes > 0 && estimateMinutes !== stayMinutes) {
      return carpark.price_now_label || "Calculating...";
    }
    return `$${estimate.toFixed(2)} / ${stayDurationLabel}`;
  }
  if (carpark.price_now_label) {
    return carpark.price_now_label;
  }
  const estimate60 = Number(carpark.price_now_estimate_60min);
  if (Number.isFinite(estimate60) && estimate60 >= 0) {
    return `$${estimate60.toFixed(2)} / 60 mins`;
  }
  return "Price unavailable";
}

function isFreeParkingPriceLabel(priceLabel) {
  const label = String(priceLabel || "").trim();
  if (!label) {
    return false;
  }
  return /^\$\s*0(?:\.0+)?\b/.test(label);
}

function getSortablePriceValue(carpark) {
  const estimate = Number(carpark?.price_now_estimate);
  if (Number.isFinite(estimate) && estimate >= 0) {
    return estimate;
  }
  const estimate60 = Number(carpark?.price_now_estimate_60min);
  if (Number.isFinite(estimate60) && estimate60 >= 0) {
    return estimate60;
  }
  return Number.POSITIVE_INFINITY;
}

function formatEntryRateLabel(segment) {
  const display = String(segment?.display_rate || segment?.rule || "").trim();
  const explicit = /\$\s*\d+(?:\.\d+)?\s*per\s*entry/i.exec(display);
  if (explicit) {
    return explicit[0].replace(/\s+/g, " ");
  }
  const money = /\$\s*\d+(?:\.\d+)?/.exec(display);
  if (money) {
    return `${money[0]} per entry`;
  }
  return "Per entry";
}

function formatCostBreakdownLine(segment) {
  const fromLabel = segment.from_label || segment.from_iso || "";
  const toLabel = segment.to_label || segment.to_iso || "";
  const mins = Number(segment.mins);
  const mode = String(segment.mode || "").toLowerCase();
  const display = String(segment.display_rate || segment.rule || "").toLowerCase();
  const segmentCost = Number(segment.cost);
  const costLabel = Number.isFinite(segmentCost) ? `$${segmentCost.toFixed(2)}` : "$0.00";
  const isEntry = mode === "entry" || display.includes("per entry");

  if (isEntry) {
    return `${fromLabel} to ${toLabel}: ${formatEntryRateLabel(segment)}`;
  }
  const minsLabel = Number.isFinite(mins) && mins > 0 ? `${Math.round(mins)} mins` : "0 mins";
  return `${fromLabel} to ${toLabel}: ${costLabel} / ${minsLabel}`;
}

function getDirectionsUrl(carpark, userFix) {
  const destination = `${carpark.lat},${carpark.lon}`;
  if (!userFix) {
    return `https://www.google.com/maps/dir/?api=1&destination=${encodeURIComponent(destination)}`;
  }
  const origin = `${userFix.lat},${userFix.lng}`;
  return (
    "https://www.google.com/maps/dir/?api=1" +
    `&origin=${encodeURIComponent(origin)}` +
    `&destination=${encodeURIComponent(destination)}`
  );
}

function getNavigationLinks(carpark, userFix) {
  const lat = Number(carpark.lat);
  const lon = Number(carpark.lon);
  const destination = `${lat},${lon}`;

  const google = getDirectionsUrl(carpark, userFix);
  const waze = `https://www.waze.com/ul?ll=${encodeURIComponent(destination)}&navigate=yes`;
  const apple = userFix
    ? `https://maps.apple.com/?saddr=${encodeURIComponent(
        `${userFix.lat},${userFix.lng}`
      )}&daddr=${encodeURIComponent(destination)}&dirflg=d`
    : `https://maps.apple.com/?daddr=${encodeURIComponent(destination)}&dirflg=d`;

  return { google, waze, apple };
}

function haversineKm(lat1, lon1, lat2, lon2) {
  const toRad = (d) => (d * Math.PI) / 180;
  const R = 6371;
  const dLat = toRad(lat2 - lat1);
  const dLon = toRad(lon2 - lon1);
  const a =
    Math.sin(dLat / 2) ** 2 +
    Math.cos(toRad(lat1)) * Math.cos(toRad(lat2)) * Math.sin(dLon / 2) ** 2;
  const c = 2 * Math.atan2(Math.sqrt(a), Math.sqrt(1 - a));
  return R * c;
}

function formatDistance(distanceKm) {
  if (distanceKm == null || Number.isNaN(distanceKm)) {
    return "Distance unavailable";
  }
  if (distanceKm < 1) {
    return `${Math.round(distanceKm * 1000)} m away`;
  }
  return `${distanceKm.toFixed(2)} km away`;
}

function formatAvailableLots(carpark) {
  const rawLots = carpark?.available_lots;
  if (rawLots == null || rawLots === "") {
    return "N/A";
  }
  const lots = Number(rawLots);
  if (Number.isFinite(lots) && lots >= 0) {
    return String(Math.trunc(lots));
  }
  return "N/A";
}

function parseAvailableLots(carpark) {
  const rawLots = carpark?.available_lots;
  if (rawLots == null || rawLots === "") {
    return null;
  }
  const lots = Number(rawLots);
  if (!Number.isFinite(lots)) {
    return null;
  }
  return Math.trunc(lots);
}

function isUraCarpark(carpark) {
  const source = String(carpark?.availability_source || "").toLowerCase();
  if (source.includes("ura")) {
    return true;
  }
  const agency = String(carpark?.availability_agency || "").toUpperCase();
  if (!agency) {
    return false;
  }
  return agency
    .split(",")
    .map((part) => part.trim())
    .includes("URA");
}

function classifyCarparkFilterGroup(carpark, displayedPriceLabel) {
  if (isUraCarpark(carpark)) {
    return "ura";
  }
  if (isFreeParkingPriceLabel(displayedPriceLabel)) {
    return "free_non_ura";
  }
  return "paid_non_ura";
}

function getLotStatus(carpark) {
  if (isUraCarpark(carpark)) {
    return { key: "blue", label: "URA" };
  }
  const lots = parseAvailableLots(carpark);
  if (lots == null) {
    return { key: "green", label: "N/A" };
  }
  if (lots <= 0) {
    return { key: "gray", label: "None" };
  }
  if (lots <= 20) {
    return { key: "red", label: "Running out" };
  }
  if (lots <= 80) {
    return { key: "yellow", label: "Half full" };
  }
  return { key: "green", label: "Lots available" };
}

function normalizePlaceSearchResult(result) {
  const lat = Number(result?.lat);
  const lon = Number(result?.lon);
  if (!Number.isFinite(lat) || !Number.isFinite(lon)) {
    return null;
  }
  const label = String(result?.label || "").trim();
  const address = String(result?.address || "").trim();
  const source = String(result?.source || "").trim();
  return {
    lat,
    lon,
    label: label || `${lat.toFixed(5)}, ${lon.toFixed(5)}`,
    address,
    source,
  };
}

function circleGeoJson(lng, lat, radiusMeters, points = 64) {
  const R = 6378137;
  const latRad = (lat * Math.PI) / 180;
  const coords = [];
  for (let i = 0; i <= points; i += 1) {
    const angle = (i / points) * 2 * Math.PI;
    const dx = radiusMeters * Math.sin(angle);
    const dy = radiusMeters * Math.cos(angle);
    const dLat = dy / R;
    const dLng = dx / (R * Math.cos(latRad));
    const pLat = lat + (dLat * 180) / Math.PI;
    const pLng = lng + (dLng * 180) / Math.PI;
    coords.push([pLng, pLat]);
  }
  return {
    type: "FeatureCollection",
    features: [
      {
        type: "Feature",
        geometry: { type: "Polygon", coordinates: [coords] },
        properties: {},
      },
    ],
  };
}

function UserAccuracyLayer({ userFix }) {
  const { map, isLoaded } = useMap();

  useEffect(() => {
    if (!map || !isLoaded || !userFix) {
      return;
    }

    const circleData = circleGeoJson(userFix.lng, userFix.lat, userFix.accuracy);
    const source = map.getSource(USER_ACCURACY_SOURCE);
    if (!source) {
      map.addSource(USER_ACCURACY_SOURCE, { type: "geojson", data: circleData });
      map.addLayer({
        id: USER_ACCURACY_FILL,
        type: "fill",
        source: USER_ACCURACY_SOURCE,
        paint: { "fill-color": "#4aa3ff", "fill-opacity": 0.15 },
      });
      map.addLayer({
        id: USER_ACCURACY_LINE,
        type: "line",
        source: USER_ACCURACY_SOURCE,
        paint: { "line-color": "#1a73e8", "line-width": 1 },
      });
    } else {
      source.setData(circleData);
    }
  }, [isLoaded, map, userFix]);

  return null;
}

function CarparkPopup({ carpark, distanceKm, userFix, stayMinutes, stayDurationLabel, stayRangeLabel }) {
  const lotStatus = getLotStatus(carpark);
  const priceLabel = getParkingPriceLabel(carpark, stayMinutes, stayDurationLabel);
  const showFreeParkingWarning = isFreeParkingPriceLabel(priceLabel);
  const currentRule = carpark.rate_now_rule && carpark.rate_now_rule !== "-" ? carpark.rate_now_rule : null;
  const navLinks = getNavigationLinks(carpark, userFix);
  const breakdownSegments = Array.isArray(carpark.price_breakdown_segments)
    ? carpark.price_breakdown_segments.filter((segment) => Number(segment?.mins) > 0)
    : [];
  const relevantRateSegments = Array.isArray(carpark.price_relevant_rate_segments)
    ? carpark.price_relevant_rate_segments.filter((segment) => Number(segment?.mins) > 0)
    : [];
  const relevantRateLines = Array.from(
    new Set(
      relevantRateSegments
        .map((segment) => String(segment?.display_rate || segment?.rule || "").trim())
        .filter(Boolean)
    )
  );
  const multiRateWindow = breakdownSegments.length > 1 && relevantRateLines.length > 1;
  const showCurrentRule = Boolean(currentRule) && !multiRateWindow;
  const totalEstimate = Number(carpark.price_now_estimate);
  const totalEstimateLabel = Number.isFinite(totalEstimate) ? `$${totalEstimate.toFixed(2)}` : null;

  return (
    <div className="popup-card">
      <p className="popup-title">{carpark.carpark}</p>
      <p className="popup-address">{carpark.address || "No address"}</p>
      <p className="popup-distance">{formatDistance(distanceKm)}</p>
      <p className={`popup-lots popup-lots-${lotStatus.key}`}>
        Available Lots: {formatAvailableLots(carpark)} ({lotStatus.label})
      </p>
      <p className="popup-rate">Estimate ({stayRangeLabel}): {priceLabel}</p>
      {showFreeParkingWarning ? (
        <p className="popup-free-warning">
          PSA: Free parking usually has eligibility criteria. Research terms before parking.
        </p>
      ) : null}
      {showCurrentRule ? <p className="popup-rule">{currentRule}</p> : null}
      {multiRateWindow ? (
        <div className="popup-breakdown">
          <p className="popup-breakdown-title">Cost breakdown</p>
          {breakdownSegments.map((segment, index) => {
            const fromLabel = segment.from_label || segment.from_iso || "";
            const toLabel = segment.to_label || segment.to_iso || "";
            return (
              <p key={`${fromLabel}-${toLabel}-${index}`} className="popup-breakdown-line">
                {formatCostBreakdownLine(segment)}
              </p>
            );
          })}
          {totalEstimateLabel ? <p className="popup-breakdown-total">Total: {totalEstimateLabel}</p> : null}
        </div>
      ) : null}
      {multiRateWindow ? (
        <div className="popup-breakdown">
          <p className="popup-breakdown-title">Relevant rates</p>
          {relevantRateLines.map((line, index) => {
            return (
              <p key={`${line}-${index}`} className="popup-breakdown-line">
                {line}
              </p>
            );
          })}
        </div>
      ) : null}
      <div className="popup-actions">
        <div className="popup-nav-grid">
          <a className="popup-directions google" href={navLinks.google} target="_blank" rel="noopener noreferrer">
            <img src="/logos/google-maps.png" alt="" className="nav-icon" />
            <span>Google Maps</span>
          </a>
          <a className="popup-directions waze" href={navLinks.waze} target="_blank" rel="noopener noreferrer">
            <img src="/logos/waze.png" alt="" className="nav-icon" />
            <span>Waze</span>
          </a>
          <a className="popup-directions apple" href={navLinks.apple} target="_blank" rel="noopener noreferrer">
            <img src="/logos/apple-maps.png" alt="" className="nav-icon" />
            <span>Apple Maps</span>
          </a>
        </div>
      </div>
    </div>
  );
}

export default function App() {
  const mapRef = useRef(null);
  const hasCenteredRef = useRef(false);
  const recenterRetryRef = useRef(null);

  const [theme, setTheme] = useState(() => {
    const saved = window.localStorage.getItem("parklah-theme");
    if (saved === "light" || saved === "dark") {
      return saved;
    }
    return window.matchMedia("(prefers-color-scheme: dark)").matches ? "dark" : "light";
  });
  const [radiusKm, setRadiusKm] = useState(5);
  const [filterChecks, setFilterChecks] = useState({
    showUra: true,
    showPaidNonUra: true,
    showFreeNonUra: true,
  });
  const [allCarparks, setAllCarparks] = useState([]);
  const [locationStatus, setLocationStatus] = useState("Location: waiting for browser permission...");
  const [geocodeStatus, setGeocodeStatus] = useState("Coordinates: loading...");
  const [userFix, setUserFix] = useState(null);
  const [searchQuery, setSearchQuery] = useState("");
  const [searchCenter, setSearchCenter] = useState(null);
  const [isSearchingPlace, setIsSearchingPlace] = useState(false);
  const [placeSearchError, setPlaceSearchError] = useState("");
  const [isMenuOpen, setIsMenuOpen] = useState(false);
  const [isBestPlacesOpen, setIsBestPlacesOpen] = useState(false);
  const [bestPlacesCount, setBestPlacesCount] = useState(20);
  const [hideFreeInBestPlaces, setHideFreeInBestPlaces] = useState(false);
  const initialStayWindow = useRef(getDefaultStayWindow()).current;
  const [stayFromTime, setStayFromTime] = useState(initialStayWindow.from);
  const [stayToTime, setStayToTime] = useState(initialStayWindow.to);
  const [appliedStayFromTime, setAppliedStayFromTime] = useState(initialStayWindow.from);
  const [appliedStayToTime, setAppliedStayToTime] = useState(initialStayWindow.to);
  const [isApplyingStay, setIsApplyingStay] = useState(false);
  const [stayApplyError, setStayApplyError] = useState("");

  useEffect(() => {
    window.localStorage.setItem("parklah-theme", theme);
  }, [theme]);

  const draftStayDurationMinutes = useMemo(
    () => computeStayDurationMinutes(stayFromTime, stayToTime),
    [stayFromTime, stayToTime]
  );
  const appliedStayDurationMinutes = useMemo(
    () => computeStayDurationMinutes(appliedStayFromTime, appliedStayToTime),
    [appliedStayFromTime, appliedStayToTime]
  );
  const stayDurationLabel = `${appliedStayDurationMinutes} mins`;
  const appliedStayRangeLabel =
    `${formatTimeInputLabel(appliedStayFromTime)} to ${formatTimeInputLabel(appliedStayToTime)}`;
  const stayRangePreview = `${stayFromTime} to ${stayToTime} (${formatDurationLabel(draftStayDurationMinutes)})`;
  const appliedStayPreview =
    `${appliedStayFromTime} to ${appliedStayToTime} ` +
    `(${formatDurationLabel(appliedStayDurationMinutes)})`;
  const hasPendingStayChanges = stayFromTime !== appliedStayFromTime || stayToTime !== appliedStayToTime;
  const nearbyCenter = useMemo(() => {
    if (searchCenter) {
      return { lat: searchCenter.lat, lng: searchCenter.lon };
    }
    if (userFix) {
      return { lat: userFix.lat, lng: userFix.lng };
    }
    return null;
  }, [searchCenter, userFix]);
  const nearbyContextLabel = useMemo(() => {
    if (searchCenter) {
      return `Nearby search: ${searchCenter.label}`;
    }
    if (userFix) {
      return "Nearby your current location";
    }
    return "No center selected: showing all carparks";
  }, [searchCenter, userFix]);
  const filterSummaryLabel = useMemo(() => {
    const labels = [];
    if (filterChecks.showUra) {
      labels.push("URA");
    }
    if (filterChecks.showPaidNonUra) {
      labels.push("Parking + HDB + Rest (Paid)");
    }
    if (filterChecks.showFreeNonUra) {
      labels.push("Parking + HDB + Rest (Free)");
    }
    if (!labels.length) {
      return "None";
    }
    return labels.join(", ");
  }, [filterChecks]);

  const fetchCarparksForWindow = useCallback(async (fromTime, toTime) => {
    const stayMinutes = computeStayDurationMinutes(fromTime, toTime);
    const fromMinutes = parseTimeInputToMinutes(fromTime);
    const toMinutes = parseTimeInputToMinutes(toTime);
    const params = new URLSearchParams();
    params.set("stay_min", String(stayMinutes));
    if (Number.isFinite(fromMinutes)) {
      params.set("stay_from", String(fromMinutes));
    }
    if (Number.isFinite(toMinutes)) {
      params.set("stay_to", String(toMinutes));
    }
    const resp = await fetch(`/api/carparks?${params.toString()}`, { cache: "no-store" });
    if (!resp.ok) {
      throw new Error(`HTTP ${resp.status}`);
    }
    const payload = await resp.json();
    return Array.isArray(payload.carparks) ? payload.carparks : [];
  }, []);

  const refreshStatus = useCallback(async () => {
    try {
      const resp = await fetch("/api/status", { cache: "no-store" });
      if (!resp.ok) {
        throw new Error(`HTTP ${resp.status}`);
      }
      const status = await resp.json();
      const done = Number(status.geocode_done || 0);
      const total = Number(status.geocode_total || 0);
      const withCoords = Number(status.carparks_with_coordinates || 0);
      const totalCarparks = Number(status.total_carparks || 0);
      if (status.geocode_running && total > 0) {
        setGeocodeStatus(
          `Coordinates: ${withCoords}/${totalCarparks} carparks ready (geocoding ${done}/${total})`
        );
      } else {
        setGeocodeStatus(`Coordinates: ${withCoords}/${totalCarparks} carparks ready`);
      }
    } catch (error) {
      setGeocodeStatus(`Status fetch failed: ${error.message}`);
    }
  }, []);

  const onApplyStayWindow = useCallback(async () => {
    if (isApplyingStay) {
      return;
    }

    setIsApplyingStay(true);
    setStayApplyError("");

    try {
      const carparks = await fetchCarparksForWindow(stayFromTime, stayToTime);
      setAllCarparks(carparks);
      setAppliedStayFromTime(stayFromTime);
      setAppliedStayToTime(stayToTime);
      await new Promise((resolve) => window.requestAnimationFrame(resolve));
      setIsMenuOpen(false);
    } catch (error) {
      setStayApplyError(`Apply failed: ${error.message}`);
    } finally {
      setIsApplyingStay(false);
    }
  }, [fetchCarparksForWindow, isApplyingStay, stayFromTime, stayToTime]);

  const applySearchCenter = useCallback((rawResult, fallbackLabel = "") => {
    const normalized = normalizePlaceSearchResult(rawResult);
    if (!normalized) {
      return false;
    }

    const chosen = {
      ...normalized,
      label: normalized.label || fallbackLabel || "Searched place",
    };
    setSearchCenter(chosen);
    setPlaceSearchError("");
    const map = mapRef.current;
    if (map) {
      map.flyTo({ center: [chosen.lon, chosen.lat], zoom: 14, essential: true });
    }
    return true;
  }, []);

  const onSearchPlace = useCallback(async (event) => {
    event.preventDefault();
    if (isSearchingPlace) {
      return;
    }

    const query = searchQuery.trim();
    if (query.length < 2) {
      setPlaceSearchError("Enter at least 2 characters.");
      return;
    }

    setIsSearchingPlace(true);
    setPlaceSearchError("");
    try {
      const params = new URLSearchParams();
      params.set("q", query);
      params.set("limit", "1");
      const resp = await fetch(`/api/place-search?${params.toString()}`, { cache: "no-store" });
      const payload = await resp.json().catch(() => ({}));
      if (!resp.ok) {
        throw new Error(payload?.error || `HTTP ${resp.status}`);
      }
      const result = Array.isArray(payload?.results) ? payload.results[0] : null;
      if (!result || !applySearchCenter(result, query)) {
        setPlaceSearchError("No matching place found.");
      }
    } catch (error) {
      setPlaceSearchError(`Search failed: ${error.message}`);
    } finally {
      setIsSearchingPlace(false);
    }
  }, [applySearchCenter, isSearchingPlace, searchQuery]);

  const onClearSearchCenter = useCallback(() => {
    setSearchCenter(null);
    setPlaceSearchError("");
    const map = mapRef.current;
    if (map && userFix) {
      map.flyTo({ center: [userFix.lng, userFix.lat], zoom: 14, essential: true });
    }
  }, [userFix]);

  useEffect(() => {
    const loadInitial = async () => {
      try {
        const carparks = await fetchCarparksForWindow(initialStayWindow.from, initialStayWindow.to);
        setAllCarparks(carparks);
      } catch (error) {
        setGeocodeStatus(`Coordinates fetch failed: ${error.message}`);
      }
    };
    loadInitial();
    refreshStatus();
    const statusTimer = window.setInterval(refreshStatus, 10000);
    return () => {
      window.clearInterval(statusTimer);
    };
  }, [fetchCarparksForWindow, initialStayWindow.from, initialStayWindow.to, refreshStatus]);

  useEffect(() => {
    if (!navigator.geolocation) {
      setLocationStatus("Location: browser geolocation is not supported.");
      return undefined;
    }
    const watchId = navigator.geolocation.watchPosition(
      (position) => {
        const { latitude, longitude, accuracy } = position.coords;
        setUserFix({ lat: latitude, lng: longitude, accuracy: Math.max(accuracy || 0, 25) });
        setLocationStatus(
          `Location: ${latitude.toFixed(5)}, ${longitude.toFixed(5)} (+/-${Math.round(accuracy)}m)`
        );
      },
      (error) => setLocationStatus(`Location error: ${error.message}`),
      { enableHighAccuracy: true, maximumAge: 5000, timeout: 12000 }
    );
    return () => navigator.geolocation.clearWatch(watchId);
  }, []);

  useEffect(() => {
    if (!userFix || hasCenteredRef.current || searchCenter) {
      return;
    }

    let attempts = 0;
    const maxAttempts = 20;
    const intervalMs = 50;

    const tryCenter = () => {
      const map = mapRef.current;
      if (!map) {
        return false;
      }
      map.flyTo({ center: [userFix.lng, userFix.lat], zoom: 14, essential: true });
      hasCenteredRef.current = true;
      return true;
    };

    if (tryCenter()) {
      return;
    }

    recenterRetryRef.current = window.setInterval(() => {
      attempts += 1;
      if (tryCenter() || attempts >= maxAttempts) {
        window.clearInterval(recenterRetryRef.current);
        recenterRetryRef.current = null;
      }
    }, intervalMs);

    return () => {
      if (recenterRetryRef.current) {
        window.clearInterval(recenterRetryRef.current);
        recenterRetryRef.current = null;
      }
    };
  }, [searchCenter, userFix]);

  const visibleCarparks = useMemo(() => {
    return allCarparks
      .map((cp) => ({
        ...cp,
        lat: Number(cp.lat),
        lon: Number(cp.lon),
      }))
      .filter((cp) => Number.isFinite(cp.lat) && Number.isFinite(cp.lon))
      .map((cp) => {
        let distanceKm = null;
        if (nearbyCenter) {
          distanceKm = haversineKm(nearbyCenter.lat, nearbyCenter.lng, cp.lat, cp.lon);
        }
        const displayedPriceLabel = getParkingPriceLabel(cp, appliedStayDurationMinutes, stayDurationLabel);
        const filterGroup = classifyCarparkFilterGroup(cp, displayedPriceLabel);
        return { cp, distanceKm, filterGroup, displayedPriceLabel };
      })
      .filter(({ distanceKm, filterGroup }) => {
        const passesGroup =
          (filterGroup === "ura" && filterChecks.showUra) ||
          (filterGroup === "paid_non_ura" && filterChecks.showPaidNonUra) ||
          (filterGroup === "free_non_ura" && filterChecks.showFreeNonUra);
        if (!passesGroup) {
          return false;
        }
        if (!nearbyCenter) {
          return true;
        }
        return distanceKm <= radiusKm;
      });
  }, [allCarparks, appliedStayDurationMinutes, filterChecks, nearbyCenter, radiusKm, stayDurationLabel]);

  const markerNodes = useMemo(
    () =>
      visibleCarparks.map(({ cp, distanceKm }) => (
        <MapMarker key={`${cp.id ?? cp.carpark}-${cp.postal_code ?? ""}`} longitude={cp.lon} latitude={cp.lat}>
          <MarkerContent>
            <div
              className={`mapcn-marker-badge mapcn-marker-lots-${getLotStatus(cp).key}`}
              aria-label={`Carpark marker: ${cp.carpark}`}
            >
              <CircleParking size={16} className="mapcn-marker-icon" />
            </div>
          </MarkerContent>
          <MarkerPopup closeButton>
            <CarparkPopup
              carpark={cp}
              distanceKm={distanceKm}
              userFix={userFix}
              stayMinutes={appliedStayDurationMinutes}
              stayDurationLabel={stayDurationLabel}
              stayRangeLabel={appliedStayRangeLabel}
            />
          </MarkerPopup>
        </MapMarker>
      )),
    [appliedStayDurationMinutes, appliedStayRangeLabel, stayDurationLabel, userFix, visibleCarparks]
  );

  const bestPlaces = useMemo(() => {
    return visibleCarparks
      .filter(({ displayedPriceLabel }) => (hideFreeInBestPlaces ? !isFreeParkingPriceLabel(displayedPriceLabel) : true))
      .map(({ cp, distanceKm, displayedPriceLabel }) => ({
        cp,
        distanceKm,
        displayedPriceLabel,
        sortPrice: getSortablePriceValue(cp),
      }))
      .sort((a, b) => {
        if (a.sortPrice !== b.sortPrice) {
          return a.sortPrice - b.sortPrice;
        }
        const dA = Number.isFinite(a.distanceKm) ? a.distanceKm : Number.POSITIVE_INFINITY;
        const dB = Number.isFinite(b.distanceKm) ? b.distanceKm : Number.POSITIVE_INFINITY;
        if (dA !== dB) {
          return dA - dB;
        }
        return String(a.cp?.carpark || "").localeCompare(String(b.cp?.carpark || ""));
      });
  }, [hideFreeInBestPlaces, visibleCarparks]);

  const topBestPlaces = useMemo(
    () => bestPlaces.slice(0, Math.max(1, Number(bestPlacesCount) || 20)),
    [bestPlaces, bestPlacesCount]
  );

  const onRecenter = useCallback(() => {
    const map = mapRef.current;
    if (!map) {
      return;
    }
    if (searchCenter) {
      map.flyTo({ center: [searchCenter.lon, searchCenter.lat], zoom: 14, essential: true });
      return;
    }
    if (!userFix) {
      return;
    }
    map.flyTo({ center: [userFix.lng, userFix.lat], zoom: 14, essential: true });
  }, [searchCenter, userFix]);

  const toggleTheme = useCallback(() => {
    setTheme((prev) => (prev === "dark" ? "light" : "dark"));
  }, []);

  const onUseCurrentTimeStart = useCallback(() => {
    const now = new Date();
    const minutes = now.getHours() * 60 + now.getMinutes();
    setStayFromTime(minutesToTimeInput(minutes));
    setStayApplyError("");
  }, []);

  useEffect(() => {
    if (!isMenuOpen) {
      return;
    }
    const onEscape = (event) => {
      if (event.key === "Escape") {
        setIsMenuOpen(false);
      }
    };
    window.addEventListener("keydown", onEscape);
    return () => window.removeEventListener("keydown", onEscape);
  }, [isMenuOpen]);

  useEffect(() => {
    if (!isBestPlacesOpen) {
      return;
    }
    const onEscape = (event) => {
      if (event.key === "Escape") {
        setIsBestPlacesOpen(false);
      }
    };
    window.addEventListener("keydown", onEscape);
    return () => window.removeEventListener("keydown", onEscape);
  }, [isBestPlacesOpen]);

  return (
    <main className={`app-shell theme-${theme}`}>
      <section className="map-shell">
        <Map
          ref={mapRef}
          className="mapcn-map"
          center={DEFAULT_CENTER}
          zoom={11.8}
          theme={theme}
          styles={MAP_STYLES}
        >
          <MapControls
            position="top-right"
            showZoom
            showLocate
            showFullscreen
            showNavigation
          />

          {userFix ? (
            <MapMarker longitude={userFix.lng} latitude={userFix.lat}>
              <MarkerContent>
                <div className="user-dot" />
              </MarkerContent>
            </MapMarker>
          ) : null}

          {searchCenter ? (
            <MapMarker longitude={searchCenter.lon} latitude={searchCenter.lat}>
              <MarkerContent>
                <div className="search-dot" />
              </MarkerContent>
              <MarkerPopup closeButton>
                <div className="popup-card">
                  <p className="popup-title">{searchCenter.label}</p>
                  {searchCenter.address ? <p className="popup-address">{searchCenter.address}</p> : null}
                  <p className="popup-distance">Search center</p>
                </div>
              </MarkerPopup>
            </MapMarker>
          ) : null}

          {userFix ? <UserAccuracyLayer userFix={userFix} /> : null}
          {markerNodes}

        </Map>
      </section>

      <button
        type="button"
        className={`menu-toggle ${isMenuOpen ? "open" : ""}`}
        aria-label={isMenuOpen ? "Close settings" : "Open settings"}
        aria-expanded={isMenuOpen}
        onClick={() => setIsMenuOpen((prev) => !prev)}
      >
        <span className="menu-toggle-bar" />
        <span className="menu-toggle-bar" />
        <span className="menu-toggle-bar" />
      </button>

      {isMenuOpen ? (
        <button
          type="button"
          className="drawer-backdrop"
          aria-label="Close settings menu"
          onClick={() => setIsMenuOpen(false)}
        />
      ) : null}

      <aside className={`settings-drawer ${isMenuOpen ? "open" : ""}`}>
        <section className="panel">
          <h1>ParkLah!</h1>
          <div className="place-search-controls">
            <p className="place-search-title">Search by place</p>
            <form className="place-search-form" onSubmit={onSearchPlace}>
              <input
                type="search"
                className="place-search-input"
                placeholder="e.g. Orchard Road"
                value={searchQuery}
                onChange={(event) => {
                  setSearchQuery(event.target.value);
                  setPlaceSearchError("");
                }}
              />
              <button type="submit" className="place-search-btn" disabled={isSearchingPlace}>
                {isSearchingPlace ? "Searching..." : "Search"}
              </button>
            </form>
            {searchCenter ? <p className="place-search-applied">Using: {searchCenter.label}</p> : null}
            {searchCenter?.source ? <p className="place-search-source">Source: {searchCenter.source}</p> : null}
            <button
              type="button"
              className="place-clear-btn"
              disabled={!searchCenter}
              onClick={onClearSearchCenter}
            >
              Use my location instead
            </button>
            {placeSearchError ? <p className="place-search-error">{placeSearchError}</p> : null}
          </div>
          <div className="carpark-filter-controls">
            <p className="carpark-filter-title">Show / hide carparks</p>
            <label className="carpark-filter-check">
              <input
                type="checkbox"
                checked={filterChecks.showUra}
                onChange={(event) =>
                  setFilterChecks((prev) => ({ ...prev, showUra: event.target.checked }))
                }
              />
              <span>URA</span>
            </label>
            <label className="carpark-filter-check">
              <input
                type="checkbox"
                checked={filterChecks.showPaidNonUra}
                onChange={(event) =>
                  setFilterChecks((prev) => ({ ...prev, showPaidNonUra: event.target.checked }))
                }
              />
              <span>Parking + HDB + Rest (Paid)</span>
            </label>
            <label className="carpark-filter-check">
              <input
                type="checkbox"
                checked={filterChecks.showFreeNonUra}
                onChange={(event) =>
                  setFilterChecks((prev) => ({ ...prev, showFreeNonUra: event.target.checked }))
                }
              />
              <span>Parking + HDB + Rest (Free)</span>
            </label>
          </div>
          <label htmlFor="radiusRange">Show carparks within {radiusKm} km</label>
          <input
            id="radiusRange"
            type="range"
            min="1"
            max="25"
            step="1"
            value={radiusKm}
            onChange={(event) => setRadiusKm(Number(event.target.value || 5))}
          />
          <div className="stay-controls">
            <p className="stay-title">When are you parking?</p>
            <div className="stay-time-grid">
              <label className="stay-time-field">
                <span className="stay-time-label-row">
                  <span className="stay-time-label">From</span>
                  <button
                    type="button"
                    className="stay-now-btn"
                    onClick={onUseCurrentTimeStart}
                  >
                    Current time
                  </button>
                </span>
                <input
                  type="time"
                  step="60"
                  className="stay-time-input"
                  value={stayFromTime}
                  onChange={(event) => {
                    setStayFromTime(event.target.value);
                    setStayApplyError("");
                  }}
                />
              </label>
              <label className="stay-time-field">
                <span className="stay-time-label">To</span>
                <input
                  type="time"
                  step="60"
                  className="stay-time-input"
                  value={stayToTime}
                  onChange={(event) => {
                    setStayToTime(event.target.value);
                    setStayApplyError("");
                  }}
                />
              </label>
            </div>
            <p className="stay-preview">Using: {stayRangePreview}</p>
            <p className="stay-applied">Applied: {appliedStayPreview}</p>
            <button
              type="button"
              className="stay-apply-btn"
              disabled={isApplyingStay || !hasPendingStayChanges}
              onClick={onApplyStayWindow}
            >
              {isApplyingStay ? "Applying..." : hasPendingStayChanges ? "Apply" : "Applied"}
            </button>
            {stayApplyError ? <p className="stay-error">{stayApplyError}</p> : null}
          </div>
          <div className="theme-toggle-row">
            <span className="theme-toggle-text">{theme === "dark" ? "Dark Mode" : "Light Mode"}</span>
            <button
              type="button"
              className={`theme-switch ${theme === "dark" ? "is-dark" : "is-light"}`}
              aria-label={theme === "dark" ? "Switch to light mode" : "Switch to dark mode"}
              onClick={toggleTheme}
            >
              <span className="theme-switch-knob" />
            </button>
          </div>
          <div className="stats">
            <p>{locationStatus}</p>
            <p>{nearbyContextLabel}</p>
            <p>Type filter: {filterSummaryLabel}</p>
            <p>Visible carparks: {visibleCarparks.length}</p>
            <p>{geocodeStatus}</p>
          </div>
        </section>
      </aside>

      {isBestPlacesOpen ? (
        <>
          <button
            type="button"
            className="best-places-backdrop"
            aria-label="Close best places modal"
            onClick={() => setIsBestPlacesOpen(false)}
          />
          <section className="best-places-modal" role="dialog" aria-modal="true" aria-label="Best places to park">
            <div className="best-places-header">
              <h2>Best places to park</h2>
              <div className="best-places-controls">
                <label htmlFor="bestPlacesCountSelect">Show</label>
                <select
                  id="bestPlacesCountSelect"
                  value={bestPlacesCount}
                  onChange={(event) => setBestPlacesCount(Number(event.target.value) || 20)}
                >
                  <option value={10}>10</option>
                  <option value={20}>20</option>
                  <option value={30}>30</option>
                  <option value={50}>50</option>
                </select>
                <label className="best-places-toggle">
                  <input
                    type="checkbox"
                    checked={hideFreeInBestPlaces}
                    onChange={(event) => setHideFreeInBestPlaces(event.target.checked)}
                  />
                  <span>Hide free parking places</span>
                </label>
                <button
                  type="button"
                  className="best-places-close-btn"
                  onClick={() => setIsBestPlacesOpen(false)}
                >
                  Close
                </button>
              </div>
            </div>
            <p className="best-places-subtitle">
              Sorted by estimated parking price ({appliedStayRangeLabel}). {bestPlaces.length} candidates.
            </p>
            <div className="best-places-list">
              {topBestPlaces.length ? (
                topBestPlaces.map(({ cp, distanceKm, sortPrice, displayedPriceLabel }, index) => {
                  const navLinks = getNavigationLinks(cp, userFix);
                  const showFreeParkingWarning = isFreeParkingPriceLabel(displayedPriceLabel);
                  const hasPrice = Number.isFinite(sortPrice);
                  return (
                    <article
                      key={`best-${cp.id ?? cp.carpark}-${cp.postal_code ?? ""}`}
                      className="best-place-item"
                    >
                      <div className="best-place-main">
                        <p className="best-place-rank">#{index + 1}</p>
                        <div className="best-place-copy">
                          <p className="best-place-name">{cp.carpark}</p>
                          <p className="best-place-address">{cp.address || "No address"}</p>
                          <p className="best-place-meta">
                            {formatDistance(distanceKm)} · Available Lots: {formatAvailableLots(cp)}
                          </p>
                          {showFreeParkingWarning ? (
                            <p className="best-place-warning">
                              PSA: Free parking usually has criteria. Please check terms first.
                            </p>
                          ) : null}
                        </div>
                        <p className={`best-place-price${hasPrice ? "" : " is-na"}`}>
                          {displayedPriceLabel}
                        </p>
                      </div>
                      <div className="best-place-nav">
                        <a
                          className="best-place-nav-link"
                          href={navLinks.google}
                          target="_blank"
                          rel="noopener noreferrer"
                          aria-label={`Open ${cp.carpark} in Google Maps`}
                          title="Google Maps"
                        >
                          <img src="/logos/google-maps.png" alt="" className="nav-icon" />
                        </a>
                        <a
                          className="best-place-nav-link"
                          href={navLinks.waze}
                          target="_blank"
                          rel="noopener noreferrer"
                          aria-label={`Open ${cp.carpark} in Waze`}
                          title="Waze"
                        >
                          <img src="/logos/waze.png" alt="" className="nav-icon" />
                        </a>
                        <a
                          className="best-place-nav-link"
                          href={navLinks.apple}
                          target="_blank"
                          rel="noopener noreferrer"
                          aria-label={`Open ${cp.carpark} in Apple Maps`}
                          title="Apple Maps"
                        >
                          <img src="/logos/apple-maps.png" alt="" className="nav-icon" />
                        </a>
                      </div>
                    </article>
                  );
                })
              ) : (
                <p className="best-places-empty">No carparks available in the current area.</p>
              )}
            </div>
          </section>
        </>
      ) : null}

      <div className="map-action-row">
        <button
          type="button"
          className="best-places-btn"
          onClick={() => setIsBestPlacesOpen(true)}
          disabled={!bestPlaces.length}
        >
          Best places to park
        </button>
        <button type="button" className="map-recenter-btn" aria-label="Recenter on me" onClick={onRecenter}>
          <LocateFixed size={18} />
        </button>
      </div>
    </main>
  );
}
