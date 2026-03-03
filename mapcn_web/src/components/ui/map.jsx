import {
  Children,
  createContext,
  forwardRef,
  isValidElement,
  useContext,
  useEffect,
  useImperativeHandle,
  useMemo,
  useRef,
  useState,
} from "react";
import { createRoot } from "react-dom/client";
import maplibregl from "maplibre-gl";

const MapContext = createContext({ map: null, isLoaded: false });

export function useMap() {
  return useContext(MapContext);
}

function resolveStyle(styles, theme) {
  if (!styles) {
    return "https://demotiles.maplibre.org/style.json";
  }
  if (typeof styles === "string") {
    return styles;
  }
  if (theme === "dark" && styles.dark) {
    return styles.dark;
  }
  if (styles.light) {
    return styles.light;
  }
  if (styles.dark) {
    return styles.dark;
  }
  return "https://demotiles.maplibre.org/style.json";
}

export const Map = forwardRef(function Map(
  { center = [103.8198, 1.3521], zoom = 11, styles, theme = "light", className, children },
  ref
) {
  const containerRef = useRef(null);
  const mapRef = useRef(null);
  const [isLoaded, setIsLoaded] = useState(false);
  const initialCenterRef = useRef(center);
  const initialZoomRef = useRef(zoom);

  useImperativeHandle(ref, () => mapRef.current, [isLoaded]);

  useEffect(() => {
    if (!containerRef.current || mapRef.current) {
      return;
    }

    const style = resolveStyle(styles, theme);
    const map = new maplibregl.Map({
      container: containerRef.current,
      style,
      center: initialCenterRef.current,
      zoom: initialZoomRef.current,
      minZoom: 1,
      maxZoom: 19,
      attributionControl: false,
    });
    map.addControl(new maplibregl.AttributionControl({ compact: true }), "bottom-left");

    mapRef.current = map;
    const handleLoad = () => setIsLoaded(true);
    map.on("load", handleLoad);

    return () => {
      map.off("load", handleLoad);
      map.remove();
      mapRef.current = null;
      setIsLoaded(false);
    };
  }, []);

  useEffect(() => {
    const map = mapRef.current;
    if (!map || !isLoaded) {
      return;
    }
    map.setStyle(resolveStyle(styles, theme));
  }, [isLoaded, styles, theme]);

  const ctx = useMemo(
    () => ({
      map: mapRef.current,
      isLoaded,
    }),
    [isLoaded]
  );

  return (
    <MapContext.Provider value={ctx}>
      <div ref={containerRef} className={className} />
      {children}
    </MapContext.Provider>
  );
});

function attachControl(map, control, position) {
  map.addControl(control, position);
  return () => {
    try {
      map.removeControl(control);
    } catch {
      // no-op
    }
  };
}

export function MapControls({
  position = "top-right",
  showZoom,
  showLocate,
  showFullscreen,
  showNavigation,
}) {
  const { map, isLoaded } = useMap();

  useEffect(() => {
    if (!map || !isLoaded) {
      return;
    }
    const cleanups = [];

    if (showZoom || showNavigation) {
      cleanups.push(attachControl(map, new maplibregl.NavigationControl(), position));
    }
    if (showLocate) {
      cleanups.push(
        attachControl(
          map,
          new maplibregl.GeolocateControl({
            positionOptions: { enableHighAccuracy: true },
            trackUserLocation: true,
          }),
          position
        )
      );
    }
    if (showFullscreen) {
      cleanups.push(attachControl(map, new maplibregl.FullscreenControl(), position));
    }

    return () => {
      for (const cleanup of cleanups) {
        cleanup();
      }
    };
  }, [isLoaded, map, position, showFullscreen, showLocate, showNavigation, showZoom]);

  return null;
}

const MARKER_CONTENT_TYPE = Symbol("MarkerContent");
const MARKER_POPUP_TYPE = Symbol("MarkerPopup");
const MARKER_POPUP_OFFSET = 18;

export function MarkerContent({ children }) {
  return children ?? null;
}
MarkerContent.$$type = MARKER_CONTENT_TYPE;

export function MarkerPopup({ children }) {
  return children ?? null;
}
MarkerPopup.$$type = MARKER_POPUP_TYPE;

function createDefaultMarkerDot() {
  const dot = document.createElement("div");
  dot.style.width = "14px";
  dot.style.height = "14px";
  dot.style.borderRadius = "999px";
  dot.style.background = "#2a7f62";
  dot.style.border = "2px solid #184d3a";
  dot.style.boxSizing = "border-box";
  return dot;
}

export function MapMarker({ longitude, latitude, children }) {
  const { map, isLoaded } = useMap();

  useEffect(() => {
    if (!map || !isLoaded || !Number.isFinite(longitude) || !Number.isFinite(latitude)) {
      return;
    }

    let markerContent = null;
    let markerPopup = null;

    Children.forEach(children, (child) => {
      if (!isValidElement(child)) {
        return;
      }
      if (child.type?.$$type === MARKER_CONTENT_TYPE) {
        markerContent = child.props.children;
      } else if (child.type?.$$type === MARKER_POPUP_TYPE) {
        markerPopup = child.props.children;
      }
    });

    const markerEl = document.createElement("div");
    let markerRoot = null;
    let popupRoot = null;

    if (markerContent) {
      const markerMount = document.createElement("div");
      markerEl.appendChild(markerMount);
      markerRoot = createRoot(markerMount);
      markerRoot.render(markerContent);
    } else {
      markerEl.appendChild(createDefaultMarkerDot());
    }

    const marker = new maplibregl.Marker({ element: markerEl })
      .setLngLat([longitude, latitude])
      .addTo(map);

    if (markerPopup) {
      const popupMount = document.createElement("div");
      popupRoot = createRoot(popupMount);
      popupRoot.render(markerPopup);
      const popup = new maplibregl.Popup({ closeButton: true, offset: MARKER_POPUP_OFFSET }).setDOMContent(
        popupMount
      );
      marker.setPopup(popup);
    }

    return () => {
      marker.remove();
      markerRoot?.unmount();
      popupRoot?.unmount();
    };
  }, [children, isLoaded, latitude, longitude, map]);

  return null;
}
