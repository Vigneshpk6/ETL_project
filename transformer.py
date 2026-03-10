"""
transformer.py — TRANSFORM layer
Cleans, validates, enriches, and normalises raw Open-Meteo API data
into flat dictionaries ready for database insertion.
"""

import logging
import math
from datetime import datetime
from typing import Any

logger = logging.getLogger("etl.transformer")


# WMO Weather Interpretation Codes → human-readable description
WMO_CODES: dict[int, str] = {
    0: "Clear sky",
    1: "Mainly clear", 2: "Partly cloudy", 3: "Overcast",
    45: "Fog", 48: "Icy fog",
    51: "Light drizzle", 53: "Moderate drizzle", 55: "Dense drizzle",
    61: "Slight rain", 63: "Moderate rain", 65: "Heavy rain",
    71: "Slight snow", 73: "Moderate snow", 75: "Heavy snow",
    77: "Snow grains",
    80: "Slight showers", 81: "Moderate showers", 82: "Violent showers",
    85: "Slight snow showers", 86: "Heavy snow showers",
    95: "Thunderstorm", 96: "Thunderstorm with hail", 99: "Thunderstorm with heavy hail",
}


class WeatherTransformer:
    """
    Transforms a raw Open-Meteo API response into a list of clean records.

    Transformations applied:
        1. Flatten nested hourly arrays into per-row dicts
        2. Parse ISO timestamps → datetime objects
        3. Null / out-of-range value handling
        4. Unit conversions (°C→°F, m/s→mph, hPa→inHg, km→miles)
        5. Derived fields: heat index, wind chill, dew point, Beaufort scale
        6. Categorical enrichment: weather description, wind cardinal direction
        7. Time-of-day / season tagging
    """

    # ── Public ────────────────────────────────────────────────────────────────

    def transform(self, raw: dict[str, Any], city: dict) -> list[dict]:
        """
        Convert raw API payload to a list of cleaned record dicts.

        Args:
            raw:  Full Open-Meteo JSON response
            city: City metadata dict

        Returns:
            List of flat dicts, one per hourly observation
        """
        hourly = raw.get("hourly", {})
        timestamps = hourly.get("time", [])

        records = []
        for i, ts in enumerate(timestamps):
            try:
                record = self._build_record(hourly, city, raw, i, ts)
                records.append(record)
            except Exception as exc:
                logger.warning(f"Skipping row {i} for {city['name']}: {exc}")

        logger.debug(f"Transformed {len(records)}/{len(timestamps)} records for {city['name']}")
        return records

    # ── Private — record assembly ─────────────────────────────────────────────

    def _build_record(self, hourly, city, raw, i, ts) -> dict:
        dt = datetime.fromisoformat(ts)

        temp_c     = self._get(hourly, "temperature_2m", i)
        apparent_c = self._get(hourly, "apparent_temperature", i)
        humidity   = self._get(hourly, "relative_humidity_2m", i)
        precip     = self._get(hourly, "precipitation", i)
        rain       = self._get(hourly, "rain", i)
        snowfall   = self._get(hourly, "snowfall", i)
        wind_ms    = self._get(hourly, "wind_speed_10m", i)
        wind_dir   = self._get(hourly, "wind_direction_10m", i)
        wind_gust  = self._get(hourly, "wind_gusts_10m", i)
        pressure   = self._get(hourly, "surface_pressure", i)
        cloud      = self._get(hourly, "cloud_cover", i)
        visibility = self._get(hourly, "visibility", i)
        uv_index   = self._get(hourly, "uv_index", i)
        wmo_code   = self._get(hourly, "weather_code", i)

        temp_f     = self._c_to_f(temp_c)
        wind_mph   = self._ms_to_mph(wind_ms)
        wind_kph   = self._ms_to_kph(wind_ms)
        pressure_inhg = self._hpa_to_inhg(pressure)
        vis_miles  = self._km_to_miles(visibility / 1000) if visibility is not None else None

        dew_point_c   = self._dew_point(temp_c, humidity)
        heat_index_c  = self._heat_index(temp_c, humidity)
        wind_chill_c  = self._wind_chill(temp_c, wind_ms)

        return {
            # ── Identity ───────────────────────────────────────────
            "city":              city["name"],
            "latitude":          city["lat"],
            "longitude":         city["lon"],
            "timezone":          city["timezone"],
            "observed_at":       dt,
            "ingested_at":       datetime.utcnow(),
            "elevation_m":       raw.get("elevation"),

            # ── Temperature ────────────────────────────────────────
            "temp_c":            round(temp_c, 2)            if temp_c     is not None else None,
            "temp_f":            round(temp_f, 2)            if temp_f     is not None else None,
            "apparent_temp_c":   round(apparent_c, 2)        if apparent_c is not None else None,
            "apparent_temp_f":   self._c_to_f(apparent_c),
            "dew_point_c":       round(dew_point_c, 2)       if dew_point_c is not None else None,
            "heat_index_c":      round(heat_index_c, 2)      if heat_index_c is not None else None,
            "wind_chill_c":      round(wind_chill_c, 2)      if wind_chill_c is not None else None,

            # ── Humidity & Precipitation ───────────────────────────
            "humidity_pct":      self._clamp(humidity, 0, 100),
            "precipitation_mm":  round(precip, 3)            if precip    is not None else None,
            "rain_mm":           round(rain, 3)              if rain      is not None else None,
            "snowfall_cm":       round(snowfall, 3)          if snowfall  is not None else None,

            # ── Wind ───────────────────────────────────────────────
            "wind_speed_ms":     round(wind_ms, 2)           if wind_ms   is not None else None,
            "wind_speed_mph":    round(wind_mph, 2)          if wind_mph  is not None else None,
            "wind_speed_kph":    round(wind_kph, 2)          if wind_kph  is not None else None,
            "wind_direction_deg":self._clamp(wind_dir, 0, 360),
            "wind_cardinal":     self._cardinal(wind_dir),
            "wind_gust_ms":      round(wind_gust, 2)         if wind_gust is not None else None,
            "beaufort_scale":    self._beaufort(wind_ms),

            # ── Pressure / Atmosphere ──────────────────────────────
            "pressure_hpa":      round(pressure, 2)          if pressure  is not None else None,
            "pressure_inhg":     round(pressure_inhg, 4)     if pressure_inhg is not None else None,
            "cloud_cover_pct":   self._clamp(cloud, 0, 100),
            "visibility_m":      visibility,
            "visibility_miles":  round(vis_miles, 2)         if vis_miles is not None else None,
            "uv_index":          round(uv_index, 1)          if uv_index  is not None else None,
            "uv_category":       self._uv_category(uv_index),

            # ── Categorical ────────────────────────────────────────
            "weather_code":      int(wmo_code)               if wmo_code  is not None else None,
            "weather_desc":      WMO_CODES.get(int(wmo_code), "Unknown") if wmo_code is not None else None,

            # ── Temporal enrichment ────────────────────────────────
            "hour_of_day":       dt.hour,
            "day_of_week":       dt.strftime("%A"),
            "month":             dt.month,
            "season":            self._season(dt.month, city["lat"]),
            "is_daytime":        6 <= dt.hour < 20,
            "is_weekend":        dt.weekday() >= 5,
        }

    # ── Private — derived calculations ────────────────────────────────────────

    @staticmethod
    def _get(hourly: dict, key: str, i: int):
        vals = hourly.get(key, [])
        val = vals[i] if i < len(vals) else None
        return None if val is None or (isinstance(val, float) and math.isnan(val)) else val

    @staticmethod
    def _c_to_f(c):
        return round(c * 9 / 5 + 32, 2) if c is not None else None

    @staticmethod
    def _ms_to_mph(ms):
        return round(ms * 2.23694, 2) if ms is not None else None

    @staticmethod
    def _ms_to_kph(ms):
        return round(ms * 3.6, 2) if ms is not None else None

    @staticmethod
    def _hpa_to_inhg(hpa):
        return round(hpa * 0.02953, 4) if hpa is not None else None

    @staticmethod
    def _km_to_miles(km):
        return round(km * 0.621371, 3) if km is not None else None

    @staticmethod
    def _clamp(val, lo, hi):
        if val is None:
            return None
        return max(lo, min(hi, val))

    @staticmethod
    def _dew_point(temp_c, rh):
        """Magnus formula approximation."""
        if temp_c is None or rh is None or rh <= 0:
            return None
        a, b = 17.27, 237.7
        alpha = (a * temp_c) / (b + temp_c) + math.log(rh / 100.0)
        return round((b * alpha) / (a - alpha), 2)

    @staticmethod
    def _heat_index(temp_c, rh):
        """Rothfusz regression (valid when temp ≥ 27 °C and rh ≥ 40%)."""
        if temp_c is None or rh is None or temp_c < 27 or rh < 40:
            return None
        t = temp_c * 9 / 5 + 32  # convert to °F for formula
        hi = (-42.379 + 2.04901523*t + 10.14333127*rh
              - 0.22475541*t*rh - 0.00683783*t**2
              - 0.05481717*rh**2 + 0.00122874*t**2*rh
              + 0.00085282*t*rh**2 - 0.00000199*t**2*rh**2)
        return round((hi - 32) * 5 / 9, 2)   # back to °C

    @staticmethod
    def _wind_chill(temp_c, wind_ms):
        """Wind chill formula (valid when temp ≤ 10 °C and wind ≥ 1.3 m/s)."""
        if temp_c is None or wind_ms is None or temp_c > 10 or wind_ms < 1.3:
            return None
        v = wind_ms * 3.6  # km/h
        wc = 13.12 + 0.6215*temp_c - 11.37*(v**0.16) + 0.3965*temp_c*(v**0.16)
        return round(wc, 2)

    @staticmethod
    def _cardinal(deg) -> str | None:
        if deg is None:
            return None
        dirs = ["N","NNE","NE","ENE","E","ESE","SE","SSE",
                "S","SSW","SW","WSW","W","WNW","NW","NNW"]
        return dirs[round(deg / 22.5) % 16]

    @staticmethod
    def _beaufort(wind_ms) -> int | None:
        if wind_ms is None:
            return None
        thresholds = [0.3,1.6,3.4,5.5,8.0,10.8,13.9,17.2,20.8,24.5,28.5,32.7]
        for bf, threshold in enumerate(thresholds):
            if wind_ms < threshold:
                return bf
        return 12

    @staticmethod
    def _uv_category(uv) -> str | None:
        if uv is None:
            return None
        if uv < 3:   return "Low"
        if uv < 6:   return "Moderate"
        if uv < 8:   return "High"
        if uv < 11:  return "Very High"
        return "Extreme"

    @staticmethod
    def _season(month: int, lat: float) -> str:
        seasons_north = {12: "Winter", 1: "Winter", 2: "Winter",
                         3: "Spring", 4: "Spring", 5: "Spring",
                         6: "Summer", 7: "Summer", 8: "Summer",
                         9: "Autumn", 10: "Autumn", 11: "Autumn"}
        seasons_south = {12: "Summer", 1: "Summer", 2: "Summer",
                         3: "Autumn", 4: "Autumn", 5: "Autumn",
                         6: "Winter", 7: "Winter", 8: "Winter",
                         9: "Spring", 10: "Spring", 11: "Spring"}
        return (seasons_north if lat >= 0 else seasons_south)[month]
