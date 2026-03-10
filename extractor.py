"""
extractor.py — EXTRACT layer
Fetches raw hourly weather data from the Open-Meteo REST API.
Open-Meteo is completely free and requires no API key.
"""

import logging
from typing import Any

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

from config import API_CONFIG


logger = logging.getLogger("etl.extractor")


class WeatherExtractor:
    """
    Fetches hourly weather data for a given city from Open-Meteo.

    Includes:
        - Automatic retries with exponential backoff
        - Request timeout enforcement
        - Structured error reporting
    """

    def __init__(self):
        self.session = self._build_session()
        self.base_url = API_CONFIG["base_url"]

    # ── Public ────────────────────────────────────────────────────────────────

    def fetch(self, city: dict) -> dict[str, Any]:
        """
        Fetch raw API response for one city.

        Args:
            city: dict with keys: name, lat, lon, timezone

        Returns:
            Parsed JSON response dict from Open-Meteo

        Raises:
            requests.HTTPError: on non-2xx API response
            requests.Timeout:   if request exceeds configured timeout
        """
        params = self._build_params(city)
        logger.debug(f"GET {self.base_url} params={params}")

        response = self.session.get(
            self.base_url,
            params=params,
            timeout=API_CONFIG["timeout_seconds"],
        )
        response.raise_for_status()
        data = response.json()

        # Attach city metadata so downstream steps can use it
        data["_city"] = city
        return data

    # ── Private ───────────────────────────────────────────────────────────────

    def _build_params(self, city: dict) -> dict:
        return {
            "latitude":       city["lat"],
            "longitude":      city["lon"],
            "timezone":       city["timezone"],
            "past_days":      API_CONFIG["past_days"],
            "forecast_days":  API_CONFIG["forecast_days"],
            "hourly":         ",".join(API_CONFIG["hourly_vars"]),
        }

    @staticmethod
    def _build_session() -> requests.Session:
        """Session with retry strategy and connection pooling."""
        session = requests.Session()
        retry = Retry(
            total=3,
            backoff_factor=1,
            status_forcelist=[429, 500, 502, 503, 504],
            allowed_methods=["GET"],
        )
        adapter = HTTPAdapter(max_retries=retry)
        session.mount("https://", adapter)
        session.mount("http://", adapter)
        session.headers.update({"Accept": "application/json"})
        return session
