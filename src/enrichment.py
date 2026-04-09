"""Physician enrichment using the NPPES NPI Registry."""

from __future__ import annotations

import logging
from pathlib import Path
from typing import Any, Dict, List, Optional

import requests

from .models import OrderRecord
from .utils import (
    extract_digits,
    load_json,
    normalize_text,
    request_with_retries,
    save_json,
    split_patient_name,
)


class NPIEnricher:
    """Enriches physician data using NPI registry with local cache."""

    def __init__(self, config: Dict[str, Any], logger: logging.Logger):
        self.config = config
        self.logger = logger
        self.npi_cfg = config.get("npi", {})
        self.processing_cfg = config.get("processing", {})

        cache_dir = Path(self.processing_cfg.get("cache_dir", "./cache"))
        if not cache_dir.is_absolute():
            cache_dir = (Path.cwd() / cache_dir).resolve()
        cache_dir.mkdir(parents=True, exist_ok=True)

        self.cache_file = cache_dir / "npi_cache.json"
        self.cache: Dict[str, Dict[str, Any]] = load_json(self.cache_file, default={}) or {}

        self.session = requests.Session()
        self.api_url = normalize_text(self.npi_cfg.get("api_url", "https://npiregistry.cms.hhs.gov/api/")).rstrip("/")
        self.version = normalize_text(self.npi_cfg.get("version", "2.1"))
        self.limit = int(self.npi_cfg.get("max_results", 20))
        self.retries = int(self.npi_cfg.get("max_retries", 3))
        self.backoff = float(self.npi_cfg.get("retry_backoff_seconds", 2.0))
        self.timeout = int(self.npi_cfg.get("timeout_seconds", 20))

    def enrich(self, records: List[OrderRecord]) -> List[OrderRecord]:
        """Enrich each order record with normalized physician information."""
        if not records:
            return records

        enriched_count = 0
        for record in records:
            details = self._lookup(record.npi, record.physician_name)
            if not details:
                record.metadata["physician_verification"] = "unverified"
                continue

            if details.get("npi") and not record.npi:
                record.npi = details["npi"]

            record.metadata["physician_verification"] = "verified"
            record.metadata["physician_standardized_name"] = details.get("name")
            record.metadata["physician_specialty"] = details.get("specialty")
            record.metadata["physician_address"] = details.get("address")
            enriched_count += 1

        self._persist_cache()
        self.logger.info("NPI enrichment completed | enriched=%d | total=%d", enriched_count, len(records))
        return records

    def _lookup(self, npi: Optional[str], physician_name: str) -> Optional[Dict[str, Any]]:
        npi_digits = extract_digits(npi)
        cache_key = self._cache_key(npi_digits, physician_name)
        if cache_key in self.cache:
            return self.cache[cache_key]

        result = None
        if npi_digits:
            result = self._query_registry(number=npi_digits)

        if not result:
            first, _, last = split_patient_name(physician_name)
            if first and last:
                result = self._query_registry(first_name=first, last_name=last)

        if result:
            self.cache[cache_key] = result
        return result

    def _query_registry(
        self,
        *,
        number: Optional[str] = None,
        first_name: Optional[str] = None,
        last_name: Optional[str] = None,
    ) -> Optional[Dict[str, Any]]:
        if not number and not (first_name and last_name):
            return None

        params: Dict[str, Any] = {
            "version": self.version,
            "limit": self.limit,
        }
        if number:
            params["number"] = number
        else:
            params["first_name"] = first_name
            params["last_name"] = last_name

        url = f"{self.api_url}/"
        try:
            response = request_with_retries(
                self.session,
                "GET",
                url,
                self.logger,
                retries=self.retries,
                backoff_seconds=self.backoff,
                timeout=self.timeout,
                params=params,
            )
        except Exception as error:
            self.logger.warning("NPI API lookup failed (%s): %s", params, error)
            return None

        payload = response.json()
        if payload.get("result_count", 0) <= 0:
            return None

        result = payload.get("results", [{}])[0]
        basic = result.get("basic", {})
        addresses = result.get("addresses", [])
        taxonomies = result.get("taxonomies", [])

        specialty = ""
        if taxonomies:
            specialty = normalize_text(taxonomies[0].get("desc"))

        address = ""
        if addresses:
            address_obj = addresses[0]
            address = ", ".join(
                filter(
                    None,
                    [
                        normalize_text(address_obj.get("address_1")),
                        normalize_text(address_obj.get("city")),
                        normalize_text(address_obj.get("state")),
                        normalize_text(address_obj.get("postal_code")),
                    ],
                )
            )

        full_name = " ".join(
            filter(
                None,
                [
                    normalize_text(basic.get("first_name")),
                    normalize_text(basic.get("middle_name")),
                    normalize_text(basic.get("last_name")),
                ],
            )
        )

        return {
            "npi": extract_digits(result.get("number")),
            "name": full_name,
            "specialty": specialty,
            "address": address,
            "enumeration_type": normalize_text(result.get("enumeration_type")),
        }

    def _cache_key(self, npi: str, physician_name: str) -> str:
        if npi:
            return f"npi:{npi}"
        return f"name:{normalize_text(physician_name).lower()}"

    def _persist_cache(self) -> None:
        save_json(self.cache_file, self.cache)
