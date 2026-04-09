"""MediSync API synchronization layer."""

from __future__ import annotations

import logging
from typing import Any, Dict, List, Optional

import pandas as pd
import requests

from .models import OrderRecord, SyncResult
from .utils import (
    normalize_text,
    pdf_to_base64,
    request_with_retries,
    split_patient_name,
    to_bool,
)


DEFAULT_ENDPOINTS = {
    "auth_login": "/auth/login",
    "patients_list": "/api/patients",
    "patient_create": "/api/patients",
    "patient_update": "/api/patients/{id}",
    "episodes_list": "/api/episodes",
    "episode_create": "/api/episodes",
    "orders_list": "/api/orders",
    "order_create": "/api/orders",
    "order_update": "/api/orders/{id}",
    "documents_upload": "/api/documents/upload",
    "physician_lookup": "/api/physicians/{npi}",
    "physician_create": "/api/physicians",
    "sync_run": "/sync/run",
    "metrics": "/metrics",
}


class MediSyncClient:
    """Syncs normalized records into the MediSync backend."""

    def __init__(self, config: Dict[str, Any], logger: logging.Logger):
        self.config = config
        self.logger = logger
        self.medisync_cfg = config.get("medisync", {})

        self.base_url = normalize_text(self.medisync_cfg.get("api_base_url", "")).rstrip("/")
        if not self.base_url:
            raise ValueError("medisync.api_base_url is required")

        self.endpoints = {**DEFAULT_ENDPOINTS, **self.medisync_cfg.get("endpoints", {})}
        self.session = requests.Session()
        self.token: Optional[str] = None

        self.api_key = normalize_text(self.medisync_cfg.get("api_key"))
        self.timeout = int(self.medisync_cfg.get("timeout_seconds", 30))
        self.max_retries = int(self.medisync_cfg.get("max_retries", 3))
        self.retry_backoff = float(self.medisync_cfg.get("retry_backoff_seconds", 2.0))
        self.auth_required = to_bool(self.medisync_cfg.get("auth_required", False), default=False)

    def authenticate(self) -> None:
        """Authenticate to MediSync if auth is configured."""
        if not self.auth_required:
            self.logger.info("MediSync auth_required is false; skipping token auth")
            return

        auth = self.medisync_cfg.get("auth", {})
        username = normalize_text(auth.get("username"))
        password = normalize_text(auth.get("password"))
        if not username or not password:
            raise ValueError("medisync.auth.username and medisync.auth.password are required when auth_required=true")

        payload = {
            "username": username,
            "password": password,
        }
        response = self._request("POST", "auth_login", json=payload, use_auth=False)
        body = response.json()

        token = (
            body.get("access_token")
            or body.get("token")
            or body.get("jwt")
            or body.get("data", {}).get("access_token")
        )
        if not token:
            raise RuntimeError(f"Auth succeeded but token was not found in response: {body}")

        self.token = token
        self.logger.info("MediSync authentication successful")

    def sync_records(self, records: List[OrderRecord]) -> List[SyncResult]:
        """Sync all records with patient -> episode -> order -> physician -> document linkage."""
        results: List[SyncResult] = []

        for record in records:
            if not record.is_valid:
                reason = "Validation failed: " + "; ".join(record.validation_errors)
                results.append(
                    SyncResult(
                        order_number=record.order_number,
                        patient_name=record.patient_name,
                        status="failed",
                        reason=reason,
                    )
                )
                continue

            try:
                physician_id = self._upsert_physician(record)
                patient_id = self._upsert_patient(record)
                episode_id = self._upsert_episode(record, patient_id)
                order_id = self._upsert_order(record, patient_id, episode_id, physician_id)
                document_id = self._upload_document(record, order_id)

                results.append(
                    SyncResult(
                        order_number=record.order_number,
                        patient_name=record.patient_name,
                        status="success",
                        patient_id=patient_id,
                        physician_id=physician_id,
                        episode_id=episode_id,
                        order_id=order_id,
                        document_id=document_id,
                    )
                )
            except Exception as error:
                results.append(
                    SyncResult(
                        order_number=record.order_number,
                        patient_name=record.patient_name,
                        status="failed",
                        reason=str(error),
                    )
                )

        self._post_run_summary(results)
        return results

    def results_to_dataframe(self, results: List[SyncResult]) -> pd.DataFrame:
        """Serialize sync results for CSV/Excel export."""
        return pd.DataFrame([result.to_dict() for result in results])

    def _upsert_physician(self, record: OrderRecord) -> Optional[str]:
        npi = normalize_text(record.npi)
        if not npi:
            return None

        existing = self._lookup_physician(npi)
        if existing:
            return self._entity_id(existing)

        payload = {
            "npi": npi,
            "name": record.metadata.get("physician_standardized_name") or record.physician_name,
            "specialty": record.metadata.get("physician_specialty", ""),
            "address": record.metadata.get("physician_address", ""),
            "source": "kinnser",
        }
        response = self._request("POST", "physician_create", json=payload)
        return self._entity_id(response.json())

    def _upsert_patient(self, record: OrderRecord) -> str:
        existing = self._lookup_patient(record.mrn)
        first_name, middle_name, last_name = split_patient_name(record.patient_name)
        payload = {
            "patient_name": record.patient_name,
            "first_name": first_name,
            "middle_name": middle_name,
            "last_name": last_name,
            "mrn": record.mrn,
            "dob": record.dob,
            "phone": record.phone,
            "source": "kinnser",
        }

        if existing:
            patient_id = self._entity_id(existing)
            self._request("PUT", "patient_update", endpoint_kwargs={"id": patient_id}, json=payload)
            return patient_id

        response = self._request("POST", "patient_create", json=payload)
        return self._entity_id(response.json())

    def _upsert_episode(self, record: OrderRecord, patient_id: str) -> str:
        existing = self._lookup_episode(patient_id, record.episode)
        if existing:
            return self._entity_id(existing)

        payload = {
            "patient_id": patient_id,
            "episode_identifier": record.episode,
            "status": "active",
            "source": "kinnser",
        }
        response = self._request("POST", "episode_create", json=payload)
        return self._entity_id(response.json())

    def _upsert_order(
        self,
        record: OrderRecord,
        patient_id: str,
        episode_id: str,
        physician_id: Optional[str],
    ) -> str:
        existing = self._lookup_order(record.order_number)
        payload = {
            "order_number": record.order_number,
            "patient_id": patient_id,
            "episode_id": episode_id,
            "physician_id": physician_id,
            "order_type": record.order_type,
            "order_date": record.order_date,
            "clinic": record.clinic,
            "source": "kinnser",
        }

        if existing:
            order_id = self._entity_id(existing)
            self._request("PUT", "order_update", endpoint_kwargs={"id": order_id}, json=payload)
            return order_id

        response = self._request("POST", "order_create", json=payload)
        return self._entity_id(response.json())

    def _upload_document(self, record: OrderRecord, order_id: str) -> Optional[str]:
        if not record.pdf_path:
            return None

        payload = {
            "order_id": order_id,
            "order_number": record.order_number,
            "patient_name": record.patient_name,
            "physician": record.physician_name,
            "document_base64": pdf_to_base64(record.pdf_path),
            "metadata": {
                "source": "kinnser",
                **record.metadata,
            },
        }
        response = self._request("POST", "documents_upload", json=payload)
        return self._entity_id(response.json())

    def _lookup_physician(self, npi: str) -> Optional[Dict[str, Any]]:
        try:
            response = self._request("GET", "physician_lookup", endpoint_kwargs={"npi": npi})
            return self._first_item(response.json())
        except Exception:
            return None

    def _lookup_patient(self, mrn: str) -> Optional[Dict[str, Any]]:
        if not normalize_text(mrn):
            return None
        response = self._request("GET", "patients_list", params={"mrn": mrn})
        return self._first_item(response.json())

    def _lookup_episode(self, patient_id: str, episode_identifier: str) -> Optional[Dict[str, Any]]:
        response = self._request(
            "GET",
            "episodes_list",
            params={"patient_id": patient_id, "episode_identifier": episode_identifier},
        )
        return self._first_item(response.json())

    def _lookup_order(self, order_number: str) -> Optional[Dict[str, Any]]:
        response = self._request("GET", "orders_list", params={"order_number": order_number})
        return self._first_item(response.json())

    def _post_run_summary(self, results: List[SyncResult]) -> None:
        success_count = len([result for result in results if result.status == "success"])
        failed_count = len([result for result in results if result.status == "failed"])
        total = len(results)

        payload = {
            "run_time": pd.Timestamp.utcnow().isoformat(),
            "total": total,
            "success": success_count,
            "failed": failed_count,
            "source": "kinnser",
        }

        # These are optional telemetry endpoints. Failures should not break the core run.
        for endpoint in ["sync_run", "metrics"]:
            try:
                self._request("POST", endpoint, json=payload)
            except Exception as error:
                self.logger.warning("Unable to post %s payload: %s", endpoint, error)

    def _request(
        self,
        method: str,
        endpoint_key: str,
        *,
        endpoint_kwargs: Optional[Dict[str, Any]] = None,
        use_auth: bool = True,
        **kwargs: Any,
    ) -> requests.Response:
        endpoint_kwargs = endpoint_kwargs or {}
        path_template = self.endpoints[endpoint_key]
        path = path_template.format(**endpoint_kwargs)
        url = f"{self.base_url}{path}"

        headers = dict(kwargs.pop("headers", {}))
        if self.api_key:
            headers.setdefault("X-SERVICE-KEY", self.api_key)
            headers.setdefault("x-service-key", self.api_key)
            headers.setdefault("X-API-KEY", self.api_key)
        if use_auth and self.token:
            headers["Authorization"] = f"Bearer {self.token}"
        headers.setdefault("Content-Type", "application/json")

        return request_with_retries(
            self.session,
            method,
            url,
            self.logger,
            retries=self.max_retries,
            backoff_seconds=self.retry_backoff,
            timeout=self.timeout,
            headers=headers,
            **kwargs,
        )

    @staticmethod
    def _first_item(payload: Any) -> Optional[Dict[str, Any]]:
        if isinstance(payload, list):
            return payload[0] if payload else None

        if isinstance(payload, dict):
            for key in ["data", "items", "results", "value"]:
                data = payload.get(key)
                if isinstance(data, list):
                    return data[0] if data else None
                if isinstance(data, dict):
                    return data
            return payload

        return None

    @staticmethod
    def _entity_id(payload: Any) -> str:
        entity = MediSyncClient._first_item(payload)
        if not entity:
            raise RuntimeError(f"Entity id not found in response: {payload}")

        for key in ["id", "_id", "patient_id", "episode_id", "order_id", "document_id"]:
            if entity.get(key):
                return str(entity[key])
        raise RuntimeError(f"Entity id field missing in response: {payload}")
