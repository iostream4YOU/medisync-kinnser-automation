"""Dashboard data source backed by Firebase Data Connect."""

from __future__ import annotations

import base64
import json
import re
import time
from datetime import date, datetime, timezone
from typing import Any, Dict, List, Optional, Tuple

from src.dataconnect_client import DataConnectClient
from src.utils import normalize_text


class DataConnectDashboardStore:
    """Read-only gateway serving dashboard payloads from Data Connect."""

    PLACEHOLDER_VALUES = {
        "n/a",
        "na",
        "none",
        "null",
        "unknown",
        "not available",
        "not entered",
        "-",
        "--",
        "n\\a",
    }

    def __init__(self, config: Dict[str, Any]):
        self.config = config
        dashboard_cfg = config.get("dashboard", {})

        self.cache_seconds = max(3, int(dashboard_cfg.get("cache_seconds", 15)))
        self._cache_payload: Optional[Dict[str, Any]] = None
        self._cache_loaded_at: float = 0.0
        self._cached_error: str = ""

        self.client = DataConnectClient(config, logger=_NullLogger())
        self.agency_id = self.client.agency_id
        self.agency_name = self.client.agency_name

        self._connect()

    def _connect(self) -> None:
        try:
            if not self.client.enabled:
                self._cached_error = "Data Connect dashboard mode is disabled"
                return
            self.client.ping()
            self._cached_error = ""
        except Exception as error:
            self._cached_error = f"Unable to connect to Data Connect: {error}"

    def get_payload(self, *, force_refresh: bool = False) -> Dict[str, Any]:
        if self._cached_error and not force_refresh:
            return self._error_payload(self._cached_error)

        now = time.time()
        if (
            not force_refresh
            and self._cache_payload is not None
            and (now - self._cache_loaded_at) <= self.cache_seconds
        ):
            return self._cache_payload

        try:
            payload = self._build_payload()
            self._cache_payload = payload
            self._cache_loaded_at = now
            self._cached_error = ""
            return payload
        except Exception as error:
            self._cached_error = f"Dashboard data read failed: {error}"
            return self._error_payload(self._cached_error)

    def _build_payload(self) -> Dict[str, Any]:
        result = self.client.execute("GetDashboardData", {"agencyId": self.agency_id})
        data = result.get("data", {}) if isinstance(result, dict) else {}

        agencies = data.get("agencies", []) if isinstance(data.get("agencies"), list) else []
        agency = agencies[0] if agencies else {}
        agency_name = normalize_text(agency.get("name")) or self.agency_name

        patient_docs = data.get("patients", []) if isinstance(data.get("patients"), list) else []
        physician_docs = data.get("physicians", []) if isinstance(data.get("physicians"), list) else []
        order_docs = data.get("orders", []) if isinstance(data.get("orders"), list) else []
        order_document_docs = (
            data.get("orderDocuments", []) if isinstance(data.get("orderDocuments"), list) else []
        )
        run_docs = data.get("importRuns", []) if isinstance(data.get("importRuns"), list) else []

        physicians_by_id = {
            normalize_text(item.get("id")): item
            for item in physician_docs
            if normalize_text(item.get("id"))
        }

        order_documents_by_order_id: Dict[str, Dict[str, Any]] = {}
        for document in order_document_docs:
            order_id = normalize_text(document.get("orderId"))
            if not order_id:
                continue
            current = order_documents_by_order_id.get(order_id)
            if current is None:
                order_documents_by_order_id[order_id] = document
                continue

            current_updated = self._date_sort_key(current.get("updatedAt"))
            next_updated = self._date_sort_key(document.get("updatedAt"))
            if next_updated >= current_updated:
                order_documents_by_order_id[order_id] = document

        patients_by_id: Dict[str, Dict[str, Any]] = {}
        for patient in patient_docs:
            patient_id = normalize_text(patient.get("id"))
            if not patient_id:
                continue

            patient_name = self._clean_field_value(patient.get("name"))
            patient_mrn = self._clean_field_value(patient.get("mrn"))
            if not patient_name and patient_mrn:
                patient_name = f"Patient {patient_mrn}"

            patients_by_id[patient_id] = {
                "id": patient_id,
                "name": patient_name,
                "mrn": patient_mrn,
                "dob": normalize_text(patient.get("dob")),
                "gender": normalize_text(patient.get("gender")),
                "email": normalize_text(patient.get("email")),
                "ssn": normalize_text(patient.get("ssn")),
                "phone": normalize_text(patient.get("phone")),
                "episode": normalize_text(patient.get("episode")),
                "address": normalize_text(patient.get("address")),
                "city": normalize_text(patient.get("city")),
                "state": normalize_text(patient.get("state")),
                "zip": normalize_text(patient.get("zip")),
                "maritalStatus": normalize_text(patient.get("maritalStatus")),
                "primaryLanguage": normalize_text(patient.get("primaryLanguage")),
                "insurance": normalize_text(patient.get("insurance")),
                "emergencyContact": normalize_text(patient.get("emergencyContact")),
                "allergies": normalize_text(patient.get("allergies")),
                "primaryPhysicianName": normalize_text(patient.get("primaryPhysicianName")),
                "diagnoses": normalize_text(patient.get("diagnoses")),
                "diagnosisCodes": normalize_text(patient.get("diagnosisCodes")),
                "profileUrl": normalize_text(patient.get("profileUrl")),
                "profileExtractedAt": self._as_iso(patient.get("profileExtractedAt"))
                or normalize_text(patient.get("profileExtractedAt")),
                "profileDataPath": normalize_text(patient.get("profileDataPath")),
                "profileHtmlPath": normalize_text(patient.get("profileHtmlPath")),
                "profilePairs": self._parse_profile_pairs_json(patient.get("profilePairsJson")),
                "profileTextPreview": normalize_text(patient.get("profileTextPreview")),
                "createdAt": self._as_iso(patient.get("createdAt")),
                "updatedAt": self._as_iso(patient.get("updatedAt")),
                "orders": [],
            }

        for order in order_docs:
            patient_id = normalize_text(order.get("patientId"))
            if not patient_id:
                continue

            if patient_id not in patients_by_id:
                patients_by_id[patient_id] = {
                    "id": patient_id,
                    "name": "Patient",
                    "mrn": "",
                    "dob": "",
                    "gender": "",
                    "email": "",
                    "ssn": "",
                    "phone": "",
                    "episode": "",
                    "address": "",
                    "city": "",
                    "state": "",
                    "zip": "",
                    "maritalStatus": "",
                    "primaryLanguage": "",
                    "insurance": "",
                    "emergencyContact": "",
                    "allergies": "",
                    "primaryPhysicianName": "",
                    "diagnoses": "",
                    "diagnosisCodes": "",
                    "profileUrl": "",
                    "profileExtractedAt": "",
                    "profileDataPath": "",
                    "profileHtmlPath": "",
                    "profilePairs": [],
                    "profileTextPreview": "",
                    "createdAt": "",
                    "updatedAt": "",
                    "orders": [],
                }

            order_id = normalize_text(order.get("id"))
            physician_id = normalize_text(order.get("physicianId"))
            physician_name = normalize_text(order.get("physicianName"))
            if not physician_name and physician_id and physician_id in physicians_by_id:
                physician_name = normalize_text(physicians_by_id[physician_id].get("name"))

            db_document = order_documents_by_order_id.get(order_id)
            has_db_document = bool(db_document and bool(db_document.get("hasContent", True)))
            document_size = int(db_document.get("sizeBytes") or 0) if isinstance(db_document, dict) else 0

            order_row = {
                "id": order_id,
                "orderNumber": normalize_text(order.get("orderNumber")),
                "orderType": normalize_text(order.get("orderType")),
                "orderDate": normalize_text(order.get("orderedDate")),
                "status": normalize_text(order.get("status")),
                "version": int(order.get("version") or 1),
                "isCurrent": bool(order.get("isCurrent", True)),
                "physicianName": physician_name,
                "documentName": normalize_text(order.get("documentName"))
                or normalize_text(order.get("orderType"))
                or "Order Document",
                "documentLink": normalize_text(order.get("documentLink")),
                "pdfPath": normalize_text(order.get("pdfPath")),
                "hasLocalDocument": has_db_document,
                "hasDatabaseDocument": has_db_document,
                "documentStorage": "database" if has_db_document else "none",
                "documentSizeBytes": document_size,
                "documentViewUrl": f"/api/orders/{order_id}/document" if has_db_document else "",
                "documentDownloadUrl": (
                    f"/api/orders/{order_id}/document?download=true" if has_db_document else ""
                ),
                "sourceFileName": normalize_text(order.get("sourceFileName")),
                "sourceRowNumber": int(order.get("sourceRowNumber") or 0),
                "updatedAt": self._as_iso(order.get("updatedAt")),
                "createdAt": self._as_iso(order.get("createdAt")),
            }
            self._clean_order_record_in_place(order_row)
            patients_by_id[patient_id]["orders"].append(order_row)

        patients: List[Dict[str, Any]] = []
        for patient in patients_by_id.values():
            patient_orders = patient.get("orders", [])
            for order in patient_orders:
                self._clean_order_record_in_place(order)
            self._clean_patient_record_in_place(patient)

            patient_orders.sort(
                key=lambda item: self._date_sort_key(item.get("updatedAt") or item.get("createdAt")),
                reverse=True,
            )

            if not normalize_text(patient.get("primaryPhysicianName")):
                for order in patient_orders:
                    physician_name = normalize_text(order.get("physicianName"))
                    if physician_name:
                        patient["primaryPhysicianName"] = physician_name
                        break

            doc_count = sum(1 for order in patient_orders if order.get("hasLocalDocument"))
            latest_update = ""
            if patient_orders:
                latest_update = patient_orders[0].get("updatedAt") or patient_orders[0].get("createdAt") or ""
            elif patient.get("profileExtractedAt"):
                latest_update = patient.get("profileExtractedAt")
            elif patient.get("updatedAt"):
                latest_update = patient.get("updatedAt")

            patient["orderCount"] = len(patient_orders)
            patient["documentCount"] = doc_count
            patient["latestUpdate"] = latest_update
            patient["hasDocuments"] = doc_count > 0
            patient["initial"] = (patient.get("name", "")[:1] or "").upper()
            patients.append(patient)

        patients.sort(key=lambda item: (normalize_text(item.get("name")).upper(), normalize_text(item.get("mrn"))))

        recent_runs = [self._serialize_run(run_doc) for run_doc in run_docs][:10]
        recent_runs.sort(key=lambda item: self._date_sort_key(item.get("runAt")), reverse=True)
        last_run_at = recent_runs[0].get("runAt") if recent_runs else ""

        total_orders = sum(patient["orderCount"] for patient in patients)
        total_documents = sum(patient["documentCount"] for patient in patients)
        patients_with_documents = sum(1 for patient in patients if patient["documentCount"] > 0)

        return {
            "connected": True,
            "sourceMode": "database",
            "database": "firebase-dataconnect",
            "agencyId": self.agency_id,
            "agencyName": agency_name,
            "generatedAt": self._as_iso(datetime.now(timezone.utc)),
            "summary": {
                "totalPatients": len(patients),
                "totalOrders": total_orders,
                "totalDocuments": total_documents,
                "patientsWithDocuments": patients_with_documents,
                "patientsWithoutDocuments": len(patients) - patients_with_documents,
                "lastRunAt": last_run_at,
            },
            "patients": patients,
            "recentRuns": recent_runs,
        }

    def resolve_document_content_for_order(self, order_id: str) -> Tuple[Optional[bytes], str, str]:
        oid = normalize_text(order_id)
        if not oid:
            return None, "Invalid order id", ""

        result = self.client.execute("GetOrderDocumentContent", {"orderId": oid})
        data = result.get("data", {}) if isinstance(result, dict) else {}
        docs = data.get("orderDocuments", []) if isinstance(data.get("orderDocuments"), list) else []

        if not docs:
            return None, "Document is not stored in database for this order", self._safe_pdf_filename("", oid)

        document_doc = docs[0]
        if not bool(document_doc.get("hasContent", True)):
            return None, "Document content is not available in database", self._safe_pdf_filename("", oid)

        encoded_content = normalize_text(document_doc.get("contentBase64"))
        if not encoded_content:
            return None, "Document content is empty in database", self._safe_pdf_filename("", oid)

        try:
            content = base64.b64decode(encoded_content, validate=True)
        except Exception:
            return None, "Document content in database is invalid", self._safe_pdf_filename("", oid)

        if not content:
            return None, "Document content is empty in database", self._safe_pdf_filename("", oid)

        fallback_name = normalize_text(document_doc.get("documentName")) or normalize_text(document_doc.get("orderNumber"))
        file_name = self._safe_pdf_filename(normalize_text(document_doc.get("fileName")) or fallback_name, oid)
        return content, "ok", file_name

    def filter_patients(
        self,
        payload: Dict[str, Any],
        *,
        search: str = "",
        initial: str = "",
        document_filter: str = "all",
        sort_by: str = "name",
    ) -> Dict[str, Any]:
        filtered = list(payload.get("patients", []))

        initial = normalize_text(initial).upper()
        if initial and initial != "ALL":
            filtered = [
                patient
                for patient in filtered
                if normalize_text(patient.get("name", "")).upper().startswith(initial)
            ]

        search_text = normalize_text(search).lower()
        if search_text:
            next_rows = []
            for patient in filtered:
                haystack = [
                    normalize_text(patient.get("name")),
                    normalize_text(patient.get("mrn")),
                    normalize_text(patient.get("dob")),
                    normalize_text(patient.get("gender")),
                    normalize_text(patient.get("email")),
                    normalize_text(patient.get("ssn")),
                    normalize_text(patient.get("phone")),
                    normalize_text(patient.get("episode")),
                    normalize_text(patient.get("maritalStatus")),
                    normalize_text(patient.get("primaryLanguage")),
                    normalize_text(patient.get("insurance")),
                    normalize_text(patient.get("emergencyContact")),
                    normalize_text(patient.get("allergies")),
                    normalize_text(patient.get("primaryPhysicianName")),
                    normalize_text(patient.get("diagnoses")),
                    normalize_text(patient.get("diagnosisCodes")),
                    normalize_text(patient.get("address")),
                    normalize_text(patient.get("city")),
                    normalize_text(patient.get("state")),
                    normalize_text(patient.get("zip")),
                    normalize_text(patient.get("profileTextPreview")),
                ]
                for pair in patient.get("profilePairs", []):
                    if not isinstance(pair, dict):
                        continue
                    haystack.append(normalize_text(pair.get("label")))
                    haystack.append(normalize_text(pair.get("value")))
                for order in patient.get("orders", []):
                    haystack.extend(
                        [
                            normalize_text(order.get("orderNumber")),
                            normalize_text(order.get("orderType")),
                            normalize_text(order.get("documentName")),
                            normalize_text(order.get("physicianName")),
                        ]
                    )
                combined = " ".join(haystack).lower()
                if search_text in combined:
                    next_rows.append(patient)
            filtered = next_rows

        if document_filter == "with-docs":
            filtered = [patient for patient in filtered if int(patient.get("documentCount", 0)) > 0]
        elif document_filter == "without-docs":
            filtered = [patient for patient in filtered if int(patient.get("documentCount", 0)) == 0]

        if sort_by == "orders":
            filtered.sort(key=lambda item: int(item.get("orderCount", 0)), reverse=True)
        elif sort_by == "documents":
            filtered.sort(key=lambda item: int(item.get("documentCount", 0)), reverse=True)
        elif sort_by == "recent":
            filtered.sort(key=lambda item: self._date_sort_key(item.get("latestUpdate")), reverse=True)
        else:
            filtered.sort(key=lambda item: normalize_text(item.get("name")).upper())

        filtered_summary = {
            "visiblePatients": len(filtered),
            "visibleOrders": sum(int(patient.get("orderCount", 0)) for patient in filtered),
            "visibleDocuments": sum(int(patient.get("documentCount", 0)) for patient in filtered),
        }

        response = dict(payload)
        response["patients"] = filtered
        response["filteredSummary"] = filtered_summary
        return response

    @staticmethod
    def _safe_pdf_filename(name: str, order_id: str) -> str:
        cleaned = normalize_text(name)
        if not cleaned:
            cleaned = f"order-{order_id}"
        cleaned = re.sub(r"[^A-Za-z0-9._-]+", "_", cleaned).strip("._")
        if not cleaned:
            cleaned = f"order-{order_id}"
        if not cleaned.lower().endswith(".pdf"):
            cleaned = f"{cleaned}.pdf"
        return cleaned

    @staticmethod
    def _parse_profile_pairs_json(raw_value: Any, limit: int = 300) -> List[Dict[str, str]]:
        text = normalize_text(raw_value)
        if not text:
            return []

        try:
            loaded = json.loads(text)
        except Exception:
            return []

        if not isinstance(loaded, list):
            return []

        pairs: List[Dict[str, str]] = []
        seen: set[Tuple[str, str]] = set()
        for entry in loaded:
            if isinstance(entry, dict):
                label = normalize_text(entry.get("label"))
                value = normalize_text(entry.get("value"))
            elif isinstance(entry, (list, tuple)) and len(entry) >= 2:
                label = normalize_text(entry[0])
                value = normalize_text(entry[1])
            else:
                continue

            if not label or not value:
                continue

            key = (label.lower(), value.lower())
            if key in seen:
                continue
            seen.add(key)

            pairs.append({"label": label[:120], "value": value[:1000]})
            if len(pairs) >= limit:
                break

        return pairs

    @staticmethod
    def _clean_field_value(value: Any) -> str:
        text = normalize_text(value)
        if not text:
            return ""

        compact = " ".join(text.split())
        lowered = compact.lower()

        if lowered in DataConnectDashboardStore.PLACEHOLDER_VALUES:
            return ""
        if re.fullmatch(r"n\s*/\s*a", lowered):
            return ""
        if lowered in {
            "primary insurance secondary insurance tertiary insurance",
            "address phone relationship",
        }:
            return ""

        return compact

    def _clean_patient_record_in_place(self, patient: Dict[str, Any]) -> None:
        text_fields = [
            "name",
            "mrn",
            "dob",
            "gender",
            "email",
            "ssn",
            "phone",
            "episode",
            "address",
            "city",
            "state",
            "zip",
            "maritalStatus",
            "primaryLanguage",
            "insurance",
            "emergencyContact",
            "allergies",
            "primaryPhysicianName",
            "diagnoses",
            "diagnosisCodes",
            "profileUrl",
            "profileExtractedAt",
            "profileDataPath",
            "profileHtmlPath",
            "profileTextPreview",
        ]
        for key in text_fields:
            patient[key] = self._clean_field_value(patient.get(key))

        if not patient.get("name"):
            patient["name"] = "Patient"

    def _clean_order_record_in_place(self, order: Dict[str, Any]) -> None:
        text_fields = [
            "orderNumber",
            "orderType",
            "orderDate",
            "status",
            "physicianName",
            "documentName",
            "documentLink",
            "sourceFileName",
        ]
        for key in text_fields:
            order[key] = self._clean_field_value(order.get(key))

        if not order.get("documentName"):
            order["documentName"] = "Order Document"

    @staticmethod
    def _to_datetime(value: Any) -> Optional[datetime]:
        if isinstance(value, datetime):
            if value.tzinfo is None:
                return value.replace(tzinfo=timezone.utc)
            return value

        if isinstance(value, date):
            return datetime.combine(value, datetime.min.time(), tzinfo=timezone.utc)

        text = normalize_text(value)
        if not text:
            return None

        for fmt in ("%Y-%m-%dT%H:%M:%S.%f%z", "%Y-%m-%dT%H:%M:%S%z", "%m/%d/%Y", "%Y-%m-%d"):
            try:
                parsed = datetime.strptime(text, fmt)
                if parsed.tzinfo is None:
                    parsed = parsed.replace(tzinfo=timezone.utc)
                return parsed
            except ValueError:
                continue

        try:
            parsed = datetime.fromisoformat(text.replace("Z", "+00:00"))
            if parsed.tzinfo is None:
                parsed = parsed.replace(tzinfo=timezone.utc)
            return parsed
        except ValueError:
            return None

    @staticmethod
    def _date_sort_key(value: Any) -> float:
        dt_value = DataConnectDashboardStore._to_datetime(value)
        if dt_value is None:
            return 0.0
        return dt_value.timestamp()

    @staticmethod
    def _as_iso(value: Any) -> str:
        dt_value = DataConnectDashboardStore._to_datetime(value)
        if dt_value is None:
            return ""
        return dt_value.astimezone(timezone.utc).isoformat()

    def _serialize_run(self, run_doc: Dict[str, Any]) -> Dict[str, Any]:
        return {
            "id": normalize_text(run_doc.get("id")),
            "status": normalize_text(run_doc.get("status")),
            "runAt": self._as_iso(run_doc.get("runAt")),
            "finishedAt": self._as_iso(run_doc.get("finishedAt")),
            "totalRows": int(run_doc.get("totalRows") or 0),
            "successfulRows": int(run_doc.get("successfulRows") or 0),
            "failedRows": int(run_doc.get("failedRows") or 0),
            "fileName": normalize_text(run_doc.get("fileName")),
            "workflowName": normalize_text(run_doc.get("workflowName")),
            "errorMessage": normalize_text(run_doc.get("errorMessage")),
        }

    def _error_payload(self, message: str) -> Dict[str, Any]:
        return {
            "connected": False,
            "sourceMode": "none",
            "database": "firebase-dataconnect",
            "agencyId": self.agency_id,
            "agencyName": self.agency_name,
            "generatedAt": self._as_iso(datetime.now(timezone.utc)),
            "error": normalize_text(message),
            "summary": {
                "totalPatients": 0,
                "totalOrders": 0,
                "totalDocuments": 0,
                "patientsWithDocuments": 0,
                "patientsWithoutDocuments": 0,
                "lastRunAt": "",
            },
            "filteredSummary": {
                "visiblePatients": 0,
                "visibleOrders": 0,
                "visibleDocuments": 0,
            },
            "patients": [],
            "recentRuns": [],
        }


class _NullLogger:
    def warning(self, *_args, **_kwargs):
        return None
