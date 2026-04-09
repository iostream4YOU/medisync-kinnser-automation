"""Interactive dashboard backend for MediSync extraction outputs."""

from __future__ import annotations

import base64
import hashlib
import os
import re
import time
from datetime import date, datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple
from urllib.parse import urlparse

from bson import ObjectId
from fastapi import FastAPI, Query
from fastapi.responses import FileResponse, JSONResponse, Response
from fastapi.staticfiles import StaticFiles
import pandas as pd
from pymongo import DESCENDING, MongoClient

from dashboard.dataconnect_store import DataConnectDashboardStore
from src.utils import load_config, load_json, normalize_text, to_bool


MEDISYNC_ROOT = Path(__file__).resolve().parents[1]
WORKSPACE_ROOT = MEDISYNC_ROOT.parent
STATIC_DIR = Path(__file__).resolve().parent / "static"
DEFAULT_CONFIG_PATH = str(MEDISYNC_ROOT / "config.json")


class DashboardStore:
    """Read-only gateway that serves dashboard data from Firestore Mongo collections."""

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
        self.firestore_cfg = config.get("firestore", {})
        dashboard_cfg = config.get("dashboard", {})

        self.cache_seconds = max(3, int(dashboard_cfg.get("cache_seconds", 15)))
        self.allowed_document_roots = [
            WORKSPACE_ROOT.resolve(),
            MEDISYNC_ROOT.resolve(),
            (WORKSPACE_ROOT / "downloads").resolve(),
            (MEDISYNC_ROOT / "downloads").resolve(),
        ]

        self._cache_payload: Optional[Dict[str, Any]] = None
        self._cache_loaded_at: float = 0.0
        self._cached_error: str = ""
        self._local_order_files: Dict[str, Path] = {}
        self._profile_artifact_cache: Dict[str, Dict[str, Any]] = {}

        self.mongo_uri = self._resolve_mongo_uri()
        self.database_name = self._resolve_database_name()

        self.client: Optional[MongoClient] = None
        self.db = None
        self.col_agency = None
        self.col_patient = None
        self.col_physician = None
        self.col_order = None
        self.col_order_document = None
        self.col_import_run = None

        self._agency_id: str = ""
        self._agency_name: str = ""

        self._connect()

    def _resolve_mongo_uri(self) -> str:
        uri = normalize_text(self.firestore_cfg.get("mongo_uri")) or normalize_text(os.getenv("FIRESTORE_MONGO_URI"))
        return uri

    def _resolve_database_name(self) -> str:
        configured = normalize_text(self.firestore_cfg.get("database")) or normalize_text(
            os.getenv("FIRESTORE_MONGO_DATABASE")
        )
        if configured:
            return configured

        if self.mongo_uri:
            parsed = urlparse(self.mongo_uri)
            parsed_name = normalize_text(parsed.path.lstrip("/"))
            if parsed_name:
                return parsed_name
        return ""

    def _connect(self) -> None:
        if not self.mongo_uri or not self.database_name:
            missing_parts = []
            if not self.mongo_uri:
                missing_parts.append("mongo_uri")
            if not self.database_name:
                missing_parts.append("database")
            self._cached_error = (
                "Dashboard DB connection is not configured. Missing: " + ", ".join(missing_parts)
            )
            return

        try:
            timeout_ms = int(self.firestore_cfg.get("server_selection_timeout_ms", 12000))
            self.client = MongoClient(self.mongo_uri, serverSelectionTimeoutMS=timeout_ms)
            self.db = self.client[self.database_name]
            self.db.command("ping")

            self.col_agency = self.db["Agency"]
            self.col_patient = self.db["Patient"]
            self.col_physician = self.db["Physician"]
            self.col_order = self.db["Order"]
            self.col_order_document = self.db["OrderDocument"]
            self.col_import_run = self.db["ImportRun"]
            self._cached_error = ""
        except Exception as error:
            self._cached_error = f"Unable to connect to Firestore Mongo endpoint: {error}"

    def get_payload(self, *, force_refresh: bool = False) -> Dict[str, Any]:
        if self._cached_error and not self.client:
            local_payload = self._build_local_payload(connection_error=self._cached_error)
            if local_payload is not None:
                self._cache_payload = local_payload
                self._cache_loaded_at = time.time()
                return local_payload
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
            local_payload = self._build_local_payload(connection_error=self._cached_error)
            if local_payload is not None:
                self._cache_payload = local_payload
                self._cache_loaded_at = now
                return local_payload
            return self._error_payload(self._cached_error)

    def _build_payload(self) -> Dict[str, Any]:
        if not self.db:
            raise RuntimeError("Database connection is not available")

        self.db.command("ping")
        agency_id, agency_name = self._resolve_agency_context()

        order_query = {"agencyId": agency_id} if agency_id else {}
        patient_query = {"agencyId": agency_id} if agency_id else {}
        physician_query = {"agencyId": agency_id} if agency_id else {}
        run_query = {"agencyId": agency_id} if agency_id else {}

        patient_docs = list(
            self.col_patient.find(
                patient_query,
                {
                    "name": 1,
                    "mrn": 1,
                    "dob": 1,
                    "gender": 1,
                    "email": 1,
                    "ssn": 1,
                    "phone": 1,
                    "episode": 1,
                    "address": 1,
                    "city": 1,
                    "state": 1,
                    "zip": 1,
                    "maritalStatus": 1,
                    "primaryLanguage": 1,
                    "insurance": 1,
                    "emergencyContact": 1,
                    "allergies": 1,
                    "primaryPhysicianName": 1,
                    "diagnoses": 1,
                    "diagnosisCodes": 1,
                    "profileUrl": 1,
                    "profileExtractedAt": 1,
                    "profileDataPath": 1,
                    "profileHtmlPath": 1,
                    "profilePairs": 1,
                    "profileTextPreview": 1,
                    "createdAt": 1,
                    "updatedAt": 1,
                },
            )
        )
        physician_docs = list(
            self.col_physician.find(
                physician_query,
                {
                    "name": 1,
                    "npi": 1,
                },
            )
        )
        order_docs = list(
            self.col_order.find(
                order_query,
                {
                    "patientId": 1,
                    "physicianId": 1,
                    "physicianName": 1,
                    "orderNumber": 1,
                    "orderType": 1,
                    "orderedDate": 1,
                    "status": 1,
                    "documentName": 1,
                    "documentLink": 1,
                    "pdfPath": 1,
                    "sourceFileName": 1,
                    "sourceRowNumber": 1,
                    "updatedAt": 1,
                    "createdAt": 1,
                    "isCurrent": 1,
                    "version": 1,
                },
            ).sort("updatedAt", DESCENDING)
        )

        order_ids = [str(doc.get("_id")) for doc in order_docs if doc.get("_id") is not None]
        order_documents_by_id: Dict[str, Dict[str, Any]] = {}
        if order_ids and self.col_order_document is not None:
            document_docs = list(
                self.col_order_document.find(
                    {"orderId": {"$in": order_ids}},
                    {
                        "orderId": 1,
                        "fileName": 1,
                        "contentType": 1,
                        "sizeBytes": 1,
                        "checksum": 1,
                        "hasContent": 1,
                        "updatedAt": 1,
                    },
                )
            )
            for document in document_docs:
                oid = normalize_text(document.get("orderId"))
                if oid:
                    order_documents_by_id[oid] = document

        run_docs = list(
            self.col_import_run.find(
                run_query,
                {
                    "status": 1,
                    "runAt": 1,
                    "finishedAt": 1,
                    "totalRows": 1,
                    "successfulRows": 1,
                    "failedRows": 1,
                    "fileName": 1,
                    "workflowName": 1,
                    "errorMessage": 1,
                },
            )
            .sort("runAt", DESCENDING)
            .limit(10)
        )

        physicians_by_id = {str(item.get("_id")): item for item in physician_docs}

        patients_by_id: Dict[str, Dict[str, Any]] = {}
        for patient in patient_docs:
            patient_id = str(patient.get("_id"))
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
                "profilePairs": self._normalize_profile_pairs(patient.get("profilePairs")),
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
                fallback_name = self._clean_field_value(order.get("patientName"))
                fallback_mrn = self._clean_field_value(order.get("mrn"))
                if not fallback_name and fallback_mrn:
                    fallback_name = f"Patient {fallback_mrn}"
                patients_by_id[patient_id] = {
                    "id": patient_id,
                    "name": fallback_name,
                    "mrn": fallback_mrn,
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

            order_id = str(order.get("_id"))
            physician_id = normalize_text(order.get("physicianId"))
            physician_name = normalize_text(order.get("physicianName"))
            if not physician_name and physician_id and physician_id in physicians_by_id:
                physician_name = normalize_text(physicians_by_id[physician_id].get("name"))

            document_info = self._document_info(
                order_id,
                normalize_text(order.get("pdfPath")),
                order_documents_by_id.get(order_id),
            )
            document_exists = bool(document_info.get("exists", False))

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
                "pdfPath": document_info.get("relativePath", ""),
                "hasLocalDocument": document_exists,
                "hasDatabaseDocument": bool(document_exists and document_info.get("storageMode") == "database"),
                "documentStorage": normalize_text(document_info.get("storageMode")),
                "documentSizeBytes": int(document_info.get("sizeBytes") or 0),
                "documentViewUrl": f"/api/orders/{order_id}/document" if document_exists else "",
                "documentDownloadUrl": (
                    f"/api/orders/{order_id}/document?download=true" if document_exists else ""
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

        total_orders = sum(patient["orderCount"] for patient in patients)
        total_documents = sum(patient["documentCount"] for patient in patients)
        patients_with_documents = sum(1 for patient in patients if patient["documentCount"] > 0)

        recent_runs = [self._serialize_run(run_doc) for run_doc in run_docs]
        last_run_at = recent_runs[0].get("runAt") if recent_runs else ""

        return {
            "connected": True,
            "sourceMode": "database",
            "database": self.database_name,
            "agencyId": agency_id,
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

    def _build_local_payload(self, *, connection_error: str = "") -> Optional[Dict[str, Any]]:
        source_file = self._latest_data_file()
        if source_file is None:
            return None

        dataframe = self._read_dataframe(source_file)
        if dataframe is None or dataframe.empty:
            return None

        normalized_enrichment = self._load_normalized_enrichment_map()

        self._local_order_files = {}
        patients_by_id: Dict[str, Dict[str, Any]] = {}

        for idx, row in dataframe.iterrows():
            row_data = row.to_dict()

            patient_name = self._pick_cell(row_data, ["patient_name", "Patient Name", "Patient", "name"])
            if not patient_name:
                continue

            mrn = self._pick_cell(row_data, ["mrn", "MRN", "Medical Record Number", "Medical Record"])
            dob = self._pick_cell(row_data, ["dob", "DOB", "Date of Birth"])
            gender = self._pick_cell(row_data, ["gender", "Gender", "Sex"])
            email = self._pick_cell(row_data, ["email", "Email", "E-mail"])
            ssn = self._pick_cell(row_data, ["ssn", "SSN", "Social Security", "Social Security Number"])
            phone = self._pick_cell(row_data, ["phone", "Phone", "Patient Phone", "Contact"])
            episode = self._pick_cell(row_data, ["episode", "Episode", "Episode Number", "Episode ID"])
            address = self._pick_cell(row_data, ["address", "Address", "Street", "Street Address"])
            city = self._pick_cell(row_data, ["city", "City"])
            state = self._pick_cell(row_data, ["state", "State"])
            zip_code = self._pick_cell(row_data, ["zip", "Zip", "ZIP", "Postal", "Postal Code"])
            marital_status = self._pick_cell(row_data, ["marital_status", "Marital Status"])
            primary_language = self._pick_cell(row_data, ["primary_language", "Primary Language", "Language"])
            insurance = self._pick_cell(row_data, ["insurance", "Insurance", "Payer", "Primary Insurance"])
            emergency_contact = self._pick_cell(
                row_data,
                ["emergency_contact", "Emergency Contact", "Responsible Party"],
            )
            allergies = self._pick_cell(row_data, ["allergies", "Allergies", "Allergy"])
            profile_physician = self._pick_cell(
                row_data,
                ["primary_physician", "Primary Physician", "physician_name", "Physician", "Physician Name"],
            )
            diagnoses = self._pick_cell(
                row_data,
                ["diagnoses", "Diagnoses", "diagnosis", "Diagnosis", "Primary Diagnosis"],
            )
            diagnosis_codes = self._pick_cell(row_data, ["diagnosis_codes", "Diagnosis Codes"])
            profile_url = self._pick_cell(row_data, ["profile_url", "Profile URL"])
            profile_extracted_at = self._pick_cell(
                row_data,
                ["profile_extracted_at", "Profile Extracted At"],
            )
            profile_data_path = self._pick_cell(row_data, ["profile_data_path", "Profile Data Path"])
            profile_html_path = self._pick_cell(row_data, ["profile_html_path", "Profile HTML Path"])
            profile_text_preview = self._pick_cell(row_data, ["profile_text_preview", "Profile Text Preview"])
            profile_artifact = self._load_profile_artifact(profile_data_path)

            profile_pairs = self._normalize_profile_pairs(profile_artifact.get("profile_pairs"))
            artifact_text = normalize_text(profile_artifact.get("profile_text"))
            if not profile_text_preview and artifact_text:
                profile_text_preview = artifact_text[:6000] + (" ..." if len(artifact_text) > 6000 else "")

            parsed_fields = profile_artifact.get("parsed_fields") if isinstance(profile_artifact, dict) else {}
            if isinstance(parsed_fields, dict):
                dob = dob or normalize_text(parsed_fields.get("dob"))
                gender = gender or normalize_text(parsed_fields.get("gender"))
                email = email or normalize_text(parsed_fields.get("email"))
                ssn = ssn or normalize_text(parsed_fields.get("ssn"))
                phone = phone or normalize_text(parsed_fields.get("phone"))
                episode = episode or normalize_text(parsed_fields.get("episode"))
                address = address or normalize_text(parsed_fields.get("address"))
                city = city or normalize_text(parsed_fields.get("city"))
                state = state or normalize_text(parsed_fields.get("state"))
                zip_code = zip_code or normalize_text(parsed_fields.get("zip"))
                marital_status = marital_status or normalize_text(parsed_fields.get("marital_status"))
                primary_language = primary_language or normalize_text(parsed_fields.get("primary_language"))
                insurance = insurance or normalize_text(parsed_fields.get("insurance"))
                emergency_contact = emergency_contact or normalize_text(parsed_fields.get("emergency_contact"))
                allergies = allergies or normalize_text(parsed_fields.get("allergies"))
                profile_physician = profile_physician or normalize_text(parsed_fields.get("primary_physician"))
                diagnoses = diagnoses or normalize_text(parsed_fields.get("diagnoses"))
                diagnosis_codes = diagnosis_codes or normalize_text(parsed_fields.get("diagnosis_codes"))

            patient_id = self._stable_local_id("patient", f"{patient_name}|{mrn}")
            if patient_id not in patients_by_id:
                patients_by_id[patient_id] = {
                    "id": patient_id,
                    "name": patient_name,
                    "mrn": mrn,
                    "dob": dob,
                    "gender": gender,
                    "email": email,
                    "ssn": ssn,
                    "phone": phone,
                    "episode": episode,
                    "address": address,
                    "city": city,
                    "state": state,
                    "zip": zip_code,
                    "maritalStatus": marital_status,
                    "primaryLanguage": primary_language,
                    "insurance": insurance,
                    "emergencyContact": emergency_contact,
                    "allergies": allergies,
                    "primaryPhysicianName": profile_physician,
                    "diagnoses": diagnoses,
                    "diagnosisCodes": diagnosis_codes,
                    "profileUrl": profile_url,
                    "profileExtractedAt": profile_extracted_at,
                    "profileDataPath": profile_data_path,
                    "profileHtmlPath": profile_html_path,
                    "profilePairs": profile_pairs,
                    "profileTextPreview": profile_text_preview,
                    "createdAt": "",
                    "updatedAt": "",
                    "orders": [],
                }

            order_number = self._pick_cell(row_data, ["order_number", "Order #", "Order Number", "Order No"])
            order_type = self._pick_cell(row_data, ["order_type", "Order Type", "Document Name", "Doc Type"])
            order_date = self._pick_cell(row_data, ["order_date", "Order Date", "Date"])
            physician_name = self._pick_cell(
                row_data,
                ["physician_name", "Physician", "Physician Name", "assigned_to", "Assigned To"],
            )
            if self._looks_like_non_physician_text(physician_name):
                physician_name = ""
            status = self._pick_cell(row_data, ["status", "document_status", "Document Status"])
            if not status:
                validation = self._pick_cell(row_data, ["validation_errors"])
                status = "VALID" if not validation else "REVIEW"

            document_name = self._pick_cell(
                row_data,
                ["document_name", "Document Name", "order_type", "Order Type"],
            )
            if not document_name:
                document_name = order_type or "Order Document"

            document_link = self._pick_cell(row_data, ["print_view_url", "Print View URL", "document_link"])
            pdf_path = self._pick_cell(row_data, ["pdf_path", "PDF Path", "Document Path", "File Path"])
            source_row_number = self._pick_cell(row_data, ["source_row", "Source Row"])

            if "cms_485" in normalize_text(pdf_path).lower() or "cms 485" in normalize_text(pdf_path).lower():
                document_name = "CMS 485"
                if not order_type:
                    order_type = "CMS 485"

            enrichment = self._lookup_normalized_enrichment(normalized_enrichment, patient_name, mrn, order_number)
            if enrichment:
                dob = dob or enrichment.get("dob", "")
                phone = phone or enrichment.get("phone", "")
                episode = episode or enrichment.get("episode", "")
                address = address or enrichment.get("address", "")
                city = city or enrichment.get("city", "")
                state = state or enrichment.get("state", "")
                zip_code = zip_code or enrichment.get("zip", "")
                gender = gender or enrichment.get("gender", "")
                email = email or enrichment.get("email", "")
                ssn = ssn or enrichment.get("ssn", "")
                marital_status = marital_status or enrichment.get("marital_status", "")
                primary_language = primary_language or enrichment.get("primary_language", "")
                insurance = insurance or enrichment.get("insurance", "")
                emergency_contact = emergency_contact or enrichment.get("emergency_contact", "")
                allergies = allergies or enrichment.get("allergies", "")
                profile_physician = profile_physician or enrichment.get("primary_physician", "")
                diagnoses = diagnoses or enrichment.get("diagnoses", "")
                diagnosis_codes = diagnosis_codes or enrichment.get("diagnosis_codes", "")
                profile_url = profile_url or enrichment.get("profile_url", "")
                profile_extracted_at = profile_extracted_at or enrichment.get("profile_extracted_at", "")
                profile_data_path = profile_data_path or enrichment.get("profile_data_path", "")
                profile_html_path = profile_html_path or enrichment.get("profile_html_path", "")
                profile_text_preview = profile_text_preview or enrichment.get("profile_text_preview", "")
                order_date = order_date or enrichment.get("order_date", "")
                physician_name = physician_name or enrichment.get("physician_name", "")
                if self._looks_like_non_physician_text(physician_name):
                    physician_name = ""
                order_number = order_number or enrichment.get("order_number", "")
                order_type = order_type or enrichment.get("order_type", "")
                if not document_name or normalize_text(document_name).lower() == "order document":
                    document_name = enrichment.get("document_name", "") or order_type or document_name
                document_link = document_link or enrichment.get("document_link", "")
                pdf_path = pdf_path or enrichment.get("pdf_path", "")
                source_row_number = source_row_number or enrichment.get("source_row", "")
                if (not status or status in {"VALID", "REVIEW"}) and enrichment.get("status"):
                    status = enrichment.get("status", "")

            if profile_data_path and (not profile_pairs or not profile_text_preview):
                profile_artifact = self._load_profile_artifact(profile_data_path)
                if not profile_pairs:
                    profile_pairs = self._normalize_profile_pairs(profile_artifact.get("profile_pairs"))
                if not profile_text_preview:
                    artifact_text = normalize_text(profile_artifact.get("profile_text"))
                    if artifact_text:
                        profile_text_preview = artifact_text[:6000] + (" ..." if len(artifact_text) > 6000 else "")

            if not patients_by_id[patient_id].get("dob") and dob:
                patients_by_id[patient_id]["dob"] = dob
            if not patients_by_id[patient_id].get("gender") and gender:
                patients_by_id[patient_id]["gender"] = gender
            if not patients_by_id[patient_id].get("email") and email:
                patients_by_id[patient_id]["email"] = email
            if not patients_by_id[patient_id].get("ssn") and ssn:
                patients_by_id[patient_id]["ssn"] = ssn
            if not patients_by_id[patient_id].get("phone") and phone:
                patients_by_id[patient_id]["phone"] = phone
            if not patients_by_id[patient_id].get("episode") and episode:
                patients_by_id[patient_id]["episode"] = episode
            if not patients_by_id[patient_id].get("address") and address:
                patients_by_id[patient_id]["address"] = address
            if not patients_by_id[patient_id].get("city") and city:
                patients_by_id[patient_id]["city"] = city
            if not patients_by_id[patient_id].get("state") and state:
                patients_by_id[patient_id]["state"] = state
            if not patients_by_id[patient_id].get("zip") and zip_code:
                patients_by_id[patient_id]["zip"] = zip_code
            if not patients_by_id[patient_id].get("maritalStatus") and marital_status:
                patients_by_id[patient_id]["maritalStatus"] = marital_status
            if not patients_by_id[patient_id].get("primaryLanguage") and primary_language:
                patients_by_id[patient_id]["primaryLanguage"] = primary_language
            if not patients_by_id[patient_id].get("insurance") and insurance:
                patients_by_id[patient_id]["insurance"] = insurance
            if not patients_by_id[patient_id].get("emergencyContact") and emergency_contact:
                patients_by_id[patient_id]["emergencyContact"] = emergency_contact
            if not patients_by_id[patient_id].get("allergies") and allergies:
                patients_by_id[patient_id]["allergies"] = allergies
            if not patients_by_id[patient_id].get("primaryPhysicianName") and profile_physician:
                patients_by_id[patient_id]["primaryPhysicianName"] = profile_physician
            if not patients_by_id[patient_id].get("diagnoses") and diagnoses:
                patients_by_id[patient_id]["diagnoses"] = diagnoses
            if not patients_by_id[patient_id].get("diagnosisCodes") and diagnosis_codes:
                patients_by_id[patient_id]["diagnosisCodes"] = diagnosis_codes
            if not patients_by_id[patient_id].get("profileUrl") and profile_url:
                patients_by_id[patient_id]["profileUrl"] = profile_url
            if not patients_by_id[patient_id].get("profileExtractedAt") and profile_extracted_at:
                patients_by_id[patient_id]["profileExtractedAt"] = profile_extracted_at
            if not patients_by_id[patient_id].get("profileDataPath") and profile_data_path:
                patients_by_id[patient_id]["profileDataPath"] = profile_data_path
            if not patients_by_id[patient_id].get("profileHtmlPath") and profile_html_path:
                patients_by_id[patient_id]["profileHtmlPath"] = profile_html_path
            if not patients_by_id[patient_id].get("profilePairs") and profile_pairs:
                patients_by_id[patient_id]["profilePairs"] = profile_pairs
            if not patients_by_id[patient_id].get("profileTextPreview") and profile_text_preview:
                patients_by_id[patient_id]["profileTextPreview"] = profile_text_preview

            has_order_data = any(
                [
                    normalize_text(order_number),
                    normalize_text(order_type),
                    normalize_text(order_date),
                    normalize_text(document_link),
                    normalize_text(pdf_path),
                ]
            )
            if not has_order_data:
                continue

            key_payload = f"{patient_name}|{mrn}|{order_number}|{order_type}|{idx}"
            order_id = self._stable_local_id("order", key_payload)

            document_info = self._document_info(order_id, pdf_path)
            if document_info.get("exists"):
                self._local_order_files[order_id] = Path(document_info["absolutePath"])

            order_row = {
                "id": order_id,
                "orderNumber": order_number,
                "orderType": order_type,
                "orderDate": order_date,
                "status": status,
                "version": 1,
                "isCurrent": True,
                "physicianName": physician_name,
                "documentName": document_name,
                "documentLink": document_link,
                "pdfPath": document_info.get("relativePath", ""),
                "hasLocalDocument": bool(document_info.get("exists", False)),
                "documentViewUrl": f"/api/orders/{order_id}/document" if document_info.get("exists", False) else "",
                "documentDownloadUrl": (
                    f"/api/orders/{order_id}/document?download=true" if document_info.get("exists", False) else ""
                ),
                "sourceFileName": source_file.name,
                "sourceRowNumber": int(source_row_number) if source_row_number.isdigit() else int(idx) + 2,
                "updatedAt": "",
                "createdAt": "",
            }
            self._clean_order_record_in_place(order_row)
            patients_by_id[patient_id]["orders"].append(order_row)

        if not patients_by_id:
            return None

        patients: List[Dict[str, Any]] = []
        for patient in patients_by_id.values():
            patient_orders = patient.get("orders", [])
            for order in patient_orders:
                self._clean_order_record_in_place(order)
            self._clean_patient_record_in_place(patient)
            patient_orders.sort(
                key=lambda item: (
                    normalize_text(item.get("orderDate")),
                    normalize_text(item.get("orderNumber")),
                ),
                reverse=True,
            )

            doc_count = sum(1 for order in patient_orders if order.get("hasLocalDocument"))
            patient["orderCount"] = len(patient_orders)
            patient["documentCount"] = doc_count
            patient["latestUpdate"] = normalize_text(patient.get("profileExtractedAt"))
            patient["hasDocuments"] = doc_count > 0
            patient["initial"] = (patient.get("name", "")[:1] or "").upper()
            patients.append(patient)

        patients.sort(key=lambda item: (normalize_text(item.get("name")).upper(), normalize_text(item.get("mrn"))))

        recent_runs = self._local_run_history()
        last_run_at = recent_runs[0].get("runAt") if recent_runs else ""

        total_orders = sum(patient["orderCount"] for patient in patients)
        total_documents = sum(patient["documentCount"] for patient in patients)
        patients_with_documents = sum(1 for patient in patients if patient["documentCount"] > 0)

        warning = ""
        if connection_error:
            warning = f"Firestore connection unavailable. Showing local extracted artifacts. {connection_error}"

        return {
            "connected": True,
            "sourceMode": "local-files",
            "database": self.database_name,
            "agencyId": "",
            "agencyName": normalize_text(self.firestore_cfg.get("agency_name", "MediSync DA")),
            "generatedAt": self._as_iso(datetime.now(timezone.utc)),
            "warning": warning,
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

    def _latest_data_file(self) -> Optional[Path]:
        profile_candidates: List[Path] = []
        patient_candidates: List[Path] = []
        workspace_downloads = WORKSPACE_ROOT / "downloads"
        if workspace_downloads.exists():
            profile_candidates.extend(workspace_downloads.glob("patient_profiles_*.xlsx"))
            profile_candidates.extend(workspace_downloads.glob("patient_profiles_*.csv"))
            patient_candidates.extend(workspace_downloads.glob("patient_orders_*.xlsx"))
            patient_candidates.extend(workspace_downloads.glob("patient_orders_*.csv"))

        medisync_downloads = MEDISYNC_ROOT / "downloads"
        if medisync_downloads.exists():
            profile_candidates.extend(medisync_downloads.glob("patient_profiles_*.xlsx"))
            profile_candidates.extend(medisync_downloads.glob("patient_profiles_*.csv"))
            patient_candidates.extend(medisync_downloads.glob("patient_orders_*.xlsx"))
            patient_candidates.extend(medisync_downloads.glob("patient_orders_*.csv"))

        combined_candidates = profile_candidates + patient_candidates
        if combined_candidates:
            return sorted(
                combined_candidates,
                key=lambda item: item.stat().st_mtime,
                reverse=True,
            )[0]

        if patient_candidates:
            return sorted(
                patient_candidates,
                key=lambda item: (item.stat().st_size, item.stat().st_mtime),
                reverse=True,
            )[0]

        normalized_candidates: List[Path] = []
        workspace_output = WORKSPACE_ROOT / "output"
        if workspace_output.exists():
            normalized_candidates.extend(workspace_output.glob("normalized_patient_profiles_*.xlsx"))
            normalized_candidates.extend(workspace_output.glob("normalized_patient_profiles_*.csv"))
            normalized_candidates.extend(workspace_output.glob("normalized_orders_*.xlsx"))
            normalized_candidates.extend(workspace_output.glob("normalized_orders_*.csv"))
        if normalized_candidates:
            return sorted(
                normalized_candidates,
                key=lambda item: (item.stat().st_size, item.stat().st_mtime),
                reverse=True,
            )[0]

        medisync_output = MEDISYNC_ROOT / "output"
        if medisync_output.exists():
            normalized_candidates.extend(medisync_output.glob("normalized_patient_profiles_*.xlsx"))
            normalized_candidates.extend(medisync_output.glob("normalized_patient_profiles_*.csv"))
            normalized_candidates.extend(medisync_output.glob("normalized_orders_*.xlsx"))
            normalized_candidates.extend(medisync_output.glob("normalized_orders_*.csv"))
        if normalized_candidates:
            return sorted(
                normalized_candidates,
                key=lambda item: (item.stat().st_size, item.stat().st_mtime),
                reverse=True,
            )[0]

        return None

    def _read_dataframe(self, source_file: Path) -> Optional[pd.DataFrame]:
        try:
            if source_file.suffix.lower() == ".csv":
                dataframe = pd.read_csv(source_file)
            else:
                dataframe = pd.read_excel(source_file)
            return dataframe
        except Exception:
            return None

    def _load_normalized_enrichment_map(self) -> Dict[str, Dict[str, str]]:
        order_candidates: List[Path] = []
        profile_candidates: List[Path] = []
        for folder in (WORKSPACE_ROOT / "output", MEDISYNC_ROOT / "output"):
            order_candidates.extend(folder.glob("normalized_orders_*.xlsx"))
            order_candidates.extend(folder.glob("normalized_orders_*.csv"))
            profile_candidates.extend(folder.glob("normalized_patient_profiles_*.xlsx"))
            profile_candidates.extend(folder.glob("normalized_patient_profiles_*.csv"))

        candidates = sorted(profile_candidates + order_candidates, key=lambda item: item.stat().st_mtime, reverse=True)
        if not candidates:
            return {}

        enrichment_map: Dict[str, Dict[str, str]] = {}
        fallback_to_exact: Dict[str, set[str]] = {}

        for candidate in candidates:
            dataframe = self._read_dataframe(candidate)
            if dataframe is None or dataframe.empty:
                continue

            for _, row in dataframe.iterrows():
                row_data = row.to_dict()
                patient_name = self._pick_cell(row_data, ["patient_name", "Patient Name", "Patient", "name"])
                mrn = self._pick_cell(row_data, ["mrn", "MRN", "Medical Record Number", "Medical Record"])
                order_number = self._pick_cell(row_data, ["order_number", "Order #", "Order Number", "Order No"])
                if not patient_name and not mrn:
                    continue

                record = {
                    "order_number": order_number,
                    "dob": self._pick_cell(row_data, ["dob", "DOB", "Date of Birth"]),
                    "gender": self._pick_cell(row_data, ["gender", "Gender", "Sex"]),
                    "email": self._pick_cell(row_data, ["email", "Email", "E-mail"]),
                    "ssn": self._pick_cell(row_data, ["ssn", "SSN", "Social Security", "Social Security Number"]),
                    "phone": self._pick_cell(row_data, ["phone", "Phone", "Patient Phone", "Contact"]),
                    "episode": self._pick_cell(row_data, ["episode", "Episode", "Episode Number", "Episode ID"]),
                    "address": self._pick_cell(row_data, ["address", "Address", "Street", "Street Address"]),
                    "city": self._pick_cell(row_data, ["city", "City"]),
                    "state": self._pick_cell(row_data, ["state", "State"]),
                    "zip": self._pick_cell(row_data, ["zip", "Zip", "ZIP", "Postal", "Postal Code"]),
                    "marital_status": self._pick_cell(row_data, ["marital_status", "Marital Status"]),
                    "primary_language": self._pick_cell(
                        row_data,
                        ["primary_language", "Primary Language", "Language"],
                    ),
                    "insurance": self._pick_cell(row_data, ["insurance", "Insurance", "Payer", "Primary Insurance"]),
                    "emergency_contact": self._pick_cell(
                        row_data,
                        ["emergency_contact", "Emergency Contact", "Responsible Party"],
                    ),
                    "allergies": self._pick_cell(row_data, ["allergies", "Allergies", "Allergy"]),
                    "primary_physician": self._pick_cell(
                        row_data,
                        ["primary_physician", "Primary Physician", "physician_name", "Physician", "Physician Name"],
                    ),
                    "diagnoses": self._pick_cell(
                        row_data,
                        ["diagnoses", "Diagnoses", "diagnosis", "Diagnosis", "Primary Diagnosis"],
                    ),
                    "diagnosis_codes": self._pick_cell(row_data, ["diagnosis_codes", "Diagnosis Codes"]),
                    "profile_url": self._pick_cell(row_data, ["profile_url", "Profile URL"]),
                    "profile_extracted_at": self._pick_cell(
                        row_data,
                        ["profile_extracted_at", "Profile Extracted At"],
                    ),
                    "profile_data_path": self._pick_cell(
                        row_data,
                        ["profile_data_path", "Profile Data Path"],
                    ),
                    "profile_html_path": self._pick_cell(
                        row_data,
                        ["profile_html_path", "Profile HTML Path"],
                    ),
                    "profile_text_preview": self._pick_cell(
                        row_data,
                        ["profile_text_preview", "Profile Text Preview"],
                    ),
                    "order_date": self._pick_cell(row_data, ["order_date", "Order Date", "Date"]),
                    "physician_name": self._pick_cell(row_data, ["physician_name", "Physician", "Physician Name"]),
                    "order_type": self._pick_cell(row_data, ["order_type", "Order Type"]),
                    "document_name": self._pick_cell(row_data, ["document_name", "Document Name", "order_type"]),
                    "document_link": self._pick_cell(
                        row_data,
                        ["print_view_url", "Print View URL", "document_link", "Document URL", "PDF Link"],
                    ),
                    "pdf_path": self._pick_cell(row_data, ["pdf_path", "PDF Path", "Document Path", "File Path"]),
                    "status": self._pick_cell(row_data, ["status", "document_status", "Document Status"]),
                    "source_row": self._pick_cell(row_data, ["source_row", "Source Row"]),
                }

                exact_key = self._canonical_enrichment_key(patient_name, mrn, order_number)
                fallback_key = self._canonical_enrichment_key(patient_name, mrn, "")

                enrichment_map[exact_key] = self._merge_enrichment_record(enrichment_map.get(exact_key, {}), record)
                enrichment_map[fallback_key] = self._merge_enrichment_record(
                    enrichment_map.get(fallback_key, {}),
                    record,
                )

                fallback_to_exact.setdefault(fallback_key, set()).add(exact_key)

        # Ensure order-specific rows inherit profile-level fields when those files don't carry all columns.
        for fallback_key, exact_keys in fallback_to_exact.items():
            baseline = enrichment_map.get(fallback_key, {})
            if not baseline:
                continue
            for exact_key in exact_keys:
                if exact_key == fallback_key:
                    continue
                enrichment_map[exact_key] = self._merge_enrichment_record(
                    enrichment_map.get(exact_key, {}),
                    baseline,
                )

        return enrichment_map

    @staticmethod
    def _merge_enrichment_record(existing: Dict[str, str], incoming: Dict[str, str]) -> Dict[str, str]:
        if not existing:
            return dict(incoming)

        merged = dict(existing)
        for key, value in incoming.items():
            if not DashboardStore._clean_field_value(merged.get(key)) and DashboardStore._clean_field_value(value):
                merged[key] = value
        return merged

    def _lookup_normalized_enrichment(
        self,
        enrichment_map: Dict[str, Dict[str, str]],
        patient_name: str,
        mrn: str,
        order_number: str,
    ) -> Dict[str, str]:
        if not enrichment_map:
            return {}

        exact_key = self._canonical_enrichment_key(patient_name, mrn, order_number)
        if exact_key in enrichment_map:
            return enrichment_map[exact_key]

        fallback_key = self._canonical_enrichment_key(patient_name, mrn, "")
        return enrichment_map.get(fallback_key, {})

    @staticmethod
    def _canonical_enrichment_key(patient_name: str, mrn: str, order_number: str) -> str:
        name_key = normalize_text(patient_name).lower()
        mrn_key = normalize_text(mrn).lower()
        order_key = normalize_text(order_number).lower()
        return f"{name_key}|{mrn_key}|{order_key}"

    @staticmethod
    def _looks_like_non_physician_text(value: str) -> bool:
        text = normalize_text(value).lower()
        if not text:
            return False
        bad_tokens = ["cms 485", "print view document", "physician order", "face to face", "plan of care"]
        return any(token in text for token in bad_tokens)

    def _local_run_history(self) -> List[Dict[str, Any]]:
        run_files: List[Path] = []
        for folder in (WORKSPACE_ROOT / "output", MEDISYNC_ROOT / "output"):
            run_files.extend(folder.glob("run_summary_*.json"))
        run_files = sorted(run_files, key=lambda item: item.stat().st_mtime, reverse=True)
        rows: List[Dict[str, Any]] = []

        for file_path in run_files[:10]:
            payload = load_json(file_path, default={}) or {}
            run_time = normalize_text(payload.get("run_time"))
            total_rows = int(payload.get("total_records") or payload.get("total") or 0)
            successful_rows = int(payload.get("success_count") or payload.get("valid_records") or total_rows)
            failed_rows = int(payload.get("failed_count") or payload.get("invalid_records") or 0)

            rows.append(
                {
                    "id": file_path.stem,
                    "status": normalize_text(payload.get("mode")) or "LOCAL",
                    "runAt": run_time,
                    "finishedAt": run_time,
                    "totalRows": total_rows,
                    "successfulRows": successful_rows,
                    "failedRows": failed_rows,
                    "fileName": Path(normalize_text(payload.get("normalized_output"))).name,
                    "workflowName": "MediSync Local Pipeline Run",
                    "errorMessage": "",
                }
            )

        return rows

    def _load_profile_artifact(self, raw_path: str) -> Dict[str, Any]:
        path_text = normalize_text(raw_path)
        if not path_text:
            return {}

        path = Path(path_text)
        if not path.is_absolute():
            path = (WORKSPACE_ROOT / path).resolve()

        cache_key = str(path)
        if cache_key in self._profile_artifact_cache:
            return self._profile_artifact_cache[cache_key]

        if not path.exists() or not path.is_file():
            self._profile_artifact_cache[cache_key] = {}
            return {}

        payload = load_json(path, default={})
        if not isinstance(payload, dict):
            payload = {}
        self._profile_artifact_cache[cache_key] = payload
        return payload

    @staticmethod
    def _normalize_profile_pairs(value: Any, limit: int = 300) -> List[Dict[str, str]]:
        if not isinstance(value, list):
            return []

        pairs: List[Dict[str, str]] = []
        seen: set[tuple[str, str]] = set()
        for entry in value:
            if isinstance(entry, dict):
                label = DashboardStore._clean_field_value(entry.get("label"))
                pair_value = DashboardStore._clean_field_value(entry.get("value"))
            elif isinstance(entry, (list, tuple)) and len(entry) >= 2:
                label = DashboardStore._clean_field_value(entry[0])
                pair_value = DashboardStore._clean_field_value(entry[1])
            else:
                continue

            if not label or not pair_value:
                continue

            key = (label.lower(), pair_value.lower())
            if key in seen:
                continue
            seen.add(key)

            pairs.append({"label": label[:120], "value": pair_value[:1000]})
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

        if lowered in DashboardStore.PLACEHOLDER_VALUES:
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

        patient["profilePairs"] = self._normalize_profile_pairs(patient.get("profilePairs"))

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
    def _pick_cell(row_data: Dict[str, Any], columns: List[str]) -> str:
        for column in columns:
            if column in row_data:
                value = DashboardStore._clean_field_value(row_data.get(column))
                if value:
                    return value
        return ""

    @staticmethod
    def _stable_local_id(prefix: str, payload: str) -> str:
        digest = hashlib.sha1(payload.encode("utf-8")).hexdigest()[:20]
        return f"{prefix}-{digest}"

    def _resolve_agency_context(self) -> Tuple[str, str]:
        if self._agency_id:
            return self._agency_id, self._agency_name

        configured_agency_name = normalize_text(self.firestore_cfg.get("agency_name"))

        agency_doc = None
        if configured_agency_name and self.col_agency is not None:
            agency_doc = self.col_agency.find_one({"name": configured_agency_name})

        if not agency_doc and self.col_import_run is not None:
            latest_run = self.col_import_run.find_one({}, sort=[("runAt", DESCENDING)])
            if latest_run:
                agency_id = normalize_text(latest_run.get("agencyId"))
                if agency_id and self.col_agency is not None:
                    agency_doc = self.col_agency.find_one({"_id": ObjectId(agency_id)})
                    if agency_doc is None:
                        agency_doc = {"_id": ObjectId(agency_id), "name": ""}

        if not agency_doc and self.col_agency is not None:
            agency_doc = self.col_agency.find_one({}, sort=[("updatedAt", DESCENDING)])

        if not agency_doc:
            return "", configured_agency_name or ""

        self._agency_id = str(agency_doc.get("_id"))
        self._agency_name = normalize_text(agency_doc.get("name")) or configured_agency_name or ""
        return self._agency_id, self._agency_name

    def _document_info(self, order_id: str, raw_path: str, db_document: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
        if isinstance(db_document, dict):
            has_content = bool(db_document.get("hasContent", True))
            return {
                "exists": has_content,
                "relativePath": normalize_text(raw_path),
                "absolutePath": "",
                "allowed": True,
                "orderId": order_id,
                "storageMode": "database",
                "fileName": normalize_text(db_document.get("fileName")),
                "contentType": normalize_text(db_document.get("contentType")) or "application/pdf",
                "sizeBytes": int(db_document.get("sizeBytes") or 0),
            }

        if not raw_path:
            return {
                "exists": False,
                "relativePath": "",
                "absolutePath": "",
                "allowed": False,
                "orderId": order_id,
                "storageMode": "none",
                "sizeBytes": 0,
            }

        candidate = Path(raw_path)
        if not candidate.is_absolute():
            candidate = (WORKSPACE_ROOT / candidate).resolve()

        exists = candidate.is_file()
        allowed = exists and self._is_allowed_document_path(candidate)

        relative_path = ""
        if exists:
            relative_path = self._best_relative_path(candidate)

        if not allowed:
            return {
                "exists": False,
                "relativePath": relative_path,
                "absolutePath": str(candidate),
                "allowed": False,
                "orderId": order_id,
                "storageMode": "local-files",
                "sizeBytes": int(candidate.stat().st_size) if exists else 0,
            }

        return {
            "exists": exists,
            "relativePath": relative_path,
            "absolutePath": str(candidate),
            "allowed": allowed,
            "orderId": order_id,
            "storageMode": "local-files",
            "sizeBytes": int(candidate.stat().st_size) if exists else 0,
        }

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

    def resolve_document_content_for_order(self, order_id: str) -> Tuple[Optional[bytes], str, str]:
        local_path = self._local_order_files.get(order_id)
        if local_path is None:
            payload = self.get_payload(force_refresh=False)
            if payload.get("sourceMode") == "local-files":
                local_path = self._local_order_files.get(order_id)

        if local_path is not None:
            try:
                resolved_local = local_path.resolve()
            except Exception:
                resolved_local = local_path

            if not resolved_local.is_file():
                return None, "Local document file is missing", self._safe_pdf_filename(local_path.name, order_id)
            if not self._is_allowed_document_path(resolved_local):
                return None, "Local document path is not allowed", self._safe_pdf_filename(local_path.name, order_id)

            try:
                content = resolved_local.read_bytes()
            except Exception:
                return None, "Unable to read local document file", self._safe_pdf_filename(local_path.name, order_id)

            if not content:
                return None, "Local document file is empty", self._safe_pdf_filename(local_path.name, order_id)

            return content, "ok", self._safe_pdf_filename(local_path.name, order_id)

        if not self.col_order:
            return None, "Order collection is not available", ""

        try:
            oid = ObjectId(order_id)
        except Exception:
            return None, "Invalid order id", ""

        order_doc = self.col_order.find_one({"_id": oid}, {"documentName": 1, "orderNumber": 1})
        if not order_doc:
            return None, "Order not found", ""

        fallback_name = self._safe_pdf_filename(
            normalize_text(order_doc.get("documentName")) or normalize_text(order_doc.get("orderNumber")),
            order_id,
        )

        if not self.col_order_document:
            return None, "OrderDocument collection is not available", fallback_name

        document_doc = self.col_order_document.find_one(
            {"orderId": order_id},
            {
                "contentBase64": 1,
                "hasContent": 1,
                "fileName": 1,
            },
        )
        if not document_doc:
            return None, "Document is not stored in database for this order", fallback_name
        if not bool(document_doc.get("hasContent", True)):
            return None, "Document content is not available in database", fallback_name

        encoded_content = normalize_text(document_doc.get("contentBase64"))
        if not encoded_content:
            return None, "Document content is empty in database", fallback_name

        try:
            content = base64.b64decode(encoded_content, validate=True)
        except Exception:
            return None, "Document content in database is invalid", fallback_name

        if not content:
            return None, "Document content is empty in database", fallback_name

        file_name = self._safe_pdf_filename(normalize_text(document_doc.get("fileName")), order_id)
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

    def _serialize_run(self, run_doc: Dict[str, Any]) -> Dict[str, Any]:
        return {
            "id": str(run_doc.get("_id")),
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

    def _is_allowed_document_path(self, path: Path) -> bool:
        try:
            resolved = path.resolve()
        except Exception:
            return False

        for root in self.allowed_document_roots:
            try:
                resolved.relative_to(root)
                return True
            except Exception:
                continue
        return False

    @staticmethod
    def _best_relative_path(path: Path) -> str:
        for root in (WORKSPACE_ROOT, MEDISYNC_ROOT):
            try:
                return str(path.resolve().relative_to(root.resolve()))
            except Exception:
                continue
        return str(path)

    @staticmethod
    def _date_sort_key(value: Any) -> float:
        dt_value = DashboardStore._to_datetime(value)
        if dt_value is None:
            return 0.0
        return dt_value.timestamp()

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
    def _as_iso(value: Any) -> str:
        dt_value = DashboardStore._to_datetime(value)
        if dt_value is None:
            return ""
        return dt_value.astimezone(timezone.utc).isoformat()

    def _error_payload(self, message: str) -> Dict[str, Any]:
        return {
            "connected": False,
            "sourceMode": "none",
            "database": self.database_name,
            "agencyId": "",
            "agencyName": "",
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


def _load_dashboard_config() -> Dict[str, Any]:
    config_path = normalize_text(os.getenv("MEDISYNC_CONFIG")) or DEFAULT_CONFIG_PATH
    return load_config(config_path)


def _create_store(config: Dict[str, Any]):
    dashboard_cfg = config.get("dashboard", {})
    force_local_files = to_bool(dashboard_cfg.get("force_local_files", False), default=False) or to_bool(
        os.getenv("MEDISYNC_DASHBOARD_FORCE_LOCAL"), default=False
    )
    if force_local_files:
        return DashboardStore(config)

    dataconnect_cfg = config.get("dataconnect", {})
    if to_bool(dataconnect_cfg.get("enabled", False), default=False):
        dataconnect_store = DataConnectDashboardStore(config)
        probe_payload = dataconnect_store.get_payload(force_refresh=True)
        if probe_payload.get("connected"):
            return dataconnect_store
    return DashboardStore(config)


store = _create_store(_load_dashboard_config())
app = FastAPI(title="MediSync Patient Dashboard", version="1.0.0")
app.mount("/static", StaticFiles(directory=str(STATIC_DIR)), name="static")


@app.get("/", include_in_schema=False)
def dashboard_home() -> FileResponse:
    return FileResponse(STATIC_DIR / "index.html")


@app.get("/api/health")
def dashboard_health() -> Dict[str, Any]:
    payload = store.get_payload(force_refresh=False)
    return {
        "ok": bool(payload.get("connected")),
        "sourceMode": payload.get("sourceMode", "none"),
        "database": payload.get("database", ""),
        "agencyName": payload.get("agencyName", ""),
        "generatedAt": payload.get("generatedAt", ""),
        "error": payload.get("error", ""),
        "warning": payload.get("warning", ""),
    }


@app.get("/api/dashboard")
def dashboard_data(
    search: str = Query(default="", max_length=120),
    initial: str = Query(default="", max_length=8),
    document_filter: str = Query(default="all", pattern="^(all|with-docs|without-docs)$"),
    sort_by: str = Query(default="name", pattern="^(name|orders|documents|recent)$"),
    force_refresh: bool = Query(default=False),
):
    payload = store.get_payload(force_refresh=force_refresh)
    if not payload.get("connected"):
        return JSONResponse(payload, status_code=503)

    response = store.filter_patients(
        payload,
        search=search,
        initial=initial,
        document_filter=document_filter,
        sort_by=sort_by,
    )
    return response


@app.get("/api/patients")
def patient_rows(
    search: str = Query(default="", max_length=120),
    initial: str = Query(default="", max_length=8),
    document_filter: str = Query(default="all", pattern="^(all|with-docs|without-docs)$"),
    sort_by: str = Query(default="name", pattern="^(name|orders|documents|recent)$"),
):
    payload = store.get_payload(force_refresh=False)
    if not payload.get("connected"):
        return JSONResponse(payload, status_code=503)

    response = store.filter_patients(
        payload,
        search=search,
        initial=initial,
        document_filter=document_filter,
        sort_by=sort_by,
    )
    return {
        "patients": response.get("patients", []),
        "summary": response.get("summary", {}),
        "filteredSummary": response.get("filteredSummary", {}),
        "generatedAt": response.get("generatedAt", ""),
    }


@app.get("/api/orders/{order_id}/document")
def order_document(order_id: str, download: bool = Query(default=False)):
    content, reason, file_name = store.resolve_document_content_for_order(order_id)
    if content is None:
        return JSONResponse({"error": reason}, status_code=404)

    response = Response(content=content, media_type="application/pdf")
    disposition = "attachment" if download else "inline"
    response.headers["Content-Disposition"] = f'{disposition}; filename="{file_name}"'
    return response


if __name__ == "__main__":
    import uvicorn

    host = normalize_text(os.getenv("MEDISYNC_DASHBOARD_HOST")) or "127.0.0.1"
    port = int(normalize_text(os.getenv("MEDISYNC_DASHBOARD_PORT")) or "8787")
    uvicorn.run("dashboard.app:app", host=host, port=port, reload=False)
