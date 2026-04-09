"""Data Connect (Cloud SQL Postgres) storage backend for MediSync pipeline."""

from __future__ import annotations

import base64
from collections import Counter
import hashlib
import json
from datetime import date, datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple
from uuid import NAMESPACE_DNS, uuid4, uuid5

import pandas as pd

from .dataconnect_client import DataConnectClient
from .models import OrderRecord, SyncResult
from .utils import normalize_text


class DataConnectStore:
    """Stores pipeline output through Firebase Data Connect operations."""

    def __init__(self, config: Dict[str, Any], logger):
        self.config = config
        self.logger = logger

        self.dataconnect_cfg = config.get("dataconnect", {})
        self.firestore_cfg = config.get("firestore", {})
        self.extraction_cfg = config.get("extraction", {})

        self.client = DataConnectClient(config, logger)
        if not self.client.enabled:
            raise ValueError("Data Connect storage is disabled in config")

        self.agency_id = self.client.agency_id
        self.agency_name = self.client.agency_name

        self.created_by_email = normalize_text(self.dataconnect_cfg.get("created_by_email")) or normalize_text(
            self.firestore_cfg.get("created_by_email", "medisync-bot@local")
        )
        self.created_by_name = normalize_text(self.dataconnect_cfg.get("created_by_name")) or normalize_text(
            self.firestore_cfg.get("created_by_name", "MediSync Bot")
        )
        self.source_system = normalize_text(self.dataconnect_cfg.get("source_system")) or normalize_text(
            self.firestore_cfg.get("source_system", "kinnser")
        ) or "kinnser"
        self.workflow_name = normalize_text(self.dataconnect_cfg.get("workflow_name")) or normalize_text(
            self.firestore_cfg.get("workflow_name", "orders-received-export")
        ) or "orders-received-export"
        self.default_order_status = normalize_text(self.dataconnect_cfg.get("default_order_status")) or normalize_text(
            self.firestore_cfg.get("default_order_status", "RECEIVED")
        ) or "RECEIVED"

        self.required_order_schema = [
            "agencyId",
            "importRunId",
            "patientId",
            "orderNumber",
            "orderType",
            "orderedDate",
            "status",
            "sourceSystem",
            "sourceFileName",
            "sourceRowNumber",
            "updatedAt",
            "isCurrent",
            "version",
        ]
        self.last_sync_report: Dict[str, Any] = {}

    def sync_records(
        self,
        records: List[OrderRecord],
        *,
        source_df: Optional[pd.DataFrame] = None,
        source_file_name: str = "",
    ) -> List[SyncResult]:
        """Persist order records and raw rows through Data Connect."""
        agency_id, user_id = self._ensure_agency_and_user()
        import_run_id = self._create_import_run(agency_id, user_id, source_file_name, len(records))

        results: List[SyncResult] = []
        success_count = 0
        failed_count = 0

        for record in records:
            try:
                patient_id = self._upsert_patient(agency_id, user_id, record)
                physician_id = self._upsert_physician(agency_id, record)
                order_id, _is_insert = self._upsert_order(
                    agency_id,
                    user_id,
                    import_run_id,
                    patient_id,
                    physician_id,
                    record,
                    source_file_name,
                )

                self._upsert_order_document(
                    agency_id=agency_id,
                    order_id=order_id,
                    record=record,
                    source_file_name=source_file_name,
                )

                raw_payload = self._raw_payload(record, source_df)
                raw_content = self._to_json(raw_payload)
                self._insert_raw_row(
                    agency_id=agency_id,
                    import_run_id=import_run_id,
                    order_id=order_id,
                    source_file_name=source_file_name,
                    source_row_number=record.source_row,
                    row_status="SUCCESS",
                    raw_content=raw_content,
                )

                results.append(
                    SyncResult(
                        order_number=record.order_number,
                        patient_name=record.patient_name,
                        status="success",
                        patient_id=patient_id,
                        physician_id=physician_id,
                        order_id=order_id,
                    )
                )
                success_count += 1
            except Exception as error:
                failed_count += 1
                reason = str(error)
                results.append(
                    SyncResult(
                        order_number=record.order_number,
                        patient_name=record.patient_name,
                        status="failed",
                        reason=reason,
                    )
                )

                try:
                    raw_payload = self._raw_payload(record, source_df)
                    raw_content = self._to_json(raw_payload)
                    self._insert_raw_row(
                        agency_id=agency_id,
                        import_run_id=import_run_id,
                        order_id=None,
                        source_file_name=source_file_name,
                        source_row_number=record.source_row,
                        row_status="FAILED",
                        raw_content=raw_content,
                        error_message=reason,
                    )
                except Exception as raw_error:
                    self.logger.warning(
                        "Failed to insert failed raw row for source row %s: %s",
                        record.source_row,
                        raw_error,
                    )

        run_status = "COMPLETED" if failed_count == 0 else "COMPLETED_WITH_ERRORS"
        self._finalize_import_run(import_run_id, run_status, success_count, failed_count)

        self.last_sync_report = {
            "import_run_id": import_run_id,
            "run_status": run_status,
            "expected_successful_orders": int(success_count),
            "failed_records": int(failed_count),
        }

        try:
            validation = self._validate_written_orders_schema(import_run_id=import_run_id)
            self.last_sync_report.update(validation)
            if validation.get("schema_invalid_orders", 0) > 0:
                self.logger.warning(
                    "DB schema verification found %d invalid orders for import run %s",
                    validation.get("schema_invalid_orders", 0),
                    import_run_id,
                )
            else:
                self.logger.info(
                    "DB schema verification passed | import_run_id=%s | orders=%d",
                    import_run_id,
                    validation.get("orders_written_for_run", 0),
                )
        except Exception as error:
            self.last_sync_report["verification_error"] = str(error)
            self.logger.warning("Post-sync DB verification failed for import run %s: %s", import_run_id, error)

        return results

    def sync_patient_profiles(
        self,
        profile_rows: List[Dict[str, Any]],
        *,
        source_df: Optional[pd.DataFrame] = None,
        source_file_name: str = "",
    ) -> List[SyncResult]:
        """Persist patient profile rows without creating order records."""
        agency_id, user_id = self._ensure_agency_and_user()
        import_run_id = self._create_import_run(
            agency_id,
            user_id,
            source_file_name,
            len(profile_rows),
            workflow_name_override="patient-profile-export",
        )

        results: List[SyncResult] = []
        success_count = 0
        failed_count = 0

        for index, row in enumerate(profile_rows):
            source_row = int(row.get("source_row") or (index + 2))
            patient_name = normalize_text(row.get("patient_name")) or "UNKNOWN PATIENT"
            mrn = normalize_text(row.get("mrn"))

            try:
                patient_id = self._upsert_patient_profile(agency_id, user_id, row)
                physician_id = self._upsert_profile_physician(agency_id, row)

                raw_content = self._to_json(self._clean_profile_row(row, source_df=source_df, source_row=source_row))
                self._insert_raw_row(
                    agency_id=agency_id,
                    import_run_id=import_run_id,
                    order_id=None,
                    source_file_name=source_file_name,
                    source_row_number=source_row,
                    row_status="SUCCESS",
                    raw_content=raw_content,
                )

                results.append(
                    SyncResult(
                        order_number=mrn or patient_name,
                        patient_name=patient_name,
                        status="success",
                        patient_id=patient_id,
                        physician_id=physician_id,
                    )
                )
                success_count += 1
            except Exception as error:
                failed_count += 1
                reason = str(error)

                results.append(
                    SyncResult(
                        order_number=mrn or patient_name,
                        patient_name=patient_name,
                        status="failed",
                        reason=reason,
                    )
                )

                try:
                    raw_content = self._to_json(self._clean_profile_row(row, source_df=source_df, source_row=source_row))
                    self._insert_raw_row(
                        agency_id=agency_id,
                        import_run_id=import_run_id,
                        order_id=None,
                        source_file_name=source_file_name,
                        source_row_number=source_row,
                        row_status="FAILED",
                        raw_content=raw_content,
                        error_message=reason,
                    )
                except Exception as raw_error:
                    self.logger.warning(
                        "Failed to insert failed profile raw row for source row %s: %s",
                        source_row,
                        raw_error,
                    )

        run_status = "COMPLETED" if failed_count == 0 else "COMPLETED_WITH_ERRORS"
        self._finalize_import_run(import_run_id, run_status, success_count, failed_count)

        self.last_sync_report = {
            "import_run_id": import_run_id,
            "run_status": run_status,
            "expected_successful_profiles": int(success_count),
            "failed_records": int(failed_count),
            "patients_written_for_run": int(success_count),
            "workflow": "patient-profile-export",
        }

        return results

    def _ensure_agency_and_user(self) -> Tuple[str, str]:
        now = self._now()

        self._execute(
            "UpsertAgency",
            {
                "id": self.agency_id,
                "name": self.agency_name,
                "createdAt": now,
                "updatedAt": now,
            },
        )

        user_id = str(uuid5(NAMESPACE_DNS, f"medisync-user:{self.agency_id}:{self.created_by_email.lower()}"))
        self._execute(
            "UpsertAppUser",
            {
                "id": user_id,
                "agencyId": self.agency_id,
                "email": self.created_by_email,
                "displayName": self.created_by_name,
                "role": "OPERATOR",
                "createdAt": now,
                "updatedAt": now,
            },
        )
        return self.agency_id, user_id

    def _create_import_run(
        self,
        agency_id: str,
        user_id: str,
        source_file_name: str,
        total_rows: int,
        workflow_name_override: Optional[str] = None,
    ) -> str:
        now = self._now()
        filter_start = normalize_text(self.extraction_cfg.get("report_start_date")) or ""
        filter_end = normalize_text(self.extraction_cfg.get("report_end_date")) or ""
        if filter_end and filter_end.lower() in {"today", "current_date", "current"}:
            filter_end = datetime.now().strftime("%m/%d/%Y")

        workflow_name = normalize_text(workflow_name_override) or self.workflow_name
        import_run_id = str(uuid4())

        self._execute(
            "InsertImportRun",
            {
                "id": import_run_id,
                "agencyId": agency_id,
                "fileName": source_file_name,
                "runAt": now,
                "status": "RUNNING",
                "totalRows": int(total_rows),
                "successfulRows": 0,
                "failedRows": 0,
                "createdAt": now,
                "createdBy": user_id,
                "dateFilterStart": filter_start,
                "dateFilterEnd": filter_end,
                "errorMessage": "",
                "sourceSystem": self.source_system,
                "workflowName": workflow_name,
            },
        )

        return import_run_id

    def _finalize_import_run(
        self,
        import_run_id: str,
        status: str,
        successful_rows: int,
        failed_rows: int,
        error_message: str = "",
    ) -> None:
        self._execute(
            "FinalizeImportRun",
            {
                "id": import_run_id,
                "status": status,
                "successfulRows": int(successful_rows),
                "failedRows": int(failed_rows),
                "errorMessage": normalize_text(error_message),
                "finishedAt": self._now(),
            },
        )

    def _upsert_patient(self, agency_id: str, user_id: str, record: OrderRecord) -> str:
        now = self._now()
        patient_name = normalize_text(record.patient_name) or "UNKNOWN PATIENT"
        mrn = normalize_text(record.mrn)
        phone = normalize_text(record.phone) or normalize_text(record.metadata.get("phone"))
        episode = normalize_text(record.episode) or normalize_text(record.metadata.get("episode"))
        dob = normalize_text(record.dob) or normalize_text(record.metadata.get("dob"))

        key_material = mrn.lower() if mrn else patient_name.lower()
        patient_id = str(uuid5(NAMESPACE_DNS, f"medisync-patient:{agency_id}:{key_material}"))

        self._execute(
            "UpsertPatient",
            {
                "id": patient_id,
                "agencyId": agency_id,
                "name": patient_name,
                "mrn": mrn or None,
                "dob": dob or None,
                "phone": phone or None,
                "episode": episode or None,
                "createdAt": now,
                "createdBy": user_id,
                "updatedAt": now,
            },
        )
        return patient_id

    def _upsert_physician(self, agency_id: str, record: OrderRecord) -> Optional[str]:
        physician_name = normalize_text(record.physician_name)
        if not physician_name:
            return None

        now = self._now()
        npi = normalize_text(record.npi)
        key_material = npi.lower() if npi else physician_name.lower()
        physician_id = str(uuid5(NAMESPACE_DNS, f"medisync-physician:{agency_id}:{key_material}"))

        self._execute(
            "UpsertPhysician",
            {
                "id": physician_id,
                "agencyId": agency_id,
                "name": physician_name,
                "npi": npi or None,
                "specialty": normalize_text(record.metadata.get("physician_specialty")) or None,
                "createdAt": now,
                "updatedAt": now,
            },
        )
        return physician_id

    def _upsert_order(
        self,
        agency_id: str,
        user_id: str,
        import_run_id: str,
        patient_id: str,
        physician_id: Optional[str],
        record: OrderRecord,
        source_file_name: str,
    ) -> Tuple[str, bool]:
        now = self._now()
        order_number = normalize_text(record.order_number)
        if not order_number:
            raise ValueError(f"Order number missing for source row {record.source_row}")

        order_id = str(uuid5(NAMESPACE_DNS, f"medisync-order:{agency_id}:{order_number.lower()}"))
        physician_name = normalize_text(record.physician_name) or normalize_text(record.metadata.get("physician_name"))
        order_type = normalize_text(record.order_type) or normalize_text(record.metadata.get("document_name"))
        ordered_date = normalize_text(record.order_date) or normalize_text(record.metadata.get("order_date"))

        pdf_path = normalize_text(record.pdf_path)
        resolved_pdf = ""
        if pdf_path:
            candidate = Path(pdf_path)
            if not candidate.is_absolute():
                candidate = (Path.cwd() / candidate).resolve()
            resolved_pdf = str(candidate)

        self._execute(
            "UpsertOrder",
            {
                "id": order_id,
                "agencyId": agency_id,
                "importRunId": import_run_id,
                "patientId": patient_id,
                "physicianId": physician_id,
                "orderNumber": order_number,
                "orderType": order_type or None,
                "orderedDate": ordered_date or None,
                "deliveryMethod": normalize_text(record.metadata.get("Delivery Method")) or None,
                "status": normalize_text(record.metadata.get("status")) or self.default_order_status,
                "sourceSystem": self.source_system,
                "sourceFileName": source_file_name,
                "sourceRowNumber": int(record.source_row),
                "documentName": normalize_text(record.metadata.get("document_name")) or order_type or None,
                "documentLink": normalize_text(record.metadata.get("print_view_url")) or None,
                "pdfPath": resolved_pdf or None,
                "sentDate": normalize_text(record.metadata.get("Sent")) or None,
                "receivedDate": normalize_text(record.metadata.get("Received")) or None,
                "comment": normalize_text(record.metadata.get("Comment")) or "",
                "physicianName": physician_name or None,
                "version": 1,
                "isCurrent": True,
                "documentStorage": "database" if resolved_pdf else "none",
                "documentStoredInDb": False,
                "createdAt": now,
                "createdBy": user_id,
                "updatedAt": now,
            },
        )

        return order_id, True

    def _upsert_order_document(
        self,
        *,
        agency_id: str,
        order_id: str,
        record: OrderRecord,
        source_file_name: str,
    ) -> bool:
        pdf_path = normalize_text(record.pdf_path)
        if not pdf_path:
            return False

        candidate = Path(pdf_path)
        if not candidate.is_absolute():
            candidate = (Path.cwd() / candidate).resolve()

        if not candidate.exists() or not candidate.is_file():
            self.logger.warning(
                "Order PDF missing for DB persistence | order_id=%s | path=%s",
                order_id,
                candidate,
            )
            return False

        try:
            pdf_bytes = candidate.read_bytes()
        except Exception as error:
            self.logger.warning(
                "Failed reading order PDF | order_id=%s | path=%s | error=%s",
                order_id,
                candidate,
                error,
            )
            return False

        if not pdf_bytes:
            self.logger.warning("Order PDF empty; skipping DB persistence | order_id=%s | path=%s", order_id, candidate)
            return False

        now = self._now()
        checksum = hashlib.sha256(pdf_bytes).hexdigest()
        content_base64 = base64.b64encode(pdf_bytes).decode("ascii")
        document_id = str(uuid5(NAMESPACE_DNS, f"medisync-order-document:{order_id}"))

        self._execute(
            "UpsertOrderDocument",
            {
                "id": document_id,
                "agencyId": agency_id,
                "orderId": order_id,
                "orderNumber": normalize_text(record.order_number) or None,
                "patientName": normalize_text(record.patient_name) or None,
                "mrn": normalize_text(record.mrn) or None,
                "documentName": normalize_text(record.metadata.get("document_name"))
                or normalize_text(record.order_type)
                or candidate.name,
                "fileName": candidate.name,
                "contentType": "application/pdf",
                "sizeBytes": int(len(pdf_bytes)),
                "checksum": checksum,
                "hasContent": True,
                "sourcePath": str(candidate),
                "sourceFileName": source_file_name,
                "sourceRowNumber": int(record.source_row),
                "contentBase64": content_base64,
                "createdAt": now,
                "updatedAt": now,
            },
        )

        return True

    def _insert_raw_row(
        self,
        *,
        agency_id: str,
        import_run_id: str,
        order_id: Optional[str],
        source_file_name: str,
        source_row_number: int,
        row_status: str,
        raw_content: str,
        error_message: str = "",
    ) -> None:
        row_id = str(uuid4())
        checksum = hashlib.sha256(raw_content.encode("utf-8")).hexdigest()

        self._execute(
            "InsertOrderRawRow",
            {
                "id": row_id,
                "agencyId": agency_id,
                "importRunId": import_run_id,
                "orderId": order_id,
                "sourceFileName": source_file_name,
                "sourceRowNumber": int(source_row_number),
                "rowStatus": normalize_text(row_status) or "UNKNOWN",
                "rawContent": raw_content,
                "checksum": checksum,
                "errorMessage": normalize_text(error_message) or None,
                "createdAt": self._now(),
            },
        )

    def _upsert_patient_profile(self, agency_id: str, user_id: str, profile_row: Dict[str, Any]) -> str:
        now = self._now()

        patient_name = normalize_text(profile_row.get("patient_name")) or "UNKNOWN PATIENT"
        mrn = normalize_text(profile_row.get("mrn"))

        key_material = mrn.lower() if mrn else patient_name.lower()
        patient_id = str(uuid5(NAMESPACE_DNS, f"medisync-patient:{agency_id}:{key_material}"))

        profile_pairs = self._normalize_profile_pairs(profile_row.get("profile_pairs"))

        self._execute(
            "UpsertPatient",
            {
                "id": patient_id,
                "agencyId": agency_id,
                "name": patient_name,
                "mrn": mrn or None,
                "dob": normalize_text(profile_row.get("dob")) or None,
                "gender": normalize_text(profile_row.get("gender")) or None,
                "email": normalize_text(profile_row.get("email")) or None,
                "ssn": normalize_text(profile_row.get("ssn")) or None,
                "phone": normalize_text(profile_row.get("phone")) or None,
                "episode": normalize_text(profile_row.get("episode")) or None,
                "address": normalize_text(profile_row.get("address")) or None,
                "city": normalize_text(profile_row.get("city")) or None,
                "state": normalize_text(profile_row.get("state")) or None,
                "zip": normalize_text(profile_row.get("zip")) or None,
                "maritalStatus": normalize_text(profile_row.get("marital_status")) or None,
                "primaryLanguage": normalize_text(profile_row.get("primary_language")) or None,
                "insurance": normalize_text(profile_row.get("insurance")) or None,
                "emergencyContact": normalize_text(profile_row.get("emergency_contact")) or None,
                "allergies": normalize_text(profile_row.get("allergies")) or None,
                "primaryPhysicianName": normalize_text(profile_row.get("primary_physician")) or None,
                "diagnoses": normalize_text(profile_row.get("diagnoses")) or None,
                "diagnosisCodes": normalize_text(profile_row.get("diagnosis_codes")) or None,
                "profileUrl": normalize_text(profile_row.get("profile_url")) or None,
                "profileExtractedAt": normalize_text(profile_row.get("profile_extracted_at")) or None,
                "profileDataPath": normalize_text(profile_row.get("profile_data_path")) or None,
                "profileHtmlPath": normalize_text(profile_row.get("profile_html_path")) or None,
                "profilePairsJson": self._to_json(profile_pairs),
                "profileTextPreview": self._truncate_text(profile_row.get("profile_text_preview"), 6000) or None,
                "profileText": self._truncate_text(profile_row.get("profile_text"), 12000) or None,
                "createdAt": now,
                "createdBy": user_id,
                "updatedAt": now,
            },
        )
        return patient_id

    def _upsert_profile_physician(self, agency_id: str, profile_row: Dict[str, Any]) -> Optional[str]:
        physician_name = normalize_text(profile_row.get("primary_physician")) or normalize_text(
            profile_row.get("physician_name")
        )
        if not physician_name:
            return None

        now = self._now()
        npi = normalize_text(profile_row.get("npi"))
        key_material = npi.lower() if npi else physician_name.lower()
        physician_id = str(uuid5(NAMESPACE_DNS, f"medisync-physician:{agency_id}:{key_material}"))

        self._execute(
            "UpsertPhysician",
            {
                "id": physician_id,
                "agencyId": agency_id,
                "name": physician_name,
                "npi": npi or None,
                "createdAt": now,
                "updatedAt": now,
            },
        )
        return physician_id

    def _validate_written_orders_schema(self, *, import_run_id: str) -> Dict[str, Any]:
        payload = self._execute("GetOrdersForImportRun", {"importRunId": import_run_id})
        orders = payload.get("orders", []) if isinstance(payload, dict) else []

        missing_field_counts: Counter[str] = Counter()
        invalid_order_ids: List[str] = []

        for order in orders:
            if not isinstance(order, dict):
                continue

            missing_fields: List[str] = []
            for field in self.required_order_schema:
                value = order.get(field)
                if isinstance(value, str):
                    if not normalize_text(value):
                        missing_fields.append(field)
                    continue
                if value is None:
                    missing_fields.append(field)

            if missing_fields:
                invalid_order_ids.append(normalize_text(order.get("id")))
                for field in missing_fields:
                    missing_field_counts[field] += 1

        invalid_count = len(invalid_order_ids)
        return {
            "orders_written_for_run": len(orders),
            "schema_valid_orders": len(orders) - invalid_count,
            "schema_invalid_orders": invalid_count,
            "missing_field_counts": dict(sorted(missing_field_counts.items())),
            "invalid_order_id_samples": [order_id for order_id in invalid_order_ids if order_id][:20],
        }

    def _raw_payload(self, record: OrderRecord, source_df: Optional[pd.DataFrame]) -> Dict[str, Any]:
        if source_df is not None:
            row_idx = record.source_row - 2
            if 0 <= row_idx < len(source_df):
                row = source_df.iloc[row_idx].to_dict()
                return {str(key): self._clean_value(value) for key, value in row.items()}

        return {
            "order_number": normalize_text(record.order_number),
            "patient_name": normalize_text(record.patient_name),
            "mrn": normalize_text(record.mrn),
            "order_type": normalize_text(record.order_type),
            "ordered_date": normalize_text(record.order_date),
            "physician_name": normalize_text(record.physician_name),
            "clinic": normalize_text(record.clinic),
            "source_row": int(record.source_row),
        }

    def _clean_profile_row(
        self,
        row: Dict[str, Any],
        *,
        source_df: Optional[pd.DataFrame],
        source_row: int,
    ) -> Dict[str, Any]:
        if source_df is not None:
            row_idx = source_row - 2
            if 0 <= row_idx < len(source_df):
                source_payload = source_df.iloc[row_idx].to_dict()
                return {str(key): self._clean_value(value) for key, value in source_payload.items()}

        return {
            "patient_name": normalize_text(row.get("patient_name")),
            "mrn": normalize_text(row.get("mrn")),
            "episode": normalize_text(row.get("episode")),
            "dob": normalize_text(row.get("dob")),
            "gender": normalize_text(row.get("gender")),
            "email": normalize_text(row.get("email")),
            "ssn": normalize_text(row.get("ssn")),
            "phone": normalize_text(row.get("phone")),
            "address": normalize_text(row.get("address")),
            "city": normalize_text(row.get("city")),
            "state": normalize_text(row.get("state")),
            "zip": normalize_text(row.get("zip")),
            "marital_status": normalize_text(row.get("marital_status")),
            "primary_language": normalize_text(row.get("primary_language")),
            "insurance": normalize_text(row.get("insurance")),
            "emergency_contact": normalize_text(row.get("emergency_contact")),
            "allergies": normalize_text(row.get("allergies")),
            "primary_physician": normalize_text(row.get("primary_physician")),
            "diagnoses": normalize_text(row.get("diagnoses")),
            "diagnosis_codes": normalize_text(row.get("diagnosis_codes")),
            "profile_url": normalize_text(row.get("profile_url")),
            "profile_extracted_at": normalize_text(row.get("profile_extracted_at")),
            "profile_data_path": normalize_text(row.get("profile_data_path")),
            "profile_html_path": normalize_text(row.get("profile_html_path")),
            "profile_text_preview": self._truncate_text(row.get("profile_text_preview"), 6000),
            "profile_text": self._truncate_text(row.get("profile_text"), 12000),
            "profile_pairs": self._normalize_profile_pairs(row.get("profile_pairs")),
            "source_row": int(source_row),
        }

    @staticmethod
    def _normalize_profile_pairs(value: Any, limit: int = 300) -> List[Dict[str, str]]:
        if not isinstance(value, list):
            return []

        pairs: List[Dict[str, str]] = []
        for entry in value:
            if isinstance(entry, dict):
                label = normalize_text(entry.get("label"))
                pair_value = normalize_text(entry.get("value"))
            elif isinstance(entry, (list, tuple)) and len(entry) >= 2:
                label = normalize_text(entry[0])
                pair_value = normalize_text(entry[1])
            else:
                continue

            if not label or not pair_value:
                continue

            pairs.append({"label": label[:120], "value": pair_value[:1000]})
            if len(pairs) >= limit:
                break

        return pairs

    @staticmethod
    def _truncate_text(value: Any, limit: int) -> str:
        text = normalize_text(value)
        if not text:
            return ""
        if len(text) <= limit:
            return text
        return text[:limit] + " ..."

    @staticmethod
    def _clean_value(value: Any) -> Any:
        if value is None:
            return None

        try:
            if pd.isna(value):
                return None
        except Exception:
            pass

        if isinstance(value, (datetime, date)):
            return value.isoformat()

        if hasattr(value, "item"):
            try:
                return value.item()
            except Exception:
                pass

        return value

    @staticmethod
    def _to_json(payload: Any) -> str:
        return json.dumps(payload, ensure_ascii=True, default=str)

    @staticmethod
    def _now() -> str:
        return datetime.now(timezone.utc).isoformat()

    def _execute(self, operation_name: str, variables: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
        result = self.client.execute(operation_name, variables)
        if not isinstance(result, dict):
            return {}

        data = result.get("data")
        if isinstance(data, dict):
            return data
        return result
