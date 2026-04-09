"""Parsing, validation, deduplication, and PDF mapping logic."""

from __future__ import annotations

import logging
import re
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional

import pandas as pd
from PyPDF2 import PdfReader

from .models import OrderRecord
from .utils import deduplicate_patients, extract_digits, normalize_text, parse_date, to_bool


DEFAULT_FIELD_MAP = {
    "order_number": ["Order #", "Order Number", "Order No", "Order"],
    "patient_name": ["Patient Name", "Patient"],
    "patient_first_name": ["First Name", "Patient First Name"],
    "patient_last_name": ["Last Name", "Patient Last Name"],
    "mrn": ["MRN", "Medical Record", "Medical Record No", "Medical Record Number"],
    "episode": ["Episode", "Episode Number", "Episode ID"],
    "order_type": ["Order Type", "Document Name", "Doc Type", "Orders"],
    "order_date": ["Order Date", "Ordered", "Date"],
    "physician_name": ["Physician", "Physician Name", "Provider"],
    "clinic": ["Clinic", "Agency", "Location"],
    "npi": ["NPI", "Physician NPI"],
    "dob": ["DOB", "Date of Birth"],
    "phone": ["Phone", "Patient Phone", "Contact"],
    "pdf_path": ["PDF Path", "Document Path", "File Path"],
    "print_view_url": ["Print View URL", "Document URL", "PDF Link"],
    "document_status": ["Document Status", "Status"],
    "source_initial": ["Source Initial", "Initial"],
    "assigned_to": ["Assigned", "Assigned To", "Physician Assigned"],
}


class DataProcessor:
    """Transforms raw exports to canonical order records."""

    def __init__(self, config: Dict[str, Any], logger: logging.Logger):
        self.config = config
        self.logger = logger
        self.processing_cfg = config.get("processing", {})
        self.extract_cfg = config.get("extraction", {})
        self.validation_cfg = config.get("validation", {})
        self.preserve_source_columns = to_bool(
            self.processing_cfg.get("preserve_source_columns", True),
            default=True,
        )
        self.dedupe_enabled = to_bool(self.processing_cfg.get("dedupe_enabled", False), default=False)
        self.last_source_df: Optional[pd.DataFrame] = None

        cfg_map = self.processing_cfg.get("field_map", {})
        self.field_map: Dict[str, List[str]] = {
            key: list(dict.fromkeys(values + cfg_map.get(key, [])))
            for key, values in DEFAULT_FIELD_MAP.items()
        }

    def process(self, excel_path: str, pdf_paths: Optional[List[str]] = None) -> List[OrderRecord]:
        """Read export file, normalize records, and enrich with PDF metadata."""
        dataframe = self._read_table(excel_path)
        if dataframe.empty:
            self.logger.warning("Extracted order table is empty: %s", excel_path)
            return []

        self.last_source_df = dataframe.copy()

        if self.dedupe_enabled:
            dataframe = deduplicate_patients(
                dataframe,
                subset=self.validation_cfg.get("dedupe_columns", ["Patient Name", "MRN"]),
            )

        column_map = self._resolve_columns(dataframe)
        records = self._rows_to_records(dataframe, column_map)
        if self.dedupe_enabled:
            records = self._dedupe_records(records)

        pdf_paths = pdf_paths or []
        if pdf_paths:
            self._attach_pdf_details(records, pdf_paths)
        else:
            inline_pdf_paths = [record.pdf_path for record in records if record.pdf_path]
            if inline_pdf_paths:
                self._attach_pdf_details(records, inline_pdf_paths)

        self.logger.info(
            "Data processing completed | total=%d | valid=%d | invalid=%d",
            len(records),
            len([r for r in records if r.is_valid]),
            len([r for r in records if not r.is_valid]),
        )
        return records

    def records_to_dataframe(self, records: Iterable[OrderRecord]) -> pd.DataFrame:
        """Serialize records to a DataFrame for reporting/export."""
        records_list = list(records)
        rows = []
        for record in records_list:
            rows.append(
                {
                    "source_row": record.source_row,
                    "order_number": record.order_number,
                    "patient_name": record.patient_name,
                    "mrn": record.mrn,
                    "episode": record.episode,
                    "order_type": record.order_type,
                    "order_date": record.order_date,
                    "physician_name": record.physician_name,
                    "clinic": record.clinic,
                    "npi": record.npi,
                    "dob": record.dob,
                    "phone": record.phone,
                    "pdf_path": record.pdf_path,
                    "is_valid": record.is_valid,
                    "validation_errors": " | ".join(record.validation_errors),
                }
            )

        normalized_df = pd.DataFrame(rows)
        if not self.preserve_source_columns or self.last_source_df is None:
            return normalized_df

        source_df = self.last_source_df.copy().reset_index(drop=True)
        source_df["source_row"] = source_df.index + 2
        merged = source_df.merge(normalized_df, on="source_row", how="left")
        return merged

    def _read_table(self, file_path: str) -> pd.DataFrame:
        source = Path(file_path)
        if not source.exists():
            raise FileNotFoundError(f"Order export file not found: {source}")

        if source.suffix.lower() == ".csv":
            dataframe = pd.read_csv(source)
        else:
            dataframe = pd.read_excel(source)

        dataframe.columns = [normalize_text(col) for col in dataframe.columns]
        return dataframe

    def _resolve_columns(self, dataframe: pd.DataFrame) -> Dict[str, Optional[str]]:
        columns_lower = {col.lower(): col for col in dataframe.columns}
        resolved: Dict[str, Optional[str]] = {}

        for canonical, aliases in self.field_map.items():
            matched = None
            for alias in aliases:
                alias_lower = alias.lower()
                if alias_lower in columns_lower:
                    matched = columns_lower[alias_lower]
                    break

            if not matched:
                for column in dataframe.columns:
                    col_lower = column.lower()
                    if any(alias.lower() in col_lower for alias in aliases):
                        matched = column
                        break

            resolved[canonical] = matched

        self.logger.info("Resolved columns: %s", {k: v for k, v in resolved.items() if v})
        return resolved

    def _rows_to_records(self, dataframe: pd.DataFrame, column_map: Dict[str, Optional[str]]) -> List[OrderRecord]:
        records: List[OrderRecord] = []
        required_fields = self.validation_cfg.get(
            "required_fields",
            ["order_number", "patient_name", "mrn", "order_type", "order_date", "physician_name"],
        )
        unmapped_required = {field for field in required_fields if not column_map.get(field)}
        if unmapped_required:
            self.logger.warning(
                "Skipping required-field validation for unmapped columns: %s",
                ", ".join(sorted(unmapped_required)),
            )

        for idx, row in dataframe.iterrows():
            source_row = int(idx) + 2
            record = OrderRecord(
                source_row=source_row,
                order_number=self._cell_text(row, column_map["order_number"]),
                patient_name=self._cell_text(row, column_map["patient_name"]),
                mrn=self._cell_text(row, column_map["mrn"]),
                episode=self._cell_text(row, column_map["episode"]),
                order_type=self._cell_text(row, column_map["order_type"]),
                order_date=parse_date(self._cell_text(row, column_map["order_date"])),
                physician_name=self._cell_text(row, column_map["physician_name"]),
                clinic=self._cell_text(row, column_map["clinic"]),
                npi=extract_digits(self._cell_text(row, column_map["npi"])) or None,
                dob=parse_date(self._cell_text(row, column_map["dob"])) or None,
                phone=self._cell_text(row, column_map["phone"]) or None,
                pdf_path=self._cell_text(row, column_map.get("pdf_path")) or None,
            )

            if not record.patient_name:
                first_name = self._cell_text(row, column_map.get("patient_first_name"))
                last_name = self._cell_text(row, column_map.get("patient_last_name"))
                record.patient_name = " ".join(part for part in [first_name, last_name] if part)

            if self._looks_like_non_physician_text(record.physician_name):
                record.physician_name = ""

            assigned_to = self._cell_text(row, column_map.get("assigned_to"))
            if not record.physician_name and assigned_to:
                record.physician_name = assigned_to

            print_view_url = self._cell_text(row, column_map.get("print_view_url"))
            if print_view_url:
                record.metadata["print_view_url"] = print_view_url

            document_status = self._cell_text(row, column_map.get("document_status"))
            if document_status:
                record.metadata["document_status"] = document_status

            source_initial = self._cell_text(row, column_map.get("source_initial"))
            if source_initial:
                record.metadata["source_initial"] = source_initial

            if assigned_to:
                record.metadata["assigned_to"] = assigned_to

            if record.order_type:
                record.metadata["document_name"] = record.order_type

            for field in required_fields:
                if field in unmapped_required:
                    continue
                value = getattr(record, field, "")
                if not normalize_text(value):
                    record.validation_errors.append(f"Missing required field: {field}")

            if record.npi and len(record.npi) != 10:
                record.validation_errors.append("NPI must be 10 digits when present")

            if not record.order_number:
                record.validation_errors.append("Order number is blank")

            records.append(record)

        return records

    def _dedupe_records(self, records: List[OrderRecord]) -> List[OrderRecord]:
        dedupe_fields = self.validation_cfg.get(
            "record_dedupe_key",
            ["order_number", "patient_name", "mrn"],
        )
        seen = set()
        deduped: List[OrderRecord] = []

        for record in records:
            key = tuple(normalize_text(getattr(record, field, "")).lower() for field in dedupe_fields)
            if key in seen:
                continue
            seen.add(key)
            deduped.append(record)

        return deduped

    def _attach_pdf_details(self, records: List[OrderRecord], pdf_paths: List[str]) -> None:
        pdf_index = self._index_pdf_files(pdf_paths)
        for record in records:
            if record.pdf_path:
                path = Path(record.pdf_path)
                if path.exists():
                    metadata = self._extract_pdf_metadata(path)
                    self._merge_pdf_metadata(record, metadata)
                    continue

            key = extract_digits(record.order_number)
            if not key:
                continue

            path = pdf_index.get(key)
            if path:
                record.pdf_path = str(path)
                metadata = self._extract_pdf_metadata(path)
                self._merge_pdf_metadata(record, metadata)

    def _merge_pdf_metadata(self, record: OrderRecord, metadata: Dict[str, str]) -> None:
        if not metadata:
            return

        record.metadata.update(metadata)

        if not record.npi and metadata.get("npi"):
            record.npi = metadata["npi"]
        if not record.mrn and metadata.get("mrn"):
            record.mrn = metadata["mrn"]
        if not record.dob and metadata.get("dob"):
            record.dob = metadata["dob"]
        if not record.phone and metadata.get("phone"):
            record.phone = metadata["phone"]
        if not record.episode and metadata.get("episode"):
            record.episode = metadata["episode"]

        inferred_physician = normalize_text(metadata.get("physician_name"))
        if inferred_physician and (
            not record.physician_name or self._looks_like_non_physician_text(record.physician_name)
        ):
            record.physician_name = inferred_physician

        inferred_document_name = normalize_text(metadata.get("document_name"))
        if inferred_document_name and (not record.order_type):
            record.order_type = inferred_document_name

        inferred_order_date = normalize_text(metadata.get("order_date"))
        if inferred_order_date and not record.order_date:
            record.order_date = parse_date(inferred_order_date)

    def _index_pdf_files(self, pdf_paths: List[str]) -> Dict[str, Path]:
        index: Dict[str, Path] = {}
        for raw_path in pdf_paths:
            path = Path(raw_path)
            if not path.exists():
                continue
            tokens = re.findall(r"\d{3,}", path.stem)
            if not tokens:
                continue
            # Prefer the longest numeric token, which is usually order/document number.
            token = sorted(tokens, key=len, reverse=True)[0]
            if token not in index:
                index[token] = path
        return index

    def _extract_pdf_metadata(self, pdf_path: Path) -> Dict[str, str]:
        metadata: Dict[str, str] = {}

        try:
            reader = PdfReader(str(pdf_path))
            text_parts = []
            for page in reader.pages[:3]:
                text_parts.append(page.extract_text() or "")
            text_blob = "\n".join(text_parts)
        except Exception as error:
            self.logger.warning("Failed reading PDF metadata for %s: %s", pdf_path.name, error)
            return metadata

        normalized_blob = re.sub(r"\s+", " ", text_blob)
        lowered_blob = normalized_blob.lower()

        if "cms 485" in lowered_blob or "cms-485" in lowered_blob or "certification and plan of care" in lowered_blob:
            metadata["document_name"] = "CMS 485"
        elif "physician order" in lowered_blob:
            metadata["document_name"] = "Physician Order"

        npi_match = re.search(r"NPI\s*[:#]?\s*(\d{10})", normalized_blob, re.IGNORECASE)
        if npi_match:
            metadata["npi"] = npi_match.group(1)

        mrn_patterns = [
            r"MRN\s*[:#]?\s*([A-Za-z0-9-]+)",
            r"MR\s*[:#]?\s*([A-Za-z0-9-]+)",
        ]
        for pattern in mrn_patterns:
            mrn_match = re.search(pattern, normalized_blob, re.IGNORECASE)
            if mrn_match:
                metadata["mrn"] = normalize_text(mrn_match.group(1))
                break

        dob_match = re.search(r"DOB\s*[:#]?\s*(\d{1,2}/\d{1,2}/\d{2,4})", normalized_blob, re.IGNORECASE)
        if dob_match:
            metadata["dob"] = parse_date(dob_match.group(1))

        episode_match = re.search(
            r"Episode(?:\s*(?:No|#|Number|ID))?\s*[:#]?\s*([A-Za-z0-9-]+)",
            normalized_blob,
            re.IGNORECASE,
        )
        if episode_match:
            metadata["episode"] = normalize_text(episode_match.group(1))

        phone_match = re.search(
            r"(?:Phone|Telephone|Tel)\s*[:#]?\s*(\(?\d{3}\)?[-\s\.]?\d{3}[-\s\.]?\d{4})",
            normalized_blob,
            re.IGNORECASE,
        )
        if phone_match:
            metadata["phone"] = normalize_text(phone_match.group(1))

        physician_patterns = [
            r"(?:Attending\s+Physician|Ordering\s+Physician|Physician|Doctor)\s*[:#]?\s*([A-Za-z\s\.,'-]{4,})",
        ]
        for pattern in physician_patterns:
            physician_match = re.search(pattern, normalized_blob, re.IGNORECASE)
            if physician_match:
                physician_name = normalize_text(physician_match.group(1))
                physician_name = re.split(r"\b(?:npi|phone|fax|address)\b", physician_name, maxsplit=1, flags=re.IGNORECASE)[0]
                physician_name = normalize_text(physician_name)
                if physician_name and not self._looks_like_non_physician_text(physician_name):
                    metadata["physician_name"] = physician_name
                    break

        patient_patterns = [
            r"(?:Patient\s*Name|Patient|Pt\.?\s*Name)\s*[:#]?\s*([A-Za-z\s\.,'-]{5,})",
        ]
        for pattern in patient_patterns:
            patient_match = re.search(pattern, normalized_blob, re.IGNORECASE)
            if patient_match:
                patient_name = normalize_text(patient_match.group(1))
                patient_name = re.split(r"\b(?:dob|mrn|episode|address|phone)\b", patient_name, maxsplit=1, flags=re.IGNORECASE)[0]
                patient_name = normalize_text(patient_name)
                if patient_name:
                    metadata["patient_name"] = patient_name
                    break

        order_date_patterns = [
            r"(?:Order\s*Date|Date\s*of\s*Order|Certification\s*Date)\s*[:#]?\s*(\d{1,2}/\d{1,2}/\d{2,4})",
            r"Certification\s*Period\s*(?:From)?\s*(\d{1,2}/\d{1,2}/\d{2,4})",
        ]
        for pattern in order_date_patterns:
            date_match = re.search(pattern, normalized_blob, re.IGNORECASE)
            if date_match:
                metadata["order_date"] = parse_date(date_match.group(1))
                break

        return metadata

    @staticmethod
    def _looks_like_non_physician_text(value: str) -> bool:
        text = normalize_text(value).lower()
        if not text:
            return False

        bad_tokens = [
            "physician order",
            "print view document",
            "cms 485",
            "plan of care",
        ]
        if any(token in text for token in bad_tokens):
            return True

        if re.match(r"^\d+\.?$", text):
            return True
        return False

    @staticmethod
    def _cell_text(row: pd.Series, column_name: Optional[str]) -> str:
        if not column_name:
            return ""
        return normalize_text(row.get(column_name, ""))
