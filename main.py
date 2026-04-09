#!/usr/bin/env python3
"""MediSync Kinnser automation pipeline entrypoint."""

from __future__ import annotations

import argparse
import os
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List

import pandas as pd

from src.enrichment import NPIEnricher
from src.extraction import KinnserExtractor
from src.dataconnect_store import DataConnectStore
from src.firestore_store import FirestoreMongoStore
from src.processing import DataProcessor
from src.sync import MediSyncClient
from src.utils import ensure_dirs, load_config, load_json, normalize_text, parse_date, save_json, setup_logging, to_bool


PROFILE_WORKFLOW_MODES = {
	"profile",
	"profiles",
	"patient_profile",
	"patient_profiles",
	"patient_profiles_then_orders",
	"patient_profiles_and_orders",
	"patient_profiles_with_orders",
	"profile_then_orders",
	"profiles_then_orders",
}


def build_parser() -> argparse.ArgumentParser:
	parser = argparse.ArgumentParser(description="Kinnser -> MediSync automation pipeline")
	parser.add_argument("--config", default="config.json", help="Path to Medisync config JSON")
	parser.add_argument(
		"--skip-extraction",
		action="store_true",
		help="Skip Selenium extraction and use existing files",
	)
	parser.add_argument("--excel-path", help="Path to existing Excel/CSV export when skipping extraction")
	parser.add_argument("--pdf-dir", help="Directory containing already-downloaded order PDFs")
	parser.add_argument("--dry-run", action="store_true", help="Run without pushing to MediSync APIs")
	parser.add_argument("--limit", type=int, default=0, help="Optional limit of records to process")
	return parser


def main() -> int:
	args = build_parser().parse_args()
	config = load_config(args.config)
	paths = ensure_dirs(config)

	log_dir = str(paths.get("log_dir", Path("./logs")))
	logger = setup_logging(log_dir)
	logger.info("Starting MediSync pipeline")

	try:
		excel_path, pdf_paths = _get_extraction_artifacts(args, config, logger)
		logger.info("Using source file: %s", excel_path)
		logger.info("PDF file count: %d", len(pdf_paths))

		workflow_mode = _normalized_workflow_mode(config)
		if workflow_mode in PROFILE_WORKFLOW_MODES:
			logger.info("Detected profile workflow mode: %s", workflow_mode)
			return _run_profile_pipeline(args, config, logger, excel_path)

		processor = DataProcessor(config, logger)
		records = processor.process(excel_path, pdf_paths)
		if args.limit > 0:
			records = records[: args.limit]
			logger.info("Record limit applied: %d", len(records))

		if not records:
			logger.warning("No records found for processing")
			return 0

		npi_enabled = to_bool(config.get("npi", {}).get("enabled", True), default=True)
		if npi_enabled:
			enricher = NPIEnricher(config, logger)
			records = enricher.enrich(records)

		output_dir = Path(config.get("processing", {}).get("output_dir", "./output"))
		if not output_dir.is_absolute():
			output_dir = (Path.cwd() / output_dir).resolve()
		output_dir.mkdir(parents=True, exist_ok=True)

		timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
		normalized_df = processor.records_to_dataframe(records)
		normalized_path = output_dir / f"normalized_orders_{timestamp}.xlsx"
		normalized_df.to_excel(normalized_path, index=False)
		logger.info("Normalized dataset saved: %s", normalized_path)

		dataconnect_enabled = _dataconnect_enabled(config)
		firestore_enabled = _firestore_enabled(config)
		api_sync_enabled = to_bool(config.get("medisync", {}).get("sync_enabled", True), default=True)

		if args.dry_run or (not dataconnect_enabled and not firestore_enabled and api_sync_enabled is False):
			summary = {
				"run_time": datetime.now(timezone.utc).isoformat(),
				"mode": "dry-run",
				"total_records": len(records),
				"valid_records": int(normalized_df["is_valid"].sum()) if "is_valid" in normalized_df else 0,
				"invalid_records": int((~normalized_df["is_valid"]).sum()) if "is_valid" in normalized_df else 0,
				"normalized_output": str(normalized_path),
			}
			summary_path = output_dir / f"run_summary_{timestamp}.json"
			save_json(summary_path, summary)
			logger.info("Dry-run summary saved: %s", summary_path)
			return 0

		db_verification = None

		if dataconnect_enabled:
			store = DataConnectStore(config, logger)
			results = store.sync_records(
				records,
				source_df=processor.last_source_df,
				source_file_name=Path(excel_path).name,
			)
			db_verification = store.last_sync_report
			mode = "dataconnect-sync"
		elif firestore_enabled:
			store = FirestoreMongoStore(config, logger)
			results = store.sync_records(
				records,
				source_df=processor.last_source_df,
				source_file_name=Path(excel_path).name,
			)
			db_verification = store.last_sync_report
			mode = "firestore-sync"
		else:
			client = MediSyncClient(config, logger)
			client.authenticate()
			results = client.sync_records(records)
			mode = "api-sync"

		results_df = pd.DataFrame([result.to_dict() for result in results])

		result_path = output_dir / f"sync_results_{timestamp}.xlsx"
		results_df.to_excel(result_path, index=False)

		success_count = int((results_df["status"] == "success").sum()) if not results_df.empty else 0
		failed_count = int((results_df["status"] == "failed").sum()) if not results_df.empty else 0

		summary = {
			"run_time": datetime.now(timezone.utc).isoformat(),
			"mode": mode,
			"total_records": len(records),
			"success_count": success_count,
			"failed_count": failed_count,
			"normalized_output": str(normalized_path),
			"sync_output": str(result_path),
		}
		if db_verification is not None:
			summary["db_verification"] = db_verification
		summary_path = output_dir / f"run_summary_{timestamp}.json"
		save_json(summary_path, summary)

		logger.info("Sync completed | success=%d | failed=%d", success_count, failed_count)
		logger.info("Summary saved: %s", summary_path)
		return 0
	except Exception as error:
		logger.exception("Pipeline failed: %s", error)
		return 1


def _dataconnect_enabled(config) -> bool:
	section = config.get("dataconnect", {})
	if to_bool(section.get("enabled", False), default=False):
		return True
	env_enabled = (os.getenv("MEDISYNC_DATACONNECT_ENABLED") or "").strip()
	if env_enabled and to_bool(env_enabled, default=False):
		return True
	return False


def _firestore_enabled(config) -> bool:
	section = config.get("firestore", {})
	if to_bool(section.get("enabled", False), default=False):
		return True
	if section.get("mongo_uri"):
		return True
	env_uri = (os.getenv("FIRESTORE_MONGO_URI") or "").strip()
	if env_uri:
		return True
	return False


def _normalized_workflow_mode(config) -> str:
	mode = normalize_text(config.get("extraction", {}).get("workflow_mode", "orders")).lower()
	return mode or "orders"


def _run_profile_pipeline(
	args: argparse.Namespace,
	config: Dict[str, Any],
	logger,
	excel_path: str,
) -> int:
	source_df = _read_profile_table(excel_path)
	if source_df.empty:
		logger.warning("Extracted patient profile table is empty: %s", excel_path)
		return 0

	profile_rows = _rows_to_profile_payload(source_df)
	if args.limit > 0:
		profile_rows = profile_rows[: args.limit]
		logger.info("Profile row limit applied: %d", len(profile_rows))

	if not profile_rows:
		logger.warning("No patient profile rows found for processing")
		return 0

	output_dir = Path(config.get("processing", {}).get("output_dir", "./output"))
	if not output_dir.is_absolute():
		output_dir = (Path.cwd() / output_dir).resolve()
	output_dir.mkdir(parents=True, exist_ok=True)

	timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
	normalized_df = pd.DataFrame(profile_rows)
	normalized_path = output_dir / f"normalized_patient_profiles_{timestamp}.xlsx"
	normalized_df.to_excel(normalized_path, index=False)
	logger.info("Normalized profile dataset saved: %s", normalized_path)

	dataconnect_enabled = _dataconnect_enabled(config)
	firestore_enabled = _firestore_enabled(config)
	api_sync_enabled = to_bool(config.get("medisync", {}).get("sync_enabled", True), default=True)

	if args.dry_run or (not dataconnect_enabled and not firestore_enabled):
		if api_sync_enabled and not dataconnect_enabled and not firestore_enabled and not args.dry_run:
			logger.warning("Profile workflow requires DB sync backend; DB is not configured, writing dry-run summary")

		summary = {
			"run_time": datetime.now(timezone.utc).isoformat(),
			"mode": "profile-dry-run",
			"workflow": "patient_profiles",
			"total_profiles": len(profile_rows),
			"normalized_output": str(normalized_path),
		}
		summary_path = output_dir / f"run_summary_{timestamp}.json"
		save_json(summary_path, summary)
		logger.info("Profile dry-run summary saved: %s", summary_path)
		return 0

	if dataconnect_enabled:
		store = DataConnectStore(config, logger)
		results = store.sync_patient_profiles(
			profile_rows,
			source_df=source_df,
			source_file_name=Path(excel_path).name,
		)
		sync_mode = "dataconnect-profile-sync"
	else:
		store = FirestoreMongoStore(config, logger)
		results = store.sync_patient_profiles(
			profile_rows,
			source_df=source_df,
			source_file_name=Path(excel_path).name,
		)
		sync_mode = "firestore-profile-sync"

	results_df = pd.DataFrame([result.to_dict() for result in results])
	result_path = output_dir / f"sync_results_profiles_{timestamp}.xlsx"
	results_df.to_excel(result_path, index=False)

	success_count = int((results_df["status"] == "success").sum()) if not results_df.empty else 0
	failed_count = int((results_df["status"] == "failed").sum()) if not results_df.empty else 0

	summary = {
		"run_time": datetime.now(timezone.utc).isoformat(),
		"mode": sync_mode,
		"workflow": "patient_profiles",
		"total_profiles": len(profile_rows),
		"success_count": success_count,
		"failed_count": failed_count,
		"normalized_output": str(normalized_path),
		"sync_output": str(result_path),
		"db_verification": store.last_sync_report,
	}
	summary_path = output_dir / f"run_summary_{timestamp}.json"
	save_json(summary_path, summary)

	logger.info("Profile sync completed | success=%d | failed=%d", success_count, failed_count)
	logger.info("Summary saved: %s", summary_path)
	return 0


def _read_profile_table(file_path: str) -> pd.DataFrame:
	source = Path(file_path)
	if not source.exists():
		raise FileNotFoundError(f"Patient profile export file not found: {source}")

	if source.suffix.lower() == ".csv":
		dataframe = pd.read_csv(source)
	else:
		dataframe = pd.read_excel(source)

	dataframe.columns = [normalize_text(col) for col in dataframe.columns]
	return dataframe


def _rows_to_profile_payload(dataframe: pd.DataFrame) -> List[Dict[str, Any]]:
	alias_map = {
		"patient_name": ["Patient Name", "patient_name", "Patient", "name"],
		"mrn": ["MRN", "mrn", "Medical Record Number", "Medical Record"],
		"episode": ["Episode", "episode", "Episode Number", "Episode ID"],
		"dob": ["DOB", "dob", "Date of Birth"],
		"gender": ["Gender", "gender", "Sex"],
		"email": ["Email", "email", "E-mail"],
		"ssn": ["SSN", "ssn", "Social Security", "Social Security Number"],
		"phone": ["Phone", "phone", "Patient Phone", "Contact"],
		"address": ["Address", "address", "Street", "Street Address"],
		"city": ["City", "city"],
		"state": ["State", "state"],
		"zip": ["Zip", "ZIP", "zip", "Postal", "Postal Code"],
		"marital_status": ["Marital Status", "marital_status"],
		"primary_language": ["Primary Language", "primary_language", "Language"],
		"insurance": ["Insurance", "insurance", "Payer", "Primary Insurance"],
		"emergency_contact": ["Emergency Contact", "emergency_contact", "Responsible Party"],
		"allergies": ["Allergies", "allergies", "Allergy"],
		"primary_physician": ["Primary Physician", "primary_physician", "Physician", "Physician Name"],
		"diagnoses": ["Diagnoses", "diagnoses", "Diagnosis", "Primary Diagnosis"],
		"diagnosis_codes": ["Diagnosis Codes", "diagnosis_codes"],
		"clinic": ["Clinic", "clinic", "Agency", "Location"],
		"source_initial": ["Source Initial", "source_initial", "Initial"],
		"profile_url": ["Profile URL", "profile_url"],
		"profile_extracted_at": ["Profile Extracted At", "profile_extracted_at"],
		"profile_data_path": ["Profile Data Path", "profile_data_path"],
		"profile_html_path": ["Profile HTML Path", "profile_html_path"],
		"profile_text_preview": ["Profile Text Preview", "profile_text_preview"],
	}

	def pick_value(row_data: Dict[str, Any], aliases: List[str]) -> str:
		for alias in aliases:
			if alias in row_data:
				value = normalize_text(row_data.get(alias))
				if value:
					return value
		return ""

	rows: List[Dict[str, Any]] = []
	for idx, row in dataframe.iterrows():
		row_data = row.to_dict()

		patient_name = pick_value(row_data, alias_map["patient_name"])
		mrn = pick_value(row_data, alias_map["mrn"])
		if not patient_name and not mrn:
			continue

		profile_data_path = pick_value(row_data, alias_map["profile_data_path"])
		profile_html_path = pick_value(row_data, alias_map["profile_html_path"])
		profile_text_preview = pick_value(row_data, alias_map["profile_text_preview"])

		profile_artifact = _load_profile_artifact(profile_data_path)
		profile_pairs = profile_artifact.get("profile_pairs", []) if isinstance(profile_artifact, dict) else []
		profile_text = normalize_text(profile_artifact.get("profile_text")) if isinstance(profile_artifact, dict) else ""
		profile_url = pick_value(row_data, alias_map["profile_url"])
		if not profile_url and isinstance(profile_artifact, dict):
			profile_url = normalize_text(profile_artifact.get("profile_url"))
		parsed_fields = profile_artifact.get("parsed_fields", {}) if isinstance(profile_artifact, dict) else {}

		def artifact_or_row(field_key: str, row_value: str) -> str:
			if isinstance(parsed_fields, dict):
				artifact_value = normalize_text(parsed_fields.get(field_key))
				if artifact_value:
					return artifact_value
			return normalize_text(row_value)

		profile_row = {
			"source_row": int(idx) + 2,
			"patient_name": patient_name,
			"mrn": mrn,
			"episode": pick_value(row_data, alias_map["episode"]),
			"dob": parse_date(artifact_or_row("dob", pick_value(row_data, alias_map["dob"]))),
			"gender": artifact_or_row("gender", pick_value(row_data, alias_map["gender"])),
			"email": artifact_or_row("email", pick_value(row_data, alias_map["email"])),
			"ssn": artifact_or_row("ssn", pick_value(row_data, alias_map["ssn"])),
			"phone": artifact_or_row("phone", pick_value(row_data, alias_map["phone"])),
			"address": artifact_or_row("address", pick_value(row_data, alias_map["address"])),
			"city": artifact_or_row("city", pick_value(row_data, alias_map["city"])),
			"state": artifact_or_row("state", pick_value(row_data, alias_map["state"])),
			"zip": artifact_or_row("zip", pick_value(row_data, alias_map["zip"])),
			"marital_status": artifact_or_row("marital_status", pick_value(row_data, alias_map["marital_status"])),
			"primary_language": artifact_or_row("primary_language", pick_value(row_data, alias_map["primary_language"])),
			"insurance": artifact_or_row("insurance", pick_value(row_data, alias_map["insurance"])),
			"emergency_contact": artifact_or_row(
				"emergency_contact",
				pick_value(row_data, alias_map["emergency_contact"]),
			),
			"allergies": artifact_or_row("allergies", pick_value(row_data, alias_map["allergies"])),
			"primary_physician": artifact_or_row(
				"primary_physician",
				pick_value(row_data, alias_map["primary_physician"]),
			),
			"diagnoses": artifact_or_row("diagnoses", pick_value(row_data, alias_map["diagnoses"])),
			"diagnosis_codes": artifact_or_row(
				"diagnosis_codes",
				pick_value(row_data, alias_map["diagnosis_codes"]),
			),
			"clinic": pick_value(row_data, alias_map["clinic"]),
			"source_initial": pick_value(row_data, alias_map["source_initial"]),
			"profile_url": profile_url,
			"profile_extracted_at": pick_value(row_data, alias_map["profile_extracted_at"]),
			"profile_data_path": profile_data_path,
			"profile_html_path": profile_html_path,
			"profile_text_preview": profile_text_preview or (profile_text[:5000] if profile_text else ""),
			"profile_pairs": profile_pairs,
			"profile_text": profile_text,
		}
		rows.append(profile_row)

	return rows


def _load_profile_artifact(raw_path: str) -> Dict[str, Any]:
	path_text = normalize_text(raw_path)
	if not path_text:
		return {}

	path = Path(path_text)
	if not path.is_absolute():
		path = (Path.cwd() / path).resolve()
	if not path.exists():
		return {}

	payload = load_json(path, default={})
	if not isinstance(payload, dict):
		return {}
	return payload


def _get_extraction_artifacts(args: argparse.Namespace, config, logger) -> tuple[str, List[str]]:
	if args.skip_extraction:
		excel_path = args.excel_path
		if not excel_path:
			excel_path = _latest_export(config)
			if not excel_path:
				raise ValueError("--skip-extraction requires --excel-path or an existing export file in download_dir")

		pdf_dir = Path(args.pdf_dir) if args.pdf_dir else None
		if pdf_dir and not pdf_dir.is_absolute():
			pdf_dir = (Path.cwd() / pdf_dir).resolve()

		pdf_paths = []
		if pdf_dir and pdf_dir.exists():
			pdf_paths = [str(path) for path in sorted(pdf_dir.glob("*.pdf"))]
		else:
			default_dir = Path(config.get("processing", {}).get("download_dir", "./downloads"))
			if not default_dir.is_absolute():
				default_dir = (Path.cwd() / default_dir).resolve()
			pdf_paths = [str(path) for path in sorted(default_dir.glob("*.pdf"))]

		return str(Path(excel_path).resolve()), pdf_paths

	extractor = KinnserExtractor(config, logger)
	result = extractor.run()
	return result.excel_path, result.pdf_paths


def _latest_export(config) -> str:
	download_dir = Path(config.get("processing", {}).get("download_dir", "./downloads"))
	if not download_dir.is_absolute():
		download_dir = (Path.cwd() / download_dir).resolve()
	if not download_dir.exists():
		return ""

	candidates = []
	for ext in ("*.xlsx", "*.xls", "*.csv"):
		candidates.extend(download_dir.glob(ext))
	if not candidates:
		return ""
	latest = sorted(candidates, key=lambda item: item.stat().st_mtime, reverse=True)[0]
	return str(latest)


if __name__ == "__main__":
	sys.exit(main())
