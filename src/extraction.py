"""Kinnser Selenium extraction layer."""

from __future__ import annotations

import base64
import json
import logging
import os
import re
import time
from dataclasses import dataclass
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple
from urllib.parse import parse_qs, urljoin, urlparse

import pandas as pd
from PyPDF2 import PdfReader
import requests
from selenium import webdriver
from selenium.common.exceptions import ElementClickInterceptedException, TimeoutException, WebDriverException
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.support.ui import Select, WebDriverWait
from webdriver_manager.chrome import ChromeDriverManager

from .utils import normalize_text, parse_date, safe_filename, to_bool


DEFAULT_SELECTORS: Dict[str, Dict[str, str]] = {
	"username_input": {"by": "id", "value": "username"},
	"password_input": {"by": "id", "value": "password"},
	"login_button": {"by": "id", "value": "login_btn"},
	"go_to_menu": {"by": "class_name", "value": "menuButton"},
	"orders_manager_link": {
		"by": "xpath",
		"value": "//a[contains(@class,'menuitem') and contains(.,'Orders Manager')]",
	},
	"patient_manager_link": {
		"by": "xpath",
		"value": "//a[contains(@class,'menuitem') and contains(.,'Patient Manager')]",
	},
	"patient_initial_link": {"by": "id", "value": "{letter}"},
	"patient_rows": {
		"by": "xpath",
		"value": "//table[contains(@class,'taskBoxTable')]//tbody[@id='sortTable1']/tr",
	},
	"patient_name_link": {"by": "xpath", "value": ".//td[contains(@class,'patientCol1')]/a"},
	"patient_mrn_cell": {"by": "xpath", "value": "./td[2]"},
	"patient_episode_cell": {"by": "xpath", "value": "./td[3]"},
	"patient_orders_tab": {"by": "id", "value": "LinkOrders"},
	"patient_view_menu": {
		"by": "xpath",
		"value": "//a[contains(@class,'menuButton') and contains(normalize-space(.),'View')]",
	},
	"patient_profile_link": {
		"by": "xpath",
		"value": "//a[contains(@class,'menuitem') and contains(normalize-space(.),'Patient Profile')]",
	},
	"patient_order_rows": {
		"by": "xpath",
		"value": "//table[@id='scheduled-task']//tbody/tr",
	},
	"patient_doc_name_cell": {"by": "xpath", "value": "./td[1]"},
	"patient_assigned_cell": {"by": "xpath", "value": "./td[2]"},
	"patient_target_date_cell": {"by": "xpath", "value": "./td[3]"},
	"patient_status_cell": {"by": "xpath", "value": "./td[4]"},
	"patient_print_view_link": {
		"by": "xpath",
		"value": "./td[5]//a[contains(normalize-space(.),'Print View')]",
	},
	"received_tab": {
		"by": "xpath",
		"value": "//*[self::a or self::button or self::span][contains(normalize-space(.),'Received')]",
	},
	"run_report_button": {
		"by": "xpath",
		"value": "//*[self::a or self::button or self::input][contains(translate(normalize-space(.), 'ABCDEFGHIJKLMNOPQRSTUVWXYZ', 'abcdefghijklmnopqrstuvwxyz'),'run report') or @value='Run Report']",
	},
	"export_all_data_button": {
		"by": "xpath",
		"value": "//*[self::a or self::button or self::input][contains(translate(normalize-space(.), 'ABCDEFGHIJKLMNOPQRSTUVWXYZ', 'abcdefghijklmnopqrstuvwxyz'),'export all data') or @value='Export all data']",
	},
	"export_button": {"by": "css", "value": "button[type='submit'], input[type='submit']"},
}


BY_MAP = {
	"id": By.ID,
	"xpath": By.XPATH,
	"css": By.CSS_SELECTOR,
	"class_name": By.CLASS_NAME,
	"name": By.NAME,
	"link_text": By.LINK_TEXT,
	"partial_link_text": By.PARTIAL_LINK_TEXT,
}


PROFILE_WORKFLOW_MODES = {"profile", "profiles", "patient_profile", "patient_profiles"}
PATIENT_MANAGER_WORKFLOW_MODES = {"patient", "patients", "patient_manager"}
PROFILE_THEN_ORDERS_WORKFLOW_MODES = {
	"patient_profiles_then_orders",
	"patient_profiles_and_orders",
	"patient_profiles_with_orders",
	"profile_then_orders",
	"profiles_then_orders",
}


@dataclass
class ExtractionResult:
	excel_path: str
	pdf_paths: List[str]
	download_dir: str
	started_at: str
	completed_at: str


class KinnserExtractor:
	"""Automates Kinnser login and data export using Selenium."""

	def __init__(self, config: Dict[str, Any], logger: logging.Logger):
		self.config = config
		self.logger = logger
		self.kinnser_cfg = config.get("kinnser", {})
		self.extract_cfg = config.get("extraction", {})
		self.processing_cfg = config.get("processing", {})
		self.selectors = {**DEFAULT_SELECTORS, **self.kinnser_cfg.get("selectors", {})}
		self.download_dir = self._resolve_path(self.processing_cfg.get("download_dir", "./downloads"))
		self.timeout = int(self.extract_cfg.get("wait_timeout_seconds", 25))

	def run(self) -> ExtractionResult:
		"""Execute extraction and return downloaded artifact paths."""
		started_at = datetime.utcnow().isoformat()
		self.download_dir.mkdir(parents=True, exist_ok=True)

		if to_bool(self.extract_cfg.get("clean_download_dir", True), default=True):
			self._clear_download_dir()

		if to_bool(self.extract_cfg.get("dry_run", False), default=False):
			self.logger.info("Extraction dry-run mode enabled; using existing files in %s", self.download_dir)
			excel_path = self._find_latest_file([".xlsx", ".xls", ".csv"])
			if not excel_path:
				raise RuntimeError("No Excel/CSV files found in download directory for dry-run mode")
			pdf_paths = self._collect_files([".pdf"])
			return ExtractionResult(
				excel_path=str(excel_path),
				pdf_paths=[str(path) for path in pdf_paths],
				download_dir=str(self.download_dir),
				started_at=started_at,
				completed_at=datetime.utcnow().isoformat(),
			)

		driver = self._build_driver()
		download_started_at = time.time()
		try:
			self._login(driver)

			# Optional quick code-level switch:
			# workflow_mode = "orders"
			# workflow_mode = "patients"
			workflow_mode = normalize_text(self.extract_cfg.get("workflow_mode", "orders")).lower() or "orders"

			if workflow_mode in PROFILE_THEN_ORDERS_WORKFLOW_MODES:
				profile_excel_path, _ = self._run_patient_profiles_workflow(driver)
				self.logger.info(
					"Patient profile extraction completed. Continuing with patient order document download in same browser session"
				)
				_, patient_pdf_paths = self._run_patient_manager_workflow(driver)
				excel_path, pdf_paths = profile_excel_path, patient_pdf_paths
			elif workflow_mode in PROFILE_WORKFLOW_MODES:
				excel_path, pdf_paths = self._run_patient_profiles_workflow(driver)
			elif workflow_mode in PATIENT_MANAGER_WORKFLOW_MODES:
				excel_path, pdf_paths = self._run_patient_manager_workflow(driver)
			else:
				excel_path, pdf_paths = self._run_orders_manager_workflow(driver, download_started_at)

			self.logger.info(
				"Extraction completed. Excel: %s | PDFs: %d",
				excel_path,
				len(pdf_paths),
			)

			return ExtractionResult(
				excel_path=str(excel_path),
				pdf_paths=[str(path) for path in pdf_paths],
				download_dir=str(self.download_dir),
				started_at=started_at,
				completed_at=datetime.utcnow().isoformat(),
			)
		finally:
			driver.quit()

	def _run_orders_manager_workflow(self, driver: webdriver.Chrome, download_started_at: float) -> Tuple[Path, List[Path]]:
		self._navigate_to_received_orders(driver)
		self._configure_export(driver)
		self._trigger_export(driver)

		excel_path = self._wait_for_file([".xlsx", ".xls", ".csv"], download_started_at)
		if not excel_path:
			raise RuntimeError("Excel export did not complete before timeout")

		self._download_order_pdfs_if_configured(driver)
		pdf_paths = self._collect_files([".pdf"])
		return excel_path, pdf_paths

	def _run_patient_manager_workflow(self, driver: webdriver.Chrome) -> Tuple[Path, List[Path]]:
		self.logger.info("Running Patient Manager workflow (A-Z)")
		self._navigate_to_patient_manager(driver)

		clinic_name = normalize_text(self.kinnser_cfg.get("agency_name")) or "Kinnser"
		docs_dir = self.download_dir / "patient_documents"
		docs_dir.mkdir(parents=True, exist_ok=True)

		max_patients_per_letter = int(self.extract_cfg.get("patient_max_per_letter", 0))
		pause_between_letters = float(self.extract_cfg.get("patient_letter_pause_seconds", 1.0))
		document_types = self._document_types_filter()

		rows: List[Dict[str, Any]] = []
		pdf_paths: List[Path] = []

		for letter in self._alphabet_letters():
			patients = self._collect_patients_for_initial(
				driver,
				letter,
				pause_between_letters=pause_between_letters,
			)
			if max_patients_per_letter > 0:
				patients = patients[:max_patients_per_letter]

			if not patients:
				self.logger.info("No patients found for initial '%s'", letter)
				continue

			self.logger.info("Initial '%s' -> processing %d patients", letter, len(patients))
			for patient in patients:
				patient_docs = self._collect_patient_documents(
					driver,
					patient,
					docs_dir,
					document_types,
					clinic_name,
				)
				for doc in patient_docs:
					rows.append(doc)
					pdf_path = normalize_text(doc.get("PDF Path"))
					if pdf_path:
						pdf_paths.append(Path(pdf_path))

		if not rows:
			raise RuntimeError("Patient Manager workflow did not produce any document rows")

		df = pd.DataFrame(rows)
		timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
		excel_path = self.download_dir / f"patient_orders_{timestamp}.xlsx"
		df.to_excel(excel_path, index=False)

		self.logger.info(
			"Patient Manager export generated | rows=%d | excel=%s | pdfs=%d",
			len(df),
			excel_path,
			len(pdf_paths),
		)
		return excel_path, pdf_paths

	def _run_patient_profiles_workflow(self, driver: webdriver.Chrome) -> Tuple[Path, List[Path]]:
		self.logger.info("Running Patient Profile workflow (A-Z)")
		self._navigate_to_patient_manager(driver)

		clinic_name = normalize_text(self.kinnser_cfg.get("agency_name")) or "Kinnser"
		max_patients_per_letter = int(self.extract_cfg.get("patient_max_per_letter", 0))
		pause_between_letters = float(self.extract_cfg.get("patient_letter_pause_seconds", 1.0))

		profiles_by_key: Dict[str, Dict[str, Any]] = {}

		for letter in self._alphabet_letters():
			patients = self._collect_patients_for_initial(
				driver,
				letter,
				pause_between_letters=pause_between_letters,
			)
			if max_patients_per_letter > 0:
				patients = patients[:max_patients_per_letter]

			if not patients:
				self.logger.info("No patients found for initial '%s'", letter)
				continue

			self.logger.info("Initial '%s' -> extracting profiles for %d patients", letter, len(patients))
			for patient in patients:
				profile_row = self._collect_patient_profile(driver, patient, clinic_name)
				if not profile_row:
					continue

				profile_key = (
					normalize_text(profile_row.get("MRN")).lower()
					or normalize_text(profile_row.get("Patient Name")).lower()
					or normalize_text(patient.get("Patient Name")).lower()
				)
				if not profile_key:
					profile_key = f"{letter}-{len(profiles_by_key) + 1}"

				existing = profiles_by_key.get(profile_key)
				if existing:
					for field, value in profile_row.items():
						if not normalize_text(existing.get(field)) and normalize_text(value):
							existing[field] = value
				else:
					profiles_by_key[profile_key] = profile_row

		rows = list(profiles_by_key.values())
		if not rows:
			raise RuntimeError("Patient Profile workflow did not produce any profile rows")

		df = pd.DataFrame(rows)
		preferred_columns = [
			"Patient Name",
			"MRN",
			"Episode",
			"DOB",
			"Gender",
			"Email",
			"SSN",
			"Phone",
			"Address",
			"City",
			"State",
			"Zip",
			"Marital Status",
			"Primary Language",
			"Insurance",
			"Emergency Contact",
			"Allergies",
			"Primary Physician",
			"Diagnoses",
			"Diagnosis Codes",
			"Clinic",
			"Source Initial",
			"Profile URL",
			"Profile Extracted At",
			"Profile Data Path",
			"Profile HTML Path",
			"Profile Text Preview",
			"Profile Pairs Count",
		]
		existing_columns = [column for column in preferred_columns if column in df.columns]
		if existing_columns:
			df = df.reindex(columns=existing_columns + [column for column in df.columns if column not in existing_columns])

		timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
		excel_path = self.download_dir / f"patient_profiles_{timestamp}.xlsx"
		df.to_excel(excel_path, index=False)

		self.logger.info(
			"Patient Profile export generated | rows=%d | excel=%s",
			len(df),
			excel_path,
		)
		return excel_path, []

	def _collect_patient_profile(
		self,
		driver: webdriver.Chrome,
		patient: Dict[str, str],
		clinic_name: str,
	) -> Dict[str, Any]:
		list_window = self._current_or_first_window(driver)
		if not list_window:
			self.logger.warning("No active browser window before opening patient profile: %s", patient.get("Patient Name"))
			return {}

		try:
			patient_window = self._open_patient_in_new_tab(driver, patient)
		except Exception as error:
			self.logger.warning("Failed to open patient tab for profile '%s': %s", patient.get("Patient Name"), error)
			self._switch_to_best_window(driver, list_window)
			return {}

		profile_row: Dict[str, Any] = {}
		try:
			if patient_window not in self._window_handles(driver):
				self.logger.warning("Patient window closed before profile extraction: %s", patient.get("Patient Name"))
				return {}

			driver.switch_to.window(patient_window)
			time.sleep(float(self.extract_cfg.get("patient_page_wait_seconds", 2.0)))

			if not self._open_patient_profile_view(driver):
				self.logger.warning("Could not open Patient Profile view for '%s'", patient.get("Patient Name"))
				return {}

			time.sleep(float(self.extract_cfg.get("patient_profile_wait_seconds", 1.5)))
			profile = self._extract_profile_data(driver)

			patient_name = normalize_text(profile.get("patient_name")) or normalize_text(patient.get("Patient Name"))
			mrn = normalize_text(profile.get("mrn")) or normalize_text(patient.get("MRN"))
			episode = normalize_text(profile.get("episode")) or normalize_text(patient.get("Episode"))
			dob = parse_date(profile.get("dob")) if normalize_text(profile.get("dob")) else ""
			profile_data_path, profile_html_path = self._save_profile_artifacts(
				patient_name=patient_name,
				mrn=mrn,
				profile_payload=profile,
			)

			profile_row = {
				"Patient Name": patient_name,
				"MRN": mrn,
				"Episode": episode,
				"DOB": dob,
				"Gender": normalize_text(profile.get("gender")),
				"Email": normalize_text(profile.get("email")),
				"SSN": normalize_text(profile.get("ssn")),
				"Phone": normalize_text(profile.get("phone")),
				"Address": normalize_text(profile.get("address")),
				"City": normalize_text(profile.get("city")),
				"State": normalize_text(profile.get("state")),
				"Zip": normalize_text(profile.get("zip")),
				"Marital Status": normalize_text(profile.get("marital_status")),
				"Primary Language": normalize_text(profile.get("primary_language")),
				"Insurance": normalize_text(profile.get("insurance")),
				"Emergency Contact": normalize_text(profile.get("emergency_contact")),
				"Allergies": normalize_text(profile.get("allergies")),
				"Primary Physician": normalize_text(profile.get("primary_physician")),
				"Diagnoses": normalize_text(profile.get("diagnoses")),
				"Diagnosis Codes": normalize_text(profile.get("diagnosis_codes")),
				"Clinic": clinic_name,
				"Source Initial": normalize_text(patient.get("Source Initial")),
				"Profile URL": normalize_text(driver.current_url),
				"Profile Extracted At": datetime.now().strftime("%m/%d/%Y %H:%M:%S"),
				"Profile Data Path": profile_data_path,
				"Profile HTML Path": profile_html_path,
				"Profile Text Preview": normalize_text(profile.get("profile_text_preview")),
				"Profile Pairs Count": len(profile.get("profile_pairs") or []),
			}
		except Exception as error:
			self.logger.warning("Patient profile extraction failed for '%s': %s", patient.get("Patient Name"), error)
			profile_row = {}
		finally:
			handles = self._window_handles(driver)
			if patient_window in handles:
				try:
					driver.switch_to.window(patient_window)
					driver.close()
				except Exception:
					pass

			self._switch_to_best_window(driver, list_window)

		return profile_row

	def _open_patient_profile_view(self, driver: webdriver.Chrome) -> bool:
		# Some tenants expose Patient Profile directly without opening the View menu first.
		if self._click_if_present(driver, "patient_profile_link"):
			return True

		if self._click_if_present(driver, "patient_view_menu"):
			time.sleep(0.5)
			if self._click_if_present(driver, "patient_profile_link"):
				return True

		fallback_clicks = [
			(By.XPATH, "//a[contains(@class,'menuButton') and contains(normalize-space(.),'View')]|//button[contains(normalize-space(.),'View')]|//span[contains(normalize-space(.),'View')]/ancestor::*[self::a or self::button][1]"),
			(By.XPATH, "//a[contains(normalize-space(.),'Patient Profile')] | //button[contains(normalize-space(.),'Patient Profile')]"),
		]

		for locator in fallback_clicks:
			try:
				element = WebDriverWait(driver, 2).until(EC.element_to_be_clickable(locator))
				try:
					element.click()
				except ElementClickInterceptedException:
					driver.execute_script("arguments[0].click();", element)
				time.sleep(0.5)
			except Exception:
				continue

		try:
			select_element = driver.find_element(
				By.XPATH,
				"//select[contains(translate(@id,'ABCDEFGHIJKLMNOPQRSTUVWXYZ','abcdefghijklmnopqrstuvwxyz'),'view') or contains(translate(@name,'ABCDEFGHIJKLMNOPQRSTUVWXYZ','abcdefghijklmnopqrstuvwxyz'),'view')]",
			)
			select = Select(select_element)
			for option in select.options:
				if "patient profile" in normalize_text(option.text).lower():
					select.select_by_visible_text(option.text)
					time.sleep(0.5)
					return True
		except Exception:
			pass

		# Final check after fallback clicks.
		try:
			WebDriverWait(driver, 2).until(
				lambda d: "patient profile" in normalize_text(d.page_source).lower()
			)
			return True
		except Exception:
			return False

	def _extract_profile_data(self, driver: webdriver.Chrome) -> Dict[str, Any]:
		pairs = self._collect_profile_pairs(driver)
		deduped_pairs = self._dedupe_profile_pairs(pairs)
		normalized_pairs = [
			(self._normalize_document_label(label), normalize_text(value))
			for label, value in pairs
			if normalize_text(label) and normalize_text(value)
		]

		def _pick_from_pairs(tokens: List[str]) -> str:
			for label, value in normalized_pairs:
				if any(token in label for token in tokens):
					return value
			return ""

		try:
			body_text = driver.find_element(By.TAG_NAME, "body").text or ""
		except Exception:
			body_text = ""

		profile_lines = [normalize_text(line) for line in body_text.splitlines() if normalize_text(line)]

		try:
			profile_html = driver.page_source or ""
		except Exception:
			profile_html = ""

		mrn = _pick_from_pairs(["mrn", "medical record"])
		dob = _pick_from_pairs(["date of birth", "dob", "birth date"])
		episode = _pick_from_pairs(["episode"])
		phone = _pick_from_pairs(["phone", "telephone", "mobile", "cell"])
		primary_physician = _pick_from_pairs(["primary physician", "attending physician", "physician", "doctor"])
		diagnoses = _pick_from_pairs(["diagnos", "icd", "dx"])
		address = _pick_from_pairs(["address", "street"])
		city = _pick_from_pairs(["city"])
		state = _pick_from_pairs(["state"])
		zip_code = _pick_from_pairs(["zip", "postal"])
		patient_name = _pick_from_pairs(["patient name", "name"])
		gender = _pick_from_pairs(["gender", "sex"])
		email = _pick_from_pairs(["email", "e mail"])
		ssn = _pick_from_pairs(["ssn", "social security"])
		marital_status = _pick_from_pairs(["marital status"])
		primary_language = _pick_from_pairs(["language"])
		insurance = _pick_from_pairs(["insurance", "payer", "medicare", "medicaid", "plan"])
		emergency_contact = _pick_from_pairs(["emergency contact", "responsible party", "contact person", "caregiver"])
		allergies = _pick_from_pairs(["allerg"])

		mrn = self._extract_first_pattern(
			f"{mrn}\n{body_text}",
			[
				r"\b(?:MRN|Medical\s+Record(?:\s+Number|\s+No)?)\b\s*[:#-]?\s*([A-Za-z0-9-]+)",
			],
		) or normalize_text(mrn)

		dob = self._extract_first_pattern(
			f"{dob}\n{body_text}",
			[
				r"\b(?:DOB|Date\s+of\s+Birth|Birth\s+Date)\b\s*[:#-]?\s*(\d{1,2}[/-]\d{1,2}[/-]\d{2,4})",
				r"\b(\d{1,2}[/-]\d{1,2}[/-]\d{2,4})\s*\(\d{1,3}\)\s*(?:Male|Female)\b",
			],
		)

		episode = self._extract_first_pattern(
			f"{episode}\n{body_text}",
			[
				r"\bEpisode(?:\s*(?:No|#|Number|ID))?\b\s*[:#-]?\s*([A-Za-z0-9-]+)",
			],
		) or normalize_text(episode)

		phone = self._extract_first_pattern(
			f"{phone}\n{body_text}",
			[
				r"Address\s+Phone\s+Triage\s+Code\s+Referral\s+Date\s+[^\n\r]+?\s+(\(?\d{3}\)?[-\s\.]?\d{3}[-\s\.]?\d{4})",
				r"\b(?:Phone|Telephone|Tel|Mobile|Cell)\b\s*[:#-]?\s*(\(?\d{3}\)?[-\s\.]?\d{3}[-\s\.]?\d{4})",
			],
		) or normalize_text(phone)

		context_phone = self._extract_patient_phone_from_lines(profile_lines)
		if context_phone:
			phone = context_phone

		primary_physician = self._extract_first_pattern(
			f"{primary_physician}\n{body_text}",
			[
				r"\bPrimary\s+Physician\b\s*[:#-]?\s*([A-Za-z][A-Za-z .,'-]{2,80})",
				r"\bAttending\s+Physician\b\s*[:#-]?\s*([A-Za-z][A-Za-z .,'-]{2,80})",
				r"\bDr\.?\s*([A-Z][A-Za-z'.-]+(?:,\s*[A-Z][A-Za-z'.-]+)?)",
			],
		) or normalize_text(primary_physician)

		primary_diag = self._extract_first_pattern(
			body_text,
			[
				r"Primary\s+Diagnosis\s*:\s*(.+?)\s+Secondary\s+Diagnosis\s*:",
			],
		)
		secondary_diag = self._extract_first_pattern(
			body_text,
			[
				r"Secondary\s+Diagnosis\s*:\s*(.+?)\s+Primary\s+Clinician",
			],
		)
		if primary_diag and secondary_diag:
			diagnoses = f"Primary: {primary_diag} | Secondary: {secondary_diag}"
		elif primary_diag:
			diagnoses = f"Primary: {primary_diag}"
		elif not diagnoses:
			diagnoses = self._extract_first_pattern(
				body_text,
				[
					r"\b(?:Diagnoses|Diagnosis|Dx)\b\s*[:#-]?\s*([^\n\r]{3,220})",
				],
			)

		address_source = f"{normalize_text(address)}\n{body_text}"
		address_line = self._extract_first_pattern(
			address_source,
			[
				r"Referral\s+Date\s+(.+?)\s+\(?\d{3}\)?[-\s\.]?\d{3}[-\s\.]?\d{4}",
				r"Address\s+Phone\s+Triage\s+Code\s+Referral\s+Date\s+(.+?)\s+\(?\d{3}\)?[-\s\.]?\d{3}[-\s\.]?\d{4}",
				r"\b(\d{1,6}\s+[A-Za-z0-9 .#'/-]+\s+[A-Za-z .'-]+\s+[A-Z]{2}\s+\d{5}(?:-\d{4})?)\b",
			],
		)
		if normalize_text(address_line).lower() in {"", "relationship"}:
			address_line = normalize_text(address)

		if not city:
			city = self._extract_first_pattern(body_text, [r"\bCity\b\s*[:#-]?\s*([A-Za-z .'-]{2,80})"])
		if len(normalize_text(state)) != 2:
			state = ""
		if not state:
			state = self._extract_first_pattern(body_text, [r"\b([A-Z]{2})\s+\d{5}(?:-\d{4})?\b"])
		if not re.match(r"^\d{5}(?:-\d{4})?$", normalize_text(zip_code)):
			zip_code = ""
		if not zip_code:
			zip_code = self._extract_first_pattern(body_text, [r"\b(\d{5}(?:-\d{4})?)\b"])

		location_match = re.search(
			r"(.+?)\s+([A-Z]{2})\s+(\d{5}(?:-\d{4})?)$",
			normalize_text(address_line),
		)
		if location_match:
			street_and_city = normalize_text(location_match.group(1))
			state = normalize_text(location_match.group(2)).upper() or state
			zip_code = normalize_text(location_match.group(3)) or zip_code

			parts = street_and_city.split()
			if parts:
				inferred_city = parts[-1]
				inferred_address = " ".join(parts[:-1])
				city = normalize_text(city) or normalize_text(inferred_city)
				address_line = normalize_text(inferred_address) or street_and_city

		address = normalize_text(address_line) or normalize_text(address)
		if city and (not address or address.lower() == city.lower() or len(address.split()) <= 2):
			street_line = self._extract_first_pattern(
				body_text,
				[
					r"\n(\d{1,6}\s+[^\n\r]{3,120})\n\s*[A-Za-z .'-]+\s+[A-Z]{2}\s+\d{5}(?:-\d{4})?",
				],
			)
			if street_line:
				address = street_line
			elif city and state and zip_code:
				address = f"{city}, {state} {zip_code}"

		if diagnoses:
			diagnoses = re.split(
				r"\b(?:Primary\s+Clinician|Frequencies:|Hospitalization/ER\s+Risk\s+Overview|Start\s+of\s+Care:)\b",
				normalize_text(diagnoses),
				maxsplit=1,
				flags=re.IGNORECASE,
			)[0]
			diagnoses = normalize_text(diagnoses)
			if re.fullmatch(r"primary\s*diagnosis\s*:?\s*secondary\s*diagnosis\s*:?,?", diagnoses.lower()):
				diagnoses = ""

		if not patient_name:
			patient_name = self._extract_first_pattern(
				body_text,
				[
					r"\bPatient\s+Name\b\s*[:#-]?\s*([^\n\r]{3,120})",
				],
			)

		gender = self._extract_first_pattern(
			f"{gender}\n{body_text}",
			[
				r"\b(?:Gender|Sex)\b\s*[:#-]?\s*(Male|Female|Other|Unknown)",
				r"\b(Male|Female|Other|Unknown)\b",
			],
		) or normalize_text(gender)

		email = self._extract_first_pattern(
			f"{email}\n{body_text}",
			[
				r"\b([A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,})\b",
			],
		) or normalize_text(email)

		ssn = self._extract_first_pattern(
			f"{ssn}\n{body_text}",
			[
				r"\b(?:SSN|Social\s+Security(?:\s+Number)?)\b\s*[:#-]?\s*(\d{3}-\d{2}-\d{4})",
			],
		) or normalize_text(ssn)

		marital_status = self._extract_first_pattern(
			f"{marital_status}\n{body_text}",
			[
				r"\bMarital\s+Status\b\s*[:#-]?\s*([A-Za-z ]{3,40})",
			],
		) or normalize_text(marital_status)

		primary_language = self._extract_first_pattern(
			f"{primary_language}\n{body_text}",
			[
				r"\b(?:Primary\s+Language|Language)\b\s*[:#-]?\s*([A-Za-z ]{3,40})",
			],
		) or normalize_text(primary_language)

		if not insurance:
			insurance = self._extract_first_pattern(
				body_text,
				[
					r"\b(?:Insurance|Payer|Primary\s+Insurance)\b\s*[:#-]?\s*([^\n\r]{3,120})",
				],
			)

		if not insurance or "primary insurance secondary insurance tertiary insurance" in insurance.lower():
			insurance_line = self._extract_section_value_from_lines(
				profile_lines,
				marker_tokens=["insurance"],
				skip_tokens=[
					"primary insurance secondary insurance tertiary insurance",
					"policy number",
					"insurance",
				],
			)
			if insurance_line:
				insurance = insurance_line

		if not emergency_contact:
			emergency_contact = self._extract_first_pattern(
				body_text,
				[
					r"\b(?:Emergency\s+Contact|Responsible\s+Party)\b\s*[:#-]?\s*([^\n\r]{3,120})",
				],
			)

		if not emergency_contact or "address phone relationship" in emergency_contact.lower():
			emergency_line = self._extract_section_value_from_lines(
				profile_lines,
				marker_tokens=["emergency contact"],
				skip_tokens=["address phone relationship", "address", "phone", "relationship"],
			)
			if emergency_line:
				emergency_contact = emergency_line

		if not allergies:
			allergies = self._extract_first_pattern(
				body_text,
				[
					r"\b(?:Allergies|Allergy)\b\s*[:#-]?\s*([^\n\r]{3,180})",
				],
			)

		diagnosis_codes = self._extract_diagnosis_codes(body_text)

		if self._looks_like_invalid_physician(primary_physician):
			primary_physician = ""

		parsed_field_pairs = self._dedupe_profile_pairs(
			[
				("Patient Name", patient_name),
				("MRN", mrn),
				("DOB", parse_date(dob) if normalize_text(dob) else ""),
				("Gender", gender),
				("Email", email),
				("SSN", ssn),
				("Phone", self._normalize_phone_number(phone)),
				("Episode", episode),
				("Address", address),
				("City", city),
				("State", normalize_text(state).upper()),
				("Zip", zip_code),
				("Marital Status", marital_status),
				("Primary Language", primary_language),
				("Insurance", insurance),
				("Emergency Contact", emergency_contact),
				("Allergies", allergies),
				("Primary Physician", primary_physician),
				("Diagnoses", diagnoses),
				("Diagnosis Codes", diagnosis_codes),
			]
		)

		if deduped_pairs:
			existing_labels = {
				normalize_text(pair.get("label")).lower()
				for pair in deduped_pairs
				if isinstance(pair, dict)
			}
			for pair in parsed_field_pairs:
				label_key = normalize_text(pair.get("label")).lower()
				if label_key in existing_labels:
					continue
				deduped_pairs.append(pair)
		else:
			deduped_pairs = parsed_field_pairs

		profile_text_preview = normalize_text(body_text)
		if len(profile_text_preview) > 5000:
			profile_text_preview = profile_text_preview[:5000] + " ..."

		return {
			"patient_name": normalize_text(patient_name),
			"mrn": normalize_text(mrn),
			"dob": parse_date(dob) if normalize_text(dob) else "",
			"episode": normalize_text(episode),
			"gender": normalize_text(gender),
			"email": normalize_text(email),
			"ssn": normalize_text(ssn),
			"phone": self._normalize_phone_number(phone),
			"marital_status": normalize_text(marital_status),
			"primary_language": normalize_text(primary_language),
			"insurance": normalize_text(insurance),
			"emergency_contact": normalize_text(emergency_contact),
			"allergies": normalize_text(allergies),
			"primary_physician": normalize_text(primary_physician),
			"diagnoses": normalize_text(diagnoses),
			"diagnosis_codes": diagnosis_codes,
			"address": normalize_text(address),
			"city": normalize_text(city),
			"state": normalize_text(state).upper(),
			"zip": normalize_text(zip_code),
			"profile_pairs": deduped_pairs,
			"profile_text": body_text,
			"profile_text_preview": profile_text_preview,
			"profile_html": profile_html,
			"profile_title": normalize_text(getattr(driver, "title", "")),
			"profile_url": normalize_text(driver.current_url),
		}

	def _save_profile_artifacts(
		self,
		*,
		patient_name: str,
		mrn: str,
		profile_payload: Dict[str, Any],
	) -> Tuple[str, str]:
		try:
			artifacts_dir = self.download_dir / "patient_profiles_data"
			artifacts_dir.mkdir(parents=True, exist_ok=True)

			timestamp = datetime.now().strftime("%Y%m%d_%H%M%S_%f")
			base_name = f"{safe_filename(patient_name or 'patient')}__{safe_filename(mrn or 'unknown')}__{timestamp}"
			html_path = artifacts_dir / f"{base_name}.html"
			json_path = artifacts_dir / f"{base_name}.json"

			profile_html = normalize_text(profile_payload.get("profile_html"))
			if not profile_html:
				profile_html = "<html><body></body></html>"
			html_path.write_text(profile_html, encoding="utf-8")

			artifact_payload = {
				"patient_name": normalize_text(patient_name),
				"mrn": normalize_text(mrn),
				"profile_url": normalize_text(profile_payload.get("profile_url")),
				"profile_title": normalize_text(profile_payload.get("profile_title")),
				"extracted_at": datetime.utcnow().isoformat(),
				"parsed_fields": {
					"dob": normalize_text(profile_payload.get("dob")),
					"gender": normalize_text(profile_payload.get("gender")),
					"email": normalize_text(profile_payload.get("email")),
					"ssn": normalize_text(profile_payload.get("ssn")),
					"phone": normalize_text(profile_payload.get("phone")),
					"episode": normalize_text(profile_payload.get("episode")),
					"address": normalize_text(profile_payload.get("address")),
					"city": normalize_text(profile_payload.get("city")),
					"state": normalize_text(profile_payload.get("state")),
					"zip": normalize_text(profile_payload.get("zip")),
					"marital_status": normalize_text(profile_payload.get("marital_status")),
					"primary_language": normalize_text(profile_payload.get("primary_language")),
					"insurance": normalize_text(profile_payload.get("insurance")),
					"emergency_contact": normalize_text(profile_payload.get("emergency_contact")),
					"allergies": normalize_text(profile_payload.get("allergies")),
					"primary_physician": normalize_text(profile_payload.get("primary_physician")),
					"diagnoses": normalize_text(profile_payload.get("diagnoses")),
					"diagnosis_codes": normalize_text(profile_payload.get("diagnosis_codes")),
				},
				"profile_pairs": profile_payload.get("profile_pairs") or [],
				"profile_text": normalize_text(profile_payload.get("profile_text")),
				"profile_html_path": str(html_path),
			}

			json_path.write_text(json.dumps(artifact_payload, ensure_ascii=True, indent=2), encoding="utf-8")
			return str(json_path), str(html_path)
		except Exception as error:
			self.logger.warning("Failed to save patient profile artifacts for '%s': %s", patient_name, error)
			return "", ""

	@staticmethod
	def _dedupe_profile_pairs(pairs: List[Tuple[str, str]], limit: int = 300) -> List[Dict[str, str]]:
		seen = set()
		results: List[Dict[str, str]] = []
		for label, value in pairs:
			clean_label = normalize_text(label)
			clean_value = normalize_text(value)
			if not clean_label or not clean_value:
				continue

			key = (re.sub(r"\s+", " ", clean_label).lower(), clean_value.lower())
			if key in seen:
				continue
			seen.add(key)

			results.append(
				{
					"label": clean_label[:120],
					"value": clean_value[:1000],
				}
			)
			if len(results) >= limit:
				break

		return results

	@staticmethod
	def _extract_diagnosis_codes(text: str) -> str:
		codes = re.findall(r"\b[A-TV-Z][0-9][0-9A-Z](?:\.[0-9A-Z]{1,4})?\b", normalize_text(text).upper())
		if not codes:
			return ""
		ordered: List[str] = []
		seen = set()
		for code in codes:
			if code in seen:
				continue
			seen.add(code)
			ordered.append(code)
			if len(ordered) >= 30:
				break
		return ", ".join(ordered)

	def _collect_profile_pairs(self, driver: webdriver.Chrome) -> List[Tuple[str, str]]:
		try:
			raw_pairs = driver.execute_script(
				"""
				const out = [];
				const normalize = (value) => (value || '').replace(/\\s+/g, ' ').trim();
				const pushPair = (label, value) => {
				  const cleanLabel = normalize(label);
				  const cleanValue = normalize(value);
				  if (!cleanLabel || !cleanValue) return;
				  out.push([cleanLabel, cleanValue]);
				};

				document.querySelectorAll('table tr').forEach((row) => {
				  const cells = Array.from(row.querySelectorAll('th,td'))
				    .map((cell) => normalize(cell.textContent))
				    .filter(Boolean);
				  if (cells.length >= 2) {
				    pushPair(cells[0], cells.slice(1).join(' '));
				  }
				});

				document.querySelectorAll('dl').forEach((dl) => {
				  const terms = dl.querySelectorAll('dt');
				  terms.forEach((term) => {
				    const next = term.nextElementSibling;
				    if (next && next.tagName && next.tagName.toLowerCase() === 'dd') {
				      pushPair(term.textContent, next.textContent);
				    }
				  });
				});

				document.querySelectorAll('label[for]').forEach((label) => {
				  const targetId = label.getAttribute('for');
				  if (!targetId) return;
				  const target = document.getElementById(targetId);
				  if (!target) return;
				  const value = target.value || target.textContent || '';
				  pushPair(label.textContent, value);
				});

				const lines = normalize((document.body && document.body.innerText) || '').split(/\n+/);
				lines.forEach((line) => {
				  const cleanLine = normalize(line);
				  if (!cleanLine || cleanLine.length < 5) return;
				  const idx = cleanLine.indexOf(':');
				  if (idx <= 0 || idx >= cleanLine.length - 1) return;
				  const label = cleanLine.slice(0, idx);
				  const value = cleanLine.slice(idx + 1);
				  pushPair(label, value);
				});

				return out;
				"""
			) or []
		except Exception:
			return []

		pairs: List[Tuple[str, str]] = []
		for item in raw_pairs:
			if not isinstance(item, (list, tuple)) or len(item) < 2:
				continue
			label = normalize_text(item[0])
			value = normalize_text(item[1])
			if not label or not value:
				continue
			pairs.append((label, value))
		return pairs

	@staticmethod
	def _extract_first_pattern(text: str, patterns: List[str]) -> str:
		for pattern in patterns:
			match = re.search(pattern, text, flags=re.IGNORECASE | re.MULTILINE)
			if match:
				return normalize_text(match.group(1))
		return ""

	@staticmethod
	def _extract_section_value_from_lines(
		lines: List[str],
		marker_tokens: List[str],
		skip_tokens: Optional[List[str]] = None,
		lookahead: int = 10,
	) -> str:
		skip_tokens = [normalize_text(token).lower() for token in (skip_tokens or []) if normalize_text(token)]
		markers = [normalize_text(token).lower() for token in marker_tokens if normalize_text(token)]
		if not markers:
			return ""

		for index, line in enumerate(lines):
			line_lower = normalize_text(line).lower()
			if not any(marker in line_lower for marker in markers):
				continue

			for next_index in range(index + 1, min(index + 1 + lookahead, len(lines))):
				candidate = normalize_text(lines[next_index])
				if not candidate:
					continue

				candidate_lower = candidate.lower()
				if any(token and token in candidate_lower for token in skip_tokens):
					continue
				if len(candidate) <= 1:
					continue
				return candidate

		return ""

	@staticmethod
	def _extract_patient_phone_from_lines(lines: List[str]) -> str:
		phone_pattern = r"\(?\d{3}\)?[-\s\.]?\d{3}[-\s\.]?\d{4}"

		for index, line in enumerate(lines):
			line_lower = normalize_text(line).lower()
			if "address phone triage code referral date" not in line_lower:
				continue

			for next_index in range(index + 1, min(index + 12, len(lines))):
				candidate = normalize_text(lines[next_index])
				if not candidate:
					continue
				if "email address language" in candidate.lower():
					break

				match = re.search(phone_pattern, candidate)
				if match:
					return normalize_text(match.group(0))

		return ""

	@staticmethod
	def _normalize_phone_number(value: str) -> str:
		text = normalize_text(value)
		if not text:
			return ""

		match = re.search(r"\(?\d{3}\)?[-\s\.]?\d{3}[-\s\.]?\d{4}", text)
		if not match:
			return text

		digits = re.sub(r"\D", "", match.group(0))
		if len(digits) == 10:
			return f"({digits[0:3]}) {digits[3:6]}-{digits[6:10]}"
		return match.group(0)

	def _build_driver(self) -> webdriver.Chrome:
		options = Options()
		browser_binary = self._resolve_browser_binary()
		if browser_binary:
			options.binary_location = str(browser_binary)
			self.logger.info("Using browser binary: %s", browser_binary)
		else:
			self.logger.warning("No browser binary override found; using chromedriver default discovery")

		prefs = {
			"download.default_directory": str(self.download_dir),
			"download.prompt_for_download": False,
			"download.directory_upgrade": True,
			"plugins.always_open_pdf_externally": True,
			"safebrowsing.enabled": True,
		}
		options.add_experimental_option("prefs", prefs)
		options.add_argument("--start-maximized")
		options.add_argument("--disable-notifications")
		options.add_argument("--no-sandbox")
		options.add_argument("--disable-dev-shm-usage")

		if to_bool(self.extract_cfg.get("headless", False), default=False):
			options.add_argument("--headless=new")

		try:
			# Prefer Selenium Manager because it resolves a matching driver for the installed browser.
			return webdriver.Chrome(options=options)
		except WebDriverException as error:
			self.logger.warning("Selenium Manager startup failed, falling back to webdriver-manager: %s", error)

		service = Service(ChromeDriverManager().install())
		return webdriver.Chrome(service=service, options=options)

	def _resolve_browser_binary(self) -> Optional[Path]:
		configured = normalize_text(
			self.extract_cfg.get("browser_binary_path") or self.kinnser_cfg.get("browser_binary_path")
		)
		if configured:
			configured_path = Path(configured).expanduser().resolve()
			if configured_path.exists():
				return configured_path
			self.logger.warning("Configured browser_binary_path not found: %s", configured_path)

		candidates = [
			"/Applications/Google Chrome.app/Contents/MacOS/Google Chrome",
			"/Applications/Brave Browser.app/Contents/MacOS/Brave Browser",
			"/Applications/Chromium.app/Contents/MacOS/Chromium",
			"/Applications/Microsoft Edge.app/Contents/MacOS/Microsoft Edge",
			"~/Applications/Google Chrome.app/Contents/MacOS/Google Chrome",
			"~/Applications/Brave Browser.app/Contents/MacOS/Brave Browser",
			"~/Applications/Chromium.app/Contents/MacOS/Chromium",
			"~/Applications/Microsoft Edge.app/Contents/MacOS/Microsoft Edge",
		]

		for candidate in candidates:
			candidate_path = Path(candidate).expanduser()
			if candidate_path.exists():
				return candidate_path.resolve()
		return None

	def _login(self, driver: webdriver.Chrome) -> None:
		login_url = normalize_text(self.kinnser_cfg.get("login_url"))
		username = normalize_text(self.kinnser_cfg.get("username"))
		password = normalize_text(self.kinnser_cfg.get("password"))

		if not login_url or not username or not password:
			raise ValueError("Kinnser credentials or login_url are missing in config.json")

		self.logger.info("Logging into Kinnser: %s", login_url)
		driver.get(login_url)

		self._type(driver, "username_input", username)
		self._type(driver, "password_input", password)
		self._click(driver, "login_button")

		post_login_wait = int(self.extract_cfg.get("post_login_wait_seconds", 8))
		time.sleep(post_login_wait)
		self._dismiss_alert_if_present(driver)

	def _navigate_to_received_orders(self, driver: webdriver.Chrome) -> None:
		self.logger.info("Navigating to Orders Manager / Received")
		self._click(driver, "go_to_menu")
		time.sleep(2)
		self._click(driver, "orders_manager_link")
		time.sleep(4)
		self._click(driver, "received_tab")
		time.sleep(2)

	def _navigate_to_patient_manager(self, driver: webdriver.Chrome) -> None:
		self.logger.info("Navigating to Patient Manager")
		self._click(driver, "go_to_menu")
		time.sleep(2)
		self._click(driver, "patient_manager_link")
		time.sleep(3)

	def _open_patient_letter(self, driver: webdriver.Chrome, letter: str) -> bool:
		locator = self._locator_from_key("patient_initial_link", letter=letter)
		if locator:
			try:
				element = WebDriverWait(driver, 2).until(EC.element_to_be_clickable(locator))
				element.click()
				return True
			except Exception:
				pass

		for fallback in ((By.ID, letter), (By.LINK_TEXT, letter)):
			try:
				element = WebDriverWait(driver, 2).until(EC.element_to_be_clickable(fallback))
				element.click()
				return True
			except Exception:
				continue
		return False

	def _collect_patients_for_initial(
		self,
		driver: webdriver.Chrome,
		letter: str,
		*,
		pause_between_letters: float,
	) -> List[Dict[str, str]]:
		for attempt in range(2):
			if not self._open_patient_letter(driver, letter):
				if attempt == 0:
					self.logger.info(
						"Skipping initial '%s' (letter control not clickable), refreshing and retrying",
						letter,
					)
					try:
						driver.refresh()
					except Exception:
						self._navigate_to_patient_manager(driver)
					time.sleep(max(pause_between_letters, 2.0))
					continue
				return []

			time.sleep(max(pause_between_letters, 2.0 if attempt > 0 else pause_between_letters))
			patients = self._collect_patients_from_current_initial(driver, letter)
			if patients:
				return patients

			if attempt == 0:
				self.logger.info(
					"Initial '%s' returned no rows, refreshing Patient Manager and retrying once",
					letter,
				)
				try:
					driver.refresh()
					self._dismiss_alert_if_present(driver)
				except Exception:
					self._navigate_to_patient_manager(driver)
				time.sleep(max(pause_between_letters, 2.0))

		return []

	def _collect_patients_from_current_initial(self, driver: webdriver.Chrome, letter: str) -> List[Dict[str, str]]:
		rows = driver.execute_script(
			"""
			const rows = Array.from(document.querySelectorAll('tbody#sortTable1 tr'));
			return rows.map((row) => {
			  const link = row.querySelector('td.patientCol1 a') || row.querySelector('a');
			  if (!link) return null;
			  const cells = row.querySelectorAll('td');
			  return {
			    patient_name: (link.textContent || '').trim(),
			    href: link.getAttribute('href') || '',
			    onclick: link.getAttribute('onclick') || '',
			    mrn: cells.length > 1 ? (cells[1].textContent || '').trim() : '',
			    episode: cells.length > 2 ? (cells[2].textContent || '').trim() : '',
			    row_text: (row.textContent || '').trim()
			  };
			}).filter(Boolean);
			"""
		) or []

		patients: List[Dict[str, str]] = []
		for item in rows:
			row_text = normalize_text(item.get("row_text"))
			if "no patient" in row_text.lower():
				continue

			patient_name = normalize_text(item.get("patient_name"))
			if not patient_name:
				continue

			last_name = normalize_text(patient_name.split(",", 1)[0])
			if not last_name:
				continue
			if last_name[0].upper() != letter.upper():
				continue

			patients.append(
				{
					"Patient Name": patient_name,
					"MRN": normalize_text(item.get("mrn")),
					"Episode": normalize_text(item.get("episode")),
					"href": normalize_text(item.get("href")),
					"onclick": normalize_text(item.get("onclick")),
					"Source Initial": letter,
				}
			)
		return patients

	def _collect_patient_documents(
		self,
		driver: webdriver.Chrome,
		patient: Dict[str, str],
		docs_dir: Path,
		document_types: List[str],
		clinic_name: str,
	) -> List[Dict[str, Any]]:
		list_window = self._current_or_first_window(driver)
		if not list_window:
			self.logger.warning("No active browser window before opening patient: %s", patient.get("Patient Name"))
			return []

		try:
			patient_window = self._open_patient_in_new_tab(driver, patient)
		except Exception as error:
			self.logger.warning("Failed to open patient tab for '%s': %s", patient.get("Patient Name"), error)
			self._switch_to_best_window(driver, list_window)
			return []
		doc_rows: List[Dict[str, Any]] = []
		seen_documents = set()
		strict_doc_filter = to_bool(self.extract_cfg.get("patient_strict_document_filter", True), default=True)

		try:
			if patient_window in driver.window_handles:
				driver.switch_to.window(patient_window)
			else:
				self.logger.warning("Patient window closed before processing: %s", patient["Patient Name"])
				return doc_rows
			time.sleep(float(self.extract_cfg.get("patient_page_wait_seconds", 2.0)))

			if not self._open_patient_orders_tab(driver):
				self.logger.warning("Could not open Orders tab for patient: %s", patient["Patient Name"])


			time.sleep(float(self.extract_cfg.get("patient_orders_wait_seconds", 2.0)))
			print_links = self._list_print_view_links(driver)
			if not print_links:
				debug_dir = self.download_dir / "debug"
				debug_dir.mkdir(parents=True, exist_ok=True)
				debug_file = debug_dir / f"no_rows_{safe_filename(patient['Patient Name'])}.png"
				driver.save_screenshot(str(debug_file))
				self.logger.warning("No Print View links found for patient '%s'; screenshot=%s", patient["Patient Name"], debug_file)
				return doc_rows

			self.logger.info("Patient '%s' -> found %d Print View links", patient["Patient Name"], len(print_links))
			default_doc_name = normalize_text(self.extract_cfg.get("patient_default_document_name", "CMS 485")) or "CMS 485"

			for index in range(len(print_links)):
				if patient_window in driver.window_handles:
					try:
						driver.switch_to.window(patient_window)
					except Exception:
						break

				try:
					current_links = self._list_print_view_links(driver)
				except Exception as error:
					self.logger.warning(
						"Unable to read Print View links for patient '%s': %s",
						patient["Patient Name"],
						error,
					)
					break

				if index >= len(current_links):
					break

				print_element = current_links[index]
				raw_row = self._find_parent_row(print_element)

				doc_name = ""
				assigned = ""
				target_date = ""
				status = ""

				if raw_row is not None:
					doc_name = self._safe_row_text(raw_row, "patient_doc_name_cell")
					assigned = self._safe_row_text(raw_row, "patient_assigned_cell")
					target_date = parse_date(self._safe_row_text(raw_row, "patient_target_date_cell"))
					status = self._safe_row_text(raw_row, "patient_status_cell")

					cells = raw_row.find_elements(By.XPATH, "./td")
					if not doc_name and cells:
						doc_name = normalize_text(cells[0].text)
					if not assigned and len(cells) > 1:
						assigned = normalize_text(cells[1].text)
					if not target_date and len(cells) > 2:
						target_date = parse_date(normalize_text(cells[2].text))
					if not status and len(cells) > 3:
						status = normalize_text(cells[3].text)

				if self._looks_like_invalid_physician(assigned):
					assigned = ""

				if doc_name and not self._is_target_document(doc_name, document_types):
					continue
				if not doc_name:
					doc_name = f"Print View Document {index + 1}"

				print_url = normalize_text(print_element.get_attribute("href"))
				onclick = normalize_text(print_element.get_attribute("onclick"))

				order_number = self._extract_order_number(
					f"{print_url} {onclick}",
					patient["Patient Name"],
					doc_name,
					index + 1,
				)
				pdf_path = ""
				resolved_doc_name = doc_name
				try:
					if print_url and not print_url.lower().startswith("javascript"):
						pdf_file = self._save_print_view_pdf(
							driver,
							print_url,
							docs_dir,
							patient["Patient Name"],
							doc_name,
							order_number,
						)
					else:
						pdf_file = self._save_print_view_pdf_from_click(
							driver,
							print_element,
							docs_dir,
							patient["Patient Name"],
							doc_name,
							order_number,
						)

					resolved_doc_name = self._infer_document_name_from_pdf(pdf_file, doc_name)
					if not self._is_target_document(resolved_doc_name, document_types):
						if not strict_doc_filter and normalize_text(doc_name).lower().startswith("print view document"):
							resolved_doc_name = default_doc_name
						else:
							try:
								pdf_file.unlink(missing_ok=True)
							except Exception:
								pass
							continue

					doc_key = (
						normalize_text(patient["Patient Name"]).lower(),
						normalize_text(order_number).lower(),
						normalize_text(resolved_doc_name).lower(),
					)
					if doc_key in seen_documents:
						try:
							pdf_file.unlink(missing_ok=True)
						except Exception:
							pass
						continue
					seen_documents.add(doc_key)

					if resolved_doc_name != doc_name:
						renamed = docs_dir / (
							f"{safe_filename(patient['Patient Name'])}__{safe_filename(resolved_doc_name)}__{safe_filename(order_number)}.pdf"
						)
						if renamed != pdf_file:
							pdf_file.rename(renamed)
							pdf_file = renamed

					pdf_path = str(pdf_file)
				except Exception as error:
					self.logger.warning(
						"Failed to export PDF for patient '%s' and document '%s': %s",
						patient["Patient Name"],
						doc_name,
						error,
					)

				doc_rows.append(
					{
						"Order #": order_number,
						"Patient Name": patient["Patient Name"],
						"MRN": patient["MRN"] or f"AUTO-{safe_filename(patient['Patient Name'])[:20]}",
						"Episode": patient["Episode"],
						"Order Type": resolved_doc_name,
						"Order Date": target_date or datetime.now().strftime("%m/%d/%Y"),
						"Physician": assigned or "UNKNOWN PHYSICIAN",
						"Clinic": clinic_name,
						"PDF Path": pdf_path,
						"Print View URL": print_url or onclick,
						"Document Status": status,
						"Source Initial": patient["Source Initial"],
					}
				)
		except Exception as error:
			self.logger.warning("Patient document extraction failed for '%s': %s", patient["Patient Name"], error)
		finally:
			try:
				handles = list(driver.window_handles)
			except Exception:
				handles = []

			should_close_patient_window = False
			if patient_window in handles:
				# Never close the only remaining tab, otherwise the session loses all windows.
				if len(handles) > 1:
					should_close_patient_window = True
				elif list_window and list_window in handles and list_window != patient_window:
					should_close_patient_window = True

			if should_close_patient_window:
				try:
					driver.switch_to.window(patient_window)
					driver.close()
				except Exception:
					pass

			try:
				handles_after = list(driver.window_handles)
			except Exception:
				handles_after = []

			target = list_window if list_window in handles_after else (handles_after[0] if handles_after else "")
			if target:
				try:
					driver.switch_to.window(target)
					if list_window and list_window not in handles_after:
						# Recover to patient list view when original list tab vanished.
						self._navigate_to_patient_manager(driver)
				except Exception:
					pass

		return doc_rows

	def _infer_document_name_from_pdf(self, pdf_file: Path, default_name: str) -> str:
		try:
			reader = PdfReader(str(pdf_file))
			text_blob = "\n".join((page.extract_text() or "") for page in reader.pages[:3]).lower()
		except Exception:
			return default_name

		normalized_blob = re.sub(r"[^a-z0-9]+", " ", text_blob)

		if (
			"cms 485" in normalized_blob
			or "cms485" in normalized_blob
			or "home health certification and plan of care" in normalized_blob
			or "certification and plan of care" in normalized_blob
		):
			return "CMS 485"
		if "physician order" in normalized_blob:
			return "Physician Order"
		return default_name

	def _open_patient_in_new_tab(self, driver: webdriver.Chrome, patient: Dict[str, str]) -> str:
		href = normalize_text(patient.get("href"))
		if href and not href.lower().startswith("javascript"):
			return self._open_url_in_new_tab(driver, href)

		handles_before = set(driver.window_handles)
		name = normalize_text(patient.get("Patient Name"))
		if not name:
			raise RuntimeError("Patient name missing while opening patient page")

		name_literal = self._xpath_literal(name)
		locator = (
			By.XPATH,
			"//table[contains(@class,'taskBoxTable')]//tbody[@id='sortTable1']//a[normalize-space(.)=" + name_literal + "]",
		)
		link = WebDriverWait(driver, self.timeout).until(EC.element_to_be_clickable(locator))
		driver.execute_script("arguments[0].setAttribute('target','_blank');", link)
		link.click()

		WebDriverWait(driver, self.timeout).until(lambda d: len(d.window_handles) > len(handles_before))
		for handle in driver.window_handles:
			if handle not in handles_before:
				return handle
		raise RuntimeError(f"Failed to open patient tab for {name}")

	def _open_patient_orders_tab(self, driver: webdriver.Chrome) -> bool:
		if self._click_if_present(driver, "patient_orders_tab"):
			return True

		fallback = (
			By.XPATH,
			"//*[self::a or self::button or self::span][contains(normalize-space(.),'Orders')]",
		)
		try:
			element = WebDriverWait(driver, 2).until(EC.element_to_be_clickable(fallback))
			element.click()
			return True
		except Exception:
			return False

	def _list_patient_order_rows(self, driver: webdriver.Chrome):
		rows_locator = self._locator_from_key("patient_order_rows")
		if not rows_locator:
			return []
		return [row for row in driver.find_elements(*rows_locator) if normalize_text(row.text)]

	def _list_patient_order_rows_fallback(self, driver: webdriver.Chrome):
		rows = driver.find_elements(By.XPATH, "//a[contains(normalize-space(.),'Print View')]/ancestor::tr[1]")
		return [row for row in rows if normalize_text(row.text)]

	def _list_print_view_links(self, driver: webdriver.Chrome):
		return driver.find_elements(
			By.XPATH,
			"//a[contains(translate(normalize-space(.), 'ABCDEFGHIJKLMNOPQRSTUVWXYZ', 'abcdefghijklmnopqrstuvwxyz'),'print view')]",
		)

	@staticmethod
	def _find_parent_row(element):
		try:
			return element.find_element(By.XPATH, "./ancestor::tr[1]")
		except Exception:
			return None

	def _is_target_document(self, doc_name: str, document_types: List[str]) -> bool:
		name = self._normalize_document_label(doc_name)
		if not name:
			return False

		if "*" in document_types:
			return True

		for token in document_types:
			normalized_token = self._normalize_document_label(token)
			if normalized_token and normalized_token in name:
				return True

		# CMS-485 pages often use long form labels without exact token match.
		if "cms 485" in document_types:
			if "certification" in name and "plan of care" in name:
				return True
		return False

	@staticmethod
	def _normalize_document_label(value: str) -> str:
		label = normalize_text(value).lower()
		label = re.sub(r"[^a-z0-9]+", " ", label)
		label = re.sub(r"\s+", " ", label).strip()
		return label

	def _save_print_view_pdf(
		self,
		driver: webdriver.Chrome,
		print_view_url: str,
		output_dir: Path,
		patient_name: str,
		document_name: str,
		order_number: str,
	) -> Path:
		current_window = self._current_or_first_window(driver)
		if not current_window:
			raise RuntimeError("No active browser window before opening Print View URL")

		new_window = self._open_url_in_new_tab(driver, print_view_url)
		opened_new_window = new_window != current_window

		try:
			driver.switch_to.window(new_window)
			filename = (
				f"{safe_filename(patient_name)}__{safe_filename(document_name)}__{safe_filename(order_number)}.pdf"
			)
			pdf_path = output_dir / filename
			return self._capture_pdf_from_active_tab(driver, pdf_path, preferred_url=print_view_url)
		finally:
			handles = self._window_handles(driver)
			if opened_new_window and new_window in handles:
				try:
					driver.switch_to.window(new_window)
					driver.close()
				except Exception:
					pass

			self._switch_to_best_window(driver, current_window)

	def _save_print_view_pdf_from_click(
		self,
		driver: webdriver.Chrome,
		print_element,
		output_dir: Path,
		patient_name: str,
		document_name: str,
		order_number: str,
	) -> Path:
		current_window = self._current_or_first_window(driver)
		if not current_window:
			raise RuntimeError("No active browser window before clicking Print View")

		handles_before = set(driver.window_handles)
		print_element.click()

		opened_new_window = False
		new_window = current_window
		try:
			WebDriverWait(driver, min(self.timeout, 8)).until(lambda d: len(d.window_handles) > len(handles_before))
			new_window = next((h for h in driver.window_handles if h not in handles_before), current_window)
			opened_new_window = new_window != current_window
		except TimeoutException:
			# Some tenants navigate in-place instead of opening a new tab.
			new_window = self._current_or_first_window(driver) or current_window
			opened_new_window = new_window != current_window

		try:
			driver.switch_to.window(new_window)
			filename = (
				f"{safe_filename(patient_name)}__{safe_filename(document_name)}__{safe_filename(order_number)}.pdf"
			)
			pdf_path = output_dir / filename
			return self._capture_pdf_from_active_tab(driver, pdf_path)
		finally:
			handles = self._window_handles(driver)
			if opened_new_window and new_window in handles:
				try:
					driver.switch_to.window(new_window)
					driver.close()
				except Exception:
					pass

			self._switch_to_best_window(driver, current_window)

	@staticmethod
	def _window_handles(driver: webdriver.Chrome) -> List[str]:
		try:
			return list(driver.window_handles)
		except Exception:
			return []

	def _current_or_first_window(self, driver: webdriver.Chrome) -> str:
		try:
			return driver.current_window_handle
		except Exception:
			handles = self._window_handles(driver)
			return handles[0] if handles else ""

	def _switch_to_best_window(self, driver: webdriver.Chrome, preferred_handle: str) -> bool:
		handles = self._window_handles(driver)
		if not handles:
			return False

		target = preferred_handle if preferred_handle in handles else handles[0]
		try:
			driver.switch_to.window(target)
			return True
		except Exception:
			return False

	def _capture_pdf_from_active_tab(self, driver: webdriver.Chrome, pdf_path: Path, preferred_url: str = "") -> Path:
		wait_seconds = float(self.extract_cfg.get("patient_print_open_wait_seconds", 1.5))
		time.sleep(wait_seconds)

		candidate_urls: List[str] = []
		if normalize_text(preferred_url):
			candidate_urls.append(normalize_text(preferred_url))

		current_url = normalize_text(driver.current_url)
		if current_url:
			candidate_urls.append(current_url)

		try:
			extra_urls = driver.execute_script(
				"""
				const attrs = [];
				document.querySelectorAll('iframe[src], embed[src], object[data]').forEach((el) => {
				  const value = el.getAttribute('src') || el.getAttribute('data') || '';
				  if (!value) return;
				  try {
				    attrs.push(new URL(value, window.location.href).href);
				  } catch (e) {
				    attrs.push(value);
				  }
				});
				return attrs;
				"""
			) or []
			candidate_urls.extend([normalize_text(url) for url in extra_urls if normalize_text(url)])
		except Exception:
			pass

		seen = set()
		ordered_urls: List[str] = []
		for raw_url in candidate_urls:
			url = normalize_text(raw_url)
			if not url or url.lower().startswith("javascript"):
				continue
			if not self._is_likely_pdf_candidate(url):
				continue
			if url in seen:
				continue
			seen.add(url)
			ordered_urls.append(url)

		ordered_urls = ordered_urls[:4]

		for url in ordered_urls:
			try:
				pdf_bytes = self._download_pdf_bytes_with_session(driver, url)
				if not pdf_bytes:
					continue
				pdf_path.write_bytes(pdf_bytes)
				if self._pdf_file_looks_valid(pdf_path):
					return pdf_path
			except Exception as error:
				self.logger.debug("Direct PDF download attempt failed for %s: %s", url, error)

		# Last resort: browser print capture (can still be blank in some tenants).
		for _ in range(2):
			pdf_payload = driver.execute_cdp_cmd(
				"Page.printToPDF",
				{
					"printBackground": True,
					"preferCSSPageSize": True,
				},
			)
			pdf_path.write_bytes(base64.b64decode(pdf_payload["data"]))
			if self._pdf_file_looks_valid(pdf_path):
				return pdf_path
			time.sleep(1.0)

		return pdf_path

	def _download_pdf_bytes_with_session(self, driver: webdriver.Chrome, url: str) -> bytes:
		session = requests.Session()
		for cookie in driver.get_cookies():
			name = normalize_text(cookie.get("name"))
			value = normalize_text(cookie.get("value"))
			if not name or not value:
				continue

			cookie_kwargs: Dict[str, Any] = {}
			domain = normalize_text(cookie.get("domain"))
			path = normalize_text(cookie.get("path"))
			if domain:
				cookie_kwargs["domain"] = domain
			if path:
				cookie_kwargs["path"] = path
			session.cookies.set(name, value, **cookie_kwargs)

		headers = {}
		try:
			user_agent = normalize_text(driver.execute_script("return navigator.userAgent;"))
			if user_agent:
				headers["User-Agent"] = user_agent
		except Exception:
			pass

		response = session.get(url, headers=headers, timeout=8, allow_redirects=True)
		if normalize_text(response.url) and not self._is_likely_pdf_candidate(response.url):
			return b""
		if response.status_code >= 400:
			return b""

		content_type = normalize_text(response.headers.get("Content-Type")).lower()
		content = response.content
		if "application/pdf" in content_type or content.startswith(b"%PDF"):
			return content

		if "text/html" in content_type:
			html = response.text
			refs = re.findall(r"(?:src|href)=['\"]([^'\"]+)['\"]", html, flags=re.IGNORECASE)
			for ref in refs[:8]:
				next_url = urljoin(response.url, ref)
				if not self._is_likely_pdf_candidate(next_url):
					continue
				if next_url == response.url:
					continue
				try:
					next_response = session.get(next_url, headers=headers, timeout=8, allow_redirects=True)
					next_type = normalize_text(next_response.headers.get("Content-Type")).lower()
					next_content = next_response.content
					if "application/pdf" in next_type or next_content.startswith(b"%PDF"):
						return next_content
				except Exception:
					continue

		return b""

	@staticmethod
	def _is_likely_pdf_candidate(url: str) -> bool:
		candidate = normalize_text(url).lower()
		if not candidate:
			return False

		markers = [
			".pdf",
			"mode=print",
			"setpath",
			"docid",
			"document",
			"print",
		]
		return any(marker in candidate for marker in markers)

	@staticmethod
	def _looks_like_invalid_physician(value: str) -> bool:
		text = normalize_text(value).lower()
		if not text:
			return False
		invalid_tokens = [
			"cms 485",
			"print view document",
			"physician order",
			"face to face",
			"plan of care",
			"log out",
			"alliance",
			"address phone",
			"npi contact",
		]
		if any(token in text for token in invalid_tokens):
			return True
		if re.match(r"^\d+[\W\s]*$", text):
			return True
		if len(re.sub(r"[^a-z]", "", text)) < 4:
			return True
		return False

	def _pdf_file_looks_valid(self, pdf_path: Path) -> bool:
		try:
			if not pdf_path.exists():
				return False

			size = pdf_path.stat().st_size
			min_pdf_bytes = int(self.extract_cfg.get("patient_min_pdf_bytes", 1500))
			if size < min_pdf_bytes:
				return False

			reader = PdfReader(str(pdf_path))
			if not reader.pages:
				return False

			text_sample = "\n".join((page.extract_text() or "") for page in reader.pages[:2])
			if normalize_text(text_sample):
				return True

			# For scanned/image PDFs with no extractable text, accept only when file size is substantial.
			min_image_pdf_bytes = int(self.extract_cfg.get("patient_min_image_pdf_bytes", 10000))
			return size >= min_image_pdf_bytes
		except Exception:
			return False

	def _open_url_in_new_tab(self, driver: webdriver.Chrome, url: str) -> str:
		handles_before = set(driver.window_handles)
		driver.execute_script("window.open(arguments[0], '_blank');", url)
		WebDriverWait(driver, self.timeout).until(lambda d: len(d.window_handles) > len(handles_before))
		for handle in driver.window_handles:
			if handle not in handles_before:
				return handle
		raise RuntimeError("Failed to open new browser tab")

	@staticmethod
	def _xpath_literal(value: str) -> str:
		if "'" not in value:
			return f"'{value}'"
		if '"' not in value:
			return f'"{value}"'
		parts = value.split("'")
		return "concat(" + ", \"'\", ".join([f"'{part}'" for part in parts]) + ")"

	def _extract_order_number(self, url: str, patient_name: str, document_name: str, row_index: int) -> str:
		parsed = urlparse(url)
		query = parse_qs(parsed.query)
		for key in ("docId.id", "docId", "id", "orderId", "orderNo"):
			value = normalize_text(query.get(key, [""])[0])
			if value:
				return value

		digits = re.findall(r"\d{4,}", url)
		if digits:
			return digits[0]

		seed = f"{patient_name}|{document_name}|{row_index}"
		return f"DOC-{safe_filename(seed)[:40]}"

	def _document_types_filter(self) -> List[str]:
		configured = self.extract_cfg.get("patient_document_types", ["CMS 485"])
		if not isinstance(configured, list):
			configured = [configured]
		tokens = [self._normalize_document_label(str(item)) for item in configured if normalize_text(item)]
		if any(token in {"all", "any", "*"} for token in tokens):
			return ["*"]
		return [token for token in tokens if token] or ["cms 485"]

	def _alphabet_letters(self) -> List[str]:
		configured = normalize_text(self.extract_cfg.get("patient_initials", "ABCDEFGHIJKLMNOPQRSTUVWXYZ"))
		letters = [ch.upper() for ch in configured if ch.isalpha()]
		return letters or list("ABCDEFGHIJKLMNOPQRSTUVWXYZ")

	def _safe_row_text(self, row, selector_key: str) -> str:
		locator = self._locator_from_key(selector_key)
		if not locator:
			return ""
		try:
			return normalize_text(row.find_element(*locator).text)
		except Exception:
			return ""

	def _required_row_locator(self, selector_key: str):
		locator = self._locator_from_key(selector_key)
		if not locator:
			raise KeyError(f"Selector not configured: {selector_key}")
		return locator

	def _configure_export(self, driver: webdriver.Chrome) -> None:
		start_date = normalize_text(self.extract_cfg.get("report_start_date", "01/01/2025")) or "01/01/2025"
		end_date_cfg = normalize_text(self.extract_cfg.get("report_end_date", "today")).lower()
		if end_date_cfg in {"", "today", "current_date", "current"}:
			end_date = datetime.now().strftime("%m/%d/%Y")
		else:
			end_date = parse_date(end_date_cfg)

		if not self._set_text_if_present(driver, "start_date_input", start_date):
			raise RuntimeError("Unable to set start date on Received page")
		if not self._set_text_if_present(driver, "end_date_input", end_date):
			raise RuntimeError("Unable to set end date on Received page")

		self.logger.info("Configured Received date range | start=%s | end=%s", start_date, end_date)

	def _trigger_export(self, driver: webdriver.Chrome) -> None:
		self.logger.info("Running report on Received page")
		try:
			self._click(driver, "run_report_button")
		except TimeoutException:
			raise RuntimeError("Run Report button was not clickable on Received page")

		list_wait_seconds = float(self.extract_cfg.get("results_wait_seconds", 5))
		time.sleep(list_wait_seconds)

		self.logger.info("Clicking Export all data")
		try:
			self._click(driver, "export_all_data_button")
			time.sleep(2)
		except TimeoutException:
			self.logger.warning("Export all data button not found; falling back to generic export selector")
			self._click(driver, "export_button")
			time.sleep(2)

	def _download_order_pdfs_if_configured(self, driver: webdriver.Chrome) -> None:
		if not to_bool(self.extract_cfg.get("download_pdfs", True), default=True):
			return

		order_rows_locator = self._locator_from_key("order_rows")
		pdf_link_locator = self._locator_from_key("order_pdf_download_link")
		if not order_rows_locator or not pdf_link_locator:
			self.logger.info("PDF row/link selectors not configured; using already-downloaded PDFs only")
			return

		rows = driver.find_elements(*order_rows_locator)
		max_downloads = int(self.extract_cfg.get("max_pdf_download", 0))
		if max_downloads > 0:
			rows = rows[:max_downloads]

		self.logger.info("Attempting PDF download from %d rows", len(rows))
		for idx, row in enumerate(rows, start=1):
			try:
				links = row.find_elements(*pdf_link_locator)
				if not links:
					continue
				links[0].click()
				time.sleep(float(self.extract_cfg.get("pdf_download_pause_seconds", 1.0)))
			except Exception as error:
				self.logger.warning("Skipping row %d PDF download due to error: %s", idx, error)

	def _dismiss_alert_if_present(self, driver: webdriver.Chrome) -> None:
		try:
			alert = driver.switch_to.alert
			alert.accept()
			self.logger.info("Dismissed post-login browser alert")
			time.sleep(2)
		except Exception:
			pass

	def _select_category_checkbox(self, driver: webdriver.Chrome, label_text: str) -> None:
		if not label_text:
			return

		normalized = normalize_text(label_text)
		xpath = (
			"//label[contains(translate(normalize-space(.), 'ABCDEFGHIJKLMNOPQRSTUVWXYZ', 'abcdefghijklmnopqrstuvwxyz'),"
			f" '{normalized.lower()}')]/preceding-sibling::input[@type='checkbox']"
		)
		try:
			checkbox = WebDriverWait(driver, 2).until(EC.presence_of_element_located((By.XPATH, xpath)))
			if not checkbox.is_selected():
				checkbox.click()
		except Exception:
			self.logger.warning("Could not auto-select category checkbox for '%s'", normalized)

	def _select_export_fields_if_available(self, driver: webdriver.Chrome) -> None:
		export_fields = self.extract_cfg.get("export_fields", [])
		if not export_fields:
			return

		locator = self._locator_from_key("export_fields_select")
		if not locator:
			return

		try:
			element = WebDriverWait(driver, 2).until(EC.presence_of_element_located(locator))
			select = Select(element)
			select.deselect_all()
			for field in export_fields:
				try:
					select.select_by_visible_text(field)
				except Exception:
					self.logger.warning("Export field not found in UI: %s", field)
		except Exception:
			self.logger.warning("Could not interact with export field selector")

	def _type(self, driver: webdriver.Chrome, key: str, value: str) -> None:
		locator = self._locator_from_key(key)
		if not locator:
			raise KeyError(f"Selector not configured: {key}")
		element = WebDriverWait(driver, self.timeout).until(EC.presence_of_element_located(locator))
		element.clear()
		element.send_keys(value)

	def _click(self, driver: webdriver.Chrome, key: str) -> None:
		locator = self._locator_from_key(key)
		if not locator:
			raise KeyError(f"Selector not configured: {key}")
		element = WebDriverWait(driver, self.timeout).until(EC.element_to_be_clickable(locator))
		try:
			element.click()
		except ElementClickInterceptedException:
			self.logger.warning("Click intercepted for '%s'; retrying with JavaScript click", key)
			driver.execute_script("arguments[0].click();", element)

	def _click_if_present(self, driver: webdriver.Chrome, key: str) -> bool:
		locator = self._locator_from_key(key)
		if not locator:
			return False
		try:
			element = WebDriverWait(driver, 2).until(EC.element_to_be_clickable(locator))
			element.click()
			return True
		except Exception:
			return False

	def _set_text_if_present(self, driver: webdriver.Chrome, key: str, value: str) -> bool:
		if not value:
			return False
		locator = self._locator_from_key(key)
		if not locator:
			return False
		try:
			element = WebDriverWait(driver, 2).until(EC.presence_of_element_located(locator))
			driver.execute_script(
				"arguments[0].value = ''; arguments[0].dispatchEvent(new Event('input', {bubbles: true}));",
				element,
			)
			element.clear()
			element.send_keys(value)
			driver.execute_script(
				"arguments[0].dispatchEvent(new Event('input', {bubbles: true}));"
				"arguments[0].dispatchEvent(new Event('change', {bubbles: true}));"
				"arguments[0].blur();",
				element,
			)

			set_value = normalize_text(element.get_attribute("value"))
			if set_value != value:
				driver.execute_script(
					"arguments[0].value = arguments[1];"
					"arguments[0].dispatchEvent(new Event('input', {bubbles: true}));"
					"arguments[0].dispatchEvent(new Event('change', {bubbles: true}));"
					"arguments[0].blur();",
					element,
					value,
				)
				set_value = normalize_text(element.get_attribute("value"))

			return set_value == value
		except Exception:
			return False

	def _locator_from_key(self, key: str, **template_vars: str) -> Optional[Tuple[str, str]]:
		selector = self.selectors.get(key)
		if not selector:
			return None
		if isinstance(selector, str):
			value = selector.format(**template_vars) if template_vars else selector
			return (By.CSS_SELECTOR, value)

		by_name = normalize_text(selector.get("by", "css")).lower()
		value = normalize_text(selector.get("value"))
		if template_vars and value:
			value = value.format(**template_vars)
		if not value:
			return None

		by = BY_MAP.get(by_name)
		if not by:
			raise ValueError(f"Unsupported selector by='{by_name}' for key='{key}'")
		return (by, value)

	def _wait_for_file(self, extensions: List[str], since_ts: float) -> Optional[Path]:
		max_wait = int(self.extract_cfg.get("download_wait_seconds", 180))
		stop_at = time.time() + max_wait
		exts = {ext.lower() for ext in extensions}

		while time.time() < stop_at:
			matches = []
			for path in self.download_dir.iterdir():
				if not path.is_file():
					continue
				if path.suffix.lower() not in exts:
					continue
				if path.name.endswith(".crdownload") or path.name.endswith(".part"):
					continue
				if path.stat().st_mtime >= since_ts:
					matches.append(path)

			if matches:
				return sorted(matches, key=lambda item: item.stat().st_mtime, reverse=True)[0]
			time.sleep(1)
		return None

	def _collect_files(self, extensions: List[str]) -> List[Path]:
		exts = {ext.lower() for ext in extensions}
		files = [
			path
			for path in self.download_dir.iterdir()
			if path.is_file() and path.suffix.lower() in exts and not path.name.endswith(".crdownload")
		]
		return sorted(files, key=lambda item: item.stat().st_mtime, reverse=True)

	def _find_latest_file(self, extensions: List[str]) -> Optional[Path]:
		files = self._collect_files(extensions)
		return files[0] if files else None

	def _clear_download_dir(self) -> None:
		for item in self.download_dir.rglob("*"):
			if item.is_file():
				item.unlink(missing_ok=True)

		# Remove now-empty artifact directories except the root download directory.
		for item in sorted(self.download_dir.rglob("*"), key=lambda p: len(p.parts), reverse=True):
			if item.is_dir() and item != self.download_dir:
				try:
					item.rmdir()
				except OSError:
					pass

	@staticmethod
	def _resolve_path(raw_path: str) -> Path:
		path = Path(raw_path)
		if not path.is_absolute():
			path = (Path.cwd() / path).resolve()
		return path
