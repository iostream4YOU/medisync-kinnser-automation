import base64
import json
import logging
import re
import time
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Tuple

import requests
from dotenv import load_dotenv

load_dotenv()


DATE_FORMATS = [
    "%m/%d/%Y",
    "%Y-%m-%d",
    "%m-%d-%Y",
    "%m/%d/%y",
    "%Y/%m/%d",
]


def setup_logging(log_dir: str = "logs", log_name: str = "pipeline.log") -> logging.Logger:
    """Set up project logging to both file and stdout."""
    Path(log_dir).mkdir(parents=True, exist_ok=True)

    logger = logging.getLogger("medisync")
    logger.setLevel(logging.INFO)
    logger.handlers.clear()

    formatter = logging.Formatter("%(asctime)s | %(levelname)s | %(message)s")

    file_handler = logging.FileHandler(str(Path(log_dir) / log_name))
    file_handler.setFormatter(formatter)

    stream_handler = logging.StreamHandler()
    stream_handler.setFormatter(formatter)

    logger.addHandler(file_handler)
    logger.addHandler(stream_handler)
    return logger


def load_config(config_path: str = "config.json") -> Dict[str, Any]:
    """Load configuration from JSON and validate required sections."""
    config_file = Path(config_path)
    if not config_file.is_absolute():
        config_file = (Path.cwd() / config_file).resolve()

    if not config_file.exists():
        raise FileNotFoundError(f"Config file not found: {config_file}")

    with config_file.open("r", encoding="utf-8") as file:
        config = json.load(file)

    required_keys = ["kinnser", "medisync", "extraction", "processing", "npi"]
    missing = [key for key in required_keys if key not in config]
    if missing:
        raise ValueError(f"Missing required config sections: {', '.join(missing)}")
    return config


def ensure_dirs(config: Dict[str, Any]) -> Dict[str, Path]:
    """Create all configured runtime directories and return resolved paths."""
    processing = config.get("processing", {})
    dir_keys = ["download_dir", "output_dir", "log_dir", "cache_dir", "temp_dir"]

    resolved: Dict[str, Path] = {}
    for key in dir_keys:
        value = processing.get(key)
        if not value:
            continue
        path = Path(value)
        if not path.is_absolute():
            path = (Path.cwd() / path).resolve()
        path.mkdir(parents=True, exist_ok=True)
        resolved[key] = path
    return resolved


def normalize_text(value: Any) -> str:
    """Convert values to a clean string."""
    if value is None:
        return ""
    text = str(value).strip()
    if text.lower() in {"nan", "none", "null"}:
        return ""
    return text


def safe_filename(value: str) -> str:
    """Convert an arbitrary string to a filesystem-safe filename segment."""
    clean = normalize_text(value)
    clean = re.sub(r"[^A-Za-z0-9._-]+", "_", clean)
    clean = re.sub(r"_+", "_", clean).strip("_.")
    return clean or "unnamed"


def parse_date(value: Any, output_format: str = "%m/%d/%Y") -> str:
    """Parse date-like values using common formats and normalize output."""
    text = normalize_text(value)
    if not text:
        return ""

    for fmt in DATE_FORMATS:
        try:
            return datetime.strptime(text, fmt).strftime(output_format)
        except ValueError:
            continue

    # Keep the original value when parsing fails to avoid data loss.
    return text


def split_patient_name(full_name: str) -> Tuple[str, str, str]:
    """Split patient name into first, middle, last with loose input tolerance."""
    text = normalize_text(full_name)
    if not text:
        return "", "", ""

    if "," in text:
        last, rest = [part.strip() for part in text.split(",", 1)]
        parts = rest.split()
        first = parts[0] if parts else ""
        middle = " ".join(parts[1:]) if len(parts) > 1 else ""
        return first, middle, last

    parts = text.split()
    if len(parts) == 1:
        return parts[0], "", ""
    if len(parts) == 2:
        return parts[0], "", parts[1]
    return parts[0], " ".join(parts[1:-1]), parts[-1]


def extract_digits(value: Any) -> str:
    """Return only numeric characters from input text."""
    return "".join(ch for ch in normalize_text(value) if ch.isdigit())


def pdf_to_base64(pdf_path: str) -> str:
    """Convert PDF file bytes to base64-encoded string."""
    with open(pdf_path, "rb") as pdf_file:
        return base64.b64encode(pdf_file.read()).decode("utf-8")


def deduplicate_patients(df, subset: Optional[List[str]] = None):
    """Deduplicate dataframe rows based on configured patient key columns."""
    subset = subset or ["Patient Name", "MRN"]
    valid_subset = [col for col in subset if col in df.columns]
    if not valid_subset:
        return df
    return df.drop_duplicates(subset=valid_subset, keep="first")


def to_bool(value: Any, default: bool = False) -> bool:
    """Normalize mixed config truthy values to bool."""
    if isinstance(value, bool):
        return value
    text = normalize_text(value).lower()
    if text in {"true", "1", "yes", "y", "on"}:
        return True
    if text in {"false", "0", "no", "n", "off"}:
        return False
    return default


def chunked(items: List[Any], size: int) -> Iterable[List[Any]]:
    """Yield fixed-size list chunks."""
    if size <= 0:
        yield items
        return
    for idx in range(0, len(items), size):
        yield items[idx : idx + size]


def request_with_retries(
    session: requests.Session,
    method: str,
    url: str,
    logger: logging.Logger,
    *,
    retries: int = 3,
    backoff_seconds: float = 2.0,
    timeout: int = 30,
    retry_status_codes: Optional[List[int]] = None,
    **kwargs: Any,
) -> requests.Response:
    """Issue HTTP request with exponential backoff for transient failures."""
    retry_status_codes = retry_status_codes or [408, 429, 500, 502, 503, 504]
    last_error: Optional[Exception] = None

    for attempt in range(1, retries + 1):
        try:
            response = session.request(method=method, url=url, timeout=timeout, **kwargs)
            if response.status_code in retry_status_codes and attempt < retries:
                wait_time = backoff_seconds * (2 ** (attempt - 1))
                logger.warning(
                    "HTTP %s for %s %s; retrying in %.1fs (%d/%d)",
                    response.status_code,
                    method,
                    url,
                    wait_time,
                    attempt,
                    retries,
                )
                time.sleep(wait_time)
                continue

            response.raise_for_status()
            return response
        except requests.RequestException as error:
            last_error = error
            if attempt >= retries:
                break
            wait_time = backoff_seconds * (2 ** (attempt - 1))
            logger.warning(
                "Request error for %s %s (%s); retrying in %.1fs (%d/%d)",
                method,
                url,
                error,
                wait_time,
                attempt,
                retries,
            )
            time.sleep(wait_time)

    raise RuntimeError(f"Request failed after {retries} attempts: {method} {url}") from last_error


def save_json(file_path: Path, payload: Any) -> None:
    """Persist JSON payload to disk with stable formatting."""
    file_path.parent.mkdir(parents=True, exist_ok=True)
    with file_path.open("w", encoding="utf-8") as file:
        json.dump(payload, file, indent=2, ensure_ascii=True)


def load_json(file_path: Path, default: Optional[Any] = None) -> Any:
    """Load JSON payload if it exists, otherwise return default."""
    if not file_path.exists():
        return default
    with file_path.open("r", encoding="utf-8") as file:
        return json.load(file)
