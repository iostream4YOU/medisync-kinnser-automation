"""Firebase Data Connect CLI client wrapper for server-side integrations."""

from __future__ import annotations

import json
import os
import re
import subprocess
import tempfile
from pathlib import Path
from typing import Any, Dict, Optional
from uuid import NAMESPACE_DNS, uuid5

from .utils import load_json, normalize_text, to_bool


class DataConnectClient:
    """Executes Data Connect operations using Firebase CLI."""

    def __init__(self, config: Dict[str, Any], logger):
        self.config = config
        self.logger = logger

        self.workspace_root = Path(__file__).resolve().parents[2]
        self.medisync_root = Path(__file__).resolve().parents[1]

        dc_cfg = config.get("dataconnect", {})
        firestore_cfg = config.get("firestore", {})

        env_enabled = normalize_text(os.getenv("MEDISYNC_DATACONNECT_ENABLED"))
        self.enabled = to_bool(dc_cfg.get("enabled", False), default=False)
        if env_enabled:
            self.enabled = to_bool(env_enabled, default=self.enabled)

        self.project_id = self._resolve_project_id(dc_cfg)
        self.service_id = normalize_text(dc_cfg.get("service_id")) or normalize_text(
            os.getenv("MEDISYNC_DATACONNECT_SERVICE")
        ) or "kinnserpatientorder"
        self.location = normalize_text(dc_cfg.get("location")) or normalize_text(
            os.getenv("MEDISYNC_DATACONNECT_LOCATION")
        ) or "us-east4"

        self.agency_name = normalize_text(dc_cfg.get("agency_name")) or normalize_text(
            firestore_cfg.get("agency_name", "MediSync DA")
        ) or "MediSync DA"
        self.agency_id = normalize_text(dc_cfg.get("agency_id")) or str(
            uuid5(NAMESPACE_DNS, f"medisync-agency:{self.agency_name.lower()}")
        )

        configured_ops = normalize_text(dc_cfg.get("operations_file")) or "dataconnect/example/queries.gql"
        self.operations_file = self._resolve_existing_path(configured_ops)
        if self.operations_file is None:
            raise FileNotFoundError(
                "Data Connect operations file not found. "
                "Set dataconnect.operations_file to an existing .gql file."
            )

    def execute(self, operation_name: str, variables: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
        """Execute an operation from configured operations file and return CLI result payload."""
        if not self.enabled:
            raise RuntimeError("Data Connect is disabled")

        op_name = normalize_text(operation_name)
        if not op_name:
            raise ValueError("operation_name is required")

        command = ["firebase", "-j"]
        if self.project_id:
            command.extend(["--project", self.project_id])
        command.extend(
            [
                "--non-interactive",
                "dataconnect:execute",
                "--service",
                self.service_id,
                "--location",
                self.location,
            ]
        )

        temp_variables_file = None
        if variables is not None:
            variables_json = json.dumps(variables, ensure_ascii=True, separators=(",", ":"), default=str)
            if len(variables_json) > 8000:
                temp_file = tempfile.NamedTemporaryFile(
                    mode="w",
                    encoding="utf-8",
                    suffix=".json",
                    prefix="medisync-dc-vars-",
                    delete=False,
                )
                temp_file.write(variables_json)
                temp_file.flush()
                temp_file.close()
                temp_variables_file = Path(temp_file.name)
                command.extend(["--variables", f"@{temp_variables_file}"])
            else:
                command.extend(["--variables", variables_json])

        command.extend([str(self.operations_file), op_name])

        env = os.environ.copy()
        env["FIREBASE_CLI_NO_COLOR"] = "1"

        try:
            proc = subprocess.run(
                command,
                cwd=str(self.workspace_root),
                env=env,
                capture_output=True,
                text=True,
            )
        finally:
            if temp_variables_file is not None:
                try:
                    temp_variables_file.unlink(missing_ok=True)
                except Exception:
                    pass

        stdout = proc.stdout or ""
        stderr = proc.stderr or ""

        if proc.returncode != 0:
            message = stderr.strip() or stdout.strip() or "Unknown Firebase CLI error"
            raise RuntimeError(
                f"Data Connect operation '{op_name}' failed (exit {proc.returncode}): {message}"
            )

        payload = self._parse_cli_json(stdout)
        status = normalize_text(payload.get("status")).lower()
        if status and status != "success":
            raise RuntimeError(f"Data Connect operation '{op_name}' failed: {payload}")

        result = payload.get("result")
        if isinstance(result, dict):
            return result
        if isinstance(payload, dict):
            return payload
        return {"data": payload}

    def ping(self) -> bool:
        """Quick connectivity validation using a simple query."""
        try:
            result = self.execute("PingAgency", {"agencyId": self.agency_id})
            data = result.get("data") if isinstance(result, dict) else None
            if isinstance(data, dict):
                _ = data.get("agencies", [])
            return True
        except Exception as error:
            self.logger.warning("Data Connect ping failed: %s", error)
            return False

    def _resolve_project_id(self, dc_cfg: Dict[str, Any]) -> str:
        direct = normalize_text(dc_cfg.get("project_id"))
        if direct:
            return direct

        for env_key in (
            "MEDISYNC_DATACONNECT_PROJECT",
            "FIREBASE_PROJECT",
            "FIREBASE_PROJECT_ID",
            "GOOGLE_CLOUD_PROJECT",
        ):
            env_value = normalize_text(os.getenv(env_key))
            if env_value:
                return env_value

        firebase_rc = self.workspace_root / ".firebaserc"
        payload = load_json(firebase_rc, default={}) if firebase_rc.exists() else {}
        if isinstance(payload, dict):
            projects = payload.get("projects")
            if isinstance(projects, dict):
                default_project = normalize_text(projects.get("default"))
                if default_project:
                    return default_project

        return ""

    def _resolve_existing_path(self, raw_path: str) -> Optional[Path]:
        text = normalize_text(raw_path)
        if not text:
            return None

        candidate = Path(text)
        if candidate.is_absolute() and candidate.exists():
            return candidate.resolve()

        roots = [Path.cwd(), self.workspace_root, self.medisync_root]
        for root in roots:
            path = (root / candidate).resolve()
            if path.exists():
                return path
        return None

    @staticmethod
    def _parse_cli_json(output: str) -> Dict[str, Any]:
        text = output.strip()
        if not text:
            return {}

        first_brace = text.find("{")
        if first_brace < 0:
            raise RuntimeError(f"Unable to parse Data Connect CLI output: {output}")

        json_text = text[first_brace:]
        parsed_text = DataConnectClient._first_balanced_json_object(json_text)
        try:
            loaded = json.loads(parsed_text)
        except json.JSONDecodeError as error:
            raise RuntimeError(f"Invalid JSON from Data Connect CLI: {error}\nOutput: {output}") from error

        if isinstance(loaded, dict):
            return loaded
        return {"result": loaded}

    @staticmethod
    def _first_balanced_json_object(text: str) -> str:
        in_string = False
        escape = False
        depth = 0
        start_idx = -1

        for idx, ch in enumerate(text):
            if start_idx < 0:
                if ch == "{":
                    start_idx = idx
                    depth = 1
                continue

            if in_string:
                if escape:
                    escape = False
                    continue
                if ch == "\\":
                    escape = True
                    continue
                if ch == '"':
                    in_string = False
                continue

            if ch == '"':
                in_string = True
                continue
            if ch == "{":
                depth += 1
                continue
            if ch == "}":
                depth -= 1
                if depth == 0:
                    return text[start_idx : idx + 1]

        trimmed = re.sub(r"\s+$", "", text[start_idx:] if start_idx >= 0 else text)
        return trimmed
