#!/usr/bin/env python3
"""Launch MediSync interactive dashboard."""

from __future__ import annotations

import os
from pathlib import Path

import uvicorn

from dashboard.app import app
from src.utils import load_config, normalize_text


def main() -> None:
    default_config = str(Path(__file__).resolve().with_name("config.json"))
    config_path = normalize_text(os.getenv("MEDISYNC_CONFIG")) or default_config
    config = load_config(config_path)
    dashboard_cfg = config.get("dashboard", {})

    host = normalize_text(os.getenv("MEDISYNC_DASHBOARD_HOST")) or normalize_text(dashboard_cfg.get("host")) or "127.0.0.1"
    port_text = normalize_text(os.getenv("MEDISYNC_DASHBOARD_PORT")) or normalize_text(dashboard_cfg.get("port")) or "8787"
    port = int(port_text)

    uvicorn.run(app, host=host, port=port, reload=False)


if __name__ == "__main__":
    main()
