import json
import logging
from pathlib import Path
from typing import Any, Dict, Optional, Union

import pandas as pd
import yaml


def setup_logger(name: str, level: int = logging.INFO) -> logging.Logger:
    logger = logging.getLogger(name)
    if not logger.handlers:
        handler = logging.StreamHandler()
        formatter = logging.Formatter(
            "%(asctime)s | %(name)s | %(levelname)s | %(message)s"
        )
        handler.setFormatter(formatter)
        logger.addHandler(handler)
        logger.setLevel(level)
    return logger


def load_config(config_path: Union[str, Path]) -> Dict[str, Any]:
    with Path(config_path).open("r", encoding="utf-8") as file:
        return yaml.safe_load(file)


def ensure_dir(path: Union[str, Path]) -> Path:
    p = Path(path)
    p.mkdir(parents=True, exist_ok=True)
    return p


def read_json(path: Union[str, Path], default: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
    default = default or {}
    p = Path(path)
    if not p.exists():
        return default
    with p.open("r", encoding="utf-8") as file:
        return json.load(file)


def write_json(path: Union[str, Path], payload: Dict[str, Any]) -> None:
    p = Path(path)
    p.parent.mkdir(parents=True, exist_ok=True)
    with p.open("w", encoding="utf-8") as file:
        json.dump(payload, file, indent=2)


def normalize_columns(df: pd.DataFrame) -> pd.DataFrame:
    df.columns = [
        col.strip().lower().replace(" ", "_").replace("-", "_")
        for col in df.columns
    ]
    return df


def compute_rsi(series: pd.Series, period: int = 14) -> pd.Series:
    delta = series.diff()
    gain = delta.where(delta > 0, 0.0)
    loss = -delta.where(delta < 0, 0.0)

    avg_gain = gain.rolling(window=period, min_periods=period).mean()
    avg_loss = loss.rolling(window=period, min_periods=period).mean()

    rs = avg_gain / avg_loss.replace(0, pd.NA)
    rsi = 100 - (100 / (1 + rs))
    return rsi.fillna(50.0)


def build_project_paths(config: Dict[str, Any], root_dir: Path) -> Dict[str, Path]:
    paths_config = config["paths"]
    return {
        "raw": root_dir / paths_config["raw"],
        "curated": root_dir / paths_config["curated"],
        "warehouse": root_dir / paths_config["warehouse"],
        "state_file": root_dir / paths_config["state_file"],
    }