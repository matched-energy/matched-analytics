import logging
import os
import pickle
import sys
from pathlib import Path
from typing import Any, Dict, Union

import numpy as np
import pandas as pd
import xxhash
import yaml
from dotenv import load_dotenv
from yaml import Dumper, ScalarNode


def get_logger(name: str, level: str = "debug") -> logging.Logger:
    logger = logging.getLogger(name)

    # Set level
    levels = dict(
        debug=logging.DEBUG,
        info=logging.INFO,
        warning=logging.WARNING,
        error=logging.ERROR,
        critical=logging.CRITICAL,
    )
    try:
        logger.setLevel(levels[level])
    except KeyError:
        raise KeyError(f"Expect level to be in {list(levels.keys())}")

    # Set handler and formatter
    handler = logging.StreamHandler(sys.stdout)
    formatter = logging.Formatter("%(asctime)s - %(name)s - %(levelname)s - %(message)s")
    handler.setFormatter(formatter)
    logger.addHandler(handler)

    return logger


def float_representer(dumper: Dumper, value: Union[float, np.float32, np.float64]) -> ScalarNode:
    return dumper.represent_scalar("tag:yaml.org,2002:float", format(value, ".2f"))


yaml.add_representer(float, float_representer)
yaml.add_representer(np.float32, float_representer)
yaml.add_representer(np.float64, float_representer)


def from_yaml_text(text: str) -> Dict:
    return yaml.load(text, Loader=yaml.FullLoader)


def from_yaml_file(path: Path) -> Dict:
    with open(path, "r") as file:
        return from_yaml_text(file.read())


def to_yaml_text(dictionary: Dict) -> str:
    return yaml.dump(dictionary, default_flow_style=False)


def to_yaml_file(dictionary: Dict, path: Path) -> None:
    with open(path, "w") as file:
        file.write(to_yaml_text(dictionary))


def get_dot_env(key: str) -> str:
    load_dotenv()
    value = os.getenv(key, None)
    if value is None:
        raise Exception(f".env key {key} is None")
    return value


def checksum(obj: Any) -> str:
    hasher = xxhash.xxh64()
    if isinstance(obj, (pd.DataFrame, pd.Series)):
        obj = obj.to_numpy()  # Convert Pandas objects to NumPy arrays for fast processing
    try:
        hasher.update(pickle.dumps(obj, protocol=4))  # Protocol 4 is more efficient for large data
    except Exception:
        hasher.update(str(obj).encode())  # Fallback for unpickleable objects

    return hasher.hexdigest()
