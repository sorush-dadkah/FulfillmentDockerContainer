from __future__ import annotations

import json
import logging
from typing import Any


def get_data_from_path(file_path: str = "") -> Any:
    try:
        with open(file_path) as rf:
            return json.load(rf)
    except FileNotFoundError as error:
        logging.warning(error)
        raise error
