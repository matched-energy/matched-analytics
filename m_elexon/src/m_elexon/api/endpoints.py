from typing import List

import httpx

BASE_URL = "https://data.elexon.co.uk/bmrs/api/v1/reference"


def bmunits_all() -> List:
    response = httpx.get(f"{BASE_URL}/bmunits/all", timeout=10)
    response.raise_for_status()
    return response.json()
