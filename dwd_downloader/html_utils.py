from typing import List
import requests
from bs4 import BeautifulSoup
from .logger import get_logger

logger = get_logger(__name__)


def get_available_files_from_html(folder_url: str, date_str: str) -> List[str]:
    """
    Scrape the HTML index of a DWD folder and return a list of files
    containing the given date string.
    """
    try:
        resp = requests.get(folder_url, timeout=30)
        resp.raise_for_status()
        soup = BeautifulSoup(resp.text, "html.parser")
        links = [
            str(a["href"])
            for a in soup.find_all("a")
            if str(a["href"]).endswith(".grib2.bz2") and date_str in str(a["href"])
        ]
        logger.debug("Found %d files at %s", len(links), folder_url)
        return links
    except Exception as e:
        logger.warning("Failed to scrape HTML index %s: %s", folder_url, e)
        return []
