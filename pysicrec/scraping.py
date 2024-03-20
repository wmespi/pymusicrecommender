from __future__ import annotations

import random
import time

import requests
from bs4 import BeautifulSoup
from stem import Signal
from stem.control import Controller

from pysicrec import *
from pysicrec import string_cleaner
# from fake_useragent import UserAgent


def _get_html(url):
    """
    Retrieves the HTML content given a Internet accessible URL.
    :param url: URL to retrieve.
    :return: HTML content formatted as String, None if there was an error.
    """
    time.sleep(random.uniform(SCRAPE_RTD_MINIMUM, SCRAPE_RTD_MAXIMUM))  # RTD
    for i in range(0, SCRAPE_RETRIES_AMOUNT):
        try:
            with Controller.from_port(port=9051) as c:
                c.authenticate()
                c.signal(Signal.NEWNYM)
            proxies = {'http': SCRAPE_PROXY, 'https': SCRAPE_PROXY}
            headers = {'User-Agent': UserAgent().random}
            response = requests.get(url, proxies=proxies, headers=headers)
            assert response.ok
            html_content = response.content
            return html_content
        except Exception as e:
            if i == SCRAPE_RETRIES_AMOUNT - 1:
                print(f'Unable to retrieve HTML from {url}: {e}')
            else:
                time.sleep(
                    random.uniform(
                        SCRAPE_RTD_ERROR_MINIMUM, SCRAPE_RTD_ERROR_MAXIMUM,
                    ),
                )
    return None
