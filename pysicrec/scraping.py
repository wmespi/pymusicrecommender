from __future__ import annotations

import random
import time

import requests

from pysicrec import *


def _get_html(url):
    """
    Retrieves the HTML content given a Internet accessible URL.
    :param url: URL to retrieve.
    :return: HTML content formatted as String, None if there was an error.
    """
    time.sleep(random.uniform(SCRAPE_RTD_MINIMUM, SCRAPE_RTD_MAXIMUM))  # RTD
    for i in range(0, SCRAPE_RETRIES_AMOUNT):

        try:

            # Attempt to get url
            response = requests.get(url)

            # Check that the response worked
            assert response.ok

            # Extract content
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
