from __future__ import annotations

import random
import time

import requests

# Scraping parameters
SCRAPE_PROXY = 'socks5://127.0.0.1:9050'
SCRAPE_RTD_MINIMUM = 1
SCRAPE_RTD_MAXIMUM = 5
SCRAPE_RETRIES_AMOUNT = 1
SCRAPE_RTD_ERROR_MINIMUM = 0.5
SCRAPE_RTD_ERROR_MAXIMUM = 1

def sleep_timer():
    time.sleep(random.uniform(SCRAPE_RTD_MINIMUM, SCRAPE_RTD_MAXIMUM))  # RTD


def get_html(url):
    """
    Retrieves the HTML content given a Internet accessible URL.
    :param url: URL to retrieve.
    :return: HTML content formatted as String, None if there was an error.
    """
    for i in range(0, SCRAPE_RETRIES_AMOUNT):

        try:

            # Attempt to get url
            response = requests.get(url)

            # Check that the response worked
            assert response.ok

            # Extract content
            html_content = response.content
            return html_content
            return url

        except Exception as e:
            if i == SCRAPE_RETRIES_AMOUNT - 1:
                print(f'Unable to retrieve HTML from {url}: {e}')
            else:
                sleep_timer()
    return None
