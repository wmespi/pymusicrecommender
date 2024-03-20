# Scrapping
from __future__ import annotations
SCRAPE_PROXY = 'socks5://127.0.0.1:9050'
SCRAPE_RTD_MINIMUM = 0.1
SCRAPE_RTD_MAXIMUM = 0.5
SCRAPE_RETRIES_AMOUNT = 1
SCRAPE_RTD_ERROR_MINIMUM = 0.5
SCRAPE_RTD_ERROR_MAXIMUM = 1

# String cleaning
STR_CLEAN_TIMES = 3
STR_CLEAN_DICT = {
    '\n\n': '\n',
    '\n\r\n': '\n',
    '\r': '',
    '\n': ', ',
    '  ': ' ',
    ' ,': ',',
    ' .': '.',
    ' :': ':',
    ' !': '!',
    ' ?': '?',
    ',,': ',',
    '..': '.',
    '::': ':',
    '!!': '!',
    '??': '?',
    '.,': '.',
    '.:': '.',
    ',.': ',',
    ',:': ',',
    ':,': ':',
    ':.': ':',
}
