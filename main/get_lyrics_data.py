from __future__ import annotations

from uuid import uuid4

import pandas as pd
from bs4 import BeautifulSoup

from pysicrec import string_cleaner
from pysicrec.scraping import _get_html
# from langchain_community.document_loaders import AZLyricsLoader

# AZLyrics website
AZ_LYRICS_BASE_URL = 'https://www.azlyrics.com'
# AZ_LYRICS_ARTIST_LETTER_LIST = [
#     'a', 'b', 'c', 'd', 'e', 'f', 'g', 'h', 'i', 'j', 'k', 'l', 'm',
#     'n', 'o', 'p', 'q', 'r', 's', 't', 'u', 'v', 'w', 'x', 'y', 'z', '19'
# ]
AZ_LYRICS_ARTIST_LETTER_LIST = [
    'a',
]

# Iteratve over every letter
artist_url_list = []
for artist_letter in AZ_LYRICS_ARTIST_LETTER_LIST:

    #### Replace with logging ####
    print(f'[1] Processing [{artist_letter}] letter...')

    try:

        # Set artist letter url
        artist_letter_url = f'{AZ_LYRICS_BASE_URL}/{artist_letter}.html'
        html_content = _get_html(artist_letter_url)

        # Extract html content
        if html_content:
            soup = BeautifulSoup(html_content, 'html.parser')
            column_list = soup.find_all('div', {'class': 'artist-col'})

            for column in column_list:

                for a in column.find_all('a'):

                    # Clean artist name
                    artist_name = string_cleaner.clean_name(a.text)

                    # Clean artist url
                    artist_url = string_cleaner.clean_url(
                        '{}/{}'.format(AZ_LYRICS_BASE_URL, a['href']),
                    )

                    # Setup dictionary
                    artist_dict = {
                        'artist_id': uuid4(),
                        'artist_name': artist_name,
                        'artist_url_az': artist_url,

                    }

                    # Add to artist list
                    artist_url_list.append(artist_dict)

        break
    except Exception as e:
        print(f'Error while getting artists from letter {artist_letter}: {e}')

# Create artist table
artist_pdf = pd.DataFrame.from_dict(artist_url_list)
print(artist_pdf)
# Initiate loader
# loader = AZLyricsLoader(
#     "https://www.azlyrics.com/lyrics/a/a1.html",
#     requests_per_second=0.5
# )

# data = loader.load()
# print(data)
