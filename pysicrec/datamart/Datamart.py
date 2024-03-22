from __future__ import annotations

from uuid import uuid4

import pandas as pd
from bs4 import BeautifulSoup

from pysicrec import string_cleaner
from pysicrec.scraping import _get_html


class Datamart:

    # AZLyrics website
    AZ_LYRICS_BASE_URL = 'https://www.azlyrics.com'
    AZ_LYRICS_ARTIST_LETTER_LIST = [
        'a',
        # 'b', 'c', 'd', 'e', 'f', 'g', 'h', 'i', 'j', 'k', 'l', 'm',
        # 'n', 'o', 'p', 'q', 'r', 's', 't', 'u', 'v', 'w', 'x', 'y', 'z', '19'
    ]

    def __init__(self) -> None:

        self.artist_table = None
        self.song_table = None

        pass

    def create_artist_table(self):

        # Iteratve over every letter
        artist_url_list = []

        #### Change to run in parallel ####
        for artist_letter in self.AZ_LYRICS_ARTIST_LETTER_LIST:

            #### Replace with logging ####
            print(f'[1] Processing [{artist_letter}] letter...')

            try:

                # Set artist letter url
                artist_letter_url = f'{
                    self.AZ_LYRICS_BASE_URL
                }/{artist_letter}.html'
                html_content = _get_html(artist_letter_url)

                # Extract html content
                if html_content:
                    soup = BeautifulSoup(html_content, 'html.parser')
                    column_list = soup.find_all('div', {'class': 'artist-col'})

                    for column in column_list:

                        for a in column.find_all('a'):

                            # Clean artist name
                            artist_name = string_cleaner.clean_name(a.text)

                            #### Replace with logging ####
                            print(f'\t[1] Processing [{artist_name}]...')

                            # Clean artist url
                            artist_url = string_cleaner.clean_url(
                                '{}/{}'.format(
                                    self.AZ_LYRICS_BASE_URL,
                                    a['href'],
                                ),
                            )

                            # Setup dictionary
                            artist_dict = {
                                'artist_id': uuid4(),
                                'artist_name': artist_name,
                                'artist_url_az': artist_url,
                            }

                            # Add to artist list
                            artist_url_list.append(artist_dict)

            except Exception as e:
                print(f'Error while getting artists from letter {
                      artist_letter
                }: {e}')

        # Create artist table
        self.artist_table = pd.DataFrame.from_dict(artist_url_list)

        pass

    def create_song_table(self):

        def get_song_url_list(artist_url, artist_id):
            """
            Retrieves the AZLyrics website URLs for all the songs from an artist AZLyrics URL.
            :param artist_url: AZLyrics URL from a given artist.
            :return: List of pairs containing the song name and its AZLyrics URL.
            """
            song_url_list = []

            try:
                html_content = _get_html(artist_url)
                if html_content:
                    soup = BeautifulSoup(html_content, 'html.parser')

                    list_album_div = soup.find('div', {'id': 'listAlbum'})
                    for a in list_album_div.find_all('a'):
                        song_name = string_cleaner.clean_name(a.text)
                        artist_url = string_cleaner.clean_url(
                            '{}/{}'.format(
                                self.AZ_LYRICS_BASE_URL,
                                a['href'].replace('../', ''),
                            ),
                        )
                        song_url_list.append({
                            'song_id': uuid4(),
                            'song_name': song_name,
                            'song_url_az': artist_url,
                            'artist_id': artist_id,
                        })
            except Exception as e:
                print(f'Error while getting songs from artist {
                      artist_url
                }: {e}')

            return song_url_list

        def get_song_lyrics(song_url):
            """
            Retrieves and cleans the lyrics of a song given its AZLyrics URL.
            :param song_url: AZLyrics URL from a given song.
            :return: Cleaned and formatted song lyrics.
            """
            song_lyrics = ''

            try:
                html_content = _get_html(song_url)
                if html_content:
                    soup = BeautifulSoup(html_content, 'html.parser')
                    div_list = [
                        div.text for div in soup.find_all(
                            'div', {'class': None},
                        )
                    ]
                    song_lyrics = max(div_list, key=len)
                    song_lyrics = string_cleaner.clean_lyrics(song_lyrics)
            except Exception as e:
                print(f'Error while getting lyrics from song {song_url}: {e}')

            return song_lyrics

        # Get list of all artist urls
        artists = self.artist_table[['artist_url_az', 'artist_id']].values

        # Store song urls
        song_url_list = []

        # Get all song information
        for artist in artists:

            song_url_list.extend(get_song_url_list(artist[0], artist[1]))

        # Get lyrics
        for i, entry in enumerate(song_url_list):

            song_url_list[i]['song_lyrics_az'] = get_song_lyrics(
                entry['song_url_az'],
            )

        # Create song table
        self.song_table = pd.DataFrame.from_dict(song_url_list)

        pass
