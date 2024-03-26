from __future__ import annotations

import os
import random
from uuid import uuid4

import numpy as np
from bs4 import BeautifulSoup
from lyricsgenius import Genius

from pysicrec import *
from pysicrec import string_cleaner
from pysicrec import webscraping as ws


#Link for getting pyspark to work
#### https://maelfabien.github.io/bigdata/SparkInstall/#

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

# AZLyrics website
AZ_LYRICS_BASE_URL = 'https://www.azlyrics.com'
AZ_LYRICS_ARTIST_LETTER_LIST = [
    'a',
    # 'b',
    # 'c', 'd', 'e', 'f', 'g', 'h', 'i', 'j', 'k', 'l', 'm',
    # 'n', 'o', 'p', 'q', 'r', 's', 't', 'u', 'v', 'w', 'x', 'y', 'z', '19'
]

# Setup API object
sp = ws.get_spotify_api()

# Initialize genuis API
_TOKEN = os.getenv('GENUIS_ACCESS_TOKEN')
genuis = Genius(_TOKEN)

class Datamart:

    def __init__(self) -> None:

        self.artist_table = None
        self.song_table = None

        pass

    def set_artist_table(self, pdf):

        self.artist_table = pdf

        pass

    def set_song_table(self, pdf):

        self.song_table = pdf

        pass

    def create_artist_table(self):

        def _get_artists_from_letter(artist_letter):

            #### Replace with logging ####
            print(f'[1] Processing [{artist_letter}] letter...')

            # Store output for the letter
            artist_list = []

            try:

                # Set artist letter url
                artist_letter_url = f'{
                    AZ_LYRICS_BASE_URL
                }/{artist_letter}.html'
                html_content = ws.get_html(artist_letter_url)

                # Extract html content
                if html_content:
                    soup = BeautifulSoup(html_content, 'html.parser')
                    column_list = soup.find_all('div', {'class': 'artist-col'})

                    return [a.text for column in column_list for a in column.find_all('a')]

            except Exception as e:
                print(f'[2] Error while getting artists from letter {
                      artist_letter
                }: {e}')

            return artist_list

        def _get_artist_info(artist_name):

            # Delay API calls
            ws.sleep_timer()

            # Log check
            print(f'\n\t[1] Processing [{artist_name}]...')

            # Setup dictionary
            try:

                # Artist query
                q = 'artist:' + artist_name

                # Query artist name
                results = sp.search(
                    q=q,
                    limit=1,
                    type='artist',
                )

                # Filter to relevant results
                results = results['artists']['items'][0]

                # Setup dictionary
                artist_dict =  {
                    'artist_id': str(uuid4()),
                    'artist_spotify_id': results['id'],
                    'artist_name': results['name'],
                    'artist_spotify_url': results['external_urls']['spotify'],
                    'artist_spotify_followers': results['followers']['total'],
                    'artist_spotify_popularity': results['popularity'],
                }

                return str(artist_dict)

            except Exception as e:
                print(f'\tFailed artist {artist_name}: {e}')

        # Pull artist pages
        artists = []
        for letter in AZ_LYRICS_ARTIST_LETTER_LIST:
            artists.extend(_get_artists_from_letter(letter))

        # Extract data from API
        artist_sdf = ws.run_parallel_calls(_get_artist_info, artists)

        # Convert text
        artist_sdf = ws.convert_str_to_json(artist_sdf, 'end')

        # Make distinct
        artist_sdf = artist_sdf.where('artist_spotify_id is not null')
        artist_sdf = artist_sdf.dropDuplicates(subset=['artist_spotify_id'])

        # Collect output
        self.artist_table = artist_sdf.toPandas()

        pass


    def create_song_table(self):

        def get_song_info(artist_id):
            """
            Retrieves the AZLyrics website URLs for all the songs from an artist AZLyrics URL.
            :param artist_url: AZLyrics URL from a given artist.
            :return: List of pairs containing the song name and its AZLyrics URL.
            """

            # Log check
            print(f'\n\t[1] Processing songs for [{artist_id}]...')

            # Save songs
            songs = ''

            try:

                # Extract top 10 songs per artist
                tracks = sp.artist_top_tracks(artist_id, country='US')['tracks']

                # Pull relevant info from dictionary
                for track in tracks:

                    songs += str({
                        'song_id': str(uuid4()),
                        'artist_spotify_id': str(artist_id),
                        'song_spotify_id': str(track['id']),
                        'song_name': str(track['name']),
                        'album_spotify_id': str(track['album']['id']),
                        'album_name': str(track['album']['name']),
                        'song_spotify_popularity': str(track['popularity']),
                        'song_spotify_preview': str(track['preview_url'])
                    }) + ';'

            except Exception as e:
                print(f'Error while getting songs from artist {
                      artist_id
                }: {e}')

            return songs

        # Get list of all artist urls
        artists = self.artist_table.drop_duplicates(subset=['artist_spotify_id'])
        artists = list(artists['artist_spotify_id'].values)

        # Split lists into sub-groups
        n_artists = len(artists)
        n_p_group = 50
        n_bins = int(n_artists / n_p_group)
        artist_groups = np.array_split(artists, n_bins)
        artist_groups = [[str(j) for j in i] for i in artist_groups]

        # Pull top 10 songs for each artist
        for i, artist_group in enumerate(artist_groups):

            # Log statement
            print(f'\n[1] Processing artist group {i} out of {n_bins} groups...')

            # Run parallel extraction for 100 artists
            sdf = ws.run_parallel_calls(get_song_info, artist_group, partitions=6)
            sdf = ws.convert_str_to_json(sdf, 'end', explode=True)
            sdf = sdf.where('song_spotify_id is not null')

            # Check if dataframe exists
            try:
                song_sdf = song_sdf.unionByName(sdf)
            except UnboundLocalError:
                print('\n\tPopulating song_sdf for first pass')
                song_sdf = sdf

            # Delay until the next
            ws.sleep_timer(min=29, max=31)

        # Convert columns to list
        self.song_table = song_sdf.toPandas()

    def create_lyrics_table(self):

        def get_song_lyrics(song_url):
            """
            Retrieves and cleans the lyrics of a song given its AZLyrics URL.
            :param song_url: AZLyrics URL from a given song.
            :return: Cleaned and formatted song lyrics.
            """

            #### Exploire musixmatch for song lyrics, ideally it has easy connection to spotify's internal IDs ####
            song_lyrics = ''

            try:
                html_content = self._get_html(song_url)
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

        pass
