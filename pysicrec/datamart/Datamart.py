from __future__ import annotations

import os
import random
from uuid import uuid4

from bs4 import BeautifulSoup
from lyricsgenius import Genius
from pyspark.sql import functions as F
from pyspark.sql import SparkSession
from pyspark.sql import types as T
from spotipy import Spotify
from spotipy.oauth2 import SpotifyOAuth

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

#create object of SparkContext
spark = SparkSession.builder.master('local').\
    appName('Word Count').\
    config('spark.driver.bindAddress','localhost').\
    config('spark.ui.port','4050').\
    getOrCreate()

# AZLyrics website
AZ_LYRICS_BASE_URL = 'https://www.azlyrics.com'
AZ_LYRICS_ARTIST_LETTER_LIST = [
    'a',
    # 'b',
    # 'c', 'd', 'e', 'f', 'g', 'h', 'i', 'j', 'k', 'l', 'm',
    # 'n', 'o', 'p', 'q', 'r', 's', 't', 'u', 'v', 'w', 'x', 'y', 'z', '19'
]

# Initialize genuis API
_TOKEN = os.getenv('GENUIS_ACCESS_TOKEN')
genuis = Genius(_TOKEN)

# Setup  Spotofy OAuth
scope = ['user-top-read', 'user-read-recently-played', 'user-library-read']
sp_oauth = SpotifyOAuth(scope=scope)

# Initialize Spotify API
sp = Spotify(auth_manager=sp_oauth, requests_timeout=10, retries=1, status_retries=1)

class Datamart:

    def __init__(self) -> None:

        self.artist_table = None
        self.song_table = None

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

        # Create spark dataframe
        artist_sdf = spark.createDataFrame([(artist, ) for artist in artists if artist is not None], ['start_name'])
        artist_sdf = artist_sdf.repartition(6)

        # Run artist extraction in parallel
        scrape_udf = F.udf(_get_artist_info)
        artist_sdf = artist_sdf.withColumn('scraped_data', scrape_udf('start_name'))

        # Specify json schema
        json_schema = 'MAP<STRING,STRING>'

        # Expand json into columns
        artist_sdf = artist_sdf.withColumn(
            'x',
            F.from_json('scraped_data', json_schema)
        )

        # Get dictionary keys
        keys = (artist_sdf
            .select(F.explode('x'))
            .select('key')
            .distinct()
            .rdd.flatMap(lambda x: x)
            .collect()
        )
        # Select final columns
        exprs = [F.col('x').getItem(k).alias(k) for k in keys]
        artist_sdf = artist_sdf.select(*exprs)

        # Make distinct
        artist_sdf = artist_sdf.where('artist_spotify_id is not null')
        artist_sdf = artist_sdf.dropDuplicates(subset=['artist_spotify_id'])

        # Collect output
        self.artist_table = artist_sdf.toPandas()

        pass

    def set_artist_table(self, pdf):

        self.artist_table = pdf

        pass

    def _get_spotify_artist_id(self, artist):

        q = 'artist:' + artist

        # Query artist name
        results = self.sp.search(
            q=q,
            limit=1,
            type='artist',
        )

        # Filter to relevant results
        results = results['artists']['items'][0]

        return {
            'artist_id': uuid4(),
            'artist_spotify_id': results['id'],
            'artist_name': results['name'],
            'artist_spotify_url': results['external_urls']['spotify'],
            'artist_spotify_followers': results['followers']['total'],
            'artist_spotify_popularity': results['popularity'],
        }


    def create_song_table(self):

        def get_song_url_list(artist_url, artist_id):
            """
            Retrieves the AZLyrics website URLs for all the songs from an artist AZLyrics URL.
            :param artist_url: AZLyrics URL from a given artist.
            :return: List of pairs containing the song name and its AZLyrics URL.
            """
            song_url_list = []

            try:
                html_content = self._get_html(artist_url)
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

        # Get list of all artist urls
        print(self.artist_table)
        artists = self.artist_table[['artist_url_az', 'artist_id']].values

        # Store song urls
        song_url_list = []

        # Get all song information
        for artist in artists:

            song_url_list.extend(get_song_url_list(artist[0], artist[1]))
            break

        # # Get lyrics
        # for i, entry in enumerate(song_url_list):

        #     song_url_list[i]['song_lyrics_az'] = get_song_lyrics(
        #         entry['song_url_az'],
        #     )

        # # Create song table
        # self.song_table = pd.DataFrame.from_dict(song_url_list)

        pass
