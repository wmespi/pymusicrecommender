import os
import pandas as pd
from lyricsgenius import Genius
from spotipy import Spotify
from spotipy.oauth2 import SpotifyOAuth

# Setup OAuth
scope = ["user-top-read", "user-read-recently-played"]
sp_oauth = SpotifyOAuth(scope=scope)

# Initialize API
sp = Spotify(auth_manager=sp_oauth)
top_tracks = sp.current_user_top_tracks(limit=50, time_range='long_term')

# Iteratively pull relevant information
track_info = []
for i, track in enumerate(top_tracks['items']):

    # Setup dictionary
    track_dict = {
        'track_id': track['id'],
        'track_name': track['name'],
        'artist': track['artists'][0]['name'],
        'duration': track['duration_ms'],
        'explicit_yn': track['explicit'],
    }

    # append to list
    track_info.append(track_dict)

# Set up dataframe
track_pdf = pd.DataFrame(track_info)

# Function to convert names to scraping format
str_process = lambda s: ''.join([s for s in list(str(s.replace(' ','-')).lower()) if s.isalnum() or s == '-'])

# Function to scrap lyrics for a song
#### Adjust to make more memory efficient ####
def scrape_lyrics(row, artist_str='artist', song_str='track_name'):

    # Update string format
    artist = str_process(row[artist_str])
    song = str_process(row[song_str])

    # Setup genuis query object
    g = Genius(os.environ['SMARTSHEET_ACCESS_TOKEN'])

    # Set artist
    a = g.search_artist(artist)

    # Set song
    s = a.song("song")

    # Get lyrics
    s.save_lyrics()

    print(s)

    # Find song


    return s

# Apply lyrics to dataframe
track_pdf['lyrics'] = track_pdf.apply(scrape_lyrics, axis=1)
print(track_pdf)