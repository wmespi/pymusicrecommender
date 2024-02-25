import pandas as pd

from spotipy import Spotify
from spotipy.oauth2 import SpotifyClientCredentials, SpotifyOAuth
from spotipy.util import prompt_for_user_token

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
print(track_pdf)