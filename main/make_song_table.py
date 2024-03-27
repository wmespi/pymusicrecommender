from __future__ import annotations

import pandas as pd

from pysicrec.datamart.Datamart import Datamart

# Write table location
artist_loc = 'output/artist_table.csv'
song_loc = 'main/song_table.csv'

# Initialize data mart
dm = Datamart()

# Set artist table
artist_pdf = pd.read_csv(artist_loc)
dm.set_artist_table(artist_pdf)

# Write table
dm.create_song_table()
dm.song_table.to_csv(song_loc)
