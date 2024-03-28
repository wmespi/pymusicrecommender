from __future__ import annotations

import pandas as pd

from pysicrec.datamart.Datamart import Datamart

# Write table location
temp_artist_loc = 'main/temp_output/artist_table.csv'

# Initialize data mart
dm = Datamart()

# Read artists
artists = pd.read_csv('main/temp_output/artist_list.csv')
artists = list(artists.columns)

# Set artists
dm.set_artist_list(artists)

# Write table
dm.create_artist_table()
dm.artist_table.to_csv(temp_artist_loc)
