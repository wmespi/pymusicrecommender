from __future__ import annotations

from pysicrec.datamart.Datamart import Datamart

# Write table location
artist_loc = 'output/artist_table.csv'
song_loc = 'main/song_table.csv'

# Initialize data mart
dm = Datamart()

# Set artist table
dm.set_artist_table(artist_loc)

# Write table
dm.create_song_table()
dm.song_table.to_csv(song_loc)
