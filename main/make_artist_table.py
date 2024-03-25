from __future__ import annotations

from pysicrec.datamart.Datamart import Datamart

# Write table location
temp_artist_loc = 'main/artist_table_temp.csv'

# Initialize data mart
dm = Datamart()

# Write table
dm.create_artist_table()
dm.artist_table.to_csv(temp_artist_loc)
