from __future__ import annotations

import csv

from pysicrec.datamart.Datamart import Datamart

# Write list location
artist_list_loc = 'main/temp_output/artist_list.csv'

# Initialize data mart
dm = Datamart()

# Get artists
dm.create_artist_list()

# Write to csv
with open(artist_list_loc, 'w') as f:
    wr = csv.writer(f, quoting=csv.QUOTE_ALL)
    wr.writerow(dm.artists)
