from __future__ import annotations

from pysicrec.datamart.Datamart import Datamart

# Intialize data mart
a = Datamart()

# Populate artist table
a.create_artist_table()
# a.artist_table.to_csv('output/artist_table.csv')

# # Populate song table
# a.create_song_table()

# # Join tables together
# master_pdf = a.song_table.merge(a.artist_table, on='artist_id')
# print('Joined Table:\n', master_pdf, '\n')

# # Write csv
# master_pdf.to_csv('output/song_data.csv')
