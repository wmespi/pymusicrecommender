from __future__ import annotations

from pysicrec.datamart.Datamart import Datamart

# Intialize data mart
a = Datamart()

# Populate artist table
a.create_artist_table()
print('Artist Table:\n', a.artist_table, '\n')

# Populate song table
a.create_song_table()
print('Song Table:]n', a.song_table, '\n')
