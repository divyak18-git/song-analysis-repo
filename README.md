# song-analysis-repo
Repository for the source code for analyzing songs from the Spotify database

## Songs Then and Now
This is a fun personal project for analysing the hits song trends in Hindi and English over the last four decades from the 1980s.

The Python Flask web framework has been used to fetch certain curated playlists of songs from each decade from the Spotify database using their APIs. After compiling 
the rough master base of songs , the audio features of each track was fetched from Spotify and finally built a simple 4 page website to visualise various trends like 
top artists in Hindi across decades, danceability quotient in Hindi over English hits etc.

MongoDb has been used for persisting , analysing and delivering the data for visualisations.
