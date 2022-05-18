# song-analysis-repo
Repository for the source code for analyzing songs from the Spotify database

## Songs Then and Now
This is a fun personal project for analysing the hits song trends in Hindi and English over the last four decades from the 1980s.

### What it does
The Python Flask web framework has been used to fetch certain curated playlists of songs from each decade from the Spotify database using their APIs. After compiling 
the rough master base of songs , the audio features of each track was fetched from Spotify to finally build a simple 4 page website to visualise various trends like 
top artists in Hindi across decades, danceability quotient in Hindi over English hits etc. Spotipy library has been used to fetch data from Spotify

### Technologies used
MongoDb 5.0.2 has been used for persisting , analysing and delivering the data for visualisations.
Python 3.7, Flask 2.1.1
HTML5, CSS

### A peek into the flow
The following image gives a high level overview of the data flow before rendering the visualisations. More details can be found in the high level document file High_Level_Technical_Design_SongsThenAndNow uploaded.

![image](https://user-images.githubusercontent.com/98416285/168839564-d6050d07-f484-43af-91b3-a3ab2357a710.png)

### A preview of the webpage for visualisation


![image](https://user-images.githubusercontent.com/98416285/168846382-6eec0479-b92d-49dc-bbf1-ffc39a1924d6.png)
