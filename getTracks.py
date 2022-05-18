import json
import spotipy
import sys
from flask import Flask

from spotipy.oauth2 import SpotifyClientCredentials

#app=Flask(__name__)

spotify = spotipy.Spotify(auth_manager=SpotifyClientCredentials())

name = 'BillBoard 2010'

#print(name)

#results = spotify.search(q=name ,type='playlist',limit=30 )
#print(results)
#json_form_results=json.dumps(results,indent=4)
#print(json_form_results)
#items = results['playlists']['items']
#if len(items) > 0:
#    for i in items:
#        print(i['name'] + '^^' +i['id'])

results=spotify.playlist_tracks('2bTAdSphpMmA3BLKZ2sA8P',limit=40)
json_form_results=json.dumps(results,indent=4)
album_info=results['items']
for a in album_info:
    print(a['track']['album']['name'])
    print(a['track']['album']['release_date'])
    print('--------------------')