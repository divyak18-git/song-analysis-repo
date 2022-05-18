import json
import spotipy
import sys
from flask import Flask
from pymongo import MongoClient
import spotipy
from spotipy.oauth2 import SpotifyClientCredentials
from dotenv import load_dotenv

import pytest

load_dotenv()

spotify = spotipy.Spotify(auth_manager=SpotifyClientCredentials())


@pytest.fixture()
def client():
    flask.app.config['TESTING'] = True

    client = MongoClient()
    testdb = client.music_deleted

    return testdb

def test_track_api(testdb):
    results = spotify.audio_features(tracks=["4DJRCTe74BMF93149XhYCF,6lY5R1WkMgA6sLPv07qA3O"])
    feature_list = results["audio_features"][0]

    print(feature_list)

