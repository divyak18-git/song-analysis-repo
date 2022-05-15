import sys

from flask import jsonify, Blueprint, make_response, request

class Album(object):
    def __init__(self, albumdata):
        self.href =albumdata.get("href")
        self.name = albumdata.get("name")
        self.release_date = albumdata.get("release_date")

class Playlist(object):

    def __init__(self, playlistdata, album):
        self.id = playlistdata.get("_id")
        self.name = playlistdata.get("name")
        self.isrc = playlistdata.get("external_ids").get("isrc")
        self.duration_ms =  playlistdata.get("duration_ms")
        self.popularity = playlistdata.get("popularity")

        self.album=album
        self.artist_names = [ a["name"] for a in playlistdata.get("artists")].sort()

    def __init__(self, playlistdata):
        self.id = playlistdata.get("_id")
        self.name = playlistdata.get("name")
        self.isrc = playlistdata.get("external_ids").get("isrc")
        self.duration_ms =  playlistdata.get("duration_ms")
        self.popularity = playlistdata.get("popularity")

        self.album= playlistdata.get("album")
        self.available_markets =  playlistdata.get("available_markets")
        self.artist_names = [ a["name"] for a in playlistdata.get("artists")].sort()

    def isduplicate(self, P):
        if ((P.name == self.name) and (P.artist_names == self.artist_names) and (P._id != self._id)):
            return True
        else :
            return False

