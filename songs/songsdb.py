import json
import spotipy
import sys
from flask import Flask, current_app
from pymongo import MongoClient, UpdateOne
#from pyspark.sql import SparkSession
#from pyspark.sql.functions import col,udf,spark_partition_id
#from pyspark.sql.types import StringType
import numpy as np
import multiprocessing as mp
from threading import Thread
import pandas as pd
from werkzeug.local import LocalProxy
import os

from spotipy.oauth2 import SpotifyClientCredentials


spotify = spotipy.Spotify(auth_manager=SpotifyClientCredentials())

#client=MongoClient()
#prod_db=current_app.config["DB_NAME"]
#deleted_db_name = current_app.config["DB_NAME_DELETED"]
#test_db_name = current_app.config["DB_NAME_TEST"]
#db=client[prod_db]
#db_deleted=client[deleted_db_name]
#db_test=client[test_db_name]
#spark = (SparkSession.builder.appName("StartSong").getOrCreate())

def get_db():
    prod_db_name = current_app.config['PROD_DB']
    db=MongoClient()[prod_db_name]
    return db

def get_db_test():
    test_db_name = current_app.config['TEST_DB']
    db_test = MongoClient()[test_db_name]
    return db_test

def get_db_deleted():
    backup_db = current_app.config['BACKUP_DB']
    db_deleted = MongoClient()[backup_db]
    return db_deleted

def get_api_limit():
    api_fetch_limit = current_app.config['API_FETCH_LIMIT']
    return api_fetch_limit


db = LocalProxy(get_db)
db_test = LocalProxy(get_db_test)
db_deleted = LocalProxy(get_db_deleted)

api_fetch_limit = LocalProxy(get_api_limit)


#@udf(returnType=StringType())
def writeApiToCollection(playlist_name_l, playlist_id_l):
    print(playlist_id_l)
    results={"next":"NA"}
    col="db."
    for pid in range(len(playlist_id_l)):
        offset=0
        limit=50
        print("Playlist ID " + playlist_id_l[pid])
        print("Collection Name "+ playlist_name_l[pid])
        results = {"next": "NA"}
        while (results['next']):
            results = spotify.playlist_tracks(playlist_id_l[pid],
                                          fields="items(track.id,track(artists,available_markets,duration_ms,explicit,external_ids,name,popularity,album(name,release_date,href))),total,next",
                                          limit=limit,
                                          offset=offset)
            # json_form_results=json.dumps(results,indent=4)
            # print(json_form_results)
            items_list = results['items']
            final_items_list = []
            for item in items_list:
                final_items_list.append(item['track'])
            # print(final_items_list[0])
            x = db[playlist_name_l[pid]].insert_many(final_items_list)
            #print(x.inserted_ids)
            if offset == 0:
                offset = offset + limit + 1
            else:
                offset = offset + limit

        #total_count = results['total']

        #print('Total Count - '+str(total_count))
    return

def callWriteToDb(fileName):
    #playlist_df = spark.read.format("csv").option("header", "true").option("inferSchema", "true").load(fileName)
    #playlist_partition_df=playlist_df.repartition(4)

    #res1=playlist_partition_df.withColumn("PartitionId",spark_partition_id())
    #res=res1.withColumn("InsertedId",writeApiToCollection(col("Playlist Name"),col("ID")))

    playlist_df = pd.read_csv(fileName, encoding='latin1')
    playlist_name = playlist_df['Playlist Name'].tolist()
    playlist_id = playlist_df['ID'].tolist()
    collection_name = playlist_df['Collection'].tolist()

    threads=[]

    for i in range(4):
        print("Processing chunk "+ str(i))
        ids=playlist_id[i::4]
        pnames=collection_name[i::4]
        t=Thread(target=writeApiToCollection, args=(pnames,ids))
        threads.append(t)

    # start the threads
    print("Starting Thread")
    [t.start() for t in threads]
    # wait for the threads to finish
    print("Waiting for Thread")
    [t.join() for t in threads]

    return

def restore_data(new_playlist , old_playlist):
    res = db[new_playlist].find()
    db[old_playlist].insert_many(list(res))

def list_collections() :
    collections = db.list_collection_names()
    return collections


def delete_tracks_by_year(playlist, lower_limit, upper_limit):
    try:
        new_playlist = "discarded_" + playlist
        db[new_playlist].delete_many({})
        match_filter = [{ "$addFields":
                {
                    "release_year": { "$substr": ["$album.release_date", 0, 4]}
                }
                },
                { "$match":
                      { "$or": [{"release_year": { "$lt": lower_limit}}, {"release_year": {"$gte": upper_limit}}]}
                }
                ]

        playlist_res = list(db[playlist].aggregate(match_filter))
        if len(playlist_res) > 0:
            insert_res = db_deleted[new_playlist].insert_many(playlist_res)
            #id_list = [ track["_id"] for track in playlist_res ]
            print(id_list)
            result = db[playlist].delete_many({"_id": { "$in": id_list}} );
            return {"status": "For playlist " + playlist+ "success - " + str(result.deleted_count) + " documents deleted"}
        return { "status" : "For playlist "+ playlist + "success - 0 Documents deleted" }
    except Exception as e:
        return {"error for playlist " + playlist : e}

def delete_duplicate_tracks(playlist):
    try:
        new_playlist = "discarded_"+playlist
        pipeline = [ { "$project" : { "name" : 1, "artists.name" : 1 } },
					{ "$group" :
							{ "_id" :
							  {
							    "name" : "$name",
							    "artist_name" : "$artists.name"
							 },
							  "numName" : { "$sum" : 1 },
							  "maxId" : { "$max" :  "$_id"  },
                              "idList": { "$push": "$_id"}
							 }
						},
						{ "$match" : { "numName" : { "$gt" : 1 } } },
 						{ "$sort" : { "numName" : -1 }}
				]

        duplicate_patterns = list(db[playlist].aggregate(pipeline))
        ids_to_retain = [ pattern["maxId"] for pattern in duplicate_patterns ]
        ids_to_search = [ id for pattern in duplicate_patterns for id in pattern["idList"] ]
        ids_to_delete = list(set(ids_to_search).difference(set(ids_to_retain)))
        #print(ids_to_delete)
        #print(len(ids_to_delete))
        docs_to_delete = list(db[playlist].find( { "_id" : {"$in" : ids_to_delete }}))
        if len(docs_to_delete) >0 :
            for doc in docs_to_delete :
                doc["duplicate"] = "true"

            insert_res = db_deleted[new_playlist].insert_many(docs_to_delete)
            result = db[playlist].delete_many({ "_id" :  { "$in": ids_to_delete }})
            return {"status" : "For playlist "+ playlist + " success - " + str(result.deleted_count) + " documents deleted"}
        return {"status" : "For playlist "+ playlist + " success - 0 documents deleted"}
    except Exception as e :
        return { "error for playlist "+ playlist : e }


def add_country_field(playlist):
    try:
        result = db[playlist].update_many({},
                                [{ "$set" : { "country" : { "$substrBytes" : [ "$external_ids.isrc",0,2 ] } } }]
                                )
        return { "status" : "For playlist "+ playlist + " Success"}
    except Exception as e:
        return {"Error for playlist "+ playlist : e}


###############################################################
## Function for fetching remaining track details listed as audio_Features in spotify
## Example features include danceability, energy, acousticness etc
#############################################################
def update_track_with_api(playlist,playlist_id_list):
    try:
        playlist_id_str = ",".join(playlist_id_list)
        # Spotipy API to fetch the track details in bulk. List of 100 playlist ids passed to this function
        results = spotify.audio_features(tracks=playlist_id_str)
        bulk_write_pipeline = []

        # Making the BulkWrite pipeline for updating the individual documents in mongodb by track id. Also removing unwanted fields
        for res in results:
            filter = { "id" : res["id"]}
            del res["id"]
            del res["track_href"]
            del res["type"]
            del res["analysis_url"]
            update = {"$set" : res }
            bulk_write_pipeline.append(UpdateOne(filter, update))

        #print(bulk_write_pipeline)
        db[playlist].bulk_write(bulk_write_pipeline)
        return {"status" : "Success"}
    except Exception as e:
        return {"error" : e }


def call_update_track_api(playlist , max_limit):
    playlist_ids_db = db[playlist].find({} , { "id" : 1 , "_id": 0})
    playlist_ids = [ item["id"] for item in playlist_ids_db ]

    threads=[]

    num_of_threads = int(len(playlist_ids)/ max_limit)
    rem = len(playlist_ids) % max_limit
    num_of_docs = len(playlist_ids)
    print("Number of docs being processed "+ str(num_of_docs))

    if rem > 0 :
        num_of_threads = num_of_threads + 1

    startIndex = 0
    for i in range(num_of_threads):
        playlist_ids_subset = playlist_ids[startIndex:startIndex+ max_limit]
        print("Processing chunk "+ str(i))
        t=Thread(target=update_track_with_api, args=(playlist,playlist_ids_subset))
        threads.append(t)
        startIndex = startIndex + max_limit


    # start the threads
    print("Starting Thread")
    [t.start() for t in threads]
    # wait for the threads to finish
    print("Waiting for Thread")
    [t.join() for t in threads]

    return

def run_etl_process_full():

    inputFileDir = os.path.abspath("InputFile")
    #for file in os.listdir(inputFileDir):
    #    fileName=os.path.join(inputFileDir,file)
    #    print("Writing file ------------------------------------>"+ fileName)
    #    if os.path.isfile(fileName):
    #        callWriteToDb(fileName)

    ### Data cleansing

    ##### Step 1 : For each playlist, delete the tracks that do not belong to the mentioned decade in the playlist
    ##### To do this , release_date field of the album sub-document of each document can be used

    print("Deleting tracks with invalid year ")
    status = delete_tracks_by_year("hindi_playlist_80s","1980","1990")
    print(status)
    status = delete_tracks_by_year("hindi_playlist_90s","1990","2000")
    print(status)
    status = delete_tracks_by_year("hindi_playlist_2000s","2000","2010")
    print(status)
    status = delete_tracks_by_year("hindi_playlist_2010s","2010","2020")
    print(status)
    status = delete_tracks_by_year("english_playlist_80s","1980","1990")
    print(status)
    status = delete_tracks_by_year("english_playlist_90s","1990","2000")
    print(status)
    status = delete_tracks_by_year("english_playlist_2000s","2000","2010")
    print(status)
    status = delete_tracks_by_year("english_playlist_2010s","2010","2020")
    print(status)
    #restore_data("discarded_english_playlist_80s", "english_playlist_80s")

    print("Deleting duplicate tracks from each playlist")
    music_collections = list_collections()
    print(music_collections)
    for coll in music_collections:
        status = delete_duplicate_tracks(coll)
        print(status)

    print("Adding new field country for each track")

    #for coll in music_collections:
    #    status = add_country_field(coll)
    #    print(status)

    print("Fetching the audio features of each track of each playlist from spotify")
    #for coll in music_collections:
    #    print("Processing playlist "+coll)
    #    call_update_track_api(coll,int(api_fetch_limit))

#-----------------------------------------------------------------------#
# Function definition to load the contents of the playlist collection
# passed as a parameter in a dataframe and return the same
#-----------------------------------------------------------------------#

def load_df(playlist):
    coll_cursor = db[playlist].find()
    coll_df = pd.DataFrame(list(coll_cursor))
    del coll_df["_id"]

    return coll_df

def load_df_list(l):
    coll_df = pd.DataFrame(l)
    print(coll_df)

    return coll_df


def country_wise_count(playlist):
    pipeline = [
        { "$group":
              { "_id" : "$country",
               "numCountry" : { "$sum" : 1 }
              }
        },
        { "$project" :
              {
                  "country" : "$_id",
                  "numCountry" : 1,
                  "_id" : 0
              }
        },
        { "$sort" : { "numCountry" : -1 }}
    ]
    coll_cursor = list(db[playlist].aggregate(pipeline))
    print(coll_cursor)
    coll_df = pd.DataFrame(coll_cursor)
    print("Dataframe formed")
    #print(coll_df)
    return coll_df

def audio_features_tracks(playlist):
    #cursor = list(db[playlist].find({}, { "acousticness": 1, "danceability": 1, "energy":1 ,"instrumentalness":1, "liveness":1, "tempo":1 }))
    cursor = list(db[playlist].find({}, {  "acousticness": 1, "danceability": 1 }))
    coll_df = pd.DataFrame(cursor)
    return coll_df

def top_artists_by_playlist(playlist):
    pipeline = [
					{ "$group" :
							{ "_id" : "$artists.name",
							  "numDocs" : { "$sum" : 1 }
							}
						},
						{ "$sort" : { "numDocs" : -1 } },
                        { "$limit" : 5}
				]
    coll_cursor = list(db[playlist].aggregate(pipeline))
    coll_df = pd.DataFrame(coll_cursor)
    return coll_df


