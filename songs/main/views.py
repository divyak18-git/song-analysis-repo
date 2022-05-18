from flask import Flask, render_template, send_file,Blueprint
from songs.songsdb import list_collections, load_df, country_wise_count, run_etl_process_full, audio_features_tracks,top_artists_by_playlist
import pandas as pd
import numpy as np
import seaborn as sns
import matplotlib
import matplotlib.pyplot as plt
from matplotlib.backends.backend_agg import FigureCanvasAgg as FigureCanvas
from io import BytesIO
#matplotlib.use("Agg")

main = Blueprint('main', __name__)


@main.route('/billboard_country_visualisation/')
def billboard_country_visualisation():
    # Visualisation code here
    coll = list_collections()
    coll_english = [ item for item in coll if item.startswith("english")]
    fig, ax = plt.subplots(2,2,figsize=(10,7))
    plt.style.use("ggplot")
    ind = 1
    for coll in coll_english:
        play = coll
        english_df = country_wise_count(play)
        count=english_df["numCountry"]
        country=english_df["country"]
        if ind==1:
            plt.subplot(221)
            color="darkviolet"
        elif ind==2:
            plt.subplot(222)
            color="mediumorchid"
        elif ind==3:
            plt.subplot(223)
            color="rebeccapurple"
        else:
            plt.subplot(224)
            color="blueviolet"
        plt.bar(country, count, label=play, color=color)
        ind=ind+1
        plt.legend()
        plt.xticks(rotation=45)


    canvas = FigureCanvas(fig)
    img=BytesIO()
    fig.savefig(img, transparent=True)
    img.seek(0)

    return send_file(img, mimetype='image/png')

@main.route('/danceability_visualisation_by_decade/')
def danceability_visualisation_by_decade():
    coll=list_collections()
    coll_80s = [item for item in coll if item.endswith('80s')]
    fig,ax = plt.subplots(figsize=(10,7))
    #ax=plt.subplot(111,polar=True)
    plt.style.use("seaborn")
    upperLimit=1
    lowerLimit=0
    ind=1
    for coll in coll_80s:
        df_80s = audio_features_tracks(coll)
        danceability = df_80s["danceability"]
        max=df_80s["danceability"].max()
        slope= (max-lowerLimit)/max
        heights = slope * df_80s["danceability"]+lowerLimit
        width = 2*np.pi/len(danceability)

        #indexes = list(range(1,len(danceability)+1))
        #angles = [ element*width for element in indexes]
        angles = 2*np.pi*heights
        if ind==1:
            ax = plt.subplot(121, polar=True)
        else:
            ax = plt.subplot(122, polar=True)
        ax.bar(x=angles, height=heights, width=width, bottom=lowerLimit, linewidth=2)
        #ax.plot(angles, heights)
        #ax.set_rticks([0.25,0.5,0.75,1])
        #ax.set_rlabel_position(-22.5)
        plt.title(coll)
        ind = ind+1
    plt.legend()
    canvas = FigureCanvas(fig)
    img = BytesIO()
    fig.savefig(img)
    img.seek(0)

    return send_file(img, mimetype="image/png")

@main.route('/danceability_visualisation_by_90sdecade/')
def danceability_visualisation_by_90sdecade():
    coll=list_collections()
    coll_90s = [item for item in coll if item.endswith('90s')]
    fig,ax = plt.subplots(figsize=(10,7))
    #ax=plt.subplot(111,polar=True)
    ax = plt.subplot(121)
    sns.set_style("dark")
    df_90s_first = audio_features_tracks(coll_90s[0])
    sns.kdeplot(x=df_90s_first["danceability"], color='b', shade=True,label=coll_90s[0])
    df_90s_second = audio_features_tracks(coll_90s[1])
    sns.kdeplot(x=df_90s_second["danceability"],color='r', shade=True, label=coll_90s[1])
    plt.legend()
    ax = plt.subplot(122)
    sns.kdeplot(x=df_90s_first["acousticness"], color='b', shade=True,label=coll_90s[0])
    df_90s = audio_features_tracks(coll_90s[1])
    sns.kdeplot(x=df_90s_second["acousticness"],color='r', shade=True, label=coll_90s[1])

    plt.legend()
    canvas = FigureCanvas(fig)
    img1 = BytesIO()
    fig.savefig(img1, transparent=True)
    img1.seek(0)

    return send_file(img1, mimetype="image/png")

@main.route('/top_artists_visualisation/')
def top_artists_visualisation():
    coll_l=list_collections()
    coll_list = [ item for item in coll_l if item.startswith("hindi")]
    coll_list.sort()
    fig,ax = plt.subplots(2,2,figsize=(11,9))
    plt.style.use("seaborn")
    ind=1
    explode=[0.2,0,0,0,0]
    colors=["coral","khaki","chocolate","gold","linen"]
    for coll in coll_list:
        df = top_artists_by_playlist(coll)
        numDocs= df["numDocs"]
        heights = df["numDocs"]

        if ind==1:
            ax = plt.subplot(221 )
        elif ind==2:
            ax = plt.subplot(222)
        elif ind==3:
            ax = plt.subplot(223)
        else :
            ax = plt.subplot(224)
        artists = [ ",".join(artistlist) for artistlist in df["_id"] ]
        ax.pie(numDocs, labels=artists, shadow=True ,explode=explode,colors=colors,startangle=90)
        ax.axis('equal')


        # Add labels
        plt.title(coll)
        ind = ind+1
    #plt.legend()
    canvas = FigureCanvas(fig)
    img = BytesIO()
    fig.savefig(img,transparent=True)
    img.seek(0)

    return send_file(img, mimetype="image/png")


@main.route('/',methods=['GET'])
def index():
    #run_etl_process_full()
    return render_template('index.html')

@main.route('/audio-features', methods=['GET'])
def render_audio_features():
    return render_template('audio_features.html')

@main.route('/audio-features_2', methods=['GET'])
def render_audio_features_more():
    return render_template('audio_features_2.html')

@main.route('/artists-hindi', methods=['GET'])
def render_hindi_artists():
    return render_template('hindi_artists.html')