from os import times
from turtle import pd
import sqlalchemy
import pandas as pd
from sqlalchemy.orm import sessionmaker
import requests
import json
from datetime import datetime
import datetime
import sqlite3

DATABASE_LOCATION = "sqlite:///my_played_tracks.sqlite"
USER_ID = "Mafer Chávez Romo"
TOKEN = "BQD8Xp2knRYE-KKQQAbnNwqk77bTeWJjm3TfuvKCEW816l_4XD_hNCLkwX4QMsr7MziYbRh1d98llDPMGyct31Zq0ZTj8txUoR5zyKbw4cq3_ffDUlcVjYPceX198Itvhq85rgQuJfyaNlWVfEpsnvZfTCGfTkBIJs2JvEBOetFSRCBxLtozclTVGAvuuu5Olys_"

def check_if_valid_data(df: pd.DataFrame)->bool:
    #Check if dataframe is empty
    if df.empty:
        print ("No songs downloaded. Finishing execution")
        return False
    
    #primary key check
    if pd.Series(df['played_at']).is_unique:
        pass
    else:
        raise Exception("Primary Check is violated")
    
    #Check for nulls
    if df.isnull().values.any():
        raise Exception ("Null value found")
    
    #check all the timestamps are from yesterday
    #yesterday = datetime.datetime.now()-datetime.timedelta(days=30)
    #yesterday = yesterday.replace (hour = 0, minute = 0, second = 0, microsecond = 0)
    
    #timestamps = df["timestamp"].tolist()
    #for timestamp in timestamps:
    #    if datetime.datetime.strptime(timestamp, "%Y-%m-%d") != yesterday:
    #        raise Exception ("At least one of the returned songs does not come from within the last 24 hours ")
    #return True


if __name__ == '__main__':

    headers = {
        "Accept" : "application/json",
        "Content-Type" : "application/jason",
        "Authorization" : "Bearer {token}".format(token = TOKEN)
    }

    today = datetime.datetime.now()
    yesterday = today - datetime.timedelta(days = 0.03)
    yesterday_unix_timestamp = int(yesterday.timestamp()) * 1000
    

    r = requests.get("https://api.spotify.com/v1/me/player/recently-played?after={time}".format(time=yesterday_unix_timestamp),headers = headers)

    data = r.json()

    #print (data)

    song_names = []
    artist_names = []
    played_at_list = []
    timestamps = []

     # Extracting only the relevant bits of data from the json object      
 
    for song in data["items"]:
        song_names.append(song["track"]["name"])
        artist_names.append(song["track"]["album"]["artists"][0]["name"])
        played_at_list.append(song["played_at"])
        timestamps.append(song["played_at"][0:10])
      
    # Prepare a dictionary in order to turn it into a pandas dataframe below       
    song_dict = {
        "song_name" : song_names,
        "artist_name": artist_names,
        "played_at" : played_at_list,
        "timestamp" : timestamps
    }

    song_df = pd.DataFrame(song_dict, columns = ["song_name", "artist_name", "played_at", "timestamp"])
    print (song_df)

    #validate 
    if check_if_valid_data(song_df):
        print ("Data valid, proceed to Load Stage")
    
    #Load

    engine = sqlalchemy.create_engine(DATABASE_LOCATION)
    conn = sqlite3.connect('my_played_tracks.sqlite')

    cursor = conn.cursor()

    sql_query = """
    CREATE TABLE IF NOT EXISTS my_played_tracks(
        song_name VARCHAR(200),
        artist_name VARHAR(200),
        played_at VARCHAR(200),
        timestamp VARCHAR(200),
        CONSTRAINT primary_key_constraint PRIMARY KEY (played_at)
    )
    """
    cursor.execute(sql_query)
    conn.commit()
    print ("Opened database succesfully")

    try:
        song_df.to_sql("my_played_tracks", engine, index=False, if_exists='append')
        conn.commit()
    except: 
        print ("Data already exists in the database")
    
    conn.close()
    print ("Close database succesfully")