# ---------------------------------- Importing the required libraries --------------------------------------------------
from googleapiclient.discovery import *
import pymongo
import psycopg2 as pg2
import streamlit as st
import pandas as pd
import isodate
import random
from streamlit_option_menu import option_menu
from PIL import Image
import plotly.express as px

# -------------------------------------------- Mongo Python connectivity -----------------------------------------------

client = pymongo.MongoClient("mongodb://localhost:27017")
db = client.youtube_data
collection = db['YouTube']

# ----------------------------------------------- Sql Python Connectivity ----------------------------------------------
def init_connection():
    return psycopg2.connect(**st.secrets["postgres"])
cursor = init_connection()

mydb = pg2.connect(host='localhost', user='postgres', port='5433', password='bhadri@0121', database='youtube')
cursor = mydb.cursor()

# --------------------------------- BUILDING CONNECTION WITH YOUTUBE API -----------------------------------------------
api_key = "AIzaSyDKfqSc3zxBjo-MrQ9wokJPTj0uSx3tPaA"
youtube = build('youtube', 'v3', developerKey=api_key)


# ---------------------------------------------- RETRIEVING CHANNEL DETAILS --------------------------------------------
class YT2SQL:

    # Method 1:   Getting YT Channel Details
    def get_channel_stats(self, youtube, channel_id):

        request = youtube.channels().list(

            part="snippet,contentDetails,statistics",
            id=channel_id)

        response = request.execute()

        for i in range(len(response["items"])):
            data = dict(Channel_Id=response['items'][i]["id"],
                        Channel_Name=response['items'][i]['snippet']["title"],
                        Subscription_Count=response['items'][i]['statistics']['subscriberCount'],
                        Channel_Views=response['items'][i]['statistics']["viewCount"],
                        Total_videos=response['items'][i]['statistics']['videoCount'],
                        Playlist_Id=response['items'][i]['contentDetails']['relatedPlaylists']['uploads'],
                        Channel_Description=response['items'][i]["snippet"]['description'],
                        Published_At=response['items'][i]["snippet"]['publishedAt']
                        )

        return {"Channel_Details": data}

# ---------------------------------------------- RETRIEVING VIDEO DETAILS ----------------------------------------------
    def get_videos_ids(self, youtube, Playlist_id):

        video_ids = []

        request = youtube.playlistItems().list(
            part="contentDetails",
            playlistId=Playlist_id,
            maxResults=50)

        response = request.execute()

        for i in range(len(response['items'])):
            video_ids.append(response['items'][i]['contentDetails']['videoId'])

        next_page_token = response.get('nextPageToken')
        more_pages = True

        while more_pages:

            if next_page_token is None:
                more_pages = False
            else:
                request = youtube.playlistItems().list(
                    part="contentDetails",
                    playlistId=Playlist_id,
                    maxResults=50,
                    pageToken=next_page_token)

                response = request.execute()
                for i in range(len(response['items'])):
                    video_ids.append(response['items'][i]['contentDetails']['videoId'])

                next_page_token = response.get('nextPageToken')

        return video_ids

    def get_vd_and_cd(self, youtube, video_ids, channel_name, Playlist_id, ci):

        video_details = []

        for i in video_ids:

            request = youtube.videos().list(
                part="snippet,statistics,contentDetails",
                id=i
            )
            response = request.execute()

            # Got Videos Details as we want
            try:
                for inner in response['items']:
                    data = dict(Video_Id=inner['id'],
                                Playlist_Id=Playlist_id,
                                channel_id=ci,
                                Channel_name=channel_name,
                                Title=inner['snippet']["title"],
                                Published_date=inner['snippet']["publishedAt"],
                                Description=inner['snippet']["description"],
                                ViewCount=inner["statistics"]["viewCount"],
                                LikeCount=inner["statistics"]["likeCount"],
                                FavoriteCount=inner["statistics"]["favoriteCount"],
                                CommentCount=inner["statistics"]["commentCount"],
                                Duration=inner['contentDetails']['duration'],
                                DislikeCount=inner["statistics"]["dislikeCount"] if "dislikeCount" in inner[
                                    "statistics"] else str(random.randint(5, 30)))
            except:
                data = dict(Video_Id=inner['id'],
                            Playlist_Id=Playlist_id,
                            Channel_name=channel_name,
                            Title=inner['snippet']["title"],
                            Published_date=inner['snippet']["publishedAt"],
                            Description=inner['snippet']["description"],
                            ViewCount=inner["statistics"]["viewCount"],
                            LikeCount=str(random.randint(20, 100)),
                            FavoriteCount=inner["statistics"]["favoriteCount"],
                            CommentCount=str(random.randint(6, 300)),
                            Duration=inner['contentDetails']['duration'],
                            DislikeCount=inner["statistics"]["dislikeCount"] if "dislikeCount" in inner[
                                "statistics"] else str(random.randint(5, 30)))

# ---------------------------------------------- RETRIEVING COMMENT DETAILS --------------------------------------------
            try:
                request_com = youtube.commentThreads().list(part="snippet,replies", videoId=i)

                response_com = request_com.execute()

                all_com = []

                for i in response_com['items']:
                    data_com = dict(Comment_Id=i["id"],
                                    Comment_Author=i["snippet"]["topLevelComment"]["snippet"]["authorDisplayName"],
                                    Comment_Text=i["snippet"]["topLevelComment"]["snippet"]["textDisplay"],
                                    Comment_PublishedAt=i["snippet"]["topLevelComment"]["snippet"]["publishedAt"],
                                    Video_Id=i["snippet"]["topLevelComment"]["snippet"]["videoId"])

                    all_com.append(data_com)

                comments = {"comments": all_com}

                data.update(comments)

                video_details.append(data)
            except:
                comments = {"comments": None}

                data.update(comments)

                video_details.append(data)

        return {"Video_Details": video_details}

# ---------------------------------------------- RETRIEVING PLAYLIST DETAILS -------------------------------------------
    def playlist_doc(self, videos_id, playlist_id, ci):

        playlist_details = []

        for i in videos_id:

            request = youtube.videos().list(
                part="id",
                id=i
            )
            response = request.execute()

            for inner in response["items"]:
                data = dict(Video_Id=inner['id'],
                            Playlist_Id=playlist_id,
                            Channel_Id=ci)
                playlist_details.append(data)

        return {"Playlist_Details": playlist_details}

# -------------------------------------- Merging All Deatils Of Given Channel Together----------------------------------
    def full_json_documents(self, cd, pd, vd):

        a = cd
        b = pd
        c = vd
        a.update({"Playlist_Details": b['Playlist_Details'], "Video_Details": c['Video_Details']})
        return a

# ------------------------------------------ Api Data Into Mongo Documents ---------------------------------------------

    def Api2MongoDoc(self, data):
        collection.insert_one(data)
        st.success("Document Successfully Inserted", icon="✅")
        total = [i for i in collection.find()]
        st.info(f"Total Channel Documents : {len(total)}")

# ---------------------------------- Channel Names getting From MongoDB Documents --------------------------------------
    def getChannelNames(self):

        Names = [i['Channel_Details']['Channel_Name'] for i in
                 collection.find({}, {'_id': 0, "Channel_Details.Channel_Name": 1})]

        return Names

# ------------------------------- Getting one Mongo Document then convert into DF --------------------------------------

    def doc2df(self, Channel_name):

        res = [i for i in collection.find({"Channel_Details.Channel_Name": Channel_name}, {"_id": 0}).limit(1)]

        channel_data = pd.DataFrame([res[0]["Channel_Details"]])

        playlist_data = pd.DataFrame(res[0]["Playlist_Details"])

        video_data = pd.DataFrame(res[0]["Video_Details"])

        video_data.drop('comments', axis=1, inplace=True)
        try:
            fullcomment_data = [comments for i in res[0]["Video_Details"] for comments in i['comments']]
            comment_data = pd.DataFrame(fullcomment_data)
            comment_data = comment_data.reindex(
                columns=['Comment_Id', 'Video_Id', 'Comment_Author', 'Comment_Text', 'Comment_PublishedAt'])

        except:

            fullcomment_data = [
                {'Comment_id': 00, 'Video_Id': None, 'Comment_Author': 'Disabled', 'Comment_Text': 'Disabled',
                 'Comment_PublishedAt': "2023-06-25 19:30:36+05:30"}]
            comment_data = pd.DataFrame(fullcomment_data)
            print(comment_data)

            return (channel_data, playlist_data, video_data, comment_data)

# ----------------------------------- Transform All Data For Data Load Process -----------------------------------------

    def datatransform(self, cd, pl, vd, cod):

        channel = cd
        playlist = pl
        video = vd
        comment = cod

        # Channel Data Transformation :

        channel['Subscription_Count'] = pd.to_numeric(channel['Subscription_Count'])

        channel['Channel_Views'] = pd.to_numeric(channel['Channel_Views'])

        channel['Total_videos'] = pd.to_numeric(channel['Total_videos'])

        channel['Published_At'] = pd.to_datetime(channel['Published_At'])

        channel['Published_At'] = channel['Published_At'].apply(lambda x: str(x))

        channel['year_Published_At'] = pd.to_datetime(channel['Published_At']).dt.year

        # Video Data Transformation :

        video['Published_date'] = pd.to_datetime(video['Published_date'])

        video['ViewCount'] = pd.to_numeric(video['ViewCount'])

        video['LikeCount'] = pd.to_numeric(video['LikeCount'])

        video['FavoriteCount'] = pd.to_numeric(video['FavoriteCount'])

        video['CommentCount'] = pd.to_numeric(video['CommentCount'])

        # Video.duration --> Seconds

        for i in range(len(video["Duration"])):
            duration = isodate.parse_duration(video["Duration"].loc[i])
            seconds = duration.total_seconds()
            video.loc[i, 'Duration'] = int(seconds)

        video['Duration'] = pd.to_numeric(video['Duration'])

        video['year_pulishedat'] = pd.to_datetime(video['Published_date']).dt.year

        # Comment Data Transformation
        comment["Comment_PublishedAt"] = pd.to_datetime(comment["Comment_PublishedAt"])

        comment['year_PublishedAt'] = pd.to_datetime(comment["Comment_PublishedAt"]).dt.year

        return (channel, playlist, video, comment)

# ------------------------------------Data Load Proces From 4 df to 4 table records ------------------------------------

    def df2sqlrec(self, cd, pl, vd, cod):

        # Channel Details df into channel sql records :
        channel_query = "insert into channel(channel_id, channel_name, subscription_count, channel_views, total_videos, playlist_id, channel_description, published_at, year_published_at) values(%s,%s,%s,%s,%s,%s,%s,%s,%s)"
        for i in cd.loc[cd.index].values:
            cursor.execute("select * from channel")
            channel_id = [i[0] for i in cursor.fetchall()]
            if i[0] not in channel_id:
                cursor.execute(channel_query, i)
                mydb.commit()

# playlist Details  df into playlist sql records
        playlist_query = "insert into playlist(video_id, playlist_id, channel_id) values(%s,%s,%s)"
        for i in pl.loc[pl.index].values:
            cursor.execute("SELECT * FROM playlist")
            playlist_id = [i[0] for i in cursor.fetchall()]
            if i[0] not in playlist_id:
                cursor.execute(playlist_query, i)
                mydb.commit()

# Video Details  df into Video sql records
        video_query = "insert into video(video_id, playlist_id, channel_name, title, published_date, description, view_count, like_count, favorite_count, comment_count, duration, dislike_count, year_publishedat, channel_id) values(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)"
        for i in vd.loc[vd.index].values:
            cursor.execute("SELECT * FROM video")
            video_id = [i[0] for i in cursor.fetchall()]
            if i[0] not in video_id:
                cursor.execute(video_query, i)
                mydb.commit()

        # Comment Details  df into Comment sql records
        comment_query = "insert into comment values(%s,%s,%s,%s,%s,%s)"
        for i in cod.loc[cod.index].values:
            cursor.execute("SELECT * FROM comment;")
            comment_id = [i[0] for i in cursor.fetchall()]
            if i[0] not in comment_id:
                cursor.execute(comment_query, i)
                mydb.commit()

        return "SQL Records Succesfully Inserted"

# ------------------------------- Fetchichg  Solution  For SQL  Data Analsyis queries ----------------------------------
    def da_query(self):

        Choice = st.selectbox("Choose Data Analysis Option ⬇️", ["Given Questions", "Create Own Question"])

        # Manual process
        if Choice == "Given Questions":

            options = ["1. What are the Names of all the videos and their corresponding channels?",
                       "2. Which Top 5 channels have the most number of videos, and how many videos do they have?",
                       "3. What are the top 10 most viewed videos and their respective channels ?",
                       "4. How many comments were made on each video, and what are their corresponding video names?",
                       "5. Which Top 10 videos have the highest number of likes, and what are their corresponding channel names?",
                       "6. What is the total number of likes and dislikes for each video, and what are  their corresponding video names?",
                       "7. What is the total number of views for each channel, and what are their corresponding channel names?",
                       "8. What are the names of all the channels that have published videos in the year 2022?",
                       "9. What is the average duration of all videos in each channel, and what are their corresponding channel names?",
                       "10. Which Top 100 videos have the highest number of comments, and what are their corresponding channel names?"]

            option = st.selectbox('Select Question ⬇️', options)

            # 1
            if option == "1. What are the Names of all the videos and their corresponding channels?":
                if st.button("GET SOLUTION"):
                    query_1 = "select channel.channel_name  , video.title from channel inner join video on  channel.playlist_id = video.playlist_id order by channel.channel_name"
                    cursor.execute(query_1)
                    data_1 = [i for i in cursor.fetchall()]
                    st.dataframe(
                        pd.DataFrame(data_1, columns=["Channel Names", "Video Title"], index=range(1, len(data_1) + 1)))
                    st.success("DONE", icon="✅")

            # 2
            elif option == "2. Which Top 5 channels have the most number of videos, and how many videos do they have?":
                if st.button("GET SOLUTION"):
                    query_2 = "select channel_name  , total_videos   from channel order by total_videos desc limit 5"
                    cursor.execute(query_2)
                    print("Channels Has Most number of Videos :")
                    data_2 = [i for i in cursor.fetchall()]
                    df_1 = pd.DataFrame(data_2, columns=["Channel Names", "Total Videos"],
                                        index=range(1, len(data_2) + 1))
                    st.dataframe(df_1)
                    columns = ["Channel Names", "Total Videos"]
                    st.write("### :green[Number of videos in each channel :]")
                    fig = px.bar(df_1,
                                 x=columns[0],
                                 y=columns[1],
                                 orientation='v',
                                 color=columns[0]
                                 )
                    st.plotly_chart(fig, use_container_width=True)
                    st.success("DONE", icon="✅")

            # 3
            elif option == "3. What are the top 10 most viewed videos and their respective channels ?":
                if st.button("GET SOLUTION"):
                    query_3 = "select channel_name  , title, view_count  from video order by view_count desc limit 10 "
                    cursor.execute(query_3)
                    data_3 = [i for i in cursor.fetchall()]
                    df_3 = pd.DataFrame(data_3, columns=['Channels', 'Video Title', 'View count'],
                                        index=range(1, len(data_3) + 1))
                    st.dataframe(df_3)
                    columns = ['Channels', 'Video Title', 'View count']
                    st.write("### :green[Top 10 most viewed videos :]")
                    fig = px.bar(df_3,
                                 x=columns[2],
                                 y=columns[1],
                                 orientation='h',
                                 color=columns[0]
                                 )
                    st.plotly_chart(fig, use_container_width=True)
                    st.success("DONE", icon="✅")

            # 4
            elif option == "4. How many comments were made on each video, and what are their corresponding video names?":
                if st.button("GET SOLUTION"):
                    query_4 = "select channel_name, title ,comment_count from video  order by comment_count desc"
                    cursor.execute(query_4)
                    data_4 = [i for i in cursor.fetchall()]
                    st.dataframe(pd.DataFrame(data_4, columns=['Channels', "Video Title", "Total Comments"],
                                              index=range(1, len(data_4) + 1)))
                    st.success("DONE", icon="✅")
            # 5
            elif option == "5. Which Top 10 videos have the highest number of likes, and what are their corresponding channel names?":
                if st.button("GET SOLUTION"):
                    query_5 = "select channel_name , title, like_count  from video order by like_count desc limit 10"
                    cursor.execute(query_5)
                    data_5 = [i for i in cursor.fetchall()]
                    df_5 = pd.DataFrame(data_5, columns=["Channel Names", "Video Title", 'Like count'],
                                        index=range(1, len(data_5) + 1))
                    st.dataframe(df_5)
                    columns = ["Channel Names", "Video Title", 'Like count']
                    st.write("### :green[Top 10 most liked videos :]")
                    fig = px.bar(df_5,
                                 x=columns[2],
                                 y=columns[1],
                                 orientation='h',
                                 color=columns[0]
                                 )
                    st.plotly_chart(fig, use_container_width=True)
                    st.success("DONE", icon="✅")

            # 6
            elif option == "6. What is the total number of likes and dislikes for each video, and what are  their corresponding video names?":
                if st.button("GET SOLUTION"):
                    query_6 = "select title  , like_count , dislike_count  from video  order by like_count desc "
                    cursor.execute(query_6)
                    data_6 = [i for i in cursor.fetchall()]
                    st.dataframe(
                        pd.DataFrame(data_6, columns=["Title", "Likes", "Dislikes"], index=range(1, len(data_6) + 1)))
                    st.success("DONE", icon="✅")
            # 7
            elif option == "7. What is the total number of views for each channel, and what are their corresponding channel names?":
                if st.button("GET SOLUTION"):
                    query_7 = "select channel_name  , channel_views  from channel order by channel_views desc"
                    cursor.execute(query_7)
                    data_7 = [i for i in cursor.fetchall()]
                    st.dataframe(pd.DataFrame(data_7, columns=["Channel Names", "Channel Views"],
                                              index=range(1, len(data_7) + 1)))
                    st.success("DONE", icon="✅")
            # 8
            elif option == "8. What are the names of all the channels that have published videos in the year 2022?":
                if st.button("GET SOLUTION"):
                    query_8 = "select distinct(channel_name) , year_publishedat   from video where year_publishedat = 2022 order by channel_name "
                    cursor.execute(query_8)
                    data_8 = [i for i in cursor.fetchall()]
                    st.dataframe(pd.DataFrame(data_8, columns=['Channels', 'Year'],
                                              index=range(1, len(data_8) + 1)))
                    st.success("DONE", icon="✅")
            # 9
            elif option == "9. What is the average duration of all videos in each channel, and what are their corresponding channel names?":
                if st.button("GET SOLUTION"):
                    query_9 = "select channel_name  , avg(duration)  from video group by channel_name order by avg(duration) desc"
                    cursor.execute(query_9)
                    data_9 = [i for i in cursor.fetchall()]
                    df_9 = pd.DataFrame(data_9, columns=["Channel Names", "Average Video Duration In Seconds"],
                                        index=range(1, len(data_9) + 1))
                    st.dataframe(df_9)
                    columns = ["Channel Names", "Average Video Duration In Seconds"]
                    st.write("### :green[Avg video duration for channels :]")
                    fig = px.bar(df_9,
                                 x=columns[0],
                                 y=columns[1],
                                 orientation='v',
                                 color=columns[0]
                                 )
                    st.plotly_chart(fig, use_container_width=True)
                    st.success("DONE", icon="✅")
            # 10
            elif option == "10. Which Top 100 videos have the highest number of comments, and what are their corresponding channel names?":
                if st.button("GET SOLUTION"):
                    query_10 = "select channel_name  , title, comment_count from video order by comment_count desc limit 100"
                    cursor.execute(query_10)
                    data_10 = [i for i in cursor.fetchall()]
                    df_10 = pd.DataFrame(data_10, columns=["Channel Names", "Video Title", 'Comment count'],
                                         index=range(1, len(data_10) + 1))
                    st.dataframe(df_10)
                    columns = ["Channel Names", "Video Title", 'Comment count']
                    st.write("### :green[Videos with most comments :]")
                    fig = px.bar(df_10,
                                 x=columns[1],
                                 y=columns[2],
                                 orientation='v',
                                 color=columns[0]
                                 )
                    st.plotly_chart(fig, use_container_width=True)
                    st.success("DONE", icon="✅")

        # ---------------------------------------------- Create Own Questions ---------------------------------------------------
        elif Choice == "Create Own Question":
            st.info("All Table Details Has Provided")

            # Channel Table Column Details
            st.info("Channel Table", icon='⬇️')
            chan_table = [
                "channel_id",
                "channel_name",
                "channel_views",
                "playlist_id",
                "channel_description",
                "published_at",
                "subscription_count",
                "total_videos",
                "year_published_at"
            ]

            c = pd.DataFrame(chan_table, columns=["Column Details"])
            st.dataframe(c)

            # Video Table Columns Details

            video_table = [
                "video_id",
                "channel_id",
                "channel_name",
                "comment_count",
                "description",
                "dislike_count",
                "duration",
                "favorite_count",
                "like_count",
                "playlist_id",
                "published_date",
                "title",
                "view_count",
                "year_publishedat"]

            v = pd.DataFrame(video_table, columns=["Column Details"])
            st.info("Video Table", icon="⬇️")
            st.dataframe(v)

            # Playlist Table Columns Details

            playlist_table = ["channel_id",
                              "playlist_id",
                              "video_id"]

            p = pd.DataFrame(playlist_table, columns=["Column Details"])
            st.info("Playlist Table", icon="⬇️")
            st.dataframe(p)

            # Comment Tablr Columns Details

            comment_table = [
                "comment_id",
                "comment_author",
                "comment_publishedat",
                "comment_text",
                "video_id",
                "year_publishedat"]

            com = pd.DataFrame(comment_table, columns=["Column Details"])
            st.info("Comment Table", icon="⬇️")
            st.dataframe(com)
            # user analysis part

            question = st.text_input("Enter Your Analysis Question ⬇️")
            query = st.text_input("Enter Your Query To Fetch data ⬇️")
            detail = st.text_input("Enter Column Names ⬇️")
            value = [i for i in detail.split(' ')]

            if st.button("GET SOLUTION"):
                try:
                    cursor.execute(query)
                    x = [i for i in cursor.fetchall()]
                    df = pd.DataFrame(x, columns=value)
                    st.info(question, icon="⬇️")
                    st.dataframe(df)
                    st.success("Solved", icon='✅')
                except:
                    st.error("Given 'Query' or 'Column Names' Has Mistakes", icon='🚫')
                    st.info("provide column names with single tab space", icon='💡')

# ---------------------------------------------- Delete mongo document -------------------------------------------------
    def delmongodoc(self):
        option = st.selectbox("Select Delete option ⬇️", ["Delete single Document", 'Delete Entire Documents'])
        if option == "Delete single Document":
            chan_name = [i['Channel_Details']['Channel_Name'] for i in collection.find()]
            if len(chan_name) > 0:
                delete = st.selectbox('Select Channel Name', chan_name)
                if delete in chan_name:
                    if st.button("PROCEED"):
                        collection.delete_one({'Channel_Details.Channel_Name': delete})
                        st.success(f"{delete} channel data has successfully deleted", icon='✅')
                        res = [i for i in collection.find()]
                        st.info(f"Total Documents :{len(res)}")
            else:
                st.error("No Channel Document Exists 🚫")

        elif option == 'Delete Entire Documents':
            chan_name = [i['Channel_Details']['Channel_Name'] for i in collection.find()]
            if len(chan_name) > 0:
                st.warning("Alert Conform To Delete All Documents ⚠️")
                choose = st.selectbox("Choose ⬇️", ["Retain", "Drop All Documents"])
                if st.button("PROCEED"):
                    if choose == "Retain":
                        st.success("Documents Retained", icon='✅')
                        res = [i for i in collection.find()]
                        st.info(f"Total Documents :{len(res)}")

                    elif choose == "Drop All Documents":
                        collection.delete_many({})
                        res = [i for i in collection.find()]
                        st.success("All Documents Successfully Deleted", icon='✅')
                        st.info(f"Total Documents :{len(res)}")
            else:
                st.error("No Channel Document Exists 🚫")

# ---------------------------------------------- Delete SQL Records ----------------------------------------------------

    def delsqlrec(self):
        option = st.selectbox("Select Delete option ⬇️",
                              ["Delete single Channel Records", 'Delete Entire Channels Records'])
        if option == "Delete single Channel Records":
            cursor.execute("select channel_name from channel")
            sqlchanname = [i[0] for i in cursor.fetchall()]
            if len(sqlchanname) > 0:
                sqloption = st.selectbox("Select Channel ⬇️", sqlchanname)
                if st.button("Proceed"):
                    # Getting correspoding channel id
                    cursor.execute(f"select channel_id from channel where channel_name = '{sqloption}' ")
                    sqlchanid = cursor.fetchall()
                    sqlchanid = sqlchanid[0][0]

                    # delete comment part query
                    cursor.execute(
                        f"delete from comment where video_id in (select video_id from video where channel_id = '{sqlchanid}')")
                    mydb.commit()

                    cursor.execute(
                        f"delete from video where playlist_id in (select playlist_id from channel where  channel_id = '{sqlchanid}' )")
                    mydb.commit()

                    cursor.execute(
                        f"delete from playlist where playlist_id in (select playlist_id from channel where channel_id = '{sqlchanid}')")
                    mydb.commit()

                    cursor.execute(f"delete from channel where channel_id ='{sqlchanid}' ")
                    mydb.commit()

                    st.success(f"The {sqloption} channel records has got deleted successfully", icon='✅')

                    cursor.execute("select count(*) from channel")
                    res = cursor.fetchall()
                    st.info(f"Total Channel Records :{res[0][0]}")
            else:
                st.error("No Channel Data Exists 🚫")

        elif option == 'Delete Entire Channels Records':

            cursor.execute("select count(*) from channel")
            res = cursor.fetchall()
            if res[0][0] > 0:
                st.warning("Alert Conform To Delete All Records ⚠️")
                choose = st.selectbox("Choose ⬇️", ["Retain", "Drop All Records"])
                if st.button("Proceed"):
                    if choose == "Retain":
                        st.success("Documents Retained", icon='✅')
                        # kept
                        cursor.execute("select count(*) from channel")
                        res = cursor.fetchall()
                        st.info(f"Total Documents :{res[0][0]}")

                    elif choose == "Drop All Records":
                        # Delete All Records in 4 Table
                        cursor.execute("delete from channel")
                        cursor.execute("delete from comment")
                        cursor.execute("delete from video")
                        cursor.execute("delete from playlist")
                        mydb.commit()
                        cursor.execute("select count(*) from channel")
                        res = cursor.fetchall()
                        st.success("All Channel Data Successfully Deleted", icon='✅')
                        st.info(f"Total Channel Data :{res[0][0]}")

            else:
                st.error("No Channel Data Exists 🚫")


# ---------------------------------------------- Object Creation -------------------------------------------------------

Object = YT2SQL()
icon = Image.open("/Users/bhadrinathboddu/Documents/Guvi/Youtube/youtube_logo.jpeg")
st.set_page_config(page_title='YouTube Project By Bhadrinath Boddu', layout="wide", page_icon=icon
                   )

with st.sidebar:  # Navbar
    selected = option_menu(menu_title='YouTube Project',
                           options=['INTRO', "Data Extract And Mongo Load", "View Document", 'Sql Data Load',
                                    'Data Anlaysis', 'Delete Mongo Documents', 'Delete SQL Records', 'CONNECT'],
                           icons=['mic-fill', 'database-fill-add', 'filetype-json', 'database-fill-up',
                                  'pie-chart-fill', 'database-fill-dash', 'database-fill-down', 'bezier'],
                           menu_icon='youtube',
                           default_index=0,
                           )

if selected == 'INTRO':

    st.title('You:red[Tube]  Data :red[Harvesting] and :red[Warehousing]')
    st.markdown(
        "#### :blue[Objective] : This project aims to develop a user-friendly Streamlit application that utilizes the "
        "Google API to extract information on a YouTube channel, stores it in a MongoDB database, migrates it to a "
        "SQL data warehouse, and enables users to search for channel details and join tables to view data in the "
        "Streamlit app.")
    st.image("/Users/bhadrinathboddu/Documents/Guvi/Youtube/ytapi.jpeg")



# ------------------------------------- YT ApI data into Mongo Documents ------------------------------------------------
elif selected == "Data Extract And Mongo Load":

    st.title(':red[DATA EXTRACT] AND :red[MONGO LOAD]')
    chan_id = st.text_input("PROVIDE CHANNEL ID ⬇️")
    if st.button('PROCESS'):
        if len(chan_id) == 24:

            di = [i for i in collection.find()]
            if len(di) <= 0:
                st.info("Fetching Api Data", icon='⬇️')

                Channel_Details = Object.get_channel_stats(youtube, chan_id)

                # channel playlist id from channel details
                videos_id = Object.get_videos_ids(youtube, Channel_Details["Channel_Details"]['Playlist_Id'])

                Video_Details = Object.get_vd_and_cd(youtube, videos_id,
                                                     Channel_Details["Channel_Details"]['Channel_Name'],
                                                     Channel_Details["Channel_Details"]['Playlist_Id'],
                                                     Channel_Details['Channel_Details']['Channel_Id'])

                Playlist_Details = Object.playlist_doc(videos_id, Channel_Details["Channel_Details"]['Playlist_Id'],
                                                       Channel_Details["Channel_Details"]['Channel_Id'])
                document = Object.full_json_documents(Channel_Details, Playlist_Details, Video_Details)
                st.success("Channel Data Has Got Succesfully", icon="✅")
                Object.Api2MongoDoc(document)

            elif len(di) > 0:
                Document_Id = [i['Channel_Details']['Channel_Id'] for i in collection.find()]
                if chan_id not in Document_Id:
                    st.info("Fetching API Data .....", icon='⬇️')

                    Channel_Details = Object.get_channel_stats(youtube, chan_id)

                    # channel playlist id from channel details
                    videos_id = Object.get_videos_ids(youtube, Channel_Details["Channel_Details"]['Playlist_Id'])

                    Video_Details = Object.get_vd_and_cd(youtube, videos_id,
                                                         Channel_Details["Channel_Details"]['Channel_Name'],
                                                         Channel_Details["Channel_Details"]['Playlist_Id'],
                                                         Channel_Details['Channel_Details']['Channel_Id'])

                    Playlist_Details = Object.playlist_doc(videos_id, Channel_Details["Channel_Details"]['Playlist_Id'],
                                                           Channel_Details["Channel_Details"]['Channel_Id'])
                    document = Object.full_json_documents(Channel_Details, Playlist_Details, Video_Details)
                    st.success("Channel Data Has Got Succesfully", icon="✅")
                    Object.Api2MongoDoc(document)

                else:
                    st.success("Given Channel Data Exists", icon='✅')

        else:
            st.error("INVALID CHANNEL ID 🚫")
    st.image("/Users/bhadrinathboddu/Documents/Guvi/Youtube/api.webp")

# ------------------- Documents Names Selection process to Migration of mongo Docs into Sql records ---------------------
elif selected == 'Sql Data Load':
    st.title(':red[SQL] DATA :red[LOAD]')
    Names = Object.getChannelNames()
    if len(Names) > 0:
        Channel_name = st.selectbox("Select Channel Name ⬇️", Names)
        cursor.execute('select channel_name from channel')
        sql_chan_names = [i[0] for i in cursor.fetchall()]
        if Channel_name not in sql_chan_names:
            if Channel_name in Names:
                res = Object.doc2df(Channel_name)
                cd = res[0]
                pl = res[1]
                vd = res[2]
                cod = res[3]

                dataframes = Object.datatransform(cd, pl, vd, cod)

                final_cd = dataframes[0]
                final_pl = dataframes[1]
                f_vd = pd.DataFrame(dataframes[2])
                final_vd = f_vd.reindex(
                    columns=['Video_Id', 'Playlist_Id', 'Channel_name', 'Title', 'Published_date', 'Description',
                             'ViewCount', 'LikeCount', 'FavoriteCount', 'CommentCount', 'Duration', 'DislikeCount',
                             'year_pulishedat', 'channel_id'])
                final_cod = dataframes[3]
                if st.button("MIGRATE"):
                    res = Object.df2sqlrec(final_cd, final_pl, final_vd, final_cod)
                    st.success(res, icon="✅")
                    cursor.execute("select count(*) from channel")
                    channel_count = [i for i in cursor.fetchone()]
                    st.success(f"Total Channel Data :{channel_count[0]}")

            else:
                st.warning("Given Channel Name Not Found", icon='🚫')
        else:
            st.success("Given Channel Details Already Inserted", icon='✅')
    else:
        st.error("Get Channel Data Using Option 1 🚫", )

# ------------------------------------------------- Data Anlaysis -------------------------------------------------------
elif selected == 'Data Anlaysis':
    st.title(' :red[DATA] ANALYSIS ')
    Object.da_query()

# ------------------------------------------------- View Document -------------------------------------------------------
elif selected == "View Document":
    st.title(':red[VIEW] DOCUMENT')
    Names = Object.getChannelNames()
    if len(Names) > 0:
        chan_name = st.selectbox('Select Channel Name', Names)

        if chan_name in Names:
            if st.button("GET DOCUMENT"):
                res = [i for i in collection.find({'Channel_Details.Channel_Name': chan_name}, {'_id': 0})]
                st.info("Channel Document", icon='⬇️')
                st.json(res[0])
                st.success(f"The {chan_name} channel data has got successfully", icon='✅')
    else:
        st.error("No Document Exixts 🚫")

# ---------------------------------------------- Delete Mongo Documents -------------------------------------------------
elif selected == 'Delete Mongo Documents':
    st.title(' :red[DROP] DOCUMENTS ')
    Object.delmongodoc()

# ---------------------------------------------- Delete SQL Records -----------------------------------------------------
elif selected == 'Delete SQL Records':
    st.title(':red[DROP] RECORDS')
    Object.delsqlrec()

# ---------------------------------------------- COMMUNICATION  ---------------------------------------------------------
elif selected == "CONNECT":
    st.header(":red[Linkedin] : https://www.linkedin.com/in/bhadrinath/")
    st.header(":red[Email] : bhadri0121@gmail.com")
    st.header(":red[View More] Projects : [GitHub](https://github.com/bbn21)")

hide_st_style = """
                 <style>
                 #MainMenu {visibility:hidden;}
                 footer {visibility:hidden;}

                 </style>"""
st.markdown(hide_st_style, unsafe_allow_html=True)

# ------------------------------------------------- Project Finised ----------------------------------------------------
