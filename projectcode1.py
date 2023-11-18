from googleapiclient.discovery import build
import pymongo
import psycopg2
import pandas as pd
import streamlit as st

#key connection
def the_key_connect():
    api_key = 'AIzaSyDcaadRm4usjF8iv3NTlPcTUzMh7TeSNoE'
    youtube = build('youtube','v3', developerKey = api_key)
    return youtube
youtube = the_key_connect()

#channel info
def the_channel_info(channel_id): 
    request = youtube.channels().list(
        part="snippet,contentDetails,statistics",
        id=channel_id
    )
    
    response = request.execute()
    for i in response['items']:
        data=dict(Channel_name = i["snippet"]["title"],
                  Channel_id = i["id"],
                  Subcription_count = i['statistics']['subscriberCount'],
                  Channel_views = i["statistics"]["viewCount"],
                  Channel_description = i["snippet"]["description"],
                  Playlist_Id = i['contentDetails']['relatedPlaylists']['uploads'],
                  Total_videos =  i["statistics"]["videoCount"],
        
                 )
    return data

def the_video_id(channel_id):
    video_ids=[]
    try:
        response = youtube.channels().list(
            id = channel_id,
            part = 'contentDetails').execute()
        Playlist_Id = response['items'][0]['contentDetails']['relatedPlaylists']['uploads']
        token_for_nextpage = None
        while True:
            response1 = youtube.playlistItems().list(
                part = 'snippet',
                playlistId = Playlist_Id,
                maxResults = 50,
                pageToken = token_for_nextpage).execute()
            for i in range(len(response1['items'])):
                video_ids.append(response1['items'][i]['snippet']['resourceId']['videoId'])
            token_for_nextpage = response1.get('nextPageToken')
            if token_for_nextpage is None:
                break
    except Exception as e:
        print(f"Error: {e}")
    print(f"Total videos collected:{len(video_ids)}")
    return video_ids

#seperate video info
def the_video_info(video_ids):
    video_data=[]
    for video_id in video_ids:
        request = youtube.videos().list(
            part = "snippet,contentDetails,statistics",
            id = video_id
        )
        response = request.execute()
        for item in response['items']:
            data = dict(Channel_Name = item["snippet"]["channelTitle"],
                        Channel_Id = item["snippet"]["channelId"],
                        Video_Id = item['id'],
                        Title = item["snippet"]["title"],
                        Tags = item['snippet'].get('tags'),
                        Thumbnail = item["snippet"]["thumbnails"]['default']['url'],
                        Description = item["snippet"].get("description"),
                        Published_Date =item["snippet"]["publishedAt"],
                        Duration =item["contentDetails"]["duration"],
                        Views = item["statistics"].get("viewCount"),
                        Likes = item["statistics"].get("likeCount"),
                        Comments = item["statistics"].get("commentCount"),
                        Favorite_Count = item["statistics"]["favoriteCount"],
                        #Definition = ["contentDetails"]["definition"],
                        #Caption_Status = ["contentDetails"]["caption"]
                       )
            video_data.append(data)
    return video_data

#comment info
def the_comment_info(vd_id):
    comment_data= []
    try:
        for video_id in vd_id:
            request = youtube.commentThreads().list(
                part = 'snippet',
                videoId = video_id,
                maxResults = 50)
            response =  request.execute()
            for item in response['items']:
                data = dict(Comment_Id = item['snippet']['topLevelComment']['id'],
                            Video_Id =item['snippet']['topLevelComment']['snippet']['videoId'],
                            Comment_Text = item['snippet']['topLevelComment']['snippet']['textDisplay'],
                            Comment_Author = item['snippet']['topLevelComment']['snippet']['authorDisplayName'],
                            Comment_Published = item['snippet']['topLevelComment']['snippet']['publishedAt'])
            comment_data.append(data)
            
    except:
        pass
    return comment_data

#playlist details
def the_playlist_info(channel_id):
    token_for_nextpage = None
    aggregate_data = []
    while True:
        request = youtube.playlists().list(
            part = 'snippet,contentDetails',
            channelId = channel_id,
            maxResults = 50,
            pageToken = token_for_nextpage
            
        )
        
        response = request.execute()
        for item in response['items']:
            data = dict(
                Playlist_Id = item['id'],
                Title = item['snippet']['title'],
                Channel_Id = item['snippet']['channelId'],
                Channel_Name = item['snippet']['channelTitle'],
                PublishedAt = item['snippet']['publishedAt'],
                Video_Count = item['contentDetails']['itemCount']
                
            )
            aggregate_data.append(data)
        token_for_nextpage = response.get('nextPageToken')
        if token_for_nextpage is None:
            break
    return aggregate_data

#mongodb uplaod 
con = pymongo.MongoClient("mongodb+srv://gayaraga:gayathri97!!@cluster0.klxzcmo.mongodb.net/?retryWrites=true&w=majority")
db = con["Youtube_Data_Harvesting"]


def channel_details(channel_id):
    chn_details = the_channel_info(channel_id)
    plylst_details = the_playlist_info(channel_id)
    v_id_details = the_video_id(channel_id)
    vi_details = the_video_info(v_id_details)
    com_details = the_comment_info(v_id_details)
    
    col2 = db['Channel_Details']
    col2.insert_one({'channel_information':chn_details,'playlist_information':plylst_details,
                          'video_information':vi_details,'comment_information':com_details,})
    return 'upload successful'


#table creation for channels
def channels_table():
    mydb= psycopg2.connect(host = 'localhost',
                             user = 'postgres',
                             password = 'gayathri97!!',
                             database = 'ytbdata1',
                             port = '5432'
                                )
    
    mycursor = mydb.cursor()
    #drop_query = '''drop table if exists channel'''
    #mycursor.execute(drop_query)
    #mydb.commit()
    try:
        create_table_query = '''CREATE TABLE IF NOT EXISTS channels(
            Channel_name VARCHAR(100),
            Channel_id VARCHAR(80) PRIMARY KEY,
            Subscription_count BIGINT,
            Channel_views BIGINT,
            Channel_description TEXT,
            Playlist_Id VARCHAR(80),
            Total_videos INT
        )'''
    
        # Execute the query
        mycursor.execute(create_table_query)
    
        # Commit the changes
        mydb.commit()
    except Exception as e:
        print(f"Error: {e}")
        print('Channel table creation failed or already exists.')
    #finally:
        # Close the cursor and connection in a finally block
       # mycursor.close()
        #mydb.close()
    #
    chn_list = []
    db = con['Youtube_Data_Harvesting']
    col2 = db["Channel_Details"]
    for chn_data in col2.find({},{'_id':0, 'channel_information':1}):
        chn_list.append(chn_data['channel_information'])
    df = pd.DataFrame(chn_list)
    
    
    #inserting into tables
    for index,row in df.iterrows():
        insert_query = '''insert into channels(Channel_name,
                            Channel_id,
                            Subscription_count,
                            Channel_views,
                            Channel_description,
                            Playlist_Id,
                            Total_videos)
                            
                            values(%s,%s,%s,%s,%s,%s,%s)'''
        values =(row['Channel_name'],
                 row['Channel_id'],
                 row['Subcription_count'],
                 row['Channel_views'],
                 row['Channel_description'],
                 row['Playlist_Id'],
                 row['Total_videos'])
        try:
            mycursor.execute(insert_query,values)
            mydb.commit()
        except:
            print('channels values are already inserted')

#playlist table creation
def playlists_table():
    mydb= psycopg2.connect(host = 'localhost',
                                 user = 'postgres',
                                 password = 'gayathri97!!',
                                 database = 'ytbdata1',
                                 port = '5432'
                                    )
        
    mycursor = mydb.cursor()
    #drop_query = '''drop table if exists channel'''
    #mycursor.execute(drop_query)
    #mydb.commit()
    try:
        create_table_query = '''CREATE TABLE IF NOT EXISTS playlists(
            Playlist_Id varchar(100) primary key,
            Title varchar(100),
            Channel_name varchar(100),
            Channel_id VARCHAR(80),
            PublishedAt timestamp ,
            Video_count int
        )'''
    
        # Execute the query
        mycursor.execute(create_table_query)
    
        # Commit the changes
        mydb.commit()
    except Exception as e:
        print(f"Error: {e}")
        print('playlist table creation failed or already exists.')
    #finally:
        # Close the cursor and connection in a finally block
       # mycursor.close()
        #mydb.close()
    ply_list = []
    db = con['Youtube_Data_Harvesting']
    col2 = db["Channel_Details"]
    for ply_data in col2.find({},{'_id':0, 'playlist_information':1}):
        for i in range(len(ply_data['playlist_information'])):
            ply_list.append(ply_data['playlist_information'][i])
    df1 = pd.DataFrame(ply_list)
    #inserting in table
    for index,row in df1.iterrows():
        insert_query = '''insert into playlists(Playlist_Id,
                            Title,
                            Channel_name,
                            Channel_id ,
                            PublishedAt,
                            Video_Count)
                            
                            values(%s,%s,%s,%s,%s,%s)'''
    
        values =(row['Playlist_Id'],
                 row['Title'],
                 row['Channel_Id'],
                 row['Channel_Name'],
                 row['PublishedAt'],
                 row['Video_Count'])
                 
        try:
            mycursor.execute(insert_query,values)
            mydb.commit()
        except:
            print('channels values are already inserted')

#video table creation
def videos_table():
    mydb= psycopg2.connect(host = 'localhost',
                                 user = 'postgres',
                                 password = 'gayathri97!!',
                                 database = 'ytbdata1',
                                 port = '5432'
                                    )
        
    mycursor = mydb.cursor()
    #drop_query = '''drop table if exists channel'''
    #mycursor.execute(drop_query)
    #mydb.commit()
    try:
        create_table_query = '''CREATE TABLE IF NOT EXISTS videos(
            Channel_Name VARCHAR(100),
            Channel_Id VARCHAR(100),
            Video_Id varchar(30),
            Title varchar(150),
            Tags TEXT,
            Thumbnail VARCHAR(200),
            Description text,
            Published_Date timestamp,
            Duration interval,
            Views bigint,
            Likes bigint,
            Comments int,
            Favorite_Count int
                
            )'''
        # Execute the query
        mycursor.execute(create_table_query)
    
        # Commit the changes
        mydb.commit()
    except Exception as e:
        print(f"Error: {e}")
        print('Channel table creation failed or already exists.')
    vd_list = []
    db = con['Youtube_Data_Harvesting']
    col2 = db["Channel_Details"]
    for vd_data in col2.find({},{'_id':0,'video_information':1}):
        for i in range(len(vd_data['video_information'])):
            vd_list.append(vd_data['video_information'][i])
    df3 = pd.DataFrame(vd_list) 
    for index,row in df3.iterrows():
        insert_query = '''insert into videos(Channel_Name,
                                                Channel_Id,
                                                Video_Id,
                                                Title,
                                                Tags,
                                                Thumbnail,
                                                Description,
                                                Published_Date,
                                                Duration,
                                                Views,
                                                Likes,
                                                Comments,
                                                Favorite_Count)
                            
                                                values(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)'''
        values =(row['Channel_Name'],
                 row['Channel_Id'],
                 row['Video_Id'],
                 row['Title'],
                 row['Tags'],
                 row['Thumbnail'],
                 row['Description'],
                 row['Published_Date'],
                 row['Duration'],
                 row['Views'],
                 row['Likes'],
                 row['Comments'],
                 row['Favorite_Count'])
        try:
            mycursor.execute(insert_query,values)
            mydb.commit()
        except:
            print('channels values are already inserted')

def comments_table():
    mydb= psycopg2.connect(host = 'localhost',
                                 user = 'postgres',
                                 password = 'gayathri97!!',
                                 database = 'ytbdata1',
                                 port = '5432'
                                    )
        
    mycursor = mydb.cursor()
    try:
        create_table_query = '''CREATE TABLE IF NOT EXISTS comments(
            Comment_Id varchar(100),
            Video_Id varchar(50),
            Comment_Text text,
            Comment_Author varchar(150),
            Comment_Published timestamp)'''
        # Execute the query
        mycursor.execute(create_table_query)
    
        # Commit the changes
        mydb.commit()
    except Exception as e:
        print(f"Error: {e}")
        print('comment table creation failed or already exists.')
    com_list = []
    db = con['Youtube_Data_Harvesting']
    col2= db["Channel_Details"]
    for com_data in col2.find({},{'_id':0,'comment_information':1}):
        for i in range(len(com_data['comment_information'])):
            com_list.append(com_data['comment_information'][i])
    df4 = pd.DataFrame(com_list) 
    for index,row in df4.iterrows():
        insert_query = '''insert into comments(Comment_Id,
                                                Video_Id,
                                                Comment_Text,
                                                Comment_Author,
                                                Comment_Published)
                                                values(%s,%s,%s,%s,%s)'''
        values =(row['Comment_Id'],
                 row['Video_Id'],
                 row['Comment_Text'],
                 row['Comment_Author'],
                 row['Comment_Published'])
        
        mycursor.execute(insert_query,values)
        mydb.commit()

def tables():
    channels_table(),
    videos_table(),
    playlists_table(),
    comments_table(),
    return 'tables created successfully'

def show_channels_table():
    chn_list = []
    db = con['Youtube_Data_Harvesting']
    col2 = db["Channel_Details"]
    for chn_data in col2.find({},{'_id':0, 'channel_information':1}):
        chn_list.append(chn_data['channel_information'])
    df = st.dataframe(chn_list)
    return df

def show_playlists_table():
    ply_list = []
    db = con['Youtube_Data_Harvesting']
    col2 = db["Channel_Details"]
    for ply_data in col2.find({},{'_id':0, 'playlist_information':1}):
        for i in range(len(ply_data['playlist_information'])):
            ply_list.append(ply_data['playlist_information'][i])
    df1 = st.dataframe(ply_list)
    return df1


def show_videos_table():
    vd_list = []
    db = con['Youtube_Data_Harvesting']
    col2 = db["Channel_Details"]
    for vd_data in col2.find({},{'_id':0,'video_information':1}):
        for i in range(len(vd_data['video_information'])):
            vd_list.append(vd_data['video_information'][i])
    df3 = st.dataframe(vd_list)
    return df3


def show_comments_table():
    com_list = []
    db = con['Youtube_Data_Harvesting']
    col2 = db["Channel_Details"]
    for com_data in col2.find({},{'_id':0,'comment_information':1}):
        for i in range(len(com_data['comment_information'])):
            com_list.append(com_data['comment_information'][i])
    df4 = st.dataframe(com_list) 
    return df4

#streamlit part
with st.sidebar:
    st.title(":red[YOUTUBE DATA HARVESTING]")
    st.header("Skills")
    st.title("Python scripting")
    st.title("Data Collections")
    st.title("mongoDB")
    st.title("API integration")
    st.title("Data management using mongoDB and SQL")
channel_id = st.text_input("ENTER THE CHANNEL ID")
if st.button("collect and store data"):
    chn_ids = []
    db = con['Youtube_Data_Harvesting']
    col2 = db["Channel_Details"]
    for chn_data in col2.find({},{'_id':0, 'channel_information':1}):
        chn_ids.append(chn_data['channel_information']['Channel_id'])
    if channel_id in chn_ids:
        st.success("channel details of given channel id already exists")
    else:
        insert=channel_details(channel_id)
        st.success(insert)
if st.button('Migrate To SQL'):
        tables_done = tables()
        st.success(tables_done)
show_table = st.radio("SELECT TABLE FOR VIEWING",("CHANNELS","VIDEOS","PLAYLISTS","COMMENTS"))
if show_table=="CHANNELS":
    show_channels_table()
elif show_table=="VIDEOS":
    show_videos_table()
elif show_table=="PLAYLISTS":
    show_playlists_table()
elif show_table=="COMMENTS":
    show_comments_table()
mydb= psycopg2.connect(host = 'localhost',
                             user = 'postgres',
                             password = 'gayathri97!!',
                             database = 'ytbdata1',
                             port = '5432'
                                )
    
mycursor = mydb.cursor()
question = st.selectbox("SELECT THE QUESTION",("1.What are the names of all the videos and their corresponding channels? ",
                                               "2.Channels having most number of videos and how many videos do they have?",
                                               "3.What are the top 10 most viewed videos and their respective channels?",
                                               "4.What are the comments on each video and their corresponding channel names?",
                                               "5.Which video have higesht likes and their corresponding channel names?",
                                               "6.What is the total likes.dislikes for each video and their corresponding video names?",
                                               "7.What is the total views for each channel and what are their corresponding channel names?",
                                               "8.What are the names of all the channels that published videos in the year 2022?",
                                               "9.What is the average duration of all videos in each channel and their corresponding channel names?",
                                               "10.Which videos have the highest number of comments and their corresponding channel names?"))


if question=="1.What are the names of all the videos and their corresponding channels? ":
    query1 = '''select title as videos,channel_name as channelname from videos'''
    mycursor.execute(query1)
    mydb.commit()
    t1=mycursor.fetchall()
    df=pd.DataFrame(t1,columns=["videotitles","channelname"])
    st.write(df)
elif question== "2.Channels having most number of videos and how many videos do they have?":
    query2= '''select channel_name as channelname,total_videos as no_videos from channels order by total_videos desc'''
    mycursor.execute(query2)
    mydb.commit()
    t2=mycursor.fetchall()
    df2=pd.DataFrame(t2,columns=["channel name","No of videos"])
    st.write(df2)
elif question== "3.What are the top 10 most viewed videos and their respective channels?":
    query3= '''select views as views,channel_name as channelname,title as videotitle from videos where views is not null order by views desc limit 10'''
    mycursor.execute(query3)
    mydb.commit()
    t3=mycursor.fetchall()
    df3=pd.DataFrame(t3,columns=["views","channelname","videotitle"])
    df3
    st.write(df3)
elif question== "4.What are the comments on each video and their corresponding channel names?":
    query4= '''select comments as no_comments,title as videotitle from videos where comments is not null'''
    mycursor.execute(query4)
    mydb.commit()
    t4=mycursor.fetchall()
    df4=pd.DataFrame(t4,columns=["no of comments","videotitle"])
    st.write(df4)
elif question== "5.Which video have higesht likes and their corresponding channel names?":
    query5= '''select title as videotitle,channel_name as channelname,likes as likecount
                from videos where likes is  not null order by likes desc'''
    mycursor.execute(query5)
    mydb.commit()
    t5=mycursor.fetchall()
    df5=pd.DataFrame(t5,columns=["videotitle","channelname","likecount"])
    st.write(df5)
elif question== "6.What is the total likes,dislikes for each video and their corresponding video names?":
    query6= '''select likes as likecount,title as videotitle from videos'''
    mycursor.execute(query6)
    mydb.commit()
    t6=mycursor.fetchall()
    df6=pd.DataFrame(t6,columns=["likecount","videotitle"])
    st.write(df6)
elif question== "7.What is the total views for each channel and what are their corresponding channel names?":
    query7= '''select channel_name as channelname ,channel_views as totalviews from channels'''
    mycursor.execute(query7)
    mydb.commit()
    t7=mycursor.fetchall()
    df7=pd.DataFrame(t7,columns=["channel name","totalviews"])
    st.write(df7)

elif question== "8.What are the names of all the channels that published videos in the year 2022?":
    query8= '''select title as videotitle,published_date as videorelease,channel_name as channelname from videos
                where extract(year from published_date)=2022'''
    mycursor.execute(query8)
    mydb.commit()
    t8=mycursor.fetchall()
    df8=pd.DataFrame(t8,columns=["videotitle","publisheddate","channelname"])
    st.write(df8)

elif question== "9.What is the average duration of all videos in each channel and their corresponding channel names?":
    query9=''' select channel_name as channelname, AVG(duration) as averageduration from videos group by channel_name'''
    mycursor.execute(query9) 
    mydb.commit () 
    t9 = mycursor.fetchall()
    df9 = pd.DataFrame(t9, columns=["channelname", "averageduration"])
    T9 = []  
    for index, row in df9.iterrows():
        channel_title = row["channelname"]
        average_duration = row["averageduration"]
        average_duration_str = str(average_duration)
        T9.append(dict(channeltitle=channel_title, avgduration=average_duration_str))
    df55 = pd.DataFrame(T9)
    st.write(df55)
elif question=="10.Which videos have the highest number of comments and their corresponding channel names?":
    query10=''' select title as videotitle,channel_name as channelname,comments as comments from videos where comments is not null
                    order by comments desc'''
    mycursor.execute(query10) 
    mydb.commit () 
    t10 = mycursor.fetchall()
    df10 = pd.DataFrame(t10, columns=["videotitle", "channel name","comments"])
    st.write(df10)
#finally
#mycursor.close()
#mydb.close()





    



                                           
    


# In[ ]:





# In[ ]:





# In[ ]:




