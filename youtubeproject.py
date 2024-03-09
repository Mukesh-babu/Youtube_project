from googleapiclient.discovery import build
import mysql.connector
import pandas as pd
import streamlit as st
import googleapiclient.discovery
from pymongo import MongoClient

def api_connect():
    
    api_service_name = "youtube"
    api_version = "v3"
    
    api_key = 'AIzaSyAX0RO5ep_WAi3d2W8vD0Zo2vSOUEvBFbU'
    youtube = googleapiclient.discovery.build(api_service_name, api_version, developerKey=api_key)
    
    return youtube

youtube = api_connect()

def get_channel_info(channel_id):
    request = youtube.channels().list(
            part="snippet,contentDetails,statistics",
            id=channel_id
        )
    response = request.execute()
    
    for i in response['items']:
        data = dict(Channel_Name=i['snippet']['title'],
                   Channel_ID=i['id'],
                   Subscribers = i['statistics']['subscriberCount'],
                   Views =i['statistics']['viewCount'],
                   Total_Videos = i['statistics']['videoCount'],
                   Channel_description =i['snippet']['description'],
                   Playlist_id=i['contentDetails']['relatedPlaylists']['uploads'],
                   )
        return data

def get_video_info(video_ids):
    video_data = []

    for video_id in video_ids:
        request = youtube.videos().list(
            part='snippet,contentDetails,statistics',
            id=video_id
        )

        response = request.execute()

        for item in response['items']:
            data = dict(
                Channel_Name=item['snippet']['channelTitle'],
                Channel_Id=item['snippet']['channelId'],
                Video_Id=item['id'],
                Title=item['snippet']['title'],
                Tags=item['snippet'].get('tags'),
                Thumbnail=item['snippet']['thumbnails']['default']['url'],
                Description=item['snippet'].get('description'),
                Published_Date=item['snippet']['publishedAt'],
                Duration=item['contentDetails']['duration'],
                Views=item['statistics'].get('viewCount'),
                Comments=item['statistics'].get('commentCount'),
                Likes = item['statistics'].get('likeCount'),
                Favorite_count=item['statistics']['favoriteCount'],
                Definition=item['contentDetails']['definition'],
                Caption_status=item['contentDetails']['caption']
            )
            video_data.append(data)

    return video_data

def get_videos_ids(channel_id):
    video_ids=[]
    response = youtube.channels().list(id= channel_id,
                                      part='contentDetails').execute()
    Playlist_id=response['items'][0]['contentDetails']['relatedPlaylists']['uploads']
    
    next_page_token= None
    
    while True:
        response1 = youtube.playlistItems().list(
                                        part = 'snippet',
                                        playlistId = Playlist_id,
                                        maxResults=50,
                                        pageToken = next_page_token).execute()
        for i in range(len(response1['items'])):
            video_ids.append(response1['items'][i]['snippet']['resourceId']['videoId'])
        next_page_token = response1.get('nextPageToken')
    
        if next_page_token is None:
            break
    return video_ids

    #get comment info
def get_comment_info(video_ids):
    Comment_data = []
    
    try:
        for video_id in video_ids:
            request = youtube.commentThreads().list(
                part='snippet',
                videoId=video_id,
                maxResults=50
            )
            response = request.execute()
    
            for item in response['items']:
                data = dict(
                    Comment_Id=item['snippet']['topLevelComment']['id'],
                    Video_Id=item['snippet']['topLevelComment']['snippet']['videoId'],
                    Comment_text=item['snippet']['topLevelComment']['snippet']['textDisplay'],
                    Comment_author=item['snippet']['topLevelComment']['snippet']['authorDisplayName'],
                    Comment_published=item['snippet']['topLevelComment']['snippet']['publishedAt']
                )
                Comment_data.append(data)
    except:
        pass
    return Comment_data

import logging
from pymongo import MongoClient

try:
    connection = MongoClient('mongodb+srv://mukeshbabu120193:WBn0gpPVqmOGkrwO@cluster0.fttcjju.mongodb.net/')
    db = connection['Youtube_data']
except Exception as e:
    print(f"Error connecting to MongoDB: {e}")

def channel_details(channel_id):
    try:
        # Assuming 'connection' is your MongoClient object
        col = connection['Youtube_data']['Channel_details']

        # Check if the channel_id already exists in the collection
        existing_channel = col.find_one({'channel_information.Channel_ID': channel_id})

        if existing_channel:
            return 'Channel details already exist'
        else:
            ch_details = get_channel_info(channel_id)
            vi_ids = get_videos_ids(channel_id)
            vi_detail = get_video_info(vi_ids)
            com_details = get_comment_info(vi_ids)

            col.insert_one({'channel_information': ch_details, 'video_information': vi_detail,
                            'comment_information': com_details})
            return 'Upload complete'
    except Exception as e:
        return f"Error storing data in MongoDB: {e}"

def channels_table():
    mycon = mysql.connector.connect(host='localhost', user='root', password='12345', database='youtube_data')
    mycursor = mycon.cursor() 

    query = '''CREATE TABLE if not exists channel_details (
                Channel_Name VARCHAR(100),
                Channel_ID VARCHAR(100) PRIMARY KEY,
                Subscribers BIGINT,
                Views BIGINT,
                Total_Videos INT,
                Channel_description TEXT,
                Playlist_ID VARCHAR(100))'''
    mycursor.execute(query)
    mycon.commit()
    
def migrate_to_sql(channel_name):
    mycon = mysql.connector.connect(host='localhost', user='root', password='12345', database='youtube_data')
    mycursor = mycon.cursor()

    check_query = '''SELECT Channel_ID FROM channel_details WHERE Channel_Name = %s'''
    mycursor.execute(check_query, (channel_name,))
    existing_channel = mycursor.fetchone()

    if existing_channel:
        st.error(f"Channel '{channel_name}' already exists in SQL. Aborting migration.")
        return

    # Retrieve the specific channel details from MongoDB
    db = connection['Youtube_data']
    col = db['Channel_details']

    channel_data = col.find_one({'channel_information.Channel_Name': channel_name})

    if channel_data:
        # Assuming df1 is your DataFrame
        df1 = pd.DataFrame([channel_data['channel_information']])

        insert_query = '''INSERT INTO channel_details (Channel_Name,
                                                    Channel_ID,
                                                    Subscribers,
                                                    Views,
                                                    Total_Videos,
                                                    Channel_description,
                                                    Playlist_id)
                        VALUES (%s, %s, %s, %s, %s, %s, %s)
                        ON DUPLICATE KEY UPDATE
                        Channel_Name = VALUES(Channel_Name),
                        Subscribers = VALUES(Subscribers),
                        Views = VALUES(Views),
                        Total_Videos = VALUES(Total_Videos),
                        Channel_description = VALUES(Channel_description),
                        Playlist_id = VALUES(Playlist_id)'''

        values_list = [(row['Channel_Name'], row['Channel_ID'], row['Subscribers'], row['Views'], row['Total_Videos'],
                        row['Channel_description'], row['Playlist_id']) for index, row in df1.iterrows()]

        try:
            mycursor.executemany(insert_query, values_list)
            mycon.commit()
            st.success('Channel details migrated to SQL successfully')
        except Exception as e:
            st.error(f'Error during migration to SQL: {e}')

        finally:
            mycursor.close()
            mycon.close()
    else:
        st.warning(f'Channel {channel_name} not found in MongoDB')
from isodate import parse_duration

def convert_iso8601_to_seconds(duration):
    # Parse ISO 8601 duration and convert to seconds
    try:
        duration_obj = parse_duration(duration)
        return duration_obj.total_seconds()
    except Exception as e:
        print(f"Error converting duration '{duration}' to seconds: {e}")
        return None

def videos_table():
    mycon = mysql.connector.connect(host='localhost', user='root', password='12345', database='youtube_data')
    mycursor = mycon.cursor()

    create_query = '''CREATE TABLE IF NOT EXISTS videos (
                Channel_Name varchar(100),
                Channel_Id varchar(100),
                Video_Id varchar(40) PRIMARY KEY,
                Title varchar(150),
                Tags text,
                Thumbnail varchar(150),
                Description text,
                Published_Date DATETIME,
                Duration int,  -- Change to int to store duration in seconds
                Views bigint,
                Comments int,
                Likes bigint,
                Favorite_count int,
                Definition varchar(20),
                Caption_status varchar(50)
                )'''
    mycursor.execute(create_query)
    mycon.commit()

    vid_list = []
    db = connection['Youtube_data']
    col = db['Channel_details']

    for vid_data in col.find({}, {'_id': 0, 'video_information': 1}):
        for i in range(len(vid_data['video_information'])):
            vid_list.append(vid_data['video_information'][i])

    df1 = pd.DataFrame(vid_list)

    # Assuming df1 is your DataFrame
    df1['Published_Date'] = pd.to_datetime(df1['Published_Date']).dt.strftime('%Y-%m-%d %H:%M:%S')

    for index, row in df1.iterrows():
        insert_query = '''INSERT IGNORE INTO videos (Channel_Name, Channel_Id, Video_Id, Title, Tags, Thumbnail, Description, Published_Date, Duration, Views, Comments, Likes, Favorite_count, Definition, Caption_status)
                        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                    '''

        # Convert the 'Tags' list to a comma-separated string
        tags_string = ','.join(row['Tags']) if row['Tags'] else None

        # Convert the 'Duration' column to seconds using the function
        duration_seconds = convert_iso8601_to_seconds(row['Duration'])

        values = (row['Channel_Name'], row['Channel_Id'], row['Video_Id'], row['Title'], tags_string, row['Thumbnail'],
                  row['Description'], row['Published_Date'], duration_seconds, row['Views'], row['Comments'],
                  row['Likes'], row['Favorite_count'], row['Definition'], row['Caption_status'])

        try:
            mycursor.execute(insert_query, values)
        except Exception as e:
            print(f"Error inserting row {index + 1}: {e}")

    mycon.commit()
    mycursor.close()
    mycon.close()

    return df1

# Call the function and store the returned DataFrame
df1 = videos_table()

# Now df1 is accessible outside the function
df1.head()  # You can perform further operations on df1 as needed


def comments_table():

    mycon = mysql.connector.connect(host ='localhost',user='root',password='12345',database ='youtube_data')
    mycursor = mycon.cursor()

    query = '''CREATE TABLE IF NOT EXISTS comments (
                Comment_Id varchar(100) primary key,
                Video_Id varchar(100),
                Comment_text text,
                Comment_author varchar(100),
                Comment_published timestamp
                )'''
    mycursor.execute(query)
    mycon.commit()

    com_list = []
    db = connection['Youtube_data']
    col = db['Channel_details']

    for com_data in col.find({}, {'_id': 0, 'comment_information': 1}):
        for i in range (len(com_data['comment_information'])):
            com_list.append(com_data['comment_information'][i])

    df2= pd.DataFrame(com_list)

    df2['Comment_published'] = pd.to_datetime(df2['Comment_published']).dt.strftime('%Y-%m-%d %H:%M:%S')

    for index, row in df2.iterrows():
        insert_query = '''INSERT INTO comments (Comment_Id,
                                                Video_Id,
                                                Comment_text,
                                                Comment_author,
                                                Comment_published)
                        VALUES (%s, %s, %s, %s, %s)
                        ON DUPLICATE KEY UPDATE
                        Comment_Id = VALUES(Comment_Id),
                        Comment_text = VALUES(Comment_text),
                        Comment_author = VALUES(Comment_author),
                        Comment_published = VALUES(Comment_published)
                        '''

        values = (row['Comment_Id'], row['Video_Id'], row['Comment_text'], row['Comment_author'], row['Comment_published'])

        try:
            mycursor.execute(insert_query, values)
            mycon.commit()
        except Exception as e:
            print(f'Error during batch insert: {e}')

    # Close the cursor and connection outside of the loop
    mycursor.close()
    mycon.close()

def tables(): 
    channels_table()
    videos_table()
    comments_table()

    return 'Tables created successfully'

import streamlit as st

def show_channel_table():
    ch_list = []
    db = connection['Youtube_data']
    col = db['Channel_details']

    for ch_data in col.find({}, {'_id': 0, 'channel_information': 1}):
        ch_list.append(ch_data['channel_information'])

    df = st.dataframe(ch_list)

    return df


def show_videos_table():
    vid_list = []
    db = connection['Youtube_data']
    col = db['Channel_details']

    for vid_data in col.find({}, {'_id': 0, 'video_information': 1}):
        for i in range(len(vid_data['video_information'])):
            vid_list.append(vid_data['video_information'][i])

        df1 = st.dataframe(vid_list)

        return df1


def show_comments_table():
    com_list = []
    db = connection['Youtube_data']
    col = db['Channel_details']

    for com_data in col.find({}, {'_id': 0, 'comment_information': 1}):
        for i in range (len(com_data['comment_information'])):
            com_list.append(com_data['comment_information'][i])

    df2= st.dataframe(com_list)

    return df2

def get_channel_names_from_mongodb():

    try:
        connection = MongoClient('mongodb+srv://mukeshbabu120193:WBn0gpPVqmOGkrwO@cluster0.fttcjju.mongodb.net/')
        db = connection['Youtube_data']
        col = db['Channel_details']

        documents = col.find({}, {'channel_information.Channel_Name': 1, '_id': 0})
        channel_names = [doc['channel_information']['Channel_Name'] for doc in documents]

        return channel_names
    except Exception as e:
        print(f"Error retrieving channel names from MongoDB: {e}")
        return []
 
    


# Streamlit code
def main():
    
    

    # Get user input for channel ID(s)
    channel_ids = st.sidebar.text_input('Enter YouTube Channel IDs (separated by commas):')

    options = st.selectbox('Select your option', ('Retrieve Data', 'Store in MongoDB', 'Migrate to SQL','Queries'))
    

    if options == 'Retrieve Data':
        if st.button('Retrieve Data'):
            channel_id_list = [channel_id.strip() for channel_id in channel_ids.split(',')]
            
            # Now you can use the channel_id_list for further processing
            for channel_id in channel_id_list:
                data = get_channel_info(channel_id)
                st.write(data)
    button_clicked = False 

    if options == 'Store in MongoDB':
        button_clicked = st.button('Store in MongoDB')

    if button_clicked:
        channel_id_list = [channel_id.strip() for channel_id in channel_ids.split(',')]
    
        for channel_id in channel_id_list:
            existing_channels = get_channel_names_from_mongodb()

            if channel_id in existing_channels: 
                st.warning(f"Channel '{channel_id}' details already exist")
            else: 
                result = channel_details(channel_id)
                st.success(f"Channel '{channel_id}' upload complete" if result else f"Failed to store data for channel '{channel_id}'")


    elif options == 'Migrate to SQL':
        selected_channel = st.selectbox('Select a Channel to Migrate Data to SQL', get_channel_names_from_mongodb())
        if st.button('Migrate to SQL'):
            migrate_to_sql(selected_channel)

    elif options == 'Queries':
        mycon = mysql.connector.connect(host ='localhost',user='root',password='12345',database ='youtube_data')
        mycursor = mycon.cursor()

        question = st.selectbox('Select your question',('1. All the videos and channel name',
                                                        '2. Channels with most number of videos',
                                                        '3. 10 most viewed videos',
                                                        '4. Comments in each videos',
                                                        '5. Videos with highest likes',
                                                        '6. Likes of all videos',
                                                        '7. Views of each channel',
                                                        '8. Videos published in the year 2022',
                                                        '9. Average duration of all videos in each channel',
                                                        '10. Videos with highest number of comments'))

        if question=='1. All the videos and channel name':
            query1 = '''SELECT title AS videos, channel_name FROM videos'''
            mycursor.execute(query1)
            t1 = mycursor.fetchall()
            df1 = pd.DataFrame(t1, columns=['video title', 'channel_name'])
            st.write(df1)

        elif question=='2. Channels with most number of videos':
            query2 = '''SELECT channel_name AS channelname, total_videos AS no_videos 
                FROM channel_details
                ORDER BY total_videos DESC'''
            mycursor.execute(query2)
            t2 = mycursor.fetchall()
            df2 = pd.DataFrame(t2, columns=['channel_name', 'No. of videos'])
            st.write(df2)

        elif question == '3. 10 most viewed videos':
            query3 = '''SELECT views AS views, channel_name AS channelname, title as video_title
                        FROM videos WHERE views IS NOT NULL
                        ORDER BY views DESC LIMIT 10'''
            mycursor.execute(query3)
            t3 = mycursor.fetchall()
            df3 = pd.DataFrame(t3, columns=['views', 'channel_name', 'video_title'])  # Adjust column names
            st.write(df3)

        elif question == '4. Comments in each videos':
            query4 = '''SELECT comments AS no_comments, title AS video_title
                    FROM videos WHERE comments IS NOT NULL order by no_comments DESC
                    '''
            mycursor.execute(query4) 
            t4 = mycursor.fetchall()
            df4 = pd.DataFrame(t4, columns=['no_comments', 'video_title'])
            st.write(df4)
            
        elif question == '5. Videos with highest likes':
            query5 = '''SELECT title AS video_title, channel_name AS channelname, Likes as likecount
                        FROM videos WHERE Likes IS NOT NULL
                        ORDER BY Likes DESC LIMIT 10'''
            mycursor.execute(query5)  # Use the correct query variable 'query5'
            t5 = mycursor.fetchall()
            df5 = pd.DataFrame(t5, columns=['video_title', 'channelname', 'likecount'])  # Adjust column names
            st.write(df5)

        elif question == '6. Likes of all videos':
            query6 = '''SELECT Likes as likecount, title as video_title
                        FROM videos
                    '''
            mycursor.execute(query6)
            t6 = mycursor.fetchall()
            df6 = pd.DataFrame(t6, columns=['Likecount', 'Video_title'])  # Adjust column names
            st.write(df6)
            
        elif question == '7. Views of each channel':
            query7 = '''SELECT channel_name as channelname, SUM(views) as totalviews
                        FROM videos GROUP BY channel_name
                    '''
            mycursor.execute(query7)
            t7 = mycursor.fetchall()
            df7 = pd.DataFrame(t7, columns=['channelname', 'totalviews'])
            st.write(df7)
            
        elif question == '8. Videos published in the year 2022':
            query8 = '''SELECT title as videotitle, Published_Date as videorelease, channel_name as channelname
                        FROM videos where extract(year from Published_Date)=2022
                    '''
            mycursor.execute(query8) 
            t8 = mycursor.fetchall()
            df8 = pd.DataFrame(t8, columns=['videotitle', 'published_data','channelname'])
            st.write(df8)

        elif question == '9. Average duration of all videos in each channel':
            query9 = '''SELECT channel_name as channelname, AVG(Duration) as averageduration
                        FROM videos group by channel_name'''
            mycursor.execute(query9)
            t9 = mycursor.fetchall()
            df9 = pd.DataFrame(t9, columns=['channelname', 'averageduration']) 

            T9=[]
            for index,row in df9.iterrows():
                channel_title=row['channelname']
                average_duration=row['averageduration']
                average_duration_str=str(average_duration)
                T9.append(dict(channeltitle=channel_title,avgduration=average_duration_str))
            df1=pd.DataFrame(T9)
            st.write(df1)

        elif question == '10. Videos with highest number of comments':
                query10 = '''SELECT title as videotitle, channel_name as channelname, comments as comments
                            FROM videos where comments is not null order by comments DESC
                        '''
                mycursor.execute(query10)
                t10 = mycursor.fetchall()
                df10 = pd.DataFrame(t10, columns=['video_title', 'channelname', 'comments'])  # Adjust column names
                st.write(df10)

        mycursor.close()
        mycon.close()


if __name__ == '__main__':
    main()


