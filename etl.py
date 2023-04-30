from googleapiclient.discovery import build
from googleapiclient.errors import HttpError
import pandas as pd
import requests
from datetime import datetime
from dateutil.parser import parse
import pytz
from langdetect import detect
from langdetect.lang_detect_exception import LangDetectException
from IPython.display import Image
from PIL import Image
import requests, time
from io import BytesIO
from apscheduler.schedulers.background import BackgroundScheduler


#________________________________________________________________________________________________
# API Call Query logic:
# Our primary query logic on the web takes the channel id as the sole input, because we want to be user friendly
# But we also support other inputs, such as video id and category id, to achieve more customized query.

#  (1) For channels:

# By specifying video ids, we get the corresponding channel ids
# By specifying channel ids, we get the following information about these channels:

        # Channel_id (VARCHAR(255), PRIMARY KEY): Unique identifier for a YouTube channel.
        # Channel_name (VARCHAR(255)): Name of the YouTube channel.
        # Subscribers (INT): Number of subscribers for the channel.
        # Views (BIGINT): Total number of views for all videos on the channel.
        # Total_videos (INT): Total number of videos uploaded to the channel.
        # playlist_id (VARCHAR(255)): Unique identifier for a playlist associated with the channel.
        # creation_date (TIMESTAMPTZ): Date and time when the channel was created.
        # age_days (INT): Age of the channel in days.
        # age_years (INT): Age of the channel in years.


# (2) For videos:

# If we don't have target video ids in mind, we can specify certain conditions to get a list of video ids:

        # order of the list (date/rating/relevance/title/viewCount),
        # length of the list, (how many videos do we want to fetch)
        # category (43 categories in total, assigned by Youtube) ,
        ## region (e.g. US),
        # language (e.g. English),
        # videoDuration (long/medium/short, long for >20 min, short for <4min),
        # publishedBefore/publishedAfter (a time filter)

# For example, we can get a list of 50 videos in the Sports category that are published today, order by viewCount (this means we are getting the most popular videos)

# If we have target channel ids in mind, by specifying a list of channel ids, we get all the video ids in these channels.
# By specifying video ids, we get information about these videos.

        # video_id (VARCHAR(255), PRIMARY KEY): Unique identifier for a video.
        # video_title (VARCHAR(255)): Title of the video.
        # Channel_id (VARCHAR(255)): Unique identifier for the channel that uploaded the video.
        # Channel_name (VARCHAR(255)): Name of the YouTube channel that uploaded the video.
        # category_id (VARCHAR(255)): Unique identifier for the video's category.
        # category_name (VARCHAR(255)): Name of the video's category.
        # description (TEXT): Description text for the video.
        # tags (VARCHAR(255)): Tags associated with the video.
        # view_count (INT): Number of views for the video.
        # like_count (INT): Number of likes for the video.
        # comment_count (INT): Number of comments on the video.
        # Published_date (TIMESTAMPTZ): Date and time when the video was published.
        # age_days (INT): Age of the video in days.
        # age_years (INT): Age of the video in years.


# (3) For comments:

# By specifying a list of video ids, we get information about the comments below these videos.

        # comment_id (INT): Unique identifier for a comment.
        # video_id (VARCHAR(255), FOREIGN KEY): Unique identifier for the video associated with the comment.
        # author (VARCHAR(255)): Author of the comment.
        # text (TEXT): Text content of the comment.
        # likes (INT): Number of likes for the comment.
        # PRIMARY KEY (comment_id, video_id): Combination of comment_id and video_id as the primary key.

# We can also filter the top 5 upvoted comments
#________________________________________________________________________________________________
#Below is the packaged functions

def is_english(text):
    try:
        return detect(text) == 'en'
    except LangDetectException:
        return False

class youtubeFetcher:
    def __init__(self, api_key) -> None:
        self.api_key = api_key
        self.youtube = build('youtube', 'v3', developerKey=api_key)
      
    #let's say our clients have no target videos or channels in mind. Our clients can specify the
    #target category and see the current popular video and channel information
    def get_most_popular_videos(self, category_id, max_results):
        videos = []
        next_page_token = None

        while len(videos) < max_results:
            request = self.youtube.search().list(
                part='id,snippet',
                type='video',  #'channel' /'playlist'
                videoCategoryId=category_id,
                maxResults=min(50, max_results - len(videos)),
                order='viewCount',  #date/rating/relevance/title/videoCount/viewCount
                pageToken=next_page_token,
                regionCode='US',
                relevanceLanguage='en',
                videoDuration='medium', #'long'/'medium'/'short' long for >20min short for <4min
                #publishedBefore= date-time value (1970-01-01T00:00:00Z)
                publishedAfter='2023-04-03T00:00:00Z'
            )

            try:
                response = request.execute()
                english_videos = [item for item in response['items'] if is_english(item['snippet']['title'])]
                videos.extend(english_videos)
                next_page_token = response.get('nextPageToken')
                if not next_page_token:
                    break
            except HttpError as e:
                print(f"An error occurred: {e}")
                return None

        video_data = []
        for video in videos:
            video_data.append({
                'video_id':video['id']['videoId']
            })
        df_vedio = pd.DataFrame(video_data)
        id_list=df_vedio['video_id'].tolist()               
        return id_list


    
    def get_video_category_id(self, video_id):
        request = self.youtube.videos().list(
            part='snippet',
            id=video_id
        )

        try:
            response = request.execute()
            return response['items'][0]['snippet']['categoryId']
        except HttpError as e:
            print(f"An error occurred: {e}")
            return None



    def get_category_name(self, category_id):
        request = self.youtube.videoCategories().list(
            part='snippet',
            id=category_id
        )
        try:
            response = request.execute()
            return response['items'][0]['snippet']['title']
        except HttpError as e:
            print(f"An error occurred: {e}")
            return None
    
      
    def get_video_stats(self, id_list):
        try:
            request = self.youtube.videos().list(
                part='snippet,statistics',
                id=','.join(id_list)
            )
            response = request.execute()
            response_items = response['items']
        except HttpError as e:
            print(f"An error occurred: {e}")

        video_data = []
        for item in response_items:
            video_data.append({
                'video_id': item['id'],
                'video_title': item['snippet']['title'],
                'Channel_id': item['snippet']['channelId'],
                'Channel_name': item['snippet']["channelTitle"],
                'category_id': item['snippet'].get('categoryId', 'null'),
                'category_name':self.get_category_name(item['snippet'].get('categoryId', 'null')),
                'description': item['snippet']['description'],
                'tags': item['snippet'].get('tags', 'null'),
                'view_count': int(item['statistics']['viewCount']),
                'like_count': int(item['statistics'].get('likeCount', 0)),  #The like property of some videos may also be hidden by youtubers.
                'comment_count': int(item['statistics']['commentCount']),
                'Published_date': item['snippet']['publishedAt'],
                'age_days' : (datetime.utcnow().replace(tzinfo=pytz.utc) - parse(item['snippet']['publishedAt'])).days,
                'age_years' :(datetime.utcnow().replace(tzinfo=pytz.utc) - parse(item['snippet']['publishedAt'])).days// 365 

            })
        df_vedio = pd.DataFrame(video_data)
        return df_vedio
    
    def get_channel_videos(self, channel_id, max_results=1000):
        request = self.youtube.search().list(
            part='snippet',
            channelId=channel_id,
            maxResults=max_results,
            type='video',
            order='date'
        )

        try:
            response = request.execute()
            return response['items']
        except HttpError as e:
            print(f"An error occurred: {e}")
            return []



    def get_video_info(self, video_id):
        request = self.youtube.videos().list(
            part='statistics',
            id=video_id
        )   
        try:
            response = request.execute()
            return response['items'][0]['statistics']
        except HttpError as e:
            print(f"An error occurred: {e}")
            return None    


    # main function 2, get all the video stats that belong to the target channel
    def get_channel_video_stats(self, channel_ids):    
        video_data = []        
        for channel_id in channel_ids:
            items = self.get_channel_videos(channel_id)
            for item in items:
                video_id = item['id']['videoId']
                stats = self.get_video_info(video_id)
                if stats:
                    video_data.append({               
                        'video_id': video_id,
                        'video_title': item['snippet']['title'],
                        'Channel_id': channel_id,
                        'Channel_name': item['snippet']["channelTitle"],
                        'category_id' : self.get_video_category_id(video_id),
                        'category_name' : self.get_category_name(self.get_video_category_id(video_id)),
                        'description': item['snippet']['description'],
                        'tags': item['snippet'].get('tags', 'null'),
                        'view_count': int(stats['viewCount']),
                        'like_count': int(stats.get('likeCount', 0)),
                        'comment_count': int(stats.get('commentCount', 0)),
                        'Published_date': parse(item['snippet']['publishedAt']),
                        'age_days' : (datetime.utcnow().replace(tzinfo=pytz.utc) - parse(item['snippet']['publishedAt'])).days,
                        'age_years' :(datetime.utcnow().replace(tzinfo=pytz.utc) - parse(item['snippet']['publishedAt'])).days// 365 
                })             
        df = pd.DataFrame(video_data)
        return df
    
    
    # main function 1, get stats about the target channel
    def get_channel_stats(self, channel_ids):
        all_data = []
        request = self.youtube.channels().list(
                    part='snippet,contentDetails,statistics',
                    id=','.join(channel_ids))
        response = request.execute() 

        for i in range(len(response['items'])):
            data = dict(
                        Channel_id = response['items'][i]['id'],
                        Channel_name = response['items'][i]['snippet']['title'],
                        Subscribers = int(response['items'][i]['statistics']['subscriberCount']),
                        Views = int(response['items'][i]['statistics']['viewCount']),
                        Total_videos = int(response['items'][i]['statistics']['videoCount']),
                        playlist_id = response['items'][i]['contentDetails']['relatedPlaylists']['uploads'],
                        creation_date=parse(response['items'][i]['snippet']['publishedAt']),
                        age_days = (datetime.utcnow().replace(tzinfo=pytz.utc) - parse(response['items'][i]['snippet']['publishedAt'])).days,
                        age_years =(datetime.utcnow().replace(tzinfo=pytz.utc) - parse(response['items'][i]['snippet']['publishedAt'])).days// 365 
            )
            all_data.append(data)
        df_all_data=pd.DataFrame(all_data)
        return df_all_data
    
    
    #get the top 5 upvoted comments for the specified video
    def get_top_comments(self, video_id, max_results=5):
        request = self.youtube.commentThreads().list(
            part='snippet',
            videoId=video_id,
            maxResults=max_results,
            order='relevance',
            textFormat='plainText'
        )

        try:
            response = request.execute()
            return response['items']
        except HttpError as e:
            print(f"An error occurred: {e}")
            return []

    #get video comments for the specified channel
    def get_comment_info(self, channel_ids):
        channel_video_stats = self.get_channel_video_stats(channel_ids)
        video_ids = channel_video_stats['video_id'].tolist()
        video_comments = []
        for vid in video_ids:
            top_comments = self.get_top_comments(vid)
            for i, comment in enumerate(top_comments, 1):
                video_comments.append({
                    "comment_id":i,
                    'video_id':vid,
                    'author' : comment['snippet']['topLevelComment']['snippet']['authorDisplayName'],
                    'text' : comment['snippet']['topLevelComment']['snippet']['textDisplay'],
                    "likes" : comment['snippet']['topLevelComment']['snippet']['likeCount'],
                 })          
        df = pd.DataFrame(video_comments)
        return df

    #We assume that clients may don't know the id of their target channel. We provide this search function.
    def fetch_channel_id(self, channel_name):
        url = f"https://www.googleapis.com/youtube/v3/search?part=snippet&q={channel_name}&type=channel&key={self.api_key}"
        response = requests.get(url)
        data = response.json()
        if data["items"]:
            channel_id = data["items"][0]["snippet"]["channelId"]
            return channel_id
        else:
            return None
    
    def get_channel_thumbnail(self, channel_id, file_path):
        request = self.youtube.channels().list(
            part="snippet",
            id=channel_id
        )
        response = request.execute()
        channel_items = response.get("items", [])
        if channel_items:
            channel_item = channel_items[0]
            default_thumbnail_url = channel_item["snippet"]["thumbnails"]["default"]["url"]
            response = requests.get(default_thumbnail_url)
            img = Image.open(BytesIO(response.content))
            img.save(file_path)
        else:
            img = Image.new('RGB', (720, 1280), (255, 255, 255))
            img.save(file_path)
    
    
    def insert_data(self, conn, tb, name):
        cur = conn.cursor()
        cols = [x for x in tb.columns]
        for index, row in tb.iterrows():
            placeholders = ", ".join("%s" for _ in range(len(cols)))
            columns_str = ", ".join(cols)
            query = f"INSERT INTO {name} ({columns_str}) VALUES ({placeholders})"
            row_values = tuple(row[column] for column in cols)
            cur.execute(query, row_values)
        print(f"All rows were sucessfully inserted in the {name} table")
        
    
    def update_data(self, conn, tb, name, conflict_column): #conflict column is the primary key of the table
        cur = conn.cursor()
        cols = [x for x in tb.columns]

        for index, row in tb.iterrows():
            placeholders = ", ".join("%s" for _ in range(len(cols)))
            columns_str = ", ".join(cols)
            update_str = ", ".join(f"{col} = EXCLUDED.{col}" for col in cols)

            query = f"""
                INSERT INTO {name} ({columns_str}) VALUES ({placeholders})
                ON CONFLICT ({conflict_column}) DO UPDATE SET {update_str}
            """

            row_values = tuple(row[column] for column in cols)
            cur.execute(query, row_values)

        conn.commit()
        print(f"All rows were successfully upserted in the {name} table")

# ________________________________________________________________________________________________
# Below we use APScheduler to automatically refresh database every 20 seconds
# We use APScheduler becasue our data volume and processing requirements currently are not very high.
# APScheduler is simple to set up with lower overhead and resource requirements.

    def fetch_and_store_data(self, conn, channel_ids):
        # fetching data
        video_stats = self.get_channel_video_stats(channel_ids)
        channel_stats = self.get_channel_stats(self.youtube, channel_ids)
        comment = self.get_comment_info(channel_ids)
        print("succeffully fetching data")

        # storing data
        self.upsert_data(conn, video_stats, 'video_statistics', 'video_id')
        self.upsert_data(conn, channel_stats, 'channel_statistics', 'channel_id')
        self.upsert_data(conn, comment, 'video_comments', 'comment_id,video_id')
        print("succeffully storing data")

    def refresh_database(self, conn, channel_ids):
        # Create a background scheduler
        scheduler = BackgroundScheduler()

        # Add a job to the scheduler to run the fetch_and_store_data function every hour
        scheduler.add_job(self.fetch_and_store_data(conn, channel_ids), 'interval', seconds=20)

        # Start the scheduler
        scheduler.start()

        # Keep the script running indefinitely so the scheduler can execute the jobs
        try:
            while True:
                time.sleep(1)
        except (KeyboardInterrupt, SystemExit):
            # Shut down the scheduler on exit
            scheduler.shutdown()