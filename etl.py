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
import requests
from io import BytesIO

def is_english(text):
    try:
        return detect(text) == 'en'
    except LangDetectException:
        return False

class youtubeFetcher:
    def __init__(self, api_key) -> None:
        self.api_key = api_key
        self.youtube = build('youtube', 'v3', developerKey=api_key)
      
        
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
# return response['items']
        except HttpError as e:
            print(f"An error occurred: {e}")
# return None
# def get_video_stats(response):
        video_data = []
        for item in response_items:
            #if item['statistics'].get('likeCount') is None:
                #continue
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
                'like_count': int(item['statistics'].get('likeCount', 0)),
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


    # main function 2
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
    
    
    # main function 1
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
        
    
    def update_data(self, conn, tb, name, conflict_column):
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