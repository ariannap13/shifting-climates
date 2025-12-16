# import required modules
from googleapiclient.discovery import build
from google_auth_oauthlib.flow import InstalledAppFlow
from google.auth.transport.requests import Request
from youtube_transcript_api import YouTubeTranscriptApi

import os
import pickle
import pandas as pd

DEVELOPER_KEY = "YOUR_API_KEY"

SCOPES = ["https://www.googleapis.com/auth/youtube.force-ssl"]

def youtube_authenticate_key():
    return build("youtube", "v3", developerKey=DEVELOPER_KEY)

# Function to retrieve video details
def get_video_details(service, video_id):
    video_details = service.videos().list(part='snippet,contentDetails,statistics', id=video_id).execute()
    return video_details.get('items', [])[0] if video_details.get('items', []) else None

videos_list = [] # your list of video ids extracted from YouTube links

if __name__ == '__main__':

    youtube = youtube_authenticate_key()

    for video_id in videos_list:
        
        video_details = get_video_details(youtube, video_id)
                
        # go on from here saving information in a json file (remember to implement the saving video-by-video, so that if the script stops you don't lose all the data you've collected so far) 