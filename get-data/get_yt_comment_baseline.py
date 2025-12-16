# import required modules
from googleapiclient.discovery import build
from google_auth_oauthlib.flow import InstalledAppFlow
from google.auth.transport.requests import Request
from youtube_transcript_api import YouTubeTranscriptApi
from pathlib import Path

import os
import pickle
import pandas as pd
import sys
import json

SCOPES = ["https://www.googleapis.com/auth/youtube.force-ssl"]

# Function to authorize API access using OAuth2
def youtube_authenticate():
    os.environ["OAUTHLIB_INSECURE_TRANSPORT"] = "1"
    api_service_name = "youtube"
    api_version = "v3"
    client_secrets_file = "credentials2.json"
    creds = None
    # the file token.pickle stores the user's access and refresh tokens, and is
    # created automatically when the authorization flow completes for the first time
    if os.path.exists("token2.pickle"):
        with open("token2.pickle", "rb") as token:
            creds = pickle.load(token)
    # if there are no (valid) credentials availablle, let the user log in.
    if not creds or not creds.valid:
        if creds and creds.expired and creds.refresh_token:
            creds.refresh(Request())
        else:
            flow = InstalledAppFlow.from_client_secrets_file(client_secrets_file, SCOPES)
            creds = flow.run_local_server(port=0)
        # save the credentials for the next run
        with open("token2.pickle", "wb") as token:
            pickle.dump(creds, token)

    return build(api_service_name, api_version, credentials=creds)

# Function to retrieve comments from a video
def get_video_comments(service, video_id, **kwargs):
    comments = []

    try:
        results = service.commentThreads().list(videoId=video_id, part='snippet,replies', **kwargs).execute()
    except:
        print("Error with video:", video_id, sys.exc_info()[0])
        return comments

    n_comments = 0
    while results:
        for item in results['items']:
            # top level comments
            comment_text = item['snippet']['topLevelComment']['snippet']['textDisplay']
            comment_id = item['snippet']['topLevelComment']['id']
            comment_parent = item['snippet']['topLevelComment']['snippet']['videoId']
            comment_likes = item['snippet']['topLevelComment']['snippet']['likeCount']
            comment_published_at = item['snippet']['topLevelComment']['snippet']['publishedAt']
            comment = [comment_id, comment_text, comment_parent, comment_likes, comment_published_at]
            comments.append(comment)
            n_comments += 1

            if n_comments > 200:
                return comments

            # check for replies
            if item['snippet']['totalReplyCount'] > 0:
                if "replies" not in item:
                    continue
                for reply_item in item['replies']['comments']:
                    reply_text = reply_item['snippet']['textDisplay']
                    reply_id = reply_item['id']
                    reply_parent = reply_item['snippet']['parentId']
                    reply_likes = reply_item['snippet']['likeCount']
                    reply_published_at = reply_item['snippet']['publishedAt']
                    reply = [reply_id, reply_text, reply_parent, reply_likes, reply_published_at]
                    comments.append(reply)
                    n_comments += 1

                    if n_comments > 1000:
                        return comments

        # Check for more pages of comments
        if 'nextPageToken' in results:
            kwargs['pageToken'] = results['nextPageToken']
            try:
                results = service.commentThreads().list(videoId=video_id, part='snippet,replies', **kwargs).execute()
            except:
                print("Error with video:", video_id, sys.exc_info()[0])
                return comments
        else:
            break

    return comments

if __name__ == '__main__':

    youtube = youtube_authenticate()

    # open transcript file
    df = pd.read_csv("./data/baseline/autocaptioned_data.csv")
    # remove possible duplicates
    df = df.drop_duplicates(subset="Video ID")

    # get list of video ids
    video_ids = df["Video ID"].tolist()

    print("Tot. number of videos:", len(video_ids))

    # check if we already have comments for some of the videos
    # open json file
    if Path(f"./data/baseline/comments_baseline.json").exists():
            with open(f"./data/baseline/comments_baseline.json", "r") as f:
                all_data = json.load(f)
            
            # if video already in json, skip
            video_ids = [video_id for video_id in video_ids if not any(d["VideoID"] == video_id for d in all_data)]

            print("Tot. number of new videos to retrieve comments:", len(video_ids))
                
    video_data = []

    iter_videos = 0 

    all_data = []

    video_ids_reversed = video_ids[::-1]

    # iterate over videos, reversed to start from the end
    for video_id in video_ids_reversed:

        if Path(f"./data/baseline/comments_baseline.json").exists():
                with open(f"./data/baseline/comments_baseline.json", "r") as f:
                    all_data = json.load(f)
                
        # if video already in json, skip
        if any(d["VideoID"] == video_id for d in all_data):
            iter_videos += 1
                
        if iter_videos % 10 == 0:
            print("N. videos:", iter_videos ,"/", len(video_ids))

        comments = get_video_comments(youtube, video_id=video_id)
        
        video_data = {
            'VideoID': video_id,
            'Comments': comments
        }

        all_data.append(video_data)

        iter_videos += 1

        if iter_videos % 10 == 0:
            print("N. videos:", iter_videos ,"/", len(video_ids))

        # save as json
        with open("./data/baseline/comments_baseline.json", "w") as f:
            json.dump(all_data, f, indent=4)
