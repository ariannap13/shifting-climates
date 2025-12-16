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

# dictionary_channelid = {"unclimatechange": "UCuLEr-GWiwCBB6zBDX3elOQ",
#                         # "natgeo": "UCpVm7bg6pXKo1Pr6k5kxG9A",
#                         # "nbcnews": "UCeY0bbntWzzVIaj2z3QigXg",
#                         "bbcnews": "UC16niRr50-MSBwiO3YDb3RA",
#                         "climateadam": "UCCu5wtZ5uOWZp_roz7wHPfg",
#                         "drgilbz": "UCjaBxCyjLpIRyKOd8uw_S4w",
#                         "dwplaneta": "UCb72Gn5LXaLEcsOuPKGfQOg",
#                         "extinctionrebellionxr": "UCYThdLKE6TDwBJh-qDC6ICA",
#                         "greenpeace_international": "UCTDTSx8kbxGECZJxOa9mIKA",
#                         "guardian": "UCHpw8xwDNhU9gdohEcJu4aA",
#                         "ourchangingclimate": "UCNXvxXpDJXp-mZu3pFMzYHQ",
#                         "ted": "UCAuUUnT6oDeKwE6v1NGQxug"}   
dictionary_channelid = {"juststopoil": "UC-t4U1Azf8AOkCBJILSNBmw",
                        "minuteearth": "UCeiYXex_fwgYDonaTcSIk6w",
                        "metoffices": "UC40Tw2tFuMzK305mi7nj8rg",
                        "zahrabiabani": "UCuRimTRSnyVlSrN4aQVCCkA",   
                        #"declareemergency": "UCi2pVvNv5s_Nh4KipoGQXxQ",
                        #"thereviveseries": "UCvpfvQqheuQRZiVjiSh71sw",
                        #"climatt": "UCoSRNBi6F4jN0CYq-sRcOHQ",
                        "rupertread": "UCm7-jS-VzbR3xEqpBGYDPcQ",
                        "friendsoftheearth": "UC9qqyIuIUoRKTG6sE5rVlhg",
                        "parleychannel": "UCieB62vq-5QByMIcq-LMntg",
                        "actionfortheclimate": "UCbewlkCKbV1B3xmKwXa1qsA",
                        "piqueaction": "UCNf0NVrB9U8YF5sFpbsME4A",
                        "margreen": "UCUDFVYGkosHtan3lGnQttew",
                        "nowthisearth": "UCFH5dQAkGIqzcFYmM4tNtXw"}    


# Function to authorize API access using OAuth2
def youtube_authenticate():
    os.environ["OAUTHLIB_INSECURE_TRANSPORT"] = "1"
    api_service_name = "youtube"
    api_version = "v3"
    client_secrets_file = "credentials.json"
    creds = None
    # the file token.pickle stores the user's access and refresh tokens, and is
    # created automatically when the authorization flow completes for the first time
    if os.path.exists("token.pickle"):
        with open("token.pickle", "rb") as token:
            creds = pickle.load(token)
    # if there are no (valid) credentials availablle, let the user log in.
    if not creds or not creds.valid:
        if creds and creds.expired and creds.refresh_token:
            creds.refresh(Request())
        else:
            flow = InstalledAppFlow.from_client_secrets_file(client_secrets_file, SCOPES)
            creds = flow.run_local_server(port=0)
        # save the credentials for the next run
        with open("token.pickle", "wb") as token:
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

    #for channel in list(dictionary_channelid.keys())[2:]:
    for channel in dictionary_channelid.keys():
        print("Creator:", channel)

        # open transcript file
        with open("./data/"+channel+"_2021-2023_newkeys.pickle", "rb") as token:
            retrieved_videos = pickle.load(token)

        # remove possible duplicates
        retrieved_videos = retrieved_videos.drop_duplicates(subset="Video ID")

        # get list of video ids
        video_ids = retrieved_videos["Video ID"].tolist()

        print("Tot. number of videos:", len(video_ids))

        # check if we already have comments for some of the videos
        # open json file
        if Path(f"./data/comments/"+channel+"_comments_2021-2023.json").exists():
            with open(f"./data/comments/"+channel+"_comments_2021-2023.json", "r") as f:
                all_data = json.load(f)
            
            # if video already in json, skip
            video_ids = [video_id for video_id in video_ids if not any(d["VideoID"] == video_id for d in all_data)]

        print("Tot. number of new videos to retrieve comments:", len(video_ids))
        
        video_data = []

        iter_videos = 0 

        all_data = []

        for video_id in video_ids:

            # if json file already exists, open
            if Path(f"./data/comments/"+channel+"_comments_2021-2023.json").exists():
                with open(f"./data/comments/"+channel+"_comments_2021-2023.json", "r") as f:
                    all_data = json.load(f) # in this way, we will overwrite the json file if we run the script again
                
                # if video already in json, skip
                if any(d["VideoID"] == video_id for d in all_data):
                    iter_videos += 1
                    
                    if iter_videos % 10 == 0:
                        print("N. videos:", iter_videos ,"/", len(video_ids))

                    continue

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
            with open("./data/comments/"+channel+"_comments_2021-2023.json", "w") as f:
                json.dump(all_data, f, indent=4)
