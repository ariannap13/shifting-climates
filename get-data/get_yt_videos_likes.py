# import required modules
from googleapiclient.discovery import build
from google_auth_oauthlib.flow import InstalledAppFlow
from google.auth.transport.requests import Request
from youtube_transcript_api import YouTubeTranscriptApi

import os
import pickle
import pandas as pd
import sys

SCOPES = ["https://www.googleapis.com/auth/youtube.force-ssl"]

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

# Function to search for videos based on a keyword
def search_videos(service, threshold_api_units, **kwargs):
    all_results = []
    next_page_token = ''

    total_api_units_used = 0
    print("Total units used: %d" % total_api_units_used)

    while total_api_units_used < threshold_api_units and next_page_token is not None:
        # Calculate the number of API units needed for the upcoming request (e.g., 100 units for search)
        api_units_needed = 100

        # Check if making the next request would exceed the threshold
        if total_api_units_used + api_units_needed <= threshold_api_units:
    
            search_results = service.search().list(**kwargs).execute()
            all_results.extend(search_results.get('items', []))

            total_api_units_used += api_units_needed
            print("Total units used: %d" % total_api_units_used)
        
            if total_api_units_used + api_units_needed <= threshold_api_units:
            # Check for more pages of results
                next_page_token = search_results.get('nextPageToken')
                total_api_units_used += api_units_needed
                print("Total units used: %d" % total_api_units_used)
                if not next_page_token:
                    break
                kwargs['pageToken'] = next_page_token
            else:
                print("Reached API unit threshold. Stopping requests.")
                break
        else:
            print("Reached API unit threshold. Stopping requests.")
            break
                
    return all_results

# Function to retrieve video details
def get_video_details(service, video_id):
    video_details = service.videos().list(part='snippet,statistics,contentDetails', id=video_id).execute()
    return video_details.get('items', [])[0] if video_details.get('items', []) else None


if __name__ == '__main__':

    # open file
    with open("./data/all_video_ids_2021-2023.pkl", "rb") as token:    
        list_videoids = pickle.load(token)

    # # filter out None values in Video Transcript column
    # retrieved_videos = retrieved_videos[retrieved_videos["Video Transcript"].notnull()]

    # # filter by topic - select only video related to the challenge or to the vegan lifestyle, delete recipes
    # retrieved_videos = retrieved_videos[retrieved_videos["topic"]==0] # a bit less than 2000 videos

    # list_videoids = list(retrieved_videos["Video ID"].values)
    
    youtube = youtube_authenticate()

    video_ids = []
    video_duration = []
    video_comments_number = []
    for video in list_videoids:

        video_details = get_video_details(youtube, video)
    
        if video_details:
            video_ids.append(video_details['id'])


            #video duration
            video_duration.append(video_details['contentDetails']['duration'])

            # video comments number
            try:
                video_comments_number.append(video_details['statistics']['commentCount'])
            except:
                video_comments_number.append(None)


    # create dataframe
    df = pd.DataFrame(columns=['video_id', 'video_duration', 'video_comments_n'])
    df['video_id'] = video_ids
    df['video_duration'] = video_duration
    df['video_comments_n'] = video_comments_number

    # save dataframe
    df.to_csv("./data/all_years_video_duration.csv", index=False)