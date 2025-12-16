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

def youtube_authenticate_key():
    return build("youtube", "v3", developerKey="change-with-key")

# # Function to authorize API access using OAuth2
# def youtube_authenticate():
#     os.environ["OAUTHLIB_INSECURE_TRANSPORT"] = "1"
#     api_service_name = "youtube"
#     api_version = "v3"
#     client_secrets_file = "credentials3.json"
#     creds = None
#     # the file token.pickle stores the user's access and refresh tokens, and is
#     # created automatically when the authorization flow completes for the first time
#     if os.path.exists("token3.pickle"):
#         with open("token3.pickle", "rb") as token:
#             creds = pickle.load(token)
#     # if there are no (valid) credentials availablle, let the user log in.
#     if not creds or not creds.valid:
#         if creds and creds.expired and creds.refresh_token:
#             creds.refresh(Request())
#         else:
#             flow = InstalledAppFlow.from_client_secrets_file(client_secrets_file, SCOPES)
#             creds = flow.run_local_server(port=0)
#         # save the credentials for the next run
#         with open("token3.pickle", "wb") as token:
#             pickle.dump(creds, token)

#     return build(api_service_name, api_version, credentials=creds)

# Function to search for videos based on a keyword
def search_videos(service, threshold_api_units, n_nextpage=3, **kwargs):
    all_results = []
    next_page_token = ''

    total_api_units_used = 0
    print("Total units used: %d" % total_api_units_used)

    count_nextpage = 0

    while total_api_units_used < threshold_api_units and next_page_token is not None and count_nextpage<=n_nextpage:
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

        count_nextpage+=1
                
    return all_results

# Function to retrieve video details
def get_video_details(service, video_id):
    video_details = service.videos().list(part='snippet,statistics', id=video_id).execute()
    return video_details.get('items', [])[0] if video_details.get('items', []) else None

# Function to retrieve videos by channel ID
def get_videos_bychannel(service, key, channel_id):
    videos = service.search().list(part="snippet,statistics", q=key, channelId=channel_id).execute()
    return videos.get('items', [])[0] if videos.get('items', []) else None


if __name__ == '__main__':

    # loop over months from 2016-09 to 2023-11, and get 10 videos for each month
    for year in range(2018, 2024):
        for month in range(1, 13):
            if year == 2016 and month < 9:
                continue
            if year == 2023 and month > 11:
                continue
            print(f"Year: {year}, Month: {month}")

            # Set the start and end date for the search
            START_DATE = f"{year}-{month:02d}-01T00:00:00Z"
            END_DATE = f"{year}-{month:02d}-28T23:59:59Z"

            youtube = youtube_authenticate_key()

            # Step 1: Search for videos, get 10 videos for each month
            search_results = search_videos(youtube, threshold_api_units = 8000, n_nextpage=0, publishedAfter=START_DATE, publishedBefore=END_DATE, relevanceLanguage="en", type="video", part='id', maxResults=20, order="date")

            video_data = []
    
            for video in search_results:
                video_id = video['id']['videoId']
                
                # Step 2: Retrieve video details
                video_details = get_video_details(youtube, video_id)
                
                if video_details:
                    video_title = video_details['snippet']['title']
                    video_description = video_details['snippet']['description']
                    video_timestamp = video_details['snippet']['publishedAt']
                    try:
                        video_views = video_details['statistics']['viewCount']
                    except:
                        video_views = None
                    
                    video_data.append({
                        'Video ID': video_id,
                        'Video Title': video_title,
                        'Video Timestamp': video_timestamp,
                        'Video Description': video_description,
                        'Video Views': video_views
                    })

            # Create a DataFrame-like structure using pandas
            df = pd.DataFrame(video_data)

            # get transcripts if automatic caption is available
            video_ids = list(df["Video ID"].values)

            transcript_list, unretrievable_videos = YouTubeTranscriptApi.get_transcripts(video_ids, continue_after_error=True)

            list_transcripts = []

            for video_id in video_ids:

                if video_id in transcript_list.keys():

                    srt = transcript_list.get(video_id)

                    text_list = []
                    for i in srt:
                        text_list.append(i['text'])

                    text = '.'.join(text_list)
                    list_transcripts.append(text)
                    
                else:
                    list_transcripts.append(None)

            df["Video Transcript"] = list_transcripts

            # count how many videos have Video Transcript not None in df
            auto_caption_size = len(df[df["Video Transcript"].notnull()])
            print("Auto-caption size: ", auto_caption_size)

            # append dataframe to existing dataframe and overwrite
            retrieved_videos = df

            # if baseline folder does not exist, create it
            if not os.path.exists("./data/baseline"):
                os.makedirs("./data/baseline")

            with open("./data/baseline/baseline_"+str(year)+"_"+str(month)+".pickle", "wb") as token:    
                pickle.dump(retrieved_videos, token) 