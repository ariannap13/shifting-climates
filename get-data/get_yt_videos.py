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

dictionary_channelid = {#"unclimatechange": "UCuLEr-GWiwCBB6zBDX3elOQ",
                        # #"natgeo": "UCpVm7bg6pXKo1Pr6k5kxG9A",
                        # # "nbcnews": "UCeY0bbntWzzVIaj2z3QigXg",
                        # "bbcnews": "UC16niRr50-MSBwiO3YDb3RA",
                        # "climateadam": "UCCu5wtZ5uOWZp_roz7wHPfg",
                        # "drgilbz": "UCjaBxCyjLpIRyKOd8uw_S4w",
                        # "dwplaneta": "UCb72Gn5LXaLEcsOuPKGfQOg",
                        # "extinctionrebellionxr": "UCYThdLKE6TDwBJh-qDC6ICA",
                        # "greenpeace_international": "UCTDTSx8kbxGECZJxOa9mIKA",
                        # "guardian": "UCHpw8xwDNhU9gdohEcJu4aA",
                        # "ourchangingclimate": "UCNXvxXpDJXp-mZu3pFMzYHQ",
                        # "ted": "UCAuUUnT6oDeKwE6v1NGQxug"
}   
dictionary_channelid2 = {"juststopoil": "UC-t4U1Azf8AOkCBJILSNBmw",
                        "minuteearth": "UCeiYXex_fwgYDonaTcSIk6w",
                        "metoffices": "UC40Tw2tFuMzK305mi7nj8rg",
                        "zahrabiabani": "UCuRimTRSnyVlSrN4aQVCCkA",   
                        # "declareemergency": "UCi2pVvNv5s_Nh4KipoGQXxQ",
                        # "thereviveseries": "UCvpfvQqheuQRZiVjiSh71sw",
                        # "climatt": "UCoSRNBi6F4jN0CYq-sRcOHQ",
                        "rupertread": "UCm7-jS-VzbR3xEqpBGYDPcQ",
                        "friendsoftheearth": "UC9qqyIuIUoRKTG6sE5rVlhg",
                        "parleychannel": "UCieB62vq-5QByMIcq-LMntg",
                        "actionfortheclimate": "UCbewlkCKbV1B3xmKwXa1qsA",
                        "piqueaction": "UCNf0NVrB9U8YF5sFpbsME4A",
                        "margreen": "UCUDFVYGkosHtan3lGnQttew",
                        "nowthisearth": "UCFH5dQAkGIqzcFYmM4tNtXw"}  

# expand dictionary
dictionary_channelid.update(dictionary_channelid2)

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

    for channel in dictionary_channelid.keys():
        print(channel)
        for YEAR in range(2021,2024):

            #for KEYWORD in ["climate change", "global warming", "savetheplanet"]:
            #for KEYWORD in ["renewable energy", "climate action"]:
            #for KEYWORD in ["climate crisis", "greenewdeal"]:
            #for KEYWORD in ["gogreen", "climatejustice"]:
            for KEYWORD in ["savetheworld", "forclimate", "savetheearth"]:
                print("Keyword:", KEYWORD)
                # check if data already exists
                if os.path.exists("./data/"+channel+"_"+str(YEAR)+"_"+KEYWORD+".pickle"):
                    print("Data already exists for", channel, YEAR)
                    continue

                username = channel
                CHANNEL_ID = dictionary_channelid[channel]

                START_DATE = str(YEAR)+"-01-01T00:00:00Z"
                END_DATE = str(YEAR)+"-12-31T00:00:00Z"

                youtube = youtube_authenticate()

                # Step 1: Search for videos based on the keyword
                search_results = search_videos(youtube, threshold_api_units = 8000, n_nextpage=4, q=KEYWORD, publishedAfter=START_DATE, publishedBefore=END_DATE, relevanceLanguage="en", type="video", part='id', channelId=CHANNEL_ID, maxResults=50, order="date")
                
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

                print(df.head())
                #print(df.tail())
                try:
                    print(df["Video Timestamp"].min(), df["Video Timestamp"].max())
                except:
                    print("No videos found for", channel, YEAR)
                    continue
                print(df.shape)

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

                # append dataframe to existing dataframe and overwrite
                retrieved_videos = df

                # with open("./data/"+username+"_"+str(YEAR)+"_"+KEYWORD+".pickle", "wb") as token:    
                #     pickle.dump(retrieved_videos, token)    
                with open("./data/"+channel+"_"+str(YEAR)+"_"+KEYWORD+".pickle", "wb") as token:    
                    pickle.dump(retrieved_videos, token)    