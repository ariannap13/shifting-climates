import pickle
import pandas as pd
import os


creators = ["unclimatechange", "bbcnews", "climateadam", "drgilbz", "dwplaneta", "extinctionrebellionxr", "greenpeace_international", 
            "guardian", "ourchangingclimate", "ted", "juststopoil", "minuteearth", "metoffices", "zahrabiabani", "rupertread", 
            "friendsoftheearth", "parleychannel", "actionfortheclimate", "piqueaction", "margreen", "nowthisearth"]

data_path = "./data/"

all_video_ids = []

for creator in creators:
    print("Creator:", creator)
    with open(data_path+creator+"_transcript_clean_withmetrics_2021-2023_newdata.pkl", "rb") as token:
        retrieved_videos = pickle.load(token)

    print("Total size: "+str(len(retrieved_videos)))

    # select only the video IDs
    video_ids = retrieved_videos["Video ID"].tolist()

    all_video_ids.extend(video_ids)

# remove duplicates
all_video_ids = list(set(all_video_ids))

print("Total number of videos:", len(all_video_ids))

# save all_video_ids
with open(data_path+"all_video_ids_2021-2023.pkl", "wb") as token:
    pickle.dump(all_video_ids, token)

