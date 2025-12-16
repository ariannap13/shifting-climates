# import required modules
import whisper
from langdetect import detect
from pytube import YouTube
import pickle
import sys
import pandas as pd

# suppress warnings
import warnings
warnings.filterwarnings("ignore")

# YEAR = 2023
# username = "natgeo"

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
dictionary_channelid = {#"juststopoil": "UC-t4U1Azf8AOkCBJILSNBmw",
#                         "minuteearth": "UCeiYXex_fwgYDonaTcSIk6w",
#                         "metoffices": "UC40Tw2tFuMzK305mi7nj8rg",
#                         "zahrabiabani": "UCuRimTRSnyVlSrN4aQVCCkA",   
#                         #"declareemergency": "UCi2pVvNv5s_Nh4KipoGQXxQ",
#                         #"thereviveseries": "UCvpfvQqheuQRZiVjiSh71sw",
#                         #"climatt": "UCoSRNBi6F4jN0CYq-sRcOHQ",
#                         "rupertread": "UCm7-jS-VzbR3xEqpBGYDPcQ",
#                         "friendsoftheearth": "UC9qqyIuIUoRKTG6sE5rVlhg",
#                         "parleychannel": "UCieB62vq-5QByMIcq-LMntg",
#                         "actionfortheclimate": "UCbewlkCKbV1B3xmKwXa1qsA",
#                         "piqueaction": "UCNf0NVrB9U8YF5sFpbsME4A",
#                         "margreen": "UCUDFVYGkosHtan3lGnQttew",
                        "nowthisearth": "UCFH5dQAkGIqzcFYmM4tNtXw"}  
 

for username in list(dictionary_channelid.keys()):
    print("Username:", username)

    with open("./data/"+username+"_2021-2023_newkeys_clean.pkl", "rb") as token:
        retrieved_videos = pickle.load(token)

    print("Total size: "+str(len(retrieved_videos)))

    # check if there is username_transcript_2021-2023.pkl file
    # if yes, load it and continue from there
    # if no, continue from retrieved_videos
    try:
        with open("./data/"+username+"_transcript_2021-2023.pkl", "rb") as token:
            transcript_videos = pickle.load(token)
        print("Transcript file found")
        # remove duplicate rows
        transcript_videos = transcript_videos.drop_duplicates(subset="Video ID")
        # merge on Video ID
        retrieved_videos = pd.merge(retrieved_videos, transcript_videos, on="Video ID", how="left")
        # create new column for Video Transcript, take values from Video Transcript_y if not None, else take values from Video Transcript_x
        retrieved_videos["Video Transcript"] = retrieved_videos["Video Transcript_y"].fillna(retrieved_videos["Video Transcript_x"])
        # drop Video Transcript_x and Video Transcript_y columns
        retrieved_videos = retrieved_videos.drop(columns=["Video Transcript_x", "Video Transcript_y"])
        # reset index
        retrieved_videos = retrieved_videos.reset_index(drop=True)
        # drop all columns ending with _y
        retrieved_videos = retrieved_videos.loc[:, ~retrieved_videos.columns.str.endswith('_y')]
        # rename all columns ending with _x
        retrieved_videos.columns = retrieved_videos.columns.str.rstrip('_x')

        # save retrieved_videos as username_transcript_2021-2023.pkl
        with open("./data/"+username+"_transcript_2021-2023.pkl", "wb") as token:    
            pickle.dump(retrieved_videos, token)
    except:
        print("Transcript file not found")
    
    # check which values in the Video Transcript column are None and save the video ids in a list  
    video_ids = []
    for i in range(len(retrieved_videos)):
        if retrieved_videos["Video Transcript"].values[i] == None:
            video_ids.append(retrieved_videos["Video ID"].values[i])

    # print number of rows with Video Transcript not None
    auto_caption_size = len(retrieved_videos) - len(video_ids)
    print("Auto-caption size: ", auto_caption_size)
    print("Video IDs to add: ", len(video_ids))

    counter = 0
    for id in video_ids:

        # if count_addition == n_toadd:
        #     break
        url = "https://www.youtube.com/watch?v=" + id

        # Create a YouTube object from the URL
        try:
            yt = YouTube(url)
        except:
            print("Error in video download")
            #delete row corresponding to video ID from retrieved_videos
            #retrieved_videos = retrieved_videos[retrieved_videos["Video ID"] != id]
            continue

        # filter by length
        if yt.length > 600:
            print("Video too long")
            #delete row corresponding to video ID from retrieved_videos
            #retrieved_videos = retrieved_videos[retrieved_videos["Video ID"] != id]
            continue

        # Get the audio stream
        try:
            audio_stream = yt.streams.filter(only_audio=True).first()

            # Download the audio stream
            output_path = "YoutubeAudios"
            filename = "audio_"+id+".mp3"
            audio_stream.download(output_path=output_path, filename=filename)

            # Load the base model and transcribe the audio
            model = whisper.load_model("base")
            result = model.transcribe("./YoutubeAudios/audio_"+id+".mp3")

            transcribed_text = result["text"]
            # assign transcribed text as value to the Video Transcript column for the given video id in retrieved_videos dataframe
            retrieved_videos.loc[retrieved_videos["Video ID"] == id, "Video Transcript"] = transcribed_text
            
        except:
            print("Error in audio download or transcription")
            #delete row corresponding to video ID from retrieved_videos
            #retrieved_videos = retrieved_videos[retrieved_videos["Video ID"] != id]
        
        counter += 1

        if counter % 10 == 0:
            print("Counter:", counter)

    # reset index of retrieved_videos
    retrieved_videos = retrieved_videos.reset_index(drop=True)
    
    with open("./data/"+username+"_transcript_2021-2023_newkeys.pkl", "wb") as token:    
        pickle.dump(retrieved_videos, token)