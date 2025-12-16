import pandas as pd
import numpy as np
import pickle
import json
from sentence_transformers import SentenceTransformer
from sklearn.decomposition import PCA
import matplotlib.pyplot as plt
import sys

data_dir_yt = "./youtube/data/"
data_dir_tt = "./tiktok-opinion-dynamics/src/tiktok_opinion_dynamics/data/search/creators/"

dictionary_channelid = {"unclimatechange": "UCuLEr-GWiwCBB6zBDX3elOQ",
                        #"natgeo": "UCpVm7bg6pXKo1Pr6k5kxG9A",
                        #"nbcnews": "UCeY0bbntWzzVIaj2z3QigXg",
                        "bbcnews": "UC16niRr50-MSBwiO3YDb3RA",
                        "climateadam": "UCCu5wtZ5uOWZp_roz7wHPfg",
                        "drgilbz": "UCjaBxCyjLpIRyKOd8uw_S4w",
                        "dwplaneta": "UCb72Gn5LXaLEcsOuPKGfQOg",
                        "extinctionrebellionxr": "UCYThdLKE6TDwBJh-qDC6ICA",
                        "greenpeace_international": "UCTDTSx8kbxGECZJxOa9mIKA",
                        "guardian": "UCHpw8xwDNhU9gdohEcJu4aA",
                        "ourchangingclimate": "UCNXvxXpDJXp-mZu3pFMzYHQ",
                        "ted": "UCAuUUnT6oDeKwE6v1NGQxug"} 

consider_transcript = True
use_pca = False

dictionary_channelid_2 = {"juststopoil": "UC-t4U1Azf8AOkCBJILSNBmw",
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

# merge dictionaries
dictionary_channelid.update(dictionary_channelid_2) 


list_diff_centroids = []
for creator in dictionary_channelid:

    only_yt = False
    print("Creator:", creator)

    # load embeddings content and comments

    if not only_yt:
        with open("./embeddings/embeddings_tt_withtrans_clean_"+creator+".pkl", "rb") as f:
            embeddings_tt_content = pickle.load(f)
        try:
            with open("./embeddings/embeddings_tt_withtrans_comments_"+creator+".pkl", "rb") as f:
                embeddings_tt_comments = pickle.load(f)
        except:
            print("No TT comments for", creator)
            only_yt = True     

    with open("./embeddings/embeddings_subset/embeddings_yt_withtrans_clean_"+creator+"_newdata.pkl", "rb") as f:
        embeddings_yt_content = pickle.load(f)
    with open("./embeddings/embeddings_subset/embeddings_yt_withtrans_comments_clean_"+creator+"_newdata.pkl", "rb") as f:
        embeddings_yt_comments = pickle.load(f)

    
    if use_pca:
        with open('pca_content_comments.pkl', 'rb') as pickle_file:
            pca = pickle.load(pickle_file)

        embeddings_tt_content = pca.transform(embeddings_tt_content)
        embeddings_yt_content = pca.transform(embeddings_yt_content)

        embeddings_tt_comments = pca.transform(embeddings_tt_comments)
        embeddings_yt_comments = pca.transform(embeddings_yt_comments)


    ###### load data content
    all_data_yt = []
    all_data_tt = []

    # open pickle
    with open(data_dir_yt+"{}_transcript_2021-2023_newkeys.pkl".format(creator), "rb") as f:
        df = pickle.load(f)
        all_data_yt.append(df)

    if not only_yt:
        with open(data_dir_tt+"all_keywords_"+creator+"_2021-2023.pkl", "rb") as f:
            df = pickle.load(f)
            all_data_tt.append(df)

    # concatenate dataframes
    df_yt = pd.concat(all_data_yt)
    if not only_yt:
        df_tt = pd.concat(all_data_tt)

    # drop duplicates
    df_yt = df_yt.drop_duplicates(subset=["Video ID"])
    if not only_yt:
        df_tt = df_tt.drop_duplicates(subset=["id"])

    # convert to datetime
    df_yt["Video Timestamp"] = pd.to_datetime(df_yt["Video Timestamp"])
    if not only_yt:
        df_tt["create_time"] = pd.to_datetime(df_tt["create_time"])

    # sentences we would like to encode
    # when available, use voice_to_text, otherwise use video_description
    if not only_yt:
        videos_tt = []
        for i in range(len(df_tt)):
            videos_tt.append([df_tt["id"].values[i]])

    # if Video Transcript is available, use it, otherwise use Video Description
    videos_yt = []
    for i in range(len(df_yt)):
        videos_yt.append([df_yt["Video ID"].values[i]])

    if creator=="rupertread":
        print(len(videos_tt), len(videos_yt))

    # create 3 dataframes
    if not only_yt:
        df_tt_videos = pd.DataFrame(videos_tt, columns=["id"])
    df_yt_videos = pd.DataFrame(videos_yt, columns=["id"])

    # compare length of df_tt_videos and embeddings_tt_content
    if not only_yt:
        if len(df_tt_videos) != len(embeddings_tt_content):
            print("Length of df_tt_videos and embeddings_tt_content do not match.")
            sys.exit()

    # compare length of df_yt_videos and embeddings_yt_content
    if len(df_yt_videos) != len(embeddings_yt_content):
        print("Length of df_yt_videos and embeddings_yt_content do not match.")
        sys.exit()

    ###### load data comments
    
    all_data_comments_yt = []
    all_data_comments_tt = []
    # open comments files
    # open YT comments
    with open("./youtube/data/comments/{}_comments_2021-2023.json".format(creator), "r") as f:
        data = json.load(f)
        df = pd.DataFrame(data)
        all_data_comments_yt.append(df)
    
    if not only_yt:
        # open TT comments
        with open("./tiktok-opinion-dynamics/src/tiktok_opinion_dynamics/data/comments/creators/all_comments_{}.json".format(creator), "r") as f:
            data = json.load(f)
            df = pd.DataFrame(data)
            all_data_comments_tt.append(df)

    # concatenate dataframes
    df_yt_comments = pd.concat(all_data_comments_yt)
    yt_comments =[] 
    for i in range(len(df_yt_comments)):
        yt_comments.append(df_yt_comments.iloc[i]["Comments"])
    yt_comments = [[item[1], item[2]] for sublist in yt_comments for item in sublist]


    # compare length of df_yt_comments and embeddings_yt_comments
    if len(yt_comments) != len(embeddings_yt_comments):
        print("Length of df_yt_comments and embeddings_yt_comments do not match.")
        print(len(yt_comments), len(embeddings_yt_comments))
        sys.exit()
    
    if not only_yt: 
        df_tt_comments = pd.concat(all_data_comments_tt)
        # compare length of df_tt_comments and embeddings_tt_comments
        if len(df_tt_comments) != len(embeddings_tt_comments):
            print("Length of df_tt_comments and embeddings_tt_comments do not match.")
            sys.exit()


    if not only_yt:
        list_video_comments_pairs_tt = []
        # for each video in df_tt_videos get the corresponding embedding and the embeddings of comments to that video
        for i, video in df_tt_videos.iterrows():
            # get embedding of video
            video_id = video["id"]
            emb_video = embeddings_tt_content[i]
            # get indices of comments in df_tt_comments whose video_id is the first element of video
            indices = df_tt_comments[df_tt_comments["video_id"] == video_id].index.values
            # if indices is empty, print
            if len(indices) == 0:
                list_video_comments_pairs_tt.append([emb_video, None])
                continue
            # get embeddings of comments of those indices
            emb_comments = embeddings_tt_comments[indices]
            # calculate centroid of comments
            centroid_comments = np.mean(emb_comments, axis=0)
            list_video_comments_pairs_tt.append([emb_video, centroid_comments])

    list_video_comments_pairs_yt = []
    # for each video in df_yt_videos get the corresponding embedding and the embeddings of comments to that video
    for i, video in df_yt_videos.iterrows():
        # get embedding of video
        video_id = video["id"]
        emb_video = embeddings_yt_content[i]
        # get indices of comments in yt_comments whose element 2 is the video id
        indices = [j for j in range(len(yt_comments)) if yt_comments[j][1] == video_id]
        if len(indices) == 0:
            list_video_comments_pairs_yt.append([emb_video, None])
            continue
        # get embeddings of comments of those indices
        emb_comments = embeddings_yt_comments[indices]
        # calculate centroid of comments
        centroid_comments = np.mean(emb_comments, axis=0)
        list_video_comments_pairs_yt.append([emb_video, centroid_comments])

    if not only_yt:
        # focus TT
        # for each video, compure ratio of cosine similarity between video and centroid of comments and avg. cosine similarity between centroid of comments and every other video not in its pair
        # get average
        centroid_videos = np.mean([pair[0] for pair in list_video_comments_pairs_tt], axis=0)
        list_ratio_tt = []
        list_diff_cos_tt = []
        list_dist_tt = []
        for i, pair in enumerate(list_video_comments_pairs_tt):
            emb_video = pair[0]
            emb_comments = pair[1]
            if emb_comments is None:
                continue
            cos_sim_pair = np.dot(emb_video, emb_comments)/(np.linalg.norm(emb_video)*np.linalg.norm(emb_comments))
            # get all other videos
            other_pairs = list_video_comments_pairs_tt[:i] + list_video_comments_pairs_tt[i+1:]
            # get all other comments
            other_videos = [pair[0] for pair in other_pairs]
            list_cosine_similarity_others_tt = []
            for other_video in other_videos:
                cos_sim = np.dot(emb_comments, other_video)/(np.linalg.norm(emb_comments)*np.linalg.norm(other_video))
                list_cosine_similarity_others_tt.append(cos_sim)
            avg_cos_sim = np.mean(list_cosine_similarity_others_tt)
            ratio = cos_sim_pair/avg_cos_sim
            list_ratio_tt.append(ratio)
            list_diff_cos_tt.append(cos_sim_pair-avg_cos_sim)
            if cos_sim_pair<0:
                print("TT")
                print(cos_sim_pair)
            list_dist_tt.append(np.dot(centroid_videos, emb_comments)/(np.linalg.norm(centroid_videos)*np.linalg.norm(emb_comments)))

    # focus YT
    # for each video, compure ratio of cosine similarity between video and centroid of comments and avg. cosine similarity between centroid of comments and every other video not in its pair
    centroid_videos = np.mean([pair[0] for pair in list_video_comments_pairs_yt], axis=0)
    list_ratio_yt = []
    list_diff_cos_yt = []
    list_dist_yt = []
    for i, pair in enumerate(list_video_comments_pairs_yt):
        emb_video = pair[0]
        emb_comments = pair[1]
        if emb_comments is None:
            continue
        cos_sim_pair = np.dot(emb_video, emb_comments)/(np.linalg.norm(emb_video)*np.linalg.norm(emb_comments))
        # get all other videos
        other_pairs = list_video_comments_pairs_yt[:i] + list_video_comments_pairs_yt[i+1:]
        # get all other comments
        other_videos = [pair[0] for pair in other_pairs]
        list_cosine_similarity_others_yt = []
        for other_video in other_videos:
            cos_sim = np.dot(emb_comments, other_video)/(np.linalg.norm(emb_comments)*np.linalg.norm(other_video))
            list_cosine_similarity_others_yt.append(cos_sim)
        avg_cos_sim = np.mean(list_cosine_similarity_others_yt)
        ratio = cos_sim_pair/avg_cos_sim
        list_ratio_yt.append(ratio)
        list_diff_cos_yt.append(cos_sim_pair-avg_cos_sim)
        if cos_sim_pair<0:
            print("YT")
            print(cos_sim_pair)
        list_dist_yt.append(np.dot(centroid_videos, emb_comments)/(np.linalg.norm(centroid_videos)*np.linalg.norm(emb_comments)))

    if not only_yt:
        #print("TT:", np.mean(list_ratio_tt), np.std(list_ratio_tt))
        print("TT:", np.mean(list_diff_cos_tt), np.std(list_diff_cos_tt))

    #print("YT:", np.mean(list_ratio_yt), np.std(list_ratio_yt))
    print("YT:", np.mean(list_diff_cos_yt), np.std(list_diff_cos_yt))


    if not only_yt:
        # save ratios
        with open("./ratios/ratios_sim_tt_"+creator+"_newdata.pkl", "wb") as f:
            pickle.dump(list_ratio_tt, f)

        with open("./ratios/dist_tt_"+creator+"_newdata.pkl", "wb") as f:
            pickle.dump(list_dist_tt, f)

        with open("./ratios/diff_cos_tt_"+creator+"_newdata.pkl", "wb") as f:
            pickle.dump(list_diff_cos_tt, f)
        
    # save ratios

    
    with open("./ratios/ratios_sim_yt_"+creator+"_newdata.pkl", "wb") as f:
        pickle.dump(list_ratio_yt, f)

    with open("./ratios/dist_yt_"+creator+"_newdata.pkl", "wb") as f:
        pickle.dump(list_dist_yt, f)

    with open("./ratios/diff_cos_yt_"+creator+"_newdata.pkl", "wb") as f:
        pickle.dump(list_diff_cos_yt, f)
