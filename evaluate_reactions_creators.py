import pickle
import json
import pandas as pd

creator = "extinctionrebellionxr"

# open dataframes in dataframes folder
with open('./dataframes/df_both_tt_yt_withtrans_'+creator+'.pkl', 'rb') as f:
    df_both = pickle.load(f)

with open('./dataframes/df_only_tt_withtrans_'+creator+'.pkl', 'rb') as f:
    df_tt = pickle.load(f)

with open('./dataframes/df_only_yt_withtrans_'+creator+'.pkl', 'rb') as f:
    df_yt = pickle.load(f)

# open tt comments json file
with open('./tiktok-opinion-dynamics/src/tiktok_opinion_dynamics/data/comments/creators/all_comments_'+creator+'.json', 'r') as f:
    tt_comments = json.load(f)

# open yt comments json file
with open('./youtube/data/comments/'+creator+'_comments_2021-2023.json', 'r') as f:
    yt_comments = json.load(f)

# for tt, filter comments that have id in df_both, df_tt
tt_comments_both = []
tt_comments_only = []
for comment in tt_comments:
    if comment["video_id"] in list(df_both["id"]):
        # check that platform is tiktok in df_both
        if df_both[df_both["id"]==comment["video_id"]]["platform"].values[0] == "TT":
            tt_comments_both.append(comment)
    elif comment["video_id"] in list(df_tt["id"]):
        tt_comments_only.append(comment)

# for yt, filter comments that have id in df_both, df_yt
yt_comments_both = []
yt_comments_only = []

for comment in yt_comments:
    if comment["VideoID"] in list(df_both["id"]):
        # check that platform is youtube in df_both
        if df_both[df_both["id"]==comment["VideoID"]]["platform"].values[0] == "YT":
            yt_comments_both.append(comment["Comments"])
    elif comment["VideoID"] in list(df_yt["id"]):
        yt_comments_only.append(comment["Comments"])

# flatten lists if necessary
if len(yt_comments_both) > 0:
    yt_comments_both = [item for sublist in yt_comments_both for item in sublist]
if len(yt_comments_only) > 0:
    yt_comments_only = [item for sublist in yt_comments_only for item in sublist]


# print number of comments
print("Number of comments in both:", len(tt_comments_both)+len(yt_comments_both))
print("Number of comments in only tt:", len(tt_comments_only))
print("Number of comments in only yt:", len(yt_comments_only))

# take only text
tt_comments_both = [comment["text"] for comment in tt_comments_both]
yt_comments_both = [comment[1] for comment in yt_comments_both]
tt_comments_only = [comment["text"] for comment in tt_comments_only]
yt_comments_only = [comment[1] for comment in yt_comments_only]

# transform to dataframes
tt_comments_both = pd.DataFrame(tt_comments_both, columns=["text"])
yt_comments_both = pd.DataFrame(yt_comments_both, columns=["text"])
tt_comments_only = pd.DataFrame(tt_comments_only, columns=["text"])
yt_comments_only = pd.DataFrame(yt_comments_only, columns=["text"])


# save dataframes
tt_comments_both.to_pickle("./dataframes/tt_comments_both_"+creator+".pkl")
yt_comments_both.to_pickle("./dataframes/yt_comments_both_"+creator+".pkl")
tt_comments_only.to_pickle("./dataframes/tt_comments_only_"+creator+".pkl")
yt_comments_only.to_pickle("./dataframes/yt_comments_only_"+creator+".pkl")