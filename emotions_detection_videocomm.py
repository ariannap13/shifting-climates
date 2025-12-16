import pickle
from tqdm import tqdm
import time
import nltk 
import sys
from nltk import word_tokenize
from nltk.stem.snowball import SnowballStemmer
import requests
from io import StringIO
import pandas as pd
from collections import Counter
import json
import numpy as np

nltk.download('punkt')

tt_datadir = "./tiktok-opinion-dynamics/src/tiktok_opinion_dynamics/data/"
yt_datadir = "./youtube/data/"


first_person_sing_pronouns = ['i', 'me', 'my', 'mine', 'myself']
first_person_plur_pronouns = ['we', 'us', 'our', 'ours', 'ourselves']
second_person_pronouns = ['you', 'your', 'yours', 'yourself', 'yourselves']
third_person_pronouns = ['he', 'him', 'his', 'himself', 'she', 'her', 'hers', 'herself', 'they', 'them', 'their', 'theirs', 'themselves']

# find whether a text contains a personal pronoun and which category it belongs to
def personal_pronouns(text):
    epsilon = 1e-6
    first_person_sing = 0
    first_person_plur = 0
    second_person = 0
    third_person = 0
    tot_words = 0
    for word in text.split():
        if word.lower() in first_person_sing_pronouns:
            first_person_sing += 1
        elif word.lower() in first_person_plur_pronouns:
            first_person_plur += 1
        elif word.lower() in second_person_pronouns:
            second_person += 1
        elif word.lower() in third_person_pronouns:
            third_person += 1
        tot_words+=1
    
    score_I_we = 0.5 + 0.5 * (first_person_sing - first_person_plur) / (first_person_sing + first_person_plur + 1)
    score_I_other = 0.5 + 0.5 * (first_person_sing - first_person_plur - second_person - third_person) / (first_person_sing + first_person_plur + second_person + third_person + 1)

    return score_I_we, score_I_other

def LeXmo(text, dict_emolex):

    '''
      Takes text and adds if to a dictionary with 10 Keys  for each of the 10 emotions in the NRC Emotion Lexicon,
      each dictionay contains the value of the text in that emotions divided to the text word count
      INPUT: string
      OUTPUT: dictionary with the text and the value of 10 emotions


    '''
    LeXmo_dict = {'text': text, 'anger': [], 'anticipation': [], 'disgust': [], 'fear': [], 'joy': [], 'negative': [],
                  'positive': [], 'sadness': [], 'surprise': [], 'trust': []}

    stemmer = SnowballStemmer("english")

    document = word_tokenize(text)
    word_count = len(document)

    # stem the document without for loop
    document = [stemmer.stem(word) for word in document]

    for emotion in dict_emolex.keys():
        LeXmo_dict[emotion] = len(list((Counter(document) & Counter(dict_emolex[emotion])).elements()))/word_count

    return LeXmo_dict


dictionary_channelid = {"unclimatechange": "UCuLEr-GWiwCBB6zBDX3elOQ",
                        "natgeo": "UCpVm7bg6pXKo1Pr6k5kxG9A",
                        "nbcnews": "UCeY0bbntWzzVIaj2z3QigXg",
                        "bbcnews": "UC16niRr50-MSBwiO3YDb3RA",
                        "climateadam": "UCCu5wtZ5uOWZp_roz7wHPfg",
                        "drgilbz": "UCjaBxCyjLpIRyKOd8uw_S4w",
                        "dwplaneta": "UCb72Gn5LXaLEcsOuPKGfQOg",
                        "extinctionrebellionxr": "UCYThdLKE6TDwBJh-qDC6ICA",
                        "greenpeace_international": "UCTDTSx8kbxGECZJxOa9mIKA",
                        "guardian": "UCHpw8xwDNhU9gdohEcJu4aA",
                        "ourchangingclimate": "UCNXvxXpDJXp-mZu3pFMzYHQ",
                        "ted": "UCAuUUnT6oDeKwE6v1NGQxug"}  

# get emotion dictionary
response = requests.get('https://raw.github.com/dinbav/LeXmo/master/NRC-Emotion-Lexicon-Wordlevel-v0.92.txt')
nrc = StringIO(response.text)

emolex_df = pd.read_csv(nrc,
                        names=["word", "emotion", "association"],
                        sep=r'\t', engine='python')

dict_emolex = {}
for emotion in list(emolex_df.emotion.unique()):
    print("Emotion:", emotion)
    dict_emolex[emotion] = list(set(list(emolex_df[(emolex_df.emotion == emotion) & (emolex_df.association == 1)].word)))


for creator in list(dictionary_channelid.keys()):

    print("Creator:", creator)

    # videos
    # TT
    with open(tt_datadir+"search/creators/all_keywords_"+creator+"_2021-2023.pkl", "rb") as token:
        tt_content = pickle.load(token)
    # remove possible duplicates
    tt_content = tt_content.drop_duplicates(subset="id")

    # YT
    with open(yt_datadir+creator+"_transcript_2021-2023.pkl", "rb") as token:
        yt_content = pickle.load(token)
    # remove possible duplicates
    yt_content = yt_content.drop_duplicates(subset="Video ID")

    # comments
    # TT
    with open(tt_datadir+"/comments/creators/all_comments_"+creator+".json", "r") as f:
        tt_comments = json.load(f)
    tt_comments = pd.DataFrame(tt_comments)

    # YT
    list_comments_yt = []
    with open(yt_datadir+"comments/"+creator+"_comments_2021-2023.json", "r") as f:
        yt_comments = json.load(f)
    yt_comments = pd.DataFrame(yt_comments)
    for comment in yt_comments["Comments"]:
        if len(comment) == 0:
            continue
        # retrieve video id
        video_id = comment[0][2]
        list_comments_yt.append([video_id, comment[0][1]])
    
    # create dataframe with comments
    yt_comments = pd.DataFrame(list_comments_yt, columns=["video_id", "text"])

    # TT analysis
    print("TT")
    if "voice_to_text" in tt_content.columns:
        tt_content["text"] = tt_content["voice_to_text"].fillna(tt_content["video_description"])
    else:
        tt_content["text"] = tt_content["video_description"]

    # iterate over content (videos)
    # create I_we, I_other, emotions columns in tt_content, initialize to 0
    tt_content["I_we"] = 0.0
    tt_content["I_other"] = 0.0
    tt_content["anger"] = 0.0
    tt_content["anticipation"] = 0.0
    tt_content["disgust"] = 0.0
    tt_content["fear"] = 0.0
    tt_content["joy"] = 0.0
    tt_content["sadness"] = 0.0
    tt_content["surprise"] = 0.0
    tt_content["trust"] = 0.0
    tt_content["negative"] = 0.0
    tt_content["positive"] = 0.0

    tt_content["I_we_comments"] = 0.0
    tt_content["I_other_comments"] = 0.0
    tt_content["anger_comments"] = 0.0
    tt_content["anticipation_comments"] = 0.0
    tt_content["disgust_comments"] = 0.0
    tt_content["fear_comments"] = 0.0
    tt_content["joy_comments"] = 0.0
    tt_content["sadness_comments"] = 0.0
    tt_content["surprise_comments"] = 0.0
    tt_content["trust_comments"] = 0.0
    tt_content["negative_comments"] = 0.0
    tt_content["positive_comments"] = 0.0
    for i in tqdm(range(len(tt_content))):
        # retrieve comments for video
        comments = tt_comments[tt_comments["video_id"]==tt_content["id"].values[i]]["text"].values

        # dataframes for comments
        df_comments = pd.DataFrame(comments, columns=["text"])

        if len(comments) == 0:
            continue

        if tt_content["text"].values[i] == "":
            continue
        
        # extract personal pronouns from content (video) and related comments
        tt_content["I_we"].values[i] = personal_pronouns(tt_content["text"].values[i])[0]
        tt_content["I_other"].values[i] = personal_pronouns(tt_content["text"].values[i])[1]
        
        df_comments["I_we"] = df_comments['text'].apply(lambda x: personal_pronouns(x)[0])
        df_comments["I_other"] = df_comments['text'].apply(lambda x: personal_pronouns(x)[1])

        # extract emotions from content (video) and related comments
        emotions_dict = LeXmo(tt_content["text"].values[i], dict_emolex)
        tt_content["anger"].values[i] = emotions_dict["anger"]
        #print(tt_content["anger"].values[i])
        tt_content["anticipation"].values[i] = emotions_dict["anticipation"]
        tt_content["disgust"].values[i] = emotions_dict["disgust"]
        tt_content["fear"].values[i] = emotions_dict["fear"]
        tt_content["joy"].values[i] = emotions_dict["joy"]
        tt_content["sadness"].values[i] = emotions_dict["sadness"]
        tt_content["surprise"].values[i] = emotions_dict["surprise"]
        tt_content["trust"].values[i] = emotions_dict["trust"]
        tt_content["negative"].values[i] = emotions_dict["negative"]
        tt_content["positive"].values[i] = emotions_dict["positive"]

        df_comments["emotions"] = df_comments['text'].apply(lambda x: LeXmo(x, dict_emolex) if x != "" else "")
        # from emotions column, create new columns
        df_comments["anger"] = df_comments["emotions"].apply(lambda x: x["anger"] if x != "" else 0)
        df_comments["anticipation"] = df_comments["emotions"].apply(lambda x: x["anticipation"] if x != "" else 0)
        df_comments["disgust"] = df_comments["emotions"].apply(lambda x: x["disgust"] if x != "" else 0)
        df_comments["fear"] = df_comments["emotions"].apply(lambda x: x["fear"] if x != "" else 0)
        df_comments["joy"] = df_comments["emotions"].apply(lambda x: x["joy"] if x != "" else 0)
        df_comments["sadness"] = df_comments["emotions"].apply(lambda x: x["sadness"] if x != "" else 0)
        df_comments["surprise"] = df_comments["emotions"].apply(lambda x: x["surprise"] if x != "" else 0)
        df_comments["trust"] = df_comments["emotions"].apply(lambda x: x["trust"] if x != "" else 0)
        df_comments["negative"] = df_comments["emotions"].apply(lambda x: x["negative"] if x != "" else 0)
        df_comments["positive"] = df_comments["emotions"].apply(lambda x: x["positive"] if x != "" else 0)

        # average emotions and personal pronouns for comments
        tt_content["I_we_comments"].values[i] = df_comments["I_we"].mean()
        tt_content["I_other_comments"].values[i] = df_comments["I_other"].mean()
        tt_content["anger_comments"].values[i] = df_comments["anger"].mean()
        tt_content["anticipation_comments"].values[i] = df_comments["anticipation"].mean()
        tt_content["disgust_comments"].values[i] = df_comments["disgust"].mean()
        tt_content["fear_comments"].values[i] = df_comments["fear"].mean()
        tt_content["joy_comments"].values[i] = df_comments["joy"].mean()
        tt_content["sadness_comments"].values[i] = df_comments["sadness"].mean()
        tt_content["surprise_comments"].values[i] = df_comments["surprise"].mean()
        tt_content["trust_comments"].values[i] = df_comments["trust"].mean()
        tt_content["negative_comments"].values[i] = df_comments["negative"].mean()
        tt_content["positive_comments"].values[i] = df_comments["positive"].mean()

    # save
    tt_content.to_pickle(tt_datadir+"search/creators/videos_comments_"+creator+"_withemo_2021-2023.pkl")

    # YT analysis
    print("YT")
    # consider transcript if available, otherwise use description
    yt_content["text"] = yt_content["Video Transcript"].fillna(yt_content["Video Description"])

    yt_content["I_we"] = 0.0
    yt_content["I_other"] = 0.0
    yt_content["anger"] = 0.0
    yt_content["anticipation"] = 0.0
    yt_content["disgust"] = 0.0
    yt_content["fear"] = 0.0
    yt_content["joy"] = 0.0
    yt_content["sadness"] = 0.0
    yt_content["surprise"] = 0.0
    yt_content["trust"] = 0.0
    yt_content["negative"] = 0.0
    yt_content["positive"] = 0.0

    yt_content["I_we_comments"] = 0.0
    yt_content["I_other_comments"] = 0.0
    yt_content["anger_comments"] = 0.0
    yt_content["anticipation_comments"] = 0.0
    yt_content["disgust_comments"] = 0.0
    yt_content["fear_comments"] = 0.0
    yt_content["joy_comments"] = 0.0
    yt_content["sadness_comments"] = 0.0
    yt_content["surprise_comments"] = 0.0
    yt_content["trust_comments"] = 0.0
    yt_content["negative_comments"] = 0.0
    yt_content["positive_comments"] = 0.0
    # iterate over content (videos)
    for i in tqdm(range(len(yt_content))):
        # retrieve comments for video
        comments = yt_comments[yt_comments["video_id"]==yt_content["Video ID"].values[i]]["text"].values

        # dataframes for comments
        df_comments = pd.DataFrame(comments, columns=["text"])

        if len(comments) == 0:
            continue

        if yt_content["text"].values[i] == "":
            continue
        
        
        # extract personal pronouns from content (video) and related comments
        yt_content["I_we"].values[i] = personal_pronouns(yt_content["text"].values[i])[0]
        yt_content["I_other"].values[i] = personal_pronouns(yt_content["text"].values[i])[1]
        
        df_comments["I_we"] = df_comments['text'].apply(lambda x: personal_pronouns(x)[0])
        df_comments["I_other"] = df_comments['text'].apply(lambda x: personal_pronouns(x)[1])      

        # extract emotions from content (video) and related comments
        emotions_dict = LeXmo(yt_content["text"].values[i], dict_emolex)
        # from emotions column, create new columns
        yt_content["anger"].values[i] = emotions_dict["anger"]
        yt_content["anticipation"].values[i] = emotions_dict["anticipation"]
        yt_content["disgust"].values[i] = emotions_dict["disgust"]
        yt_content["fear"].values[i] = emotions_dict["fear"]
        yt_content["joy"].values[i] = emotions_dict["joy"]
        yt_content["sadness"].values[i] = emotions_dict["sadness"]
        yt_content["surprise"].values[i] = emotions_dict["surprise"]
        yt_content["trust"].values[i] = emotions_dict["trust"]
        yt_content["negative"].values[i] = emotions_dict["negative"]
        yt_content["positive"].values[i] = emotions_dict["positive"]

        df_comments["emotions"] = df_comments['text'].apply(lambda x: LeXmo(x, dict_emolex) if x != "" else "")
        # from emotions column, create new columns
        df_comments["anger"] = df_comments["emotions"].apply(lambda x: x["anger"] if x != "" else 0)
        df_comments["anticipation"] = df_comments["emotions"].apply(lambda x: x["anticipation"] if x != "" else 0)
        df_comments["disgust"] = df_comments["emotions"].apply(lambda x: x["disgust"] if x != "" else 0)
        df_comments["fear"] = df_comments["emotions"].apply(lambda x: x["fear"] if x != "" else 0)
        df_comments["joy"] = df_comments["emotions"].apply(lambda x: x["joy"] if x != "" else 0)
        df_comments["sadness"] = df_comments["emotions"].apply(lambda x: x["sadness"] if x != "" else 0)
        df_comments["surprise"] = df_comments["emotions"].apply(lambda x: x["surprise"] if x != "" else 0)
        df_comments["trust"] = df_comments["emotions"].apply(lambda x: x["trust"] if x != "" else 0)
        df_comments["negative"] = df_comments["emotions"].apply(lambda x: x["negative"] if x != "" else 0)
        df_comments["positive"] = df_comments["emotions"].apply(lambda x: x["positive"] if x != "" else 0)


        # average emotions and personal pronouns for comments
        yt_content["I_we_comments"].values[i] = df_comments["I_we"].mean()
        yt_content["I_other_comments"].values[i] = df_comments["I_other"].mean()
        yt_content["anger_comments"].values[i] = df_comments["anger"].mean()
        yt_content["anticipation_comments"].values[i] = df_comments["anticipation"].mean()
        yt_content["disgust_comments"].values[i] = df_comments["disgust"].mean()
        yt_content["fear_comments"].values[i] = df_comments["fear"].mean()
        yt_content["joy_comments"].values[i] = df_comments["joy"].mean()
        yt_content["sadness_comments"].values[i] = df_comments["sadness"].mean()
        yt_content["surprise_comments"].values[i] = df_comments["surprise"].mean()
        yt_content["trust_comments"].values[i] = df_comments["trust"].mean()
        yt_content["negative_comments"].values[i] = df_comments["negative"].mean()
        yt_content["positive_comments"].values[i] = df_comments["positive"].mean()

    # save
    yt_content.to_pickle(yt_datadir+creator+"_videos_comments_withemo_2021-2023.pkl")