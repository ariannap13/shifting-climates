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
import liwc
from moralfoundations.moralstrength.moralstrength import estimate_morals
from collections import Counter
import re



nltk.download('punkt')

tt_datadir = "./tiktok-opinion-dynamics/src/tiktok_opinion_dynamics/data/"
yt_datadir = "./youtube/data/"

analyze_comments = False


first_person_sing_pronouns = ['i', 'me', 'my', 'mine', 'myself']
first_person_plur_pronouns = ['we', 'us', 'our', 'ours', 'ourselves']
second_person_pronouns = ['you', 'your', 'yours', 'yourself', 'yourselves']
third_person_pronouns = ['he', 'him', 'his', 'himself', 'she', 'her', 'hers', 'herself', 'they', 'them', 'their', 'theirs', 'themselves']

def clean_text(text):

    # if text type is float or NoneType, return None
    if type(text) == float or type(text) == type(None):
        output = ""
        return output
    #replace . with space
    output = text.replace('.', ' ')

    #remove tags such as [Music]
    output = re.sub(r'\[.*?\]', '', output)

    output = output.lower()

    #remove string if string only contains punctuation
    if sum([i.isalpha() for i in output])== 0:
        output = ""
        
    #remove if number of unique words is less than 5
    if len(set(output.split())) < 5:
        output = ""
    
    return output

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
                        # "natgeo": "UCpVm7bg6pXKo1Pr6k5kxG9A",
                        # "nbcnews": "UCeY0bbntWzzVIaj2z3QigXg",
                        "bbcnews": "UC16niRr50-MSBwiO3YDb3RA",
                        "climateadam": "UCCu5wtZ5uOWZp_roz7wHPfg",
                        "drgilbz": "UCjaBxCyjLpIRyKOd8uw_S4w",
                        "dwplaneta": "UCb72Gn5LXaLEcsOuPKGfQOg",
                        "extinctionrebellionxr": "UCYThdLKE6TDwBJh-qDC6ICA",
                        "greenpeace_international": "UCTDTSx8kbxGECZJxOa9mIKA",
                        "guardian": "UCHpw8xwDNhU9gdohEcJu4aA",
                        "ourchangingclimate": "UCNXvxXpDJXp-mZu3pFMzYHQ",
                        "ted": "UCAuUUnT6oDeKwE6v1NGQxug"}  
dictionary_channelid2 = {"juststopoil": "UC-t4U1Azf8AOkCBJILSNBmw",
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
dictionary_channelid.update(dictionary_channelid2)

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


parse, category_names = liwc.load_token_parser('LIWC2015_English.dic')

category_interest = ['affect (Affect)', 'posemo (Positive Emotions)', 'negemo (Negative Emotions)', 'anx (Anx)', 
                     'anger (Anger)', 'sad (Sad)', 'social (Social)', 'affiliation (Affiliation)', 'achieve (Achievement)', 
                     'power (Power)', 'reward (Reward)', 'risk (Risk)', 'focuspast (Past Focus)', 'focuspresent (Present Focus)', 
                     'focusfuture (Future Focus)', 'informal (Informal Language)']

for creator in list(dictionary_channelid.keys()):

    print("Creator:", creator)

    if not analyze_comments:
        # TT
        with open(tt_datadir+"search/creators/all_keywords_"+creator+"_2021-2023.pkl", "rb") as token:
            tt_content = pickle.load(token)
        # remove possible duplicates
        tt_content = tt_content.drop_duplicates(subset="id")

        # YT
        with open(yt_datadir+creator+"_transcript_2021-2023_newkeys.pkl", "rb") as token:
            yt_content = pickle.load(token)
        # remove possible duplicates
        yt_content = yt_content.drop_duplicates(subset="Video ID")

    else:
        # TT
        with open(tt_datadir+"/comments/creators/all_comments_"+creator+".json", "r") as f:
            tt_content = json.load(f)
        tt_content = pd.DataFrame(tt_content)

        # YT
        list_comments_yt = []
        with open(yt_datadir+"comments/"+creator+"_comments_2021-2023.json", "r") as f:
            yt_content = json.load(f)
        yt_content = pd.DataFrame(yt_content)
        for comment in yt_content["Comments"]:
            if len(comment) == 0:
                continue
            list_comments_yt.append(comment[0][1])
        
        # create dataframe with comments
        yt_content = pd.DataFrame(list_comments_yt, columns=["text"])

    # TT analysis
    print("TT")
    n_no_transcript_tt = 0
    if not analyze_comments:
        # consider voice_to_text if available, otherwise use description
        if "voice_to_text" in tt_content.columns:
            # clean text
            tt_content["voice_to_text"] = tt_content["voice_to_text"].apply(lambda x: clean_text(x))
            # replace "" with None
            tt_content["voice_to_text"] = tt_content["voice_to_text"].replace("", None)
            n_no_transcript_tt = len(tt_content[tt_content["voice_to_text"].isnull()])
            tt_content["text"] = tt_content["voice_to_text"].fillna(tt_content["video_description"])
        else:
            tt_content["text"] = tt_content["video_description"]
            n_no_transcript_tt = len(tt_content[tt_content["text"].isnull()])

    # print % of videos without transcript
    print("Percentage of videos without transcript TT:", n_no_transcript_tt/len(tt_content))

    # personal pronouns
    print("personal pronouns started")
    tt_content["I_we"] = tt_content['text'].apply(lambda x: personal_pronouns(x)[0])
    tt_content["I_other"] = tt_content['text'].apply(lambda x: personal_pronouns(x)[1])

    # lexmo emotions
    print("lexmo started")
    tt_content["emotions"] = tt_content['text'].apply(lambda x: LeXmo(x, dict_emolex) if x != "" else "")
    # from emotions column, create new columns
    tt_content["anger"] = tt_content["emotions"].apply(lambda x: x["anger"] if x != "" else "")
    tt_content["anticipation"] = tt_content["emotions"].apply(lambda x: x["anticipation"] if x != "" else "")
    tt_content["disgust"] = tt_content["emotions"].apply(lambda x: x["disgust"] if x != "" else "")
    tt_content["fear"] = tt_content["emotions"].apply(lambda x: x["fear"] if x != "" else "")
    tt_content["joy"] = tt_content["emotions"].apply(lambda x: x["joy"] if x != "" else "")
    tt_content["sadness"] = tt_content["emotions"].apply(lambda x: x["sadness"] if x != "" else "")
    tt_content["surprise"] = tt_content["emotions"].apply(lambda x: x["surprise"] if x != "" else "")
    tt_content["trust"] = tt_content["emotions"].apply(lambda x: x["trust"] if x != "" else "")
    tt_content["negative"] = tt_content["emotions"].apply(lambda x: x["negative"] if x != "" else "")
    tt_content["positive"] = tt_content["emotions"].apply(lambda x: x["positive"] if x != "" else "")

    # moral foundations
    print("moral foundations started")
    result_avg, result_freq = estimate_morals(tt_content["text"], process=True)

    tt_content["moral_care_rfreq"] = result_freq["care"]
    tt_content["moral_loyalty_rfreq"] = result_freq["loyalty"]
    tt_content["moral_authority_rfreq"] = result_freq["authority"]
    tt_content["moral_purity_rfreq"] = result_freq["purity"]
    tt_content["moral_fairness_rfreq"] = result_freq["fairness"]

    # LIWC categories
    # tokenize "text" column
    print("LIWC started")
    tt_content["text"] = tt_content["text"].apply(lambda x: word_tokenize(x))
    # lowercase
    tt_content["text"] = tt_content["text"].apply(lambda x: [word.lower() for word in x])
    # Counter categories in "text" column
    tt_content["liwc_categories"] = tt_content["text"].apply(lambda x: Counter(category for token in x for category in parse(token)))
    # length of text
    tt_content["text_length"] = tt_content["text"].apply(lambda x: len(x))

    for cat in category_interest:
        # compute relative frequency by dividing by text length
        tt_content[cat+" freq"] = tt_content["liwc_categories"].apply(lambda x: x[cat] if cat in x.keys() else 0)
        tt_content[cat+" rfreq"] = tt_content[cat+" freq"] / tt_content["text_length"]

    # save
    if not analyze_comments:
        tt_content.to_pickle(tt_datadir+"search/creators/all_keywords_"+creator+"_clean_withmetrics_2021-2023.pkl")
    else:
        tt_content.to_pickle(tt_datadir+"comments/creators/all_comments_"+creator+"_clean_withmetrics_2021-2023.pkl")

    # YT analysis
    print("YT")

    n_no_transcript_yt = 0
    if not analyze_comments:
        # if transcript is available, use it, otherwise use description
        # clean text
        yt_content["Video Transcript"] = yt_content["Video Transcript"].apply(lambda x: clean_text(x))
        # replace "" with None
        yt_content["Video Transcript"] = yt_content["Video Transcript"].replace("", None)
        n_no_transcript_yt = len(yt_content[yt_content["Video Transcript"].isnull()])
        yt_content["text"] = yt_content["Video Transcript"].fillna(yt_content["Video Description"])

    # print % of videos without transcript
    print("Percentage of videos without transcript YT:", n_no_transcript_yt/len(yt_content))
        

    # personal pronouns
    print("personal pronouns started")
    yt_content["I_we"] = yt_content['text'].apply(lambda x: personal_pronouns(x)[0])
    yt_content["I_other"] = yt_content['text'].apply(lambda x: personal_pronouns(x)[1])


    # lexmo emotions
    print("lexmo started")
    yt_content["emotions"] = yt_content['text'].apply(lambda x: LeXmo(x, dict_emolex) if x != "" else "")
    # from emotions column, create new columns
    yt_content["anger"] = yt_content["emotions"].apply(lambda x: x["anger"] if x != "" else "")
    yt_content["anticipation"] = yt_content["emotions"].apply(lambda x: x["anticipation"] if x != "" else "")
    yt_content["disgust"] = yt_content["emotions"].apply(lambda x: x["disgust"] if x != "" else "")
    yt_content["fear"] = yt_content["emotions"].apply(lambda x: x["fear"] if x != "" else "")
    yt_content["joy"] = yt_content["emotions"].apply(lambda x: x["joy"] if x != "" else "")
    yt_content["sadness"] = yt_content["emotions"].apply(lambda x: x["sadness"] if x != "" else "")
    yt_content["surprise"] = yt_content["emotions"].apply(lambda x: x["surprise"] if x != "" else "")
    yt_content["trust"] = yt_content["emotions"].apply(lambda x: x["trust"] if x != "" else "")
    yt_content["negative"] = yt_content["emotions"].apply(lambda x: x["negative"] if x != "" else "")
    yt_content["positive"] = yt_content["emotions"].apply(lambda x: x["positive"] if x != "" else "")

    # moral foundations
    print("moral foundations started")
    result_avg, result_freq = estimate_morals(yt_content["text"], process=True)

    yt_content["moral_care_rfreq"] = result_freq["care"]
    yt_content["moral_loyalty_rfreq"] = result_freq["loyalty"]
    yt_content["moral_authority_rfreq"] = result_freq["authority"]
    yt_content["moral_purity_rfreq"] = result_freq["purity"]
    yt_content["moral_fairness_rfreq"] = result_freq["fairness"]

    # LIWC categories
    # LIWC categories
    # tokenize "text" column
    print("LIWC started")
    yt_content["text"] = yt_content["text"].apply(lambda x: word_tokenize(x))
    # lowercase
    yt_content["text"] = yt_content["text"].apply(lambda x: [word.lower() for word in x])
    # Counter categories in "text" column
    yt_content["liwc_categories"] = yt_content["text"].apply(lambda x: Counter(category for token in x for category in parse(token)))
    # length of text
    yt_content["text_length"] = yt_content["text"].apply(lambda x: len(x))

    for cat in category_interest:
        # compute relative frequency by dividing by text length
        yt_content[cat+" freq"] = yt_content["liwc_categories"].apply(lambda x: x[cat] if cat in x.keys() else 0)
        yt_content[cat+" rfreq"] = yt_content[cat+" freq"] / yt_content["text_length"]

    # save
    if not analyze_comments:
        yt_content.to_pickle(yt_datadir+creator+"_transcript_clean_withmetrics_2021-2023_newdata.pkl")
    else:
        yt_content.to_pickle(yt_datadir+"comments/"+creator+"_comments_clean_withmetrics_2021-2023_newdata.pkl")



