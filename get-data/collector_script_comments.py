import datetime
from pathlib import Path
from typing import Any, Dict, List, Union, Iterable
import json
#from tiktok_research_client.utils import save_json, generate_date_ranges
from tiktok_research_client.data_collection.collect import TiktokClient
from dotenv import load_dotenv
import sys
import pickle
import pandas as pd

def save_json(
    path: Path, container: Union[Iterable[Dict[str, Any]], Dict[str, Any], None]
) -> None:
    """Write dict to path."""
    #print(f"Saving json to {path}")

    # Ensure the directory exists; mkdir(parents=True) will create any missing parent directories
    path.parent.mkdir(parents=True, exist_ok=True)

    # # if json file already exists, append to it. Otherwise, create it
    # if path.exists():
    #     with path.open("r") as f:
    #         data = json.load(f)
    #         data.extend(container)
    #         container = data

    with path.open("w") as f: 
        json.dump(container, f, indent=4, ensure_ascii=False)

    


client = TiktokClient()

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

# # names of creators in each community with more than 0 comments
# community0 = ["error.0092", "cricketoffi", "soufiane.climatisation", "qaid_e_tanhai3", "pakistaniextra", "codingworld786", "ansar.37", "smartserver19", "angelheart4freedom", "waderi__lashari"]
# community1 = ["greenpeaceuk", "climatekat", "thegarbagequeen", "c4news", "climate.facts.scientific", "skytg24", "srikandilestari_", "jamessavoulidis", "showme_yourmask", "juststopoil"]
# community2 = ["tech24.dk", "oklm_911", "the_nooobys", "mah97.photo", "_charxxhoyeon_", "sauvons_la_planete_stp", "sauvons_la_planete_stp"]
# community3 = ["geo.editssx", "move.story", "user1231224321087", "mose_venice", "earthscall", "her.styleofficial", "plastic2energy", "save_our_earth_23", "nesssexyy"]
# community4 = ["firstdndcreatureshow", "zargarkhan786", "greenpeaceafrica", "worldeconomicforum", "exsolarpower", "solarflare711", "solarpowerbattery1", "energybattery02", "magiccoolerrecom"]
# community5 = ["creativesociety369", "creative_society_uk", "jul_creative", "serge_cs", "iqlim_inqirozi", "mirek_travnicek", "straightouttasalem", "daniel25oelofse", "creative_society.norway", "creativesocietynorway"]
# community6 = ["prince36516", "aima.nepal", "electricdrivesnepal", "ridebeammalaysia", "go_green_id", "themanilatimes", "mirhaali30", "moniquetirah", "impactdistrict", "bullsnosenews"]
# community7 = ["wings.of.hope.7", "climate.change.2023", "bullcam17", "insideputh", "capsula.facial", "michi.h93", "thewritetiming", "krabeeputh"]
# community8 = ["kimberlyguilfo", "feed_us_billionaires", "scientist.rebellion_", "wadeabawi", "guardian_rebellion", "corn.bred_", "newzforthought", "therevcoms", "_kimwoobin", "superboognish"]
# community9 = ["god.loves_you1123", "loveofearthco", "clean_fory", "nature__pilled", "plantbasedtreaty", "livekindly", "thetrashwalker", "omillionaire_me"]

max_comments_per_video = 100

for creator in list(dictionary_channelid.keys())[9:]:
#for creator in community4:

    print("Creator:", creator)

    # open all keywords json file
    with open('./data/search/creators/all_keywords_'+creator+'_2021-2023.pkl', 'rb') as f:
        retrieved_videos = pickle.load(f)
    
    # convert to dataframe
    retrieved_videos = pd.DataFrame(retrieved_videos)

    # remove possible duplicates
    retrieved_videos = retrieved_videos.drop_duplicates(subset="id")

    # # filter by creator
    # retrieved_videos = retrieved_videos[retrieved_videos["username"]==creator]

    # get list of video ids
    video_ids = retrieved_videos["id"].tolist()

    load_dotenv()

    # keep track of comments for all videos from creator
    all_comments = []
    iter_videos = 0 # keep track of videos

    print("N. videos:", len(video_ids))


    for video_id in video_ids:

        # if json file already exists, open
        if Path(f"./data/comments/creators/all_comments_{creator}.json").exists():
            with open(f"./data/comments/creators/all_comments_{creator}.json", "r") as f:
                all_comments = json.load(f) # in this way, we will overwrite the json file if we run the script again
            
            # if video already in json, skip
            if any(d["video_id"] == video_id for d in all_comments):
                iter_videos += 1
                
                if iter_videos % 10 == 0:
                    print("N. videos:", iter_videos ,"/", len(video_ids))

                continue
        
        count_comments = len(all_comments)

        url: str = "https://open.tiktokapis.com/v2/research/video/comment/list/?fields=id,like_count,create_time,text,video_id,parent_comment_id,reply_count"

        query: Dict[str, Any] = {
            "video_id": video_id,
            "max_count": 100,
        }

        comments: List[Dict[str, str]] = list()

        has_more_data: bool = True

        while (
            has_more_data and len(comments) < max_comments_per_video
        ):  # 1000 is the max number of comments we can get
            
            response: Union[Dict[str, Any], None] = client.fetch_data(url, query)

            try:
                comments.extend(response["data"]["comments"])  # type: ignore

                has_more_data = response["data"]["has_more"]  # type: ignore

                query["cursor"] = response["data"]["cursor"]  # type: ignore

                # Check if we have reached the max size or there is no more data
                if not has_more_data:
                    del query["cursor"]
                    break
            except:
                print("Error")
                break

        for idx, comment in enumerate(comments):
            comments[idx]["create_time"] = datetime.datetime.utcfromtimestamp(
                comment["create_time"]  # type: ignore
            ).strftime("%Y-%m-%d")
        
        all_comments.extend(comments)

        iter_videos += 1

        if iter_videos % 10 == 0:
            print("N. videos:", iter_videos ,"/", len(video_ids))

        # each video, append comments to json
        save_json(Path(f"./data/comments/creators/all_comments_{creator}.json"), all_comments)

