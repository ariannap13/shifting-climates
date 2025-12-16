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

    # # Ensure the directory exists; mkdir(parents=True) will create any missing parent directories
    # path.parent.mkdir(parents=True, exist_ok=True)

    # # if json file already exists, append to it. Otherwise, create it
    # if path.exists():
    #     with path.open("r") as f:
    #         data = json.load(f)
    #         data.extend(container)
    #         container = data

    with path.open("w") as f: 
        json.dump(container, f, indent=4, ensure_ascii=False)

    


client = TiktokClient()

max_comments_per_video = 100


# open all keywords json file
with open('./data/search/baseline/baseline2_content.pickle', 'rb') as f:
    retrieved_videos = pickle.load(f)

# convert to dataframe
retrieved_videos = pd.DataFrame(retrieved_videos)

# remove possible duplicates
retrieved_videos = retrieved_videos.drop_duplicates(subset="id")

# get list of video ids
video_ids = retrieved_videos["id"].tolist()

load_dotenv()

# keep track of comments for all videos from creator
all_comments = []
iter_videos = 0 # keep track of videos

print("N. videos:", len(video_ids))


for video_id in video_ids:

    # if json file already exists, open
    if Path(f"./data/search/baseline/baseline_comments.json").exists():
        with open(f"./data/search/baseline/baseline_comments.json", "r") as f:
            all_comments = json.load(f) # in this way, we will overwrite the json file if we run the script again
        
        # if video already in json, skip
        if any(d["video_id"] == video_id for d in all_comments):
            iter_videos += 1
            
            if iter_videos % 10 == 0:
                print("N. videos:", iter_videos ,"/", len(video_ids))

            continue
    
    count_comments = len(all_comments)

    url: str = "https://open.tiktokapis.com/v2/research/video/comment/list/?fields=id,like_count,create_time,text,video_id,parent_comment_id"

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
    
    print(comments)

    for idx, comment in enumerate(comments):
        comments[idx]["create_time"] = datetime.datetime.utcfromtimestamp(
            comment["create_time"]  # type: ignore
        ).strftime("%Y-%m-%d")
    
    all_comments.extend(comments)

    iter_videos += 1

    if iter_videos % 10 == 0:
        print("N. videos:", iter_videos ,"/", len(video_ids))

    # each video, append comments to json
    save_json(Path(f"./data/search/baseline/baseline_comments.json"), all_comments)

