import datetime
from pathlib import Path
from typing import Any, Dict, List, Union, Iterable, Tuple
import json
#from tiktok_research_client.utils import save_json, generate_date_ranges
from tiktok_research_client.data_collection.collect import TiktokClient
from dotenv import load_dotenv
import sys
import pickle
import pandas as pd
import datetime
from datetime import timedelta
import logging

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

def generate_date_ranges(start_date_str: str, total_days: int) -> List[Tuple[str, str]]:
    """Generate date ranges for TikTok API.

    Args:
        start_date_str (str): Start date in string format.
        total_days (int): Total number of days to collect.

    Returns:
        List[str]: List of date ranges.
    """
    date_format = "%Y-%m-%d"
    start_date = datetime.datetime.strptime(start_date_str, date_format)
    end_date = start_date + timedelta(days=total_days)

    # If the end date is in the future, set it to today
    if end_date > datetime.datetime.now():
        end_date = datetime.datetime.now()

    logging.debug(
        "Generating date ranges from %s to %s (%s days), with 30 days per range (max allowed by TikTok API)",  # noqa
        start_date,
        end_date,
        total_days,
    )

    date_ranges = []

    while start_date < end_date:
        next_date = start_date + timedelta(days=7)

        if next_date > end_date:
            next_date = end_date

        date_ranges.append(
            (
                start_date.strftime(date_format).replace("-", ""),
                next_date.strftime(date_format).replace("-", ""),
            )
        )

        start_date = next_date + timedelta(days=1)

    return date_ranges

keywords = ["climate change","global warming","savetheplanet","climate crisis","greennewdeal","renewable energy","climateaction",
            "gogreen","climatejustice","climatechange","globalwarming","climate_change","savetheworld","climatecrisis","forclimate",
            "savetheearth","stopclimatechange","renewableenergy","climatechangeisreal","saveourplanet"]

client = TiktokClient()

# list_creators = ["bbcnews", 
#                  #"climateadam", 
#                  #"guardian", 
#                  #"tedtoks", 
#                  #"minuteearth", 
#                  #"metoffice",
#                  "greenrupertread", "friends_earth", "parleyfortheoceans"
#                  #"action4climate"
#                  ]

start = "2022-01-01"
end_date = "2022-02-01"

#username = "natgeo"

#keywords = ["climatechange"]

# for creator in list_creators:


date_ranges = generate_date_ranges(
    start_date_str=start, total_days=30
)

print(date_ranges)

load_dotenv()

for keyword in keywords:

# # if file exists, skip date
# if Path(f"./data/search/creators/all_keywords_{creator}_{start}.json").exists():
#     # start = datetime.datetime.strptime(start, "%Y-%m-%d") + datetime.timedelta(days=365)

#     # # start as string
#     # start = start.strftime("%Y-%m-%d")

#     continue

    query: Dict[str, Any] = {
                "query": {
                    "or": [                        
                        {
                            "operation": "EQ",
                            "field_name": "keyword",
                            "field_values": [keyword],
                        },   
                        {
                            "operation": "EQ",
                            "field_name": "hashtag_name",
                            "field_values": [keyword],
                        }
                    
                    ]
                },
                "max_count": 100,
                "is_random": False,
            }

    url: str = "https://open.tiktokapis.com/v2/research/video/query/?fields=id,region_code,like_count,username,video_description,music_id,comment_count,share_count,view_count,effect_ids,hashtag_names,playlist_id,voice_to_text,create_time"  # noqa

    videos: List[Dict[str, str]] = list()

    max_size = 1000

    for date_range in date_ranges:
        query["start_date"] = date_range[0]

        query["end_date"] = date_range[1]

        # Check if we have reached the max size
        if len(videos) >= max_size:
            break

        # Keep querying until there is no more data
        output = client._cursor_iterator(url, query, max_size=max_size, is_random=False)

        if output is not None:
            if len(output) > 0:
                videos.extend(client._cursor_iterator(url, query, max_size=max_size, is_random=False))
            else:
                # end script
                pass
                # sys.exit("Too many requests, try again later.")


    for idx, video in enumerate(videos):
        videos[idx]["create_time"] = datetime.datetime.utcfromtimestamp(
            video["create_time"]  # type: ignore
        ).strftime("%Y-%m-%d")

    # save json
    save_json(
        path=Path(f"./data/search/climatechange22/trial_{keyword}_{start}.json"),
        container=videos,
    )

    print(f"Saved {keyword} to json")

    sys.exit(0)