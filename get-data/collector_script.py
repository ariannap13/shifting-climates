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
import os

def save_json(
    path: Path, container: Union[Iterable[Dict[str, Any]], Dict[str, Any], None]
) -> None:
    """Write dict to path."""

    path.parent.mkdir(parents=True, exist_ok=True)

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

    # date_ranges = []

    # while start_date < end_date:
    #     next_date = start_date + timedelta(days=7)

    #     if next_date > end_date:
    #         next_date = end_date

    #     date_ranges.append(
    #         (
    #             start_date.strftime(date_format).replace("-", ""),
    #             next_date.strftime(date_format).replace("-", ""),
    #         )
    #     )

    #     start_date = next_date + timedelta(days=1)

    start_date = start_date.strftime(date_format).replace("-", "")
    end_date = end_date.strftime(date_format).replace("-", "")

    return start_date, end_date

client = TiktokClient()

keywords = ["climate change","global warming","savetheplanet","climate crisis","greennewdeal","renewable energy","climateaction",
            "gogreen","climatejustice","climatechange","globalwarming","climate_change","savetheworld","climatecrisis","forclimate",
            "savetheearth","stopclimatechange","renewableenergy","climatechangeisreal","saveourplanet", "climate action"]

#keywords = ["ecofriendly", "sustainability", "environment", "climate"]

start = "2022-01-01"
end_date = "2022-12-31"

#username = "natgeo"

#keywords = ["climatechange"]

while datetime.datetime.strptime(start, "%Y-%m-%d") < datetime.datetime.strptime(end_date, "%Y-%m-%d"):

    date_ranges = generate_date_ranges(
        start_date_str=start, total_days=7
    )

    print(date_ranges)

    load_dotenv()

    #for keyword in keywords:

    # if file exists, skip date
    if Path(f"data/search/climatechange22/all_keywords_{start}.json").exists():
        start = datetime.datetime.strptime(start, "%Y-%m-%d") + datetime.timedelta(days=7)

        # start as string
        start = start.strftime("%Y-%m-%d")

        continue

    query: Dict[str, Any] = {
                "query": {
                    "or": [
                        {
                            "operation": "IN",
                            "field_name": "keyword",
                            "field_values": keywords,
                        },
                        {
                            "operation": "IN",
                            "field_name": "hashtag_name",
                            "field_values": keywords,
                        }    
                    ],
                },
                "max_count": 100,
                "is_random": False,
            }

    # query: Dict[str, Any] = {
    #             "query": {
    #                 "and": [
    #                     {
    #                         "operation": "EQ",
    #                         "field_name": "hashtag_name",
    #                         "field_values": ["climateaction"],
    #                     },
    #                     # {
    #                     #     "operation": "IN",
    #                     #     "field_name": "keyword",
    #                     #     "field_values": ["climate change", "climate action"],
    #                     # }    
    #                 ],
    #                 # "and": [                        
    #                 #     {
    #                 #         "operation": "EQ",
    #                 #         "field_name": "username",
    #                 #         "field_values": ["seiumn"],
    #                 #     },   
                    
    #                 # ]
    #             },
    #             "max_count": 100,
    #             "is_random": False,
    #         }

    
    # query: Dict[str, Any] = {
    #             "query": {
    #                 "and": [                        
    #                     {
    #                         "operation": "EQ",
    #                         "field_name": "username",
    #                         "field_values": ["unclimatechange"],
    #                     },   
                    
    #                 ]
    #             },
    #             "max_count": 100,
    #             "is_random": False,
    #         }


    url: str = "https://open.tiktokapis.com/v2/research/video/query/?fields=id,like_count,username,video_description,comment_count,share_count,view_count,hashtag_names,create_time"  # noqa

    videos: List[Dict[str, str]] = list()

    max_size = 200

    query["start_date"] = date_ranges[0]

    query["end_date"] = date_ranges[1]

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

    print(len(videos))

    for idx, video in enumerate(videos):
        videos[idx]["create_time"] = datetime.datetime.utcfromtimestamp(
            video["create_time"]  # type: ignore
        ).strftime("%Y-%m-%d")

    save_json(Path(f"data/search/climatechange22/all_keywords_{start}.json"), videos)

    start = datetime.datetime.strptime(start, "%Y-%m-%d") + datetime.timedelta(days=7)

    # start as string
    start = start.strftime("%Y-%m-%d")
