from tiktok_research_client.data_collection.collect import TiktokClient
import tiktok_research_client.utils as utils
from pathlib import Path

client = TiktokClient()

year = 2016

keyword = "climate change"
max_count = 100
# start_date = "20230101"
# end_date = "20230130"
random_bool = True

for month in range(1, 13):

    if month == 2:
        start_date = str(year) + "0" + str(month) + "01"
        end_date = str(year) + "0" + str(month) + "28"
    elif month > 9:
        start_date = str(year) + str(month) + "01"
        end_date = str(year) + str(month) + "30"
    else:
        start_date = str(year) + "0" + str(month) + "01"
        end_date = str(year) + "0" + str(month) + "30"

    print(start_date)

    query = {
        "query": {
            "and": [
                # {
                #     "operation": "IN",
                #     "field_name": "region_code",
                #     "field_values": ["US"],
                # },
                {
                    "operation": "EQ",
                    "field_name": "keyword",
                    "field_values": [keyword],
                },
            ],
            # "not": [
            #     {"operation": "EQ", "field_name": "video_length", "field_values": ["SHORT"]}
            # ],
        },
        "max_count": max_count,
        "start_date": start_date,
        "end_date": end_date,
        "is_random": random_bool
    }

    url = "https://open.tiktokapis.com/v2/research/video/query/?fields=id,region_code,like_count,username,video_description,music_id,comment_count,share_count,view_count,effect_ids,hashtag_names,playlist_id,voice_to_text,create_time"

    data = client.query(query=query, url=url)

    # save data to csv in data/search as json
    utils.save_json(
        path=Path("data/search/" + keyword.replace(" ", "_") + "_" +start_date+ ".json"),
        container=data,
    )



