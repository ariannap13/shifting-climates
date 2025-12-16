import os
import json

hashtags = []
for file in os.listdir("./data/search/"):
    if "climate_change" in file and ".json" in file:
        # open json file
        with open("./data/search/" + file, "r") as f:
            data = json.load(f)
        # get hashtags
        for post in data["data"]["videos"]:
            hashtags.extend(post["hashtag_names"])

# count occurrences and sort
hashtags_count = {}
for hashtag in hashtags:
    if hashtag in hashtags_count:
        hashtags_count[hashtag] += 1
    else:
        hashtags_count[hashtag] = 1

print("Number of unique hashtags: " + str(len(hashtags_count)))
# sort by count
hashtags_count = {k: v for k, v in sorted(hashtags_count.items(), key=lambda item: item[1], reverse=True)}

# if hashtags expansion folder does not exist, create it
if not os.path.exists("./data/hashtags_expansion/"):
    os.makedirs("./data/hashtags_expansion/")
    
# save to csv file
with open("./data/hashtags_expansion/hashtags_climate_change_count.csv", "w") as f:
    for key, value in hashtags_count.items():
        f.write("%s,%s\n" % (key, value))