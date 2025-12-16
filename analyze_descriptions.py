import pandas as pd
import numpy as np
import pickle
import json
from sentence_transformers import SentenceTransformer
from sklearn.decomposition import PCA
import matplotlib.pyplot as plt

data_dir_yt = "./youtube/data/"
data_dir_tt = "./tiktok-opinion-dynamics/src/tiktok_opinion_dynamics/data/search/creators/"

creator = "climateadam"

all_data_yt = []
all_data_tt = []
for year in range(2021, 2024):
    # load data
    try:
        with open(data_dir_yt+creator+"_"+str(year)+".pickle", "rb") as token:
            data_yt = pickle.load(token)
        df_yt = pd.DataFrame(data_yt)
        all_data_yt.append(df_yt)
    except:
        print("No YT data for", year)

    try:
        with open(data_dir_tt+"all_keywords_"+creator+"_"+str(year)+"-01-01.json", "r") as token:
            data_tt = json.load(token)
        df_tt = pd.DataFrame(data_tt)
        all_data_tt.append(df_tt)
    except:
        print("No TT data for", year)

# concatenate dataframes
df_yt = pd.concat(all_data_yt)
df_tt = pd.concat(all_data_tt)

# convert to datetime
df_yt["Video Timestamp"] = pd.to_datetime(df_yt["Video Timestamp"])
df_tt["create_time"] = pd.to_datetime(df_tt["create_time"])

model = SentenceTransformer('all-MiniLM-L6-v2') # good performance, fast according to https://www.sbert.net/docs/pretrained_models.html

# sentences we would like to encode
sentences_tt = df_tt["video_description"].tolist()
sentences_yt = df_yt["Video Description"].tolist()

print("Number of sentences TT:", len(sentences_tt))
print("Number of sentences YT:", len(sentences_yt))

# sentences are encoded by calling model.encode()
embeddings_tt = model.encode(sentences_tt, show_progress_bar=True)
embeddings_yt = model.encode(sentences_yt, show_progress_bar=True)

# compute avg. and std. dev. of cosine similarity between sentences within each dataset

# compute cosine similarity between all pairs of sentences
cos_sim_tt = []
cos_sim_yt = []
for i in range(len(embeddings_tt)):
    for j in range(i+1, len(embeddings_tt)):
        cos_sim_tt.append(np.dot(embeddings_tt[i], embeddings_tt[j])/(np.linalg.norm(embeddings_tt[i])*np.linalg.norm(embeddings_tt[j])))
for i in range(len(embeddings_yt)):
    for j in range(i+1, len(embeddings_yt)):
        cos_sim_yt.append(np.dot(embeddings_yt[i], embeddings_yt[j])/(np.linalg.norm(embeddings_yt[i])*np.linalg.norm(embeddings_yt[j])))

# to array
cos_sim_tt = np.array(cos_sim_tt)
cos_sim_yt = np.array(cos_sim_yt)

# compute avg. and std. dev. of cosine similarity
avg_cos_sim_tt = np.mean(cos_sim_tt)
std_cos_sim_tt = np.std(cos_sim_tt)

avg_cos_sim_yt = np.mean(cos_sim_yt)
std_cos_sim_yt = np.std(cos_sim_yt)

print("TT: avg. cosine similarity:", avg_cos_sim_tt, "std. dev.:", std_cos_sim_tt)
print("YT: avg. cosine similarity:", avg_cos_sim_yt, "std. dev.:", std_cos_sim_yt)

# visualize in 2D with PCA

# plot PCA for TT and YT
pca = PCA(n_components=2, random_state=42)
pca.fit(np.vstack((embeddings_tt, embeddings_yt)))

# transform data
pca_embeddings_tt = pca.transform(embeddings_tt)
pca_embeddings_yt = pca.transform(embeddings_yt)

# plot
plt.scatter(pca_embeddings_tt[:,0], pca_embeddings_tt[:,1])
plt.scatter(pca_embeddings_yt[:,0], pca_embeddings_yt[:,1])

plt.legend(["TT", "YT"])
plt.xlabel("PC1")
plt.ylabel("PC2")

# save plot
plt.savefig("./pics/pca_embeddings_"+str(creator)+".png")

