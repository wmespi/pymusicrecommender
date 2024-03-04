import pandas as pd

from bertopic import BERTopic
from bertopic.representation import KeyBERTInspired, PartOfSpeech, MaximalMarginalRelevance
from sklearn.feature_extraction.text import CountVectorizer
from sklearn.datasets import fetch_20newsgroups

# Get model for training BERTopic
train_docs = fetch_20newsgroups(subset='all',  remove=('headers', 'footers', 'quotes'))['data']
topic_model = BERTopic(n_gram_range=(2, 3))
topics, probs = topic_model.fit_transform(train_docs)

# Input data
track_pdf = pd.read_csv('output/song_data.csv')

# Get lyrics
docs = list(track_pdf.lyrics.values)

topics, ini_probs = topic_model.transform(docs)
print(topics)
print(ini_probs)
print(topic_model.get_topic())