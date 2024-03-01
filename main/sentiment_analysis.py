import pandas as pd

from bertopic import BERTopic
from bertopic.representation import KeyBERTInspired, PartOfSpeech, MaximalMarginalRelevance
from sklearn.feature_extraction.text import CountVectorizer

# Input data
track_pdf = pd.read_csv('output/song_data.csv')

# Get lyrics
docs = list(track_pdf.lyrics.values)

main_representation_model = KeyBERTInspired()
aspect_representation_model1 = [KeyBERTInspired(top_n_words=30), 
                                MaximalMarginalRelevance(diversity=.5)]

representation_model = {
   "Main": main_representation_model,
   "Aspect1":  aspect_representation_model1
}

vectorizer_model = CountVectorizer(min_df=5, stop_words = 'english')
topic_model = BERTopic(
    nr_topics = 'auto', 
    vectorizer_model = vectorizer_model,
    representation_model = representation_model
)
 
topics, ini_probs = topic_model.fit_transform(docs)
print(topics)
print(ini_probs)