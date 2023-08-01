"""
Pipeline

Step 1: run Topic modelling

Step 2: Content analysis, Network analysis, Buzz graph??
"""
import os
import webbrowser
from matplotlib import pyplot as plt
import string
import gensim  # the library for Topic modelling
import pandas as pd
from gensim.models.ldamulticore import LdaMulticore
from gensim import corpora, models
import pyLDAvis.gensim  # LDA visualization library
from nltk.corpus import stopwords, words
from nltk.stem.wordnet import WordNetLemmatizer
from itertools import chain  # To merge multiple lists into a single list

df1 = pd.read_csv(
    '/Users/wei/Job Application 2023/CARA Network/AMR /AMR Instagram data/Antimicrobial resistance/Antimicrobial resistance 01 Jan 2017 - 01 July 2023_specific hashtags.csv')
df2 = pd.read_csv(
    '/Users/wei/Job Application 2023/CARA Network/AMR /AMR Instagram data/Antibiotic prescribing/Antibiotic prescribing 01 Jan 2017 - 01 July 2023_specific hashtags.csv')
df3 = pd.read_csv(
    '/Users/wei/Job Application 2023/CARA Network/AMR /AMR Instagram data/Antimicrobial stewardship/Antimicrobial stewardship 01 Jan 2017 - 01 July 2023_specific hashtags.csv')
df4 = pd.read_csv(
    '/Users/wei/Job Application 2023/CARA Network/AMR /AMR Instagram data/Antibiotics/Antibiotics 01 Jan 2017 - 01 July 2023_specific hashtags.csv')
df5 = pd.read_csv(
    '/Users/wei/Job Application 2023/CARA Network/AMR /AMR Instagram data/Antimicrobials/Antimicrobials 01 Jan 2017 - 01 July 2023_specific hashtags.csv')
df6 = pd.read_csv(
    '/Users/wei/Job Application 2023/CARA Network/AMR /AMR Instagram data/Bacterial infections/Bacterial infections 01 Jan 2017 - 01 July 2023_specific hashtags.csv')
df7 = pd.read_csv(
    '/Users/wei/Job Application 2023/CARA Network/AMR /AMR Instagram data/AMR/AMR 01 Jan 2017 - 01 July 2023_specific hashtags.csv')
df8 = pd.read_csv(
    '/Users/wei/Job Application 2023/CARA Network/AMR /AMR Instagram data/Infections/Infections 01 Jan 2017 - 01 July 2023_specific hashtags.csv')
df9 = pd.read_csv(
    '/Users/wei/Job Application 2023/CARA Network/AMR /AMR Instagram data/Superbugs/Superbugs 01 Jan 2017 - 01 July 2023_specific hashtags.csv')
df10 = pd.read_csv(
    '/Users/wei/Job Application 2023/CARA Network/AMR /AMR Instagram data/Antibiotic resistance/Antibiotic resistance 01 Jan 2017 - 01 July 2023_specific hashtags.csv')
df11 = pd.read_csv(
    '/Users/wei/Job Application 2023/CARA Network/AMR /AMR Instagram data/Drug resistant/Drug resistant 01 Jan 2017 - 01 July 2023_specific hashtags.csv')
merged_df = pd.concat([df1, df2, df3, df4, df5, df6, df7, df8, df9, df10, df11])

merged_df.to_csv('/Users/wei/Job Application 2023/CARA Network/AMR /AMR Instagram data/all_merged_file.csv',
                 index=False)
new_df = merged_df

print(new_df['Caption'].dtype)
print(new_df['Caption'].apply(type).unique())
print(new_df['Caption'].apply(type).value_counts())
new_df['Caption'] = new_df['Caption'].astype(str)
new_df['Caption'] = new_df['Caption'].str.lower()
new_df.to_csv('/Users/wei/Job Application 2023/CARA Network/AMR /AMR Instagram data/all_merged_file_new.csv',
                 index=False)


""""Topic modelling"""
# new_df = pd.read_csv(
#     '/Users/wei/Job Application 2023/CARA Network/AMR /AMR Instagram data/Antimicrobial resistance/Antimicrobial '
#     'resistance 01 Jan 2017 - 01 July 2023_specific hashtags.csv')


"""Step 1: clean the data"""

stop = set(stopwords.words('english'))
print("Stop words:", stop)
# 自訂你的停用詞，可以添加或刪除詞語
custom_stop_words = {'your', 'I', 'for','and','the','to','or','in','of','my'}

# 合併原本的停用詞列表和自訂的停用詞
all_stop_words = stop.union(custom_stop_words)
"""Examples of stopwords include "the," "a," "an," "in," "on," etc. The stopwords module from the nltk library
provides a list of common stopwords in different languages, and here we are using the ones for English"""
exclude = set(string.punctuation)
lemma = WordNetLemmatizer()
"""For example, the lemma of the words "running," "runs," and "ran" is "run." The WordNetLemmatizer class uses the
WordNet lexical database to perform lemmatization. This helps reduce inflected words to a common base form,
which can be useful for text analysis and processing tasks"""


def clean(text):
    try:
        if text is None:
            raise ValueError("Input 'text' is None.")
        # remove hashtags
        text_without_hashtags = ' '.join([word for word in text.lower().split() if not word.startswith('#')])
        english_vocab = set(words.words())
        words_only_english = [word for word in text.split() if word.lower() in english_vocab]

        # Non-hashtags text processing2
        stop_free = [word for word in text_without_hashtags.split() if word not in all_stop_words]
        punc_free = [ch for ch in stop_free if ch not in exclude]
        normalized = [lemma.lemmatize(word) for word in punc_free]
        # Combine normalized with words_only_english
        normalized.extend(words_only_english)

        return normalized  # no stopwords, no punc, no hashtags

    except Exception as e:
        print("Error occurred while processing text:", text)
        print("Error message:", str(e))
        return []  # return an empty list if an error occurs


# try:
#     new_df['Caption_cleaned'] = new_df['Caption'].apply(clean)
# except Exception as e:
#     print("Error occurred during 'apply' operation.")
#     print("Error message:", str(e))

new_df['Caption_cleaned'] = new_df['Caption'].apply(clean)

"""Step 2: Create a dictionary from new_df['Caption_cleaned']"""
dictionary = corpora.Dictionary(new_df['Caption_cleaned'])
# print(dictionary) --> 1925 unique words

"""Step 3: Create document term matrix"""
doc_term_matrix = [dictionary.doc2bow(doc) for doc in new_df['Caption_cleaned']]
# print(doc_term_matrix) --> calculate a word  show how many times
# The doc2bow function: Convert each text in new_df['Caption_clean'] to document-term representation.
# print(dictionary.num_nnz) --> non-repeated words
# print(len(doc_term_matrix)) --> a total words

"""Step 4: Instantiate LDA model"""
lda = gensim.models.ldamodel.LdaModel
"""This algorithm assumes that each document in the text is composed of different proportions of topics,
and each topic is composed of different proportions of words. LDA finds these latent topics and their word
combinations through an iterative process"""

"""Step 5: print the topics identified by LDA model"""
# can't overlapping the circle (see on the web)--> If overlapped--> not a good model fit --> shorter the num_topics
num_topics = 8  # num_topics: The number of topics to be identified by the LDA model
ldamodel = lda(doc_term_matrix, num_topics=num_topics, id2word=dictionary, passes=50, minimum_probability=0,
               random_state=50)
"""id2word: The dictionary created in Step 2, which maps word IDs to words.
passes: the number of times the algorithm goes through all the documents in the dataset during the training process.
Each pass allows the model to learn and update its understanding of the data, potentially improving the quality of the
identified topics.
minimum_probability:  The minimum probability value required for a word to be considered in a topic.
In this case, it's set to 0, meaning all words will be included in the topics regardless of
their probability. If set to a higher value (e.g., 0.01), the model will only include words with a probability
greater than or equal to the specified value.
random_state: """
# print(ldamodel.print_topics(num_topics=num_topics))

"""Step 6:Visualize the LDA model results"""
# warnings.simplefilter(action='ignore', category=FutureWarning)
lda_display = pyLDAvis.gensim.prepare(ldamodel, doc_term_matrix, dictionary, sort_topics=False, mds='mmds')
# save to HTML that can open on web
pyLDAvis.save_html(lda_display, 'LDA_Visualization.html')
webbrowser.open('file://' + os.path.realpath('LDA_Visualization.html'))

"""Step 7: Find which articles were marked in which cluster"""
# Assigns the topics to the documents in corpus
topic_distribution = ldamodel[doc_term_matrix]  # contains the topic distribution for each document.
# print([doc for doc in topic_distribution]) --> This result shows the topic distribution for each document. Each
# document is represented by a list, where each element in the list represents a topic along with its corresponding
# probability.
scores = list(chain(*[[score for topic_id, score in topic] for topic in [doc for doc in topic_distribution]]))
# This line extracts the probability scores for all topics for each document and stores them in the scores list
threshold = sum(scores) / len(scores)
"""The threshold is calculated as the average of all probability scores. It's used as a threshold to determine which
articles belong to which cluster. Articles with a probability score greater than this threshold are considered to
belong to a cluster"""
# print(threshold)
"""After computing the threshold, you can use it to filter out topics that have probability scores below this
threshold. Topics with probabilities lower than the threshold are considered less significant or relevant,
and you may choose to exclude them from further analysis or visualization. This threshold can help you focus on the
most important and representative topics in your topic modeling results."""

"""
Each Threshold
1. AMR: 0.8284
2. Antimicrobial resistance: 0.7608
3. Antibiotics: 0.7205
4. Antimicrobials: 0.7420
5. Antimicrobial stewardship: 0.8535
6. Drug resistant: 0.8302
7. Superbugs: 0.8308
8. Antibiotic resistance: 0.7203
9. Infections: 0.6630
10. Bacterial infections:  0.9411
11. Antibiotic prescribing: 0.7825
"""

# Create a list to store the document IDs for each cluster
clusters = [[] for _ in range(num_topics)]

# Assign each document to its corresponding cluster based on the dominant topic
for doc_index, doc_topics in enumerate(topic_distribution):
    if doc_topics:  # Check if doc_topics is not empty
        dominant_topic = max(doc_topics, key=lambda x: x[1])  # Find the dominant topic and its probability
        if dominant_topic[1] > threshold:  # Check if the probability of the dominant topic is above the threshold
            cluster_index = dominant_topic[0]  # Get the index of the dominant topic
            clusters[cluster_index].append(doc_index)  # Add the document index to the corresponding cluster
        cluster_1_df = new_df.loc[clusters[0]]
        cluster_2_df = new_df.loc[clusters[1]]
        cluster_3_df = new_df.loc[clusters[2]]
        cluster_4_df = new_df.loc[clusters[3]]
        cluster_5_df = new_df.loc[clusters[4]]
        cluster_6_df = new_df.loc[clusters[5]]
        cluster_7_df = new_df.loc[clusters[6]]
        cluster_8_df = new_df.loc[clusters[7]]

        all_clusters_df = pd.concat([cluster_1_df, cluster_2_df, cluster_3_df, cluster_4_df, cluster_5_df,
                                     cluster_6_df, cluster_7_df, cluster_8_df])
        all_clusters_df.to_csv(
            "/Users/wei/Job Application 2023/CARA Network/AMR /AMR Instagram data/all Clusters.csv",
            index=False)
#         cluster_1_df.to_csv(
#             "/Users/wei/Job Application 2023/CARA Network/AMR /AMR Instagram data/Antimicrobial resistance/Antimicrobial resistance_Cluster 1.csv",
#             index=False)
#         cluster_2_df.to_csv(
#             "/Users/wei/Job Application 2023/CARA Network/AMR /AMR Instagram data/Antimicrobial resistance/Antimicrobial resistance_Cluster 2.csv",
#             index=False)
#         cluster_3_df.to_csv(
#             "/Users/wei/Job Application 2023/CARA Network/AMR /AMR Instagram data/Antimicrobial resistance/Antimicrobial resistance_Cluster 3.csv",
#             index=False)
# #
# """Network analysis"""
# import networkx as nx
#
# # 創建一個空的無向圖形
# G = nx.Graph()
#
# # 將hashtags添加為節點
# hashtags_list = cluster_1_df['Caption'].apply(
#     lambda caption: [tag.lower() for tag in caption.split() if tag.startswith('#')])
# for hashtags in hashtags_list:
#     for tag in hashtags:
#         G.add_node(tag)
#
# # 創建共用用戶的邊
# for post_id, hashtags in zip(cluster_1_df['ID'], hashtags_list):
#     for user in cluster_1_df[cluster_1_df['ID'] == post_id]:
#         for tag in hashtags:
#             if G.has_edge(user, tag):
#                 G[user][tag]['weight'] += 1
#                 G[user][tag]['posts'].append(post_id)
#             else:
#                 G.add_edge(user, tag, weight=1, posts=[post_id])
#
# # 獲取邊的權重作為buzz graph中邊的權重
# edge_weights = nx.get_edge_attributes(G, 'weight')
#
# # 只保留邊權重大於等於一的邊
# filtered_edges = {edge: weight for edge, weight in edge_weights.items() if weight >= 1}
#
# # 創建buzz graph，這裡使用Graph類型，你也可以使用DiGraph類型來創建有向buzz graph
# buzz_graph = nx.Graph()
# buzz_graph.add_edges_from(filtered_edges.keys())
#
# # 你可以通過獲取節點和邊屬性來進一步處理buzz graph
# # 例如，獲取節點度數（即節點的連接數）
# node_degrees = buzz_graph.degree()
#
# # 你也可以使用網絡分析庫的可視化功能來可視化buzz graph
#
#
# # build buzz graph
# pos = nx.spring_layout(buzz_graph, k=0.15, iterations=20)
# nx.draw(buzz_graph, pos, with_labels=True, node_size=100, font_size=8, font_weight='bold', node_color='skyblue',
#         edge_color='gray', width=0.5, alpha=0.8)
# plt.show()
