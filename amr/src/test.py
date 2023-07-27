"""
Aims: To evaluate AMR messaging from Twitter and instagram. To do a content analysis to understand what type of
messages (themes) have been used on social media for AMR from 01 Jan 2017 to 01 July 2023.

pipeline

Step 1. read 11 hashtags files in Json (remove emoji first) and transfer to CSV file

1. AMR
2. Antimicrobial resistance
3. Antibiotics
4. Antimicrobials
5. Antimicrobial stewardship
6. Drug resistant
7. Superbugs
8. Antibiotic resistance
9. Infections
10. Bacterial infections
11. Antibiotic prescribing

Step 2. Selected Captions and Urls and drop usefulness headers

Step 3. data cleaning: create a condition to select useful hashtags, only English

Step 4: run Topic modelling

"""
import os
from pathlib import Path  # pathlib: module, Path: class. Checking if a path exist
from typing import Optional, List, Dict, Tuple  # typing: support for type hint
import pandas as pd
from typing import Union
import json
import re
import demoji
import webbrowser
import string
from itertools import chain  # To merge multiple lists into a single list
from langdetect import detect, LangDetectException
import warnings


def remove_emojis(text):
    """
    :param text: all text in files
    :return: modified text with any emojis removed
    """
    return demoji.replace(text, "")


def remove_emojis_from_json(json_data):
    """
    :param json_data: json file
    :return: modifid json file without emoji
    """
    if isinstance(json_data, dict):  # "isinstance" checks if json_data is an instance of the dict class.
        for key, value in json_data.items():
            if isinstance(value, str):
                json_data[key] = remove_emojis(value)
            elif isinstance(value, dict) or isinstance(value, list):
                remove_emojis_from_json(value)
    elif isinstance(json_data, list):
        for i in range(len(json_data)):
            if isinstance(json_data[i], str):
                json_data[i] = remove_emojis(json_data[i])
            elif isinstance(json_data[i], dict) or isinstance(json_data[i], list):
                remove_emojis_from_json(json_data[i])
    return json_data


def load_json(p: Union[Path, str]) -> pd.DataFrame:
    """
    :param p: json path or containing folder
    :return: df
    """
    if isinstance(p, str):
        p = Path(p)

    if 'json' in p.name:
        with p.open(encoding='utf-8') as file:
            json_data = json.load(file)
            # remove emoji
            json_data_without_emojis = remove_emojis_from_json(json_data)
            return pd.DataFrame(json_data_without_emojis)
    else:
        f = list(p.glob('*.json'))
        if len(f) == 0:
            raise FileNotFoundError(f'no json file under the {p}')
        elif len(f) == 1:
            with f[0].open(encoding='utf-8') as file:
                json_data = json.load(file)
                # remove emojis
                json_data_without_emojis = remove_emojis_from_json(json_data)
                return pd.DataFrame(json_data_without_emojis)
        else:
            raise RuntimeError(f'multiple json files under the {p}')


def extract_captions_urls(posts):
    """
    Extract captions and URLs from a list of posts.
    :param posts: A list of posts, where each post is represented as a dictionary.
                  Each post dictionary should contain information about the post,
                  such as 'caption' for the caption text and 'url' for the post URL.
    :return: - A list of captions extracted from the posts. Each caption is preceded by its corresponding post index.
             - A list of URLs extracted from the posts. Each URL is preceded by its corresponding post index.
    """

    captions = [post.get('caption', '') for post in posts]
    urls = [post.get('url', '') for post in posts]
    id_var = [post.get('id', '') for post in posts]
    return captions, urls, id_var
    # If need to add numbering index, use codes below
    # captions = []
    # urls = []
    # id_var =[]
    # for i, post in enumerate(posts):
    #
    #     if 'caption' in post:
    #         caption = f"{i}. {post['caption']}"
    #         captions.append(caption)
    #     if 'url' in post:
    #         url = f"{i}. {post['url']}"
    #         urls.append(url)
    #     if 'id' in post:
    #         id = f"{i}.{post['id']}"
    #         id_var.append(id)




def dropdata(df: pd.DataFrame,
             column_drop: Optional[List[str]] = None,
             keywords_drop: Optional[List[str]] = None,
             save_path: Optional[Path] = None) -> pd.DataFrame:
    """
    :param df: no emoji df
    :param column_drop: drop useless header
    :param keywords_drop: drop useless hashtags
    :param save_path: save df
    :return: modified df
    """
    df = df.copy()

    if column_drop is not None:
        df = df.drop(columns=column_drop)

    if keywords_drop is not None:
        df = df[df['name'].isin(keywords_drop) == False]

    if save_path is not None:
        df.to_csv(save_path)

    return df


def organised_data(df: pd.DataFrame,
                   save_path: Optional[Path] = None) -> pd.DataFrame:
    df['Caption'] = df['Caption'].apply(lambda x: [str(item) for item in x])
    df['URL'] = df['URL'].apply(lambda x: [str(item) for item in x])
    df['ID'] = df['ID'].apply(lambda x: [str(item) for item in x])
    # Create a new df. each Caption and URL is unique in each cell. But the name ane url keep the same
    new_df = pd.DataFrame({
        'name': df['name'].repeat(df['Caption'].apply(len)),
        'url': df['url'].repeat(df['URL'].apply(len)),
        'Caption': [caption for captions in df['Caption'] for caption in captions],
        'URL': [url for urls in df['URL'] for url in urls],
        'ID': [id for id_var in df['ID'] for id in id_var]
    })

    # reset index
    new_df.reset_index(drop=True, inplace=True)

    if save_path:
        new_df.to_csv(save_path)

    return new_df


if __name__ == '__main__':
    df = load_json('/Users/wei/Job Application 2023/CARA Network/AMR /AMR Instagram data/Antimicrobial resistance')
    # Print the captions and URLs for easy reference
    df['Caption'], df['URL'], df['ID']= zip(*df['latestPosts'].apply(extract_captions_urls))

    column_drop = ['topPostsOnly', 'profilePicUrl', 'postsCount', 'topPosts', 'latestPosts']

    keyword_sets = [["infectionsurinaires", "infectionsofadiffrentkind", "infectionsaypakpunjab",
                     "infectionsofadifferentkindpartll",
                     "infectionsofadifferentkindstep1", "infectionsexuellementtransmissible", "infectionsurinaires",
                     "infectionsband", "infectionssexuellementtransmissibles", "infectionsrespiratoires",
                     "infectionsvaginales", "infectionsportswear", "infectionsofadifferentkindstep",
                     "infectionsrespiratoires", "infectionstore", "infections_urinaires", "infectionsofdifferentkind",
                     "america",
                     "amreading", "captainamerica", "amreli", "americanstaffordshireterrier",
                     "americangirl", "americansalon", "americanbullypocket", "americanbulldog", "americanhistory",
                     "madeinamerica", "copaamerica", "amrezy", "amritsar", "discoversouthamerica", "nativeamerican",
                     "americanpitbull", "makeamericagreatagain", "american", "africanamerican", "proudamerican",
                     "américa", "latinamerica", "amrdiab", "southamerica", "americaneagle", "americanairlines",
                     "americanhorrorstory", "amerika", "americafirst", "americanboy", "americancars",
                     "americanbullies", "americanflag", "americanpitbullterrier", "americalatina", "pastaamericana",
                     "godblessamerica", "capitaoamerica", "amersfoort", "americanstaffordshire", "americasteam",
                     "feriaamericana", "visitsouthamerica", "americanbullyofficial", "americanbullypuppy",
                     "americanbullyxl",
                     "americanbully", "americancar", "amrap", "captainamericacivilwar",
                     "keepamericagreat", "amravati", "antimicrobialresistanceintanzania",
                     "antimicrobialresistanceindonesia",
                     "antimicrobialresistancetanzania",
                     "antimicrobialresistancemalaysia", "antimicrobialresistancemalaysia💊", "antimicrobialresistanceis",
                     "antimicrobialresistanceinfo➡", "antimicrobialresistance✔️", "antimicrobialresistancewhat",
                     "antimicrobialresistance💊💉", "antimicrobialresistance🙏", "antimicrobialresistancecontaintment",
                     "antimicrobialresistance😉", "antimicrobialresistanceisabooboo",
                     "antimicrobialresistancecartoonposter",
                     "antimicrobialresistanceawarness", "antimicrobialresistanceisnotathing",
                     "antimicrobialresistanceisscary",
                     'antibioticsmile', 'antibioticskickingin',
                     'antibioticsftw', 'antimicrobialsponge', 'antimicrobials2018', 'antimicrobialsensitivitytesting',
                     'antimicrobials💉', "antimicrobialstewardshipwaddup",
                     "antimicrobialstewardshiptraining2019",
                     "antimicrobialstewardshiprocks", "antimicrobialstewardchef",
                     "antimicrobialstewardshipworkshop2018",
                     "antimicrobialstewardahipprogram", "antimicrobialstewardshipinsicilia",
                     "antimicrobialstewardship✔", "antimicrobialstewardardship",
                     "antimicrobialstewardshipinpediatrics", "antimicrobialstewardshipdinner",
                     "antimicrobialstewardshipbrasil", "antimicrobialstewardofgondor",
                     "antimicrobialstewardshipprotocol", "antimicrobialstewardshipcertificate",
                     "antimicrobialstewardship🧐", "antimicrobialstewardship🎯",
                     "antimicrobialstewardaship", "antimicrobialstewardshipconference",
                     "antimicrobialstewardshippharmacist", "antimicrobialstewardship🦠",
                     "antimicrobialstewardshiprogram", "antimicrobialstewardshipcourse",
                     "antimicrobialstewardshipprogrammes", "antimicrobialstewardshipsymposium",
                     "antimicrobialstewardship💊", "antimicrobialstewardship2018", "drugresistantbugs",
                     "drugresistantchlamydia",
                     "drugresistantgerms", "drugresistantpathogens",
                     "drugresistantuti", "drugresistantstd", "drugresistantbug", "drugresistanthiv",
                     "drugresistantecoli", "drugresistantward", "drugresistantinsomnia",
                     "drugresistantacinetobacter", "drugresistantcat", "drugresistantnasasusunod",
                     "drugresistantcandidaauriscauris", "drugresistanttbcentre", "drugresistantb",
                     "drugresistantepilepsysucksevenmore", "drugresistantepilespy", "drugresistant_tuberculosis",
                     "drugresistantdepresssion", "drugresistantyak", "drugresistantbacterialinfections",
                     "drugresistantaids", "drugresistantfeline", "drugresistantplants😊", "drugresistanttbguidance",
                     "superbugsindia",
                     "superbugsy", "superbugster", "superbugsisreal", "superbugsbunny",
                     "superbugsunday", "superbugsdepkxd", "superbugslayerspolo", "superbugsboardgame",
                     "superbugs1600", "superbugsarereal", "superbugsen", "superbugs23", "superbugs_india",
                     "superbugsafari", "superbugsbunnyfunkopop", "superbugshakycam",
                     "superbugsize", "superbugs🖋️🔬", "superbugsareassholes", "superbugstotherescue", "superbugsmile",
                     "superbugshatecleanhand", "superbugstrikesagain", "superbugsslayers", "superbugsinspace",
                     "superbugss", "superbugsandyou", "superbugsと言う無料展示", "antibioticresistanceexplained",
                     "antibioticresistancemonth", "antibioticresistance💊💉",
                     "antibioticresistance⚠️", "antibioticresistanceisbad", "antibioticresistanceis4real",
                     "antibioticresistance👈", "antibioticresistanceresearch", "antibioticresistanceawarness",
                     "antibioticresistancetest", "antibioticresistancetesting", "antibioticresistanceinindia",
                     "antibioticresistanceinchildren", "antibioticresistancegenesantibióticos",
                     "antibioticresistanceawareness2021", "antibioticresistanceisanightmare",
                     "antibioticresistancefight", "antibioticresistance💊👊", "antibioticresistanceontherise",
                     "antibioticresistanceofmicrobes", "bacterialinfectionsstink", "bacterialinfectionsuck",
                     "bacterialinfectionsinchildren",
                     "bacterialinfectionsareawesome", "bacterialinfectionsepsis", "bacterialinfectionsaywhat",
                     "bacterialinfectionsaregross", "bacterialinfectionsinbotheyes", "bacterialinfectionsja",
                     "bacterialinfectionsgalore", "bacterialinfections", "bacterialinfectionsofskin",
                     "bacterialinfectionshavenothingonme", "bacterialinfectionsinherstomach",
                     "bacterialinfectionsalmostallgone", "bacterialinfectionsabound",
                     "bacterialinfectionscangetfuckedupthearsebybluewhalesdick", "bacterialinfectionsucks",
                     "bacterialinfectionscauses", "bacterialinfectionsofthe5thdimension", "bacterialinfectionsarecool",
                     "bacterialinfectionsarenotfun", "bacterialinfections😩", "bacterialinfectionsoftheskin",
                     "bacterialinfectionscantholddisdown", "bacterialinfectionsux", "bacterialinfectionspeedrun",
                     "bacterialinfectionsareabitch", "bacterialinfectionse", "bacterialinfectionsquad",
                     "bacterialinfectionsrising", "bacterialinfectionsinhindi", "bacterialinfectionsforthewin",
                     "bacterialinfectionsarenot", "bacterialinfectionsarenojoke",
                     "bacterialinfectionsmacterialinfection", "bacterialinfectionsintheblood😔💉💊",
                     "bacterialinfectionsindogs", "bacterialinfectionsfoundhere", "bacterialinfectionsarethebest",
                     "bacterialinfectionsandsethrogen"]]

    for keywords_drop in keyword_sets:
        save_path = Path( '/Users/wei/Job Application 2023/CARA Network/AMR /AMR Instagram data/Antimicrobial resistance/Antimicrobial resistance 01 Jan 2017 - 01 July 2023_hashtags.csv', index=False)

        droped_df = dropdata(df, column_drop=column_drop, keywords_drop=keywords_drop,save_path=save_path)
        new_df = organised_data(droped_df)


# remove non-English languages
def contains_non_english(text):
    pattern = r'[^\x00-\x7F]'
    contains_non_ascii = bool(re.search(pattern, text))

    try:
        language = detect(text)
    except LangDetectException:
        return contains_non_ascii

    languages = {
        'Spanish': 'es',
        'French': 'fr',
        'Portuguese': 'pt',
        'Italian': 'it',
        'German': 'de',
        'Dutch': 'nl',
        'Swedish': 'sv',
        'Danish': 'da',
        'Norwegian': 'no',
        'Finnish': 'fi',
        'Polish': 'pl',
        'Czech': 'cs',
        'Slovak': 'sk',
        'Slovenian': 'sl',
        'Hungarian': 'hu',
        'Romanian': 'ro',
        'Croatian': 'hr',
        'Serbian': 'sr',
        'Bulgarian': 'bg',
        'Greek': 'el',
        'Turkish': 'tr',
        'Estonian': 'et',
        'Latvian': 'lv',
        'Lithuanian': 'lt'
    }

    is_not_english = language != 'en' and language not in languages.values()
    return contains_non_ascii or is_not_english


# indices_to_drop = new_df[new_df['Caption'].apply(contains_non_english)].index
indices_to_drop = new_df[new_df.apply(lambda row: contains_non_english(row['Caption']), axis=1)].index

new_df.loc[indices_to_drop, ['Caption', 'URL','ID']] = None
new_df.dropna(subset=['Caption', 'URL','ID'], how='all', inplace=True)
# # Merge cells for 'name' and 'url'
# new_df['name'] = new_df['name'].mask(new_df['name'].duplicated(), '')
# new_df['url'] = new_df['url'].mask(new_df['url'].duplicated(), '')

new_df.to_csv( '/Users/wei/Job Application 2023/CARA Network/AMR /AMR Instagram data/Antimicrobial resistance/Antimicrobial resistance 01 Jan 2017 - 01 July 2023_specific hashtags.csv', index=False)
new_df.reset_index(drop=True, inplace=True)

""""Topic modelling"""
import gensim  # the library for Topic modelling
from gensim.models.ldamulticore import LdaMulticore
from gensim import corpora, models
import pyLDAvis.gensim  # LDA visualization library
from nltk.corpus import stopwords, words
from nltk.stem.wordnet import WordNetLemmatizer

"""Step 1: clean the data"""

stop = set(stopwords.words('english'))
"""Examples of stopwords include "the," "a," "an," "in," "on," etc. The stopwords module from the nltk library
provides a list of common stopwords in different languages, and here we are using the ones for English"""
exclude = set(string.punctuation)
lemma = WordNetLemmatizer()
"""For example, the lemma of the words "running," "runs," and "ran" is "run." The WordNetLemmatizer class uses the
WordNet lexical database to perform lemmatization. This helps reduce inflected words to a common base form,
which can be useful for text analysis and processing tasks"""


def clean(text):
    # remove hashtags
    text_without_hashtags = ' '.join([word for word in text.lower().split() if not word.startswith('#')])
    english_vocab = set(words.words())
    words_only_english = [word for word in text.split() if word.lower() in english_vocab]

    # Non-hashtags text processing
    stop_free = [word for word in text_without_hashtags.split() if word not in stop]
    punc_free = [ch for ch in stop_free if ch not in exclude]
    normalized = [lemma.lemmatize(word) for word in punc_free]

    # Combine normalized with words_only_english
    normalized.extend(words_only_english)

    return normalized  # no stopwords, no punc, no hashtags


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
num_topics = 3  # num_topics: The number of topics to be identified by the LDA model
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
# lda_display = pyLDAvis.gensim.prepare(ldamodel, doc_term_matrix, dictionary, sort_topics=False, mds='mmds')
# # save to HTML that can open on web
# pyLDAvis.save_html(lda_display, 'LDA_Visualization.html')
# webbrowser.open('file://' + os.path.realpath('LDA_Visualization.html'))

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
        merged_df = pd.concat([cluster_1_df, cluster_2_df, cluster_3_df])
        merged_df.to_csv(
            "/Users/wei/Job Application 2023/CARA Network/AMR /AMR Instagram data/Antimicrobial resistance/Antimicrobial resistance_Merged Clusters.csv",
            index=False)
        cluster_1_df.to_csv(
            "/Users/wei/Job Application 2023/CARA Network/AMR /AMR Instagram data/Antimicrobial resistance/Antimicrobial resistance_Cluster 1.csv",
            index=False)
        cluster_2_df.to_csv(
            "/Users/wei/Job Application 2023/CARA Network/AMR /AMR Instagram data/Antimicrobial resistance/Antimicrobial resistance_Cluster 2.csv",
            index=False)
        cluster_3_df.to_csv(
            "/Users/wei/Job Application 2023/CARA Network/AMR /AMR Instagram data/Antimicrobial resistance/Antimicrobial resistance_Cluster 3.csv",
            index=False)
