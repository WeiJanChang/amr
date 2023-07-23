"""
Aims: To evaluate AMR messaging from Twitter and instagram. To do a content analysis to understand what type of
messages (themes) have been used on social media for AMR from 01 Jan 2017 to 01 July 2023.

pipeline

Step 1. Transfer 10 hashtags files in Json to CSV file

1. AMR
2. Antimicrobial resistance
3. Antibiotics
4. Antimicrobials
5. Antimicrobial stewardship
6. Drug resistant
7. Superbugs
8. Antibiotic resistance
9. Infections
10. Antibiotic prescribing

Step 2. Selected Captions and Urls and drop usefulness headers

Step 3. data cleaning: create a condition to select useful hashtags

Step 4: Merge Instagram and Twitter data

"""
import os
from pathlib import Path  # pathlib: module, Path: class. Checking if a path exist
from typing import Optional, List, Dict, Tuple  # typing: support for type hint
import pandas as pd
from typing import Union
import json
import re
import emoji


def remove_emojis(text):
    return emoji.demojize(text)


def remove_emojis_from_json(json_data):
    if isinstance(json_data, dict):
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
    load json file
    :param p: json path or containing folder
    :return:
        pd.DataFrame
    """
    if isinstance(p, str):
        p = Path(p)

    if 'json' in p.name:
        with p.open(encoding='utf-8') as file:
            json_data = json.load(file)
            # 刪除表情符號和圖案
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


# Extract captions and URLs
def extract_captions(posts):
    captions = []
    urls = []
    for i, post in enumerate(posts, start=1):

        if 'caption' in post:
            caption = f"{i}. {post['caption']}"
            captions.append(caption)  # don't need to check for duplicate caption
        if 'url' in post:
            url = f"{i}. {post['url']}"
            urls.append(url)
            # if url not in urls:  # Check for duplicate URLs
            #     urls.append(url)

    return captions, urls


def cleandata(df: pd.DataFrame,
              column_drop: Optional[List[str]] = None,

              keywords_drop: Optional[List[str]] = None,
              save_path: Optional[Path] = None) -> pd.DataFrame:
    df = df.copy()

    if column_drop is not None:
        df = df.drop(columns=column_drop)

    if keywords_drop is not None:
        df = df[df['name'].isin(keywords_drop) == False]
    if save_path is not None:
        df.to_excel(save_path)

    return df


def organised_data(df: pd.DataFrame,
                   save_path: Optional[Path] = None) -> pd.DataFrame:
    df['Caption'] = df['Caption'].apply(lambda x: [str(item) for item in x])
    df['URL'] = df['URL'].apply(lambda x: [str(item) for item in x])

    # Create a new df. each Caption and URL is unique in each cell. But the name ane url keep the same
    new_df = pd.DataFrame({
        'name': df['name'].repeat(df['Caption'].apply(len)),
        'url': df['url'].repeat(df['URL'].apply(len)),
        'Caption': [caption for captions in df['Caption'] for caption in captions],
        'URL': [url for urls in df['URL'] for url in urls]
    })

    # Merge cells for 'name' and 'url'
    new_df['name'] = new_df['name'].mask(new_df['name'].duplicated(), '')
    new_df['url'] = new_df['url'].mask(new_df['url'].duplicated(), '')

    # reset index
    new_df.reset_index(drop=True, inplace=True)

    if save_path:
        new_df.to_csv(save_path)

    return new_df


if __name__ == '__main__':
    df = load_json('/Users/wei/Job Application 2023/CARA Network/AMR /AMR Instagram data/Bacterial infections')
    # Print the captions and URLs for easy reference
    df['Caption'], df['URL'] = zip(*df['latestPosts'].apply(extract_captions))

    column_drop = ['id', 'topPostsOnly', 'profilePicUrl', 'postsCount', 'topPosts', 'latestPosts']

    keyword_sets = [
        ["infectionsurinaires", "infectionsofadiffrentkind", "infectionsaypakpunjab",
         "infectionsofadifferentkindpartll",
         "infectionsofadifferentkindstep1", "infectionsexuellementtransmissible", "infectionsurinaires",
         "infectionsband", "infectionssexuellementtransmissibles", "infectionsrespiratoires",
         "infectionsvaginales", "infectionsportswear", "infectionsofadifferentkindstep",
         "infectionsrespiratoires", "infectionstore", "infections_urinaires", "infectionsofdifferentkind"],
        ["america", "amreading", "captainamerica", "amreli", "americanstaffordshireterrier",
         "americangirl", "americansalon", "americanbullypocket", "americanbulldog", "americanhistory",
         "madeinamerica", "copaamerica", "amrezy", "amritsar", "discoversouthamerica", "nativeamerican",
         "americanpitbull", "makeamericagreatagain", "american", "africanamerican", "proudamerican",
         "américa", "latinamerica", "amrdiab", "southamerica", "americaneagle", "americanairlines",
         "americanhorrorstory", "amerika", "americafirst", "americanboy", "americancars",
         "americanbullies", "americanflag", "americanpitbullterrier", "americalatina", "pastaamericana",
         "godblessamerica", "capitaoamerica", "amersfoort", "americanstaffordshire", "americasteam",
         "feriaamericana", "visitsouthamerica", "americanbullyofficial", "americanbullypuppy", "americanbullyxl",
         "americanbully", "americancar", "amrap", "captainamericacivilwar",
         "keepamericagreat", "amravati"],
        ["antimicrobialresistanceintanzania", "antimicrobialresistanceindonesia", "antimicrobialresistancetanzania",
         "antimicrobialresistancemalaysia", "antimicrobialresistancemalaysia💊", "antimicrobialresistanceis",
         "antimicrobialresistanceinfo➡", "antimicrobialresistance✔️", "antimicrobialresistancewhat",
         "antimicrobialresistance💊💉", "antimicrobialresistance🙏", "antimicrobialresistancecontaintment",
         "antimicrobialresistance😉", "antimicrobialresistanceisabooboo", "antimicrobialresistancecartoonposter",
         "antimicrobialresistanceawarness", "antimicrobialresistanceisnotathing", "antimicrobialresistanceisscary"],
        ['antibioticsmile', 'antibioticskickingin', 'antibioticsftw'],
        ['antimicrobialsponge', 'antimicrobials2018', 'antimicrobialsensitivitytesting', 'antimicrobials💉'],
        ["antimicrobialstewardshipwaddup", "antimicrobialstewardshiptraining2019",
         "antimicrobialstewardshiprocks", "antimicrobialstewardchef", "antimicrobialstewardshipworkshop2018",
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
         "antimicrobialstewardship💊", "antimicrobialstewardship2018"],
        ["drugresistantbugs", "drugresistantchlamydia", "drugresistantgerms", "drugresistantpathogens",
         "drugresistantuti", "drugresistantstd", "drugresistantbug", "drugresistanthiv",
         "drugresistantecoli", "drugresistantward", "drugresistantinsomnia",
         "drugresistantacinetobacter", "drugresistantcat", "drugresistantnasasusunod",
         "drugresistantcandidaauriscauris", "drugresistanttbcentre", "drugresistantb",
         "drugresistantepilepsysucksevenmore", "drugresistantepilespy", "drugresistant_tuberculosis",
         "drugresistantdepresssion", "drugresistantyak", "drugresistantbacterialinfections",
         "drugresistantaids", "drugresistantfeline", "drugresistantplants😊", "drugresistanttbguidance"],
        ["superbugsindia", "superbugsy", "superbugster", "superbugsisreal", "superbugsbunny",
         "superbugsunday", "superbugsdepkxd", "superbugslayerspolo", "superbugsboardgame",
         "superbugs1600", "superbugsarereal", "superbugsen", "superbugs23", "superbugs_india",
         "superbugsafari", "superbugsbunnyfunkopop", "superbugshakycam",
         "superbugsize", "superbugs🖋️🔬", "superbugsareassholes", "superbugstotherescue", "superbugsmile",
         "superbugshatecleanhand", "superbugstrikesagain", "superbugsslayers", "superbugsinspace",
         "superbugss", "superbugsandyou", "superbugsと言う無料展示"],
        ["antibioticresistanceexplained", "antibioticresistancemonth", "antibioticresistance💊💉",
         "antibioticresistance⚠️", "antibioticresistanceisbad", "antibioticresistanceis4real",
         "antibioticresistance👈", "antibioticresistanceresearch", "antibioticresistanceawarness",
         "antibioticresistancetest", "antibioticresistancetesting", "antibioticresistanceinindia",
         "antibioticresistanceinchildren", "antibioticresistancegenesantibióticos",
         "antibioticresistanceawareness2021", "antibioticresistanceisanightmare",
         "antibioticresistancefight", "antibioticresistance💊👊", "antibioticresistanceontherise",
         "antibioticresistanceofmicrobes"],
        ["bacterialinfectionsstink", "bacterialinfectionsuck", "bacterialinfectionsinchildren",
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

        cleaned_df = cleandata(df, column_drop=column_drop,
                               keywords_drop=keywords_drop)
    save_path = Path(
        '/Users/wei/Job Application 2023/CARA Network/AMR /AMR Instagram data/Bacterial infections/Bacterial infections 01 Jan 2017 - 01 July 2023_hashtags.csv')
    new_df = organised_data(cleaned_df, save_path=save_path)


    # # remove non-English languages
    def contains_non_english(text):
        pattern = r'[^\x00-\x7F]+'
        return bool(re.search(pattern, text))


    indices_to_drop = new_df[new_df['Caption'].apply(contains_non_english)].index

    new_df.loc[indices_to_drop, ['Caption', 'URL']] = None

    new_df.to_csv(
        '/Users/wei/Job Application 2023/CARA Network/AMR /AMR Instagram data/Bacterial infections/Bacterial infections 01 Jan 2017 - 01 July 2023_specific hashtags.csv')

print("Data successfully processed and saved to modified_test.csv.")
#
# # Topic modelling
# import gensim  # the library for Topic modelling
# from gensim.models.ldamulticore import LdaMulticore
# from gensim import corpora, models
# import pyLDAvis.gensim  # LDA visualization library
# import nltk  # Natural Language Toolkit
# from lda import lda
# from IPython.display import HTML
# from nltk.corpus import stopwords, words
#
# import string
# from nltk.stem.wordnet import WordNetLemmatizer
# import warnings
# import webbrowser
#
# # warnings.simplefilter('ignore')
# from itertools import chain  # To merge multiple lists into a single list
#
# # Step 1: clean the data
#
# stop = set(stopwords.words('english'))  # Examples of stopwords include "the," "a," "an," "in," "on," etc. The
# # stopwords module from the nltk library provides a list of common stopwords in different languages, and here we are
# # using the ones for English
#
# exclude = set(string.punctuation)
# lemma = WordNetLemmatizer()  # For example, the lemma of the words "running," "runs," and "ran" is "run." The
#
#
# # WordNetLemmatizer class uses the WordNet lexical database to perform lemmatization. This helps reduce inflected
# # words to a common base form, which can be useful for text analysis and processing tasks
#
#
# def remove_emoji(text):
#     text_without_emoji = re.sub(r'[^\w\s,]', '', text)
#     return text_without_emoji
#
#
# def clean(text):
#     # remove hashtags
#     text_without_hashtags = ' '.join([word for word in text.lower().split() if not word.startswith('#')])
#
#     # remove emoji
#     text_without_emoji = remove_emoji(text_without_hashtags)
#     english_vocab = set(words.words())
#     words_only_english = [word for word in text.split() if word.lower() in english_vocab]
#
#     # Non-hashtags and emoji text processing
#     stop_free = [word for word in text_without_emoji.split() if word not in stop]
#     punc_free = [ch for ch in stop_free if ch not in exclude]
#     normalized = [lemma.lemmatize(word) for word in punc_free]
#     # Combine normalized with words_only_english
#     normalized.extend(words_only_english)
#     return normalized
#
#
# new_df['Caption_clean'] = new_df['Caption'].apply(clean)
#
# # Step 2: Create Dictionary from the articles
# dictionary = corpora.Dictionary(new_df['Caption_clean'])
#
# # Step 3: Create document term matric
# doc_term_matrix = [dictionary.doc2bow(doc) for doc in new_df['Caption_clean']]  # The doc2bow function from the
# # dictionary object is used to convert each preprocessed document (in the form of a list of words) into a
# # bag-of-words representation. It returns a list of tuples, where each tuple contains the word's ID and its frequency
# # in the document. The resulting doc_term_matrix is a list of such tuples for each document.
#
# # print(dictionary.num_nnz)
# # print(len(doc_term_matrix))
#
# # Step 4: Instantiate LDA model
# lda = gensim.models.ldamodel.LdaModel
#
# # Step 5: print the topics identified by LDA model
# # can't overlapping the circle (see on the web)--> If overlapped--> not a good model fit --> shorter the num_topics
# num_topics = 3
# ldamodel = lda(doc_term_matrix, num_topics=num_topics, id2word=dictionary, passes=50, minimum_probability=0)
# # doc_term_matrix: The document-term matrix created in Step 3.
# # num_topics: The number of topics to be identified by the LDA model (in this case, set to 3).
# # id2word: The dictionary created in Step 2, which maps word IDs to words.
# # passes: The number of passes through the entire corpus during training.
# # minimum_probability: The minimum probability value required for a word to be considered in a topic
# # (set to 0, meaning all words are included).
# print(ldamodel.print_topics(num_topics=num_topics))
#
# # Step 6:Visualize the LDA model results
# lda_display = pyLDAvis.gensim.prepare(ldamodel, doc_term_matrix, dictionary, sort_topics=False, mds='mmds')
# # save to HTML that can open on web
# pyLDAvis.save_html(lda_display, 'LDA_Visualization.html')
#
# webbrowser.open('file://' + os.path.realpath('LDA_Visualization.html'))
#
# # Step 7: Find which articles were marked in which cluster
# # Assigns the topics to the documents in corpus
# lda_corpus = ldamodel[doc_term_matrix]
# print([doc for doc in lda_corpus])
# scores = list(chain(*[[score for topic_id, score in topic] \
#                       for topic in [doc for doc in lda_corpus]]))
# threshold = sum(scores) / len(scores)
# print(threshold)
#
# cluster1 = [j for i, j in zip(lda_corpus, df.index) if i[0][1] > threshold]
# cluster2 = [j for i, j in zip(lda_corpus, df.index) if i[0][1] > threshold]
# cluster3 = [j for i, j in zip(lda_corpus, df.index) if i[0][1] > threshold]
# print(len(cluster1))
# print(len(cluster2))
# print(len(cluster3))
#
# print(new_df.iloc[cluster1])
# print(new_df.iloc[cluster2])
# clusterdf = new_df.iloc[cluster3]
# clusterdf.to_csv(
#     '/Users/wei/Job Application 2023/CARA Network/AMR /AMR Instagram data/Antibiotics/Antibiotics 01 Jan 2017 - 01 July 2023_topic texts cluster test.csv',
#     index=False)
#
# new_df.to_csv(
#     '/Users/wei/Job Application 2023/CARA Network/AMR /AMR Instagram data/Antibiotics/Antibiotics 01 Jan 2017 - 01 July 2023_topic texts.csv',
#     index=False)
