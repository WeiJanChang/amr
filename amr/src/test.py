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


# Load json file
def load_json(p: Union[Path, str]) -> pd.DataFrame:
    """
    load json file
    :param p: json path or containing folder
    :return:
        pd.DataFrame
    """
    if isinstance(p, str):  # if the variable p is an instance of the str class
        p = Path(p)  # if yes, creates a new object of 'Path' class and assigns it to the variable 'p'

    if 'json' in p.name:
        return pd.read_json(p, encoding='utf-8')
        # If the json in the p.name--> read the file. If the string "json" is not in the "name" attribute, this block
        # of code will not execute and the function will return nothing or continue with the next step of code.
    else:
        f = list(
            p.glob('*.json'))  # To check if there is any json file present in the path p or not by using glob method
        # and return a list of all the json files stored in 'p'
        if len(f) == 0:
            raise FileNotFoundError(f'no json file under the {p}')
        elif len(f) == 1:
            return pd.read_json(f[0], encoding='utf-8')
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
    df = load_json('/Users/wei/Job Application 2023/CARA Network/AMR /AMR Instagram data/Antibiotics')
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
        cleaned_df = cleandata(df, column_drop=column_drop, keywords_drop=keywords_drop)
#        save_path = Path(
#            '/Users/wei/Job Application 2023/CARA Network/AMR /AMR Instagram data/Bacterial infections/Bacterial infections 01 Jan 2017 - 01 July 2023_specific hashtags.csv')
        new_df = organised_data(cleaned_df)

    print("Data successfully processed and saved to modified_test.csv.")

# Topic modelling
import gensim  # the library for Topic modelling
from gensim.models.ldamulticore import LdaMulticore
from gensim import corpora, models
import pyLDAvis.gensim  # LDA visualization library
import nltk
from lda import lda
from IPython.display import HTML
from nltk.corpus import stopwords
import string
from nltk.stem.wordnet import WordNetLemmatizer
import warnings

warnings.simplefilter('ignore')
from itertools import chain

# Step 1: clean the data

stop = set(stopwords.words('english'))
exclude = set(string.punctuation)
lemma = WordNetLemmatizer()
import re


def remove_emoji(text):
    emoji_pattern = re.compile("["
                               u"\U0001F600-\U0001F64F"  # Emoticons
                               u"\U0001F300-\U0001F5FF"  # Graphical and Decorative Symbols
                               u"\U0001F680-\U0001F6FF"  # Traffic and Map Symbols
                               u"\U0001F1E0-\U0001F1FF"  # National Flags
                               u"\U00002702-\U000027B0"  # Decorative Symbols
                               u"\U000024C2-\U0001F251"  # More Decorative Symbols
                               "]+", flags=re.UNICODE)

    return emoji_pattern.sub(r'', text)


def clean(text):
    # remove hashtags
    text_without_hashtags = ' '.join([word for word in text.lower().split() if not word.startswith('#')])

    # remove emoji
    text_without_emoji = remove_emoji(text_without_hashtags)

    # Non-hashtags and emoji text processing
    stop_free = [word for word in text_without_emoji.split() if word not in stop]
    punc_free = [ch for ch in stop_free if ch not in exclude]
    normalized = [lemma.lemmatize(word) for word in punc_free]

    return normalized


new_df['Caption_clean'] = new_df['Caption'].apply(clean)

# Step 2: Create Dictionary from the articles
dictionary = corpora.Dictionary(new_df['Caption_clean'])

# Step 3: Create document term matric
doc_term_matrix = [dictionary.doc2bow(doc) for doc in new_df['Caption_clean']]

# print(dictionary.num_nnz)
# print(len(doc_term_matrix))

# Step 4: Instantiate LDA model
lda = gensim.models.ldamodel.LdaModel

# Step 5: print the topics identified by LDA model

num_topics = 3
ldamodel = lda(doc_term_matrix, num_topics=num_topics, id2word=dictionary, passes=50, minimum_probability=0)
print(ldamodel.print_topics(num_topics=num_topics))

# Step 6:Visualize the LDA model results
lda_display = pyLDAvis.gensim.prepare(ldamodel, doc_term_matrix, dictionary, sort_topics=False, mds='mmds')
# save to HTML that can open on web
pyLDAvis.save_html(lda_display, 'LDA_Visualization.html')
import webbrowser

webbrowser.open('file://' + os.path.realpath('LDA_Visualization.html')) # can't overlapping the circle--> not a good model fit --> shorter the num_topics

new_df.to_csv(
    '/Users/wei/Job Application 2023/CARA Network/AMR /AMR Instagram data/Antibiotics/Antibiotics 01 Jan 2017 - 01 July 2023_topic texts.csv',
    index=False)
