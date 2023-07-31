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
"""

from pathlib import Path  # pathlib: module, Path: class. Checking if a path exist
from typing import Optional, List, Dict, Tuple  # typing: support for type hint
import pandas as pd
from typing import Union
import json
import re
import demoji
from langdetect import detect, LangDetectException


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
    """Twitter"""
    x_df = pd.read_csv(
        '/Users/wei/Job Application 2023/CARA Network/AMR /AMR Twitter data/ABR/ABR IE 01-17 to 01-23.csv')
    # print(x_df.columns)
    column_drop = ['attachments.poll_ids', 'attachments.media_keys', 'context_annotations', 'entities.annotations',
                   'entities.cashtags',
                   'entities.hashtags', 'entities.mentions', 'entities.urls', 'source', 'lang', 'reply_settings',
                   'withheld.copyright', 'withheld.country_codes', 'non_public_metrics.impression_count',
                   'non_public_metrics.url_link_clicks', 'non_public_metrics.user_profile_clicks',
                   'organic_metrics.impression_count', 'organic_metrics.like_count', 'organic_metrics.reply_count',
                   'organic_metrics.retweet_count', 'organic_metrics.url_link_clicks',
                   'organic_metrics.user_profile_clicks', 'promoted_metrics.impression_count',
                   'promoted_metrics.like_count', 'promoted_metrics.reply_count', 'promoted_metrics.retweet_count',
                   'promoted_metrics.url_link_clicks', 'promoted_metrics.user_profile_clicks', 'location',
                   'pinned_tweet_id', 'profile_image_url', 'protected', 'url', 'entities.url.urls',
                   'entities.description.urls', 'entities.description.hashtags', 'entities.description.mentions',
                   'entities.description.cashtags', 'withheld', 'contained_within', 'geo.properties', 'place_type',
                   'country_code', 'geo.type', 'geo.bbox', 'geo.coordinates.type', 'geo.coordinates.coordinates']

    save_path = '/Users/wei/Job Application 2023/CARA Network/AMR /AMR Twitter data/ABR/ABR_modified IE 01-17 to 01-23.csv'
    dropdata(x_df, column_drop=column_drop, save_path=save_path)

"""Instagram"""
#     df = load_json('/Users/wei/Job Application 2023/CARA Network/AMR /AMR Instagram data/Antimicrobial resistance')
#     # Print the captions and URLs for easy reference
#     df['Caption'], df['URL'], df['ID'] = zip(*df['latestPosts'].apply(extract_captions_urls))
#
#     column_drop = ['topPostsOnly', 'profilePicUrl', 'postsCount', 'topPosts', 'latestPosts']
#
#     keyword_sets = [["infectionsurinaires", "infectionsofadiffrentkind", "infectionsaypakpunjab",
#                      "infectionsofadifferentkindpartll",
#                      "infectionsofadifferentkindstep1", "infectionsexuellementtransmissible", "infectionsurinaires",
#                      "infectionsband", "infectionssexuellementtransmissibles", "infectionsrespiratoires",
#                      "infectionsvaginales", "infectionsportswear", "infectionsofadifferentkindstep",
#                      "infectionsrespiratoires", "infectionstore", "infections_urinaires", "infectionsofdifferentkind",
#                      "america",
#                      "amreading", "captainamerica", "amreli", "americanstaffordshireterrier",
#                      "americangirl", "americansalon", "americanbullypocket", "americanbulldog", "americanhistory",
#                      "madeinamerica", "copaamerica", "amrezy", "amritsar", "discoversouthamerica", "nativeamerican",
#                      "americanpitbull", "makeamericagreatagain", "american", "africanamerican", "proudamerican",
#                      "américa", "latinamerica", "amrdiab", "southamerica", "americaneagle", "americanairlines",
#                      "americanhorrorstory", "amerika", "americafirst", "americanboy", "americancars",
#                      "americanbullies", "americanflag", "americanpitbullterrier", "americalatina", "pastaamericana",
#                      "godblessamerica", "capitaoamerica", "amersfoort", "americanstaffordshire", "americasteam",
#                      "feriaamericana", "visitsouthamerica", "americanbullyofficial", "americanbullypuppy",
#                      "americanbullyxl",
#                      "americanbully", "americancar", "amrap", "captainamericacivilwar",
#                      "keepamericagreat", "amravati", "antimicrobialresistanceintanzania",
#                      "antimicrobialresistanceindonesia",
#                      "antimicrobialresistancetanzania",
#                      "antimicrobialresistancemalaysia", "antimicrobialresistancemalaysia💊", "antimicrobialresistanceis",
#                      "antimicrobialresistanceinfo➡", "antimicrobialresistance✔️", "antimicrobialresistancewhat",
#                      "antimicrobialresistance💊💉", "antimicrobialresistance🙏", "antimicrobialresistancecontaintment",
#                      "antimicrobialresistance😉", "antimicrobialresistanceisabooboo",
#                      "antimicrobialresistancecartoonposter",
#                      "antimicrobialresistanceawarness", "antimicrobialresistanceisnotathing",
#                      "antimicrobialresistanceisscary",
#                      'antibioticsmile', 'antibioticskickingin',
#                      'antibioticsftw', 'antimicrobialsponge', 'antimicrobials2018', 'antimicrobialsensitivitytesting',
#                      'antimicrobials💉', "antimicrobialstewardshipwaddup",
#                      "antimicrobialstewardshiptraining2019",
#                      "antimicrobialstewardshiprocks", "antimicrobialstewardchef",
#                      "antimicrobialstewardshipworkshop2018",
#                      "antimicrobialstewardahipprogram", "antimicrobialstewardshipinsicilia",
#                      "antimicrobialstewardship✔", "antimicrobialstewardardship",
#                      "antimicrobialstewardshipinpediatrics", "antimicrobialstewardshipdinner",
#                      "antimicrobialstewardshipbrasil", "antimicrobialstewardofgondor",
#                      "antimicrobialstewardshipprotocol", "antimicrobialstewardshipcertificate",
#                      "antimicrobialstewardship🧐", "antimicrobialstewardship🎯",
#                      "antimicrobialstewardaship", "antimicrobialstewardshipconference",
#                      "antimicrobialstewardshippharmacist", "antimicrobialstewardship🦠",
#                      "antimicrobialstewardshiprogram", "antimicrobialstewardshipcourse",
#                      "antimicrobialstewardshipprogrammes", "antimicrobialstewardshipsymposium",
#                      "antimicrobialstewardship💊", "antimicrobialstewardship2018", "drugresistantbugs",
#                      "drugresistantchlamydia",
#                      "drugresistantgerms", "drugresistantpathogens",
#                      "drugresistantuti", "drugresistantstd", "drugresistantbug", "drugresistanthiv",
#                      "drugresistantecoli", "drugresistantward", "drugresistantinsomnia",
#                      "drugresistantacinetobacter", "drugresistantcat", "drugresistantnasasusunod",
#                      "drugresistantcandidaauriscauris", "drugresistanttbcentre", "drugresistantb",
#                      "drugresistantepilepsysucksevenmore", "drugresistantepilespy", "drugresistant_tuberculosis",
#                      "drugresistantdepresssion", "drugresistantyak", "drugresistantbacterialinfections",
#                      "drugresistantaids", "drugresistantfeline", "drugresistantplants😊", "drugresistanttbguidance",
#                      "superbugsindia",
#                      "superbugsy", "superbugster", "superbugsisreal", "superbugsbunny",
#                      "superbugsunday", "superbugsdepkxd", "superbugslayerspolo", "superbugsboardgame",
#                      "superbugs1600", "superbugsarereal", "superbugsen", "superbugs23", "superbugs_india",
#                      "superbugsafari", "superbugsbunnyfunkopop", "superbugshakycam",
#                      "superbugsize", "superbugs🖋️🔬", "superbugsareassholes", "superbugstotherescue", "superbugsmile",
#                      "superbugshatecleanhand", "superbugstrikesagain", "superbugsslayers", "superbugsinspace",
#                      "superbugss", "superbugsandyou", "superbugsと言う無料展示", "antibioticresistanceexplained",
#                      "antibioticresistancemonth", "antibioticresistance💊💉",
#                      "antibioticresistance⚠️", "antibioticresistanceisbad", "antibioticresistanceis4real",
#                      "antibioticresistance👈", "antibioticresistanceresearch", "antibioticresistanceawarness",
#                      "antibioticresistancetest", "antibioticresistancetesting", "antibioticresistanceinindia",
#                      "antibioticresistanceinchildren", "antibioticresistancegenesantibióticos",
#                      "antibioticresistanceawareness2021", "antibioticresistanceisanightmare",
#                      "antibioticresistancefight", "antibioticresistance💊👊", "antibioticresistanceontherise",
#                      "antibioticresistanceofmicrobes", "bacterialinfectionsstink", "bacterialinfectionsuck",
#                      "bacterialinfectionsinchildren",
#                      "bacterialinfectionsareawesome", "bacterialinfectionsepsis", "bacterialinfectionsaywhat",
#                      "bacterialinfectionsaregross", "bacterialinfectionsinbotheyes", "bacterialinfectionsja",
#                      "bacterialinfectionsgalore", "bacterialinfections", "bacterialinfectionsofskin",
#                      "bacterialinfectionshavenothingonme", "bacterialinfectionsinherstomach",
#                      "bacterialinfectionsalmostallgone", "bacterialinfectionsabound",
#                      "bacterialinfectionscangetfuckedupthearsebybluewhalesdick", "bacterialinfectionsucks",
#                      "bacterialinfectionscauses", "bacterialinfectionsofthe5thdimension", "bacterialinfectionsarecool",
#                      "bacterialinfectionsarenotfun", "bacterialinfections😩", "bacterialinfectionsoftheskin",
#                      "bacterialinfectionscantholddisdown", "bacterialinfectionsux", "bacterialinfectionspeedrun",
#                      "bacterialinfectionsareabitch", "bacterialinfectionse", "bacterialinfectionsquad",
#                      "bacterialinfectionsrising", "bacterialinfectionsinhindi", "bacterialinfectionsforthewin",
#                      "bacterialinfectionsarenot", "bacterialinfectionsarenojoke",
#                      "bacterialinfectionsmacterialinfection", "bacterialinfectionsintheblood😔💉💊",
#                      "bacterialinfectionsindogs", "bacterialinfectionsfoundhere", "bacterialinfectionsarethebest",
#                      "bacterialinfectionsandsethrogen"]]
#
#     for keywords_drop in keyword_sets:
#         # save_path = Path('/Users/wei/Job Application 2023/CARA Network/AMR /AMR Instagram data/Antimicrobial resistance/Antimicrobial resistance 01 Jan 2017 - 01 July 2023_hashtags.csv', index=False)
#
#         droped_df = dropdata(df, column_drop=column_drop, keywords_drop=keywords_drop)
#         new_df = organised_data(droped_df)
#
#
# # remove non-English languages
# def contains_non_english(text):
#     pattern = r'[^\x00-\x7F]'
#     contains_non_ascii = bool(re.search(pattern, text))
#
#     try:
#         language = detect(text)
#     except LangDetectException:
#         return contains_non_ascii
#
#     languages = {
#         'Spanish': 'es',
#         'French': 'fr',
#         'Portuguese': 'pt',
#         'Italian': 'it',
#         'German': 'de',
#         'Dutch': 'nl',
#         'Swedish': 'sv',
#         'Danish': 'da',
#         'Norwegian': 'no',
#         'Finnish': 'fi',
#         'Polish': 'pl',
#         'Czech': 'cs',
#         'Slovak': 'sk',
#         'Slovenian': 'sl',
#         'Hungarian': 'hu',
#         'Romanian': 'ro',
#         'Croatian': 'hr',
#         'Serbian': 'sr',
#         'Bulgarian': 'bg',
#         'Greek': 'el',
#         'Turkish': 'tr',
#         'Estonian': 'et',
#         'Latvian': 'lv',
#         'Lithuanian': 'lt'
#     }
#
#     is_not_english = language != 'en' and language not in languages.values()
#     return contains_non_ascii or is_not_english
#
#
# # indices_to_drop = new_df[new_df['Caption'].apply(contains_non_english)].index
# indices_to_drop = new_df[new_df.apply(lambda row: contains_non_english(row['Caption']), axis=1)].index
#
# new_df.loc[indices_to_drop, ['Caption', 'URL', 'ID']] = None
# new_df.dropna(subset=['Caption', 'URL', 'ID'], how='all', inplace=True)
# # # Merge cells for 'name' and 'url'
# # new_df['name'] = new_df['name'].mask(new_df['name'].duplicated(), '')
# # new_df['url'] = new_df['url'].mask(new_df['url'].duplicated(), '')
#
# # new_df.to_csv( '/Users/wei/Job Application 2023/CARA Network/AMR /AMR Instagram data/Antimicrobial resistance/Antimicrobial resistance 01 Jan 2017 - 01 July 2023_specific hashtags.csv', index=False)
# new_df.reset_index(drop=True, inplace=True)
