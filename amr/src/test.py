"""
Aims: To evaluate AMR messaging from Instagram. To do a content analysis to understand what type of
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

Step 3. data cleaning: create a condition to select useful hashtags, only English, remove emoji
"""

from pathlib import Path  # pathlib: module, Path: class. Checking if a path exist
from typing import Optional, List, Any  # typing: support for type hint
import pandas as pd
from typing import Union
import json
import re
import demoji
from langdetect import detect, LangDetectException
from unnecessary_keywords import keyword_sets


def remove_emojis(text: str) -> str:
    """
    :param text: all text in files
    :return: modified text with any emojis removed (replaced emoji to "")
    """
    return demoji.replace(text, "")


def remove_emojis_from_json(json_data: list[Any]) -> list:
    """
    :param json_data: json file
    :return: modified json file without emoji
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
            json_emojis_removed = remove_emojis_from_json(json_data)
            return pd.DataFrame(json_emojis_removed)
    else:
        f = list(p.glob('*.json'))  # glob is used to math file path
        if len(f) == 0:
            raise FileNotFoundError(f'no json file under the {p}')
        elif len(f) == 1:
            with f[0].open(encoding='utf-8') as file:
                json_data = json.load(file)
                # remove emojis
                json_emojis_removed = remove_emojis_from_json(json_data)
                return pd.DataFrame(json_emojis_removed)
        else:
            raise RuntimeError(f'multiple json files under the {p}')


def extract_captions_urls(posts: list[Any]) -> tuple[list[str], list[str], list[str], list[int]]:
    """
    Extract captions and URLs from a list of posts.

    :param posts: A list of posts, where each post is represented as a dictionary.
    Each post dictionary should contain information about the post, such as 'caption' for the caption
    text, 'url' for the post URL, 'id' and like counts of the post.

    :return: A list of caption/URL/ID/like counts extracted from the posts.
    Each caption/URL/ID/like counts are preceded by its corresponding post index.
    """
    captions = [post.get('caption', '') for post in posts]  # from dic get "caption" in list of posts
    urls = [post.get('url', '') for post in posts]
    id_var = [post.get('id', '') for post in posts]
    likes_count = [post.get('likesCount', '') for post in posts]
    return captions, urls, id_var, likes_count


def drop_col(df: pd.DataFrame,
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
    df1 = df.copy()

    if column_drop is not None:
        df1 = df.drop(columns=column_drop)

    if keywords_drop is not None:
        df1 = df[df['name'].isin(keywords_drop) == False]

    if save_path is not None:
        df1.to_csv(save_path)

    return df1


def organised_data(df: pd.DataFrame,
                   save_path: Optional[Path] = None) -> pd.DataFrame:
    # todo: solve warning
    df['Caption'] = df['Caption'].apply(lambda x: [str(item) for item in x])
    df['URL'] = df['URL'].apply(lambda x: [str(item) for item in x])
    df['ID'] = df['ID'].apply(lambda x: [str(item) for item in x])
    df['LikesCount'] = df['LikesCount'].apply(lambda x: [str(item) for item in x])
    # Create a new df. each Caption and URL is unique in each cell. But the name ane url keep the same
    new_df = pd.DataFrame({
        'name': df['name'].repeat(df['Caption'].apply(len)),
        'url': df['url'].repeat(df['URL'].apply(len)),
        'Caption': [caption for captions in df['Caption'] for caption in captions],
        'URL': [url for urls in df['URL'] for url in urls],
        'ID': [id for id_var in df['ID'] for id in id_var],
        'LikesCount': [likesCount for likes_count in df['LikesCount'] for likesCount in likes_count]})
    # reset index
    new_df.reset_index(drop=True, inplace=True)

    if save_path is not None:
        new_df.to_csv(save_path, index=False)

    return new_df


# remove non-English languages
def contains_non_english(text: str) -> bool:
    pattern = r'[^\x00-\x7F]'
    contains_non_ascii = bool(re.search(pattern, text))  # ASCII is a character encoding standard

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

    is_not_english = language != 'en' and language not in languages.values()  # type: bool

    return contains_non_ascii or is_not_english


if __name__ == '__main__':
    df = load_json(
        '/Users/wei/Documents/CARA Network/AMR /AMR Instagram data/Superbugs/Superbugs 01 Jan 2017 - 01 July 2023.json')
    # Print the captions and URLs for easy reference
    df['Caption'], df['URL'], df['ID'], df['LikesCount'] = zip(
        *df['latestPosts'].apply(extract_captions_urls))  # todo: zip?

    column_del = ['topPostsOnly', 'profilePicUrl', 'postsCount', 'topPosts', 'latestPosts']

    save_path = Path(
        '/Users/wei/Job Application 2023/CARA Network/AMR /AMR Instagram data/AMR/Superbugs 01 Jan 2017 - 01 July 2023_hashtags.csv',
        index=False)

    droped_df = drop_col(df, column_drop=column_del, keywords_drop=keyword_sets)

    new_df = organised_data(droped_df)

    indices_to_drop = new_df[new_df['Caption'].apply(contains_non_english)].index
    indices_to_drop = new_df[new_df.apply(lambda row: contains_non_english(row['Caption']), axis=1)].index

    new_df.loc[indices_to_drop, ['Caption', 'URL', 'ID']] = None
    new_df.dropna(subset=['Caption', 'URL', 'ID'], how='all', inplace=True)

    # new_df.to_csv(
    #     '/Users/wei/Documents/CARA Network/AMR /AMR Instagram data/Superbugs/Superbugs 01 Jan 2017 - 01 July 2023_specific hashtags (non-English excluded)test.csv',
    #     index=False)
    new_df.reset_index(drop=True, inplace=True)
