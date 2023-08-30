"""
To evaluate Antimicrobial resistance (AMR) messaging from Instagram.
To do a content analysis to understand what type of messages (themes) have been used on social media for AMR.

pipeline

Step I. Data was extracted from Instagram using the Apify web tool and downloaded as JSON file

Time frame: 01 Jan 2017 to 01 July 2023
Language: English

The 11 hashtags(keywords) are:
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

Step II. Combine these 11 json files and find unique url of post

Step III. Select useful image to convey health-related information.

Step IV: Assign images to different categories below

1. The Humour
2. Shock/Disgust/Fear
3. Educational/Informative
4. Personal Stories
5. Opportunistic
6. Advocacy
"""
import itertools
import json
import os
import pickle
from pathlib import Path
from typing import TypedDict, Any

import pandas as pd


class PickleInstagram:
    hashtags: list[str]  # 11 keywords given by Sana
    urls: tuple[str]  # one url is one post. sample: "https://www.instagram.com/p/xxxxxx"
    id: tuple[str]  # "id" post a post (user id)
    caption: list[str]  # caption from a post. caption also include hashtags
    likes_count: int  # like counts from a post
    image: list[str]  # image link to the post


class PostsDict(TypedDict):  # topPosts
    id: str
    type: str
    shortCode: str
    caption: str
    hashtags: list[str]
    mentions: list[str]  # who was @mentioned in a post
    url: str  # image url = url of a post
    commentsCount: int
    firstComment: str
    latestComments: list
    dimensionsHeight: int
    dimensionsWidth: int
    displayUrl: str
    images: list
    alt: Any | None
    likesCount: int
    timestamp: str  # post of date and time
    childPosts: list
    ownerId: str


class DownLoadDict(TypedDict):
    id: str
    name: str  # hashtag name
    url: str  # url of hashtag (many posts)
    topPostsOnly: bool
    profilePicUrl: str
    postsCount: int
    topPosts: list[PostsDict | None]  # Either PostsDict or None
    latestPosts: list[PostsDict | None]


DOWNLOAD_JSON = list[DownLoadDict]  # list of DownLoadDict


def load_json(p) -> DOWNLOAD_JSON:
    import json
    with open(p, 'rb') as f:  # rb: read non-text
        ret = json.load(f)

    return ret


def find_unique(j: DOWNLOAD_JSON):
    # find unique URL (only one post, no duplicate posts included)
    image_urls = []
    for post_list in j:
        if 'latestPosts' in post_list and len(post_list['latestPosts']) > 0:
            latestPosts_list = post_list['latestPosts']  # type: list
            # print(len(latestPosts_list))
            for url_list in latestPosts_list:  # type: dict
                if 'url' in url_list and len(url_list) > 0:
                    image_urls.append(url_list['url'])
                # print(len(image_urls))

    return image_urls


def is_selected_image(url: str) -> bool:
    # select_image = []
    # selected_url = pd.read_xlsx(
    #     '/Users/wei/Job Application 2023/CARA Network/AMR /AMR Instagram data/Instagram 1 + 2 with all information (unique URL only).xlsx')
    # if ___ in selected_url['URL']:
    #     select_image.append()
    pass


def get_lastest_post(j: DOWNLOAD_JSON) -> list[PostsDict]:
    """
    from json file get "latestPosts" from different posts

    :param j:
    :return:
    """
    new_list = []
    for post in j:  # type: DownLoadDict
        if len(post['latestPosts']) != 0:
            new_list.append(post['latestPosts'])
    return new_list # todo: xx list[list]


def load(file: Path) -> list[PostsDict]:
    # TODO 要存什麼東西進pickle  # 有在pickle裡 就叫出，沒有就call get_lastest_post
    pickle.dump()
    if file.exists():
        with open(file, 'rb') as f:
            return pickle.load(file)
    else:
        save(file)


def save(output: Path):
    pass

if __name__ == '__main__':

    json_list =[]
    def open_json_files(path):

        json_file_names = [filename for filename in os.listdir(path) if filename.endswith('.json')] # type: list[str]
        for json_file in json_file_names:  #type: str
            with open(os.path.join(path, json_file)):
                json_list.append(json_file)
        return json_list


    open_json_files('/Users/wei/Job Application 2023/CARA Network/AMR /AMR Instagram data/json file')
    #print(json_list)
    for p in json_list:
        p = '/Users/wei/Job Application 2023/CARA Network/AMR /AMR Instagram data/json file/' + p
        j = load_json(p)
        print(len(find_unique(j)))



    # ret = foreach_get(j, 'latestPosts', 'id')
    # print(ret)
    # print(type(j[0]))
    # print(get_lastest_post(j))
    # print(len(find_unique(j)))
