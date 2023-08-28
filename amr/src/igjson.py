import pickle
from pathlib import Path
from typing import TypedDict, Any


class PostsDict(TypedDict):  # topPosts
    id: str
    type: str
    shortCode: str
    caption: str
    hashtags: list[str]
    mentions: list[str]
    url: str
    commentsCount: int
    firstComment: str
    latestComments: list
    dimensionsHeight: int
    dimensionsWidth: int
    displayUrl: str
    images: list
    alt: Any | None
    likesCount: int
    timestamp: str
    childPosts: list
    ownerId: str


class DownLoadDict(TypedDict):
    id: str
    name: str
    url: str
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


def get_lastest_post(j: DOWNLOAD_JSON) -> list[PostsDict]:
    """
    from json file get "latestPosts" from different posts

    :param j:
    :return:
    """
    for post in j:  # type: PostsDict

        print(post["latestPosts"])


def load(file: Path):
    # TODO pickle # 有pickle 就叫出 沒有就
    if file.exists():
        with open(file, 'rb') as f:
            return pickle.load(file)
    else:
        save(file)


def save(output: Path):
    pass


if __name__ == '__main__':
    p = '/Users/wei/Job Application 2023/CARA Network/AMR /AMR Instagram data/Antimicrobial stewardship/Antimicrobial stewardship 01 Jan 2017 - 01 July 2023.json'
    j = load_json(p)
    # ret = foreach_get(j, 'latestPosts', 'id')
    # print(ret)
    print(type(j[0]))
    print(get_lastest_post(j))
