import pickle
from pathlib import Path
from typing import TypedDict, Any




class PostsDict(TypedDict):
    id: str
    type: str
    shortCode: str
    caption: str
    hashtags: list[str]
    mentions: Any  # TODO
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
    childPosts: list[...]
    ownerId: str


class DownLoadDict(TypedDict):
    id: str
    name: str
    url: str
    topPostsOnly: bool
    profilePicUrl: str
    postsCount: int
    topPosts: list[PostsDict | None]
    latestPosts: list[PostsDict | None]

DOWNLOAD_JSON = list[DownLoadDict]

def load_json(p) -> DOWNLOAD_JSON:
    import json
    with open(p, 'rb') as f:
        ret = json.load(f)

    return ret

def get_lastest_post(j: DOWNLOAD_JSON) -> list[PostsDict]:
    pass


def load(file: Path):
    # TODO pickle
    if file.exists():
        with open(file, 'rb') as f:
            return pickle.load(file)
    else:
        save(file)

def save(output: Path):
    pass






if __name__ == '__main__':
    p = '/Users/wei/Python/caranetwork/amr/test_file/test.json'
    j= load_json(p)
    ret = foreach_get(j, 'latestPosts', 'id')
    print(ret)