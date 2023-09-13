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

Step II. Combine these 11 json files and find unique id of post

Step III. Select useful image to convey health-related information.

Step IV: Assign images to different categories below

1. The Humour
2. Shock/Disgust/Fear
3. Educational/Informative
4. Personal Stories
5. Opportunistic
6. Advocacy
"""
import json
import re
from copy import deepcopy  # copy an object which is completely independent of the original object
from os import path
from pathlib import Path
from typing import TypedDict, Any, NamedTuple, Optional, List

import instaloader
import pandas as pd
# Namedtuple is accessible like dict (key-value pairs) and is immutable(unchangeable)
import polars as pl


class PostDict(TypedDict):  # topPosts
    id: str
    type: str
    shortCode: str
    caption: str
    hashtags: list[str]
    mentions: list[str | None]  # who was @mentioned in a post
    url: str  # image url = url of a post
    commentsCount: int
    firstComment: str
    latestComments: list[Any]
    dimensionsHeight: int
    dimensionsWidth: int
    displayUrl: str
    images: list[Any]
    alt: Any | None  # alternative text. text referring to the image
    likesCount: int
    timestamp: str
    childPosts: list[...]
    ownerId: str

    def is_selected_image(self, field: str = 'URL') -> bool:
        # todo
        p = '/Users/wei/Job Application 2023/CARA Network/AMR /AMR Instagram data/Instagram 1 + 2 with all information (unique URL only).xlsx'
        df = pd.read_excel(p, engine='openpyxl')
        image_url = []
        for it in df[field]:
            image_url.append(it)
        print(image_url)
        # is_selected: bool | None  # not yet implemented

        return
        pass


class DownloadDict(TypedDict):
    id: str
    name: str  # hashtag name
    url: str  # url of hashtag (many posts)
    topPostsOnly: bool
    profilePicUrl: str
    postsCount: int
    topPosts: list[PostDict | None]  # Either PostsDict or None
    latestPosts: list[PostDict | None]


class LatestPostInfo(NamedTuple):
    # find hashtag(11 keyword) in LatestPostInfo.concat(info), it shows list[str]
    # find file name in path, it shows str
    hashtag: str | list[str]
    data: list[PostDict]

    @property
    def n_posts(self) -> int:  # number of post
        return len(self.data)

    def contain_duplicated(self, field: str = 'id') -> bool:
        # check if include duplicate id
        validate = set()  # set is unordered and can't have same element

        for it in self.data:
            validate.add(it[field])  # find unique id into set
        print('Number of total id(images):', len(self.data), ', Number of unique id(images): ', len(validate))
        print(f'These "{(len(self.data) - len(validate))}" duplicated id(images) should be deleted')

        return len(self.data) != len(validate)  # if it includes duplicate id, return True

    def remove_unused_fields(self) -> 'LatestPostInfo':

        ret = []
        for it in self.data:
            new = deepcopy(it)
            for k, v in it.items():  # type: str, Any  # key and value in data
                if k in (
                        'shortCode', 'firstComment', 'latestComments', 'dimensionsHeight', 'dimensionsWidth',
                        'displayUrl', 'images', 'alt', 'childPosts', 'videoViewCount', 'productType'):
                    del new[k]
                elif isinstance(v, list) and len(v) == 0:  # if value and len of value ==0, add null in dataset
                    new[k] = pl.Null

            ret.append(new)  # dataset includes null in cell

        return self._replace(data=ret)

    def remove_duplicate(self, field: str = 'id') -> 'LatestPostInfo':
        unique_data = []
        validate = set()
        for it in self.data:
            it_id = it[field]
            if it_id not in validate:  # "not in" as unique id added to the first loop, if there still have the same id,
                # not allow add it into validate
                unique_data.append(it)
                validate.add(it_id)
        return self._replace(data=unique_data)

    def to_dataframe(self) -> pl.DataFrame:
        return (pl.DataFrame(self.data, infer_schema_length=300)
                .unique('id'))

    @classmethod
    def concat(cls, infos: list['LatestPostInfo']) -> 'LatestPostInfo':
        hashtags = []
        batch_data = []
        for info in infos:
            hashtags.append(info.hashtag)
            batch_data.extend(info.data)

        return LatestPostInfo(hashtags, batch_data)

    def to_pickle(self):  # It can store image
        # TODO
        pass


class IgInfoFactory:
    # init: when class created, method of "__init__" is to initialize the object
    # self: __init__(self) is default. self represents the object of the class itself

    def __init__(self, file: str,
                 data: list[DownloadDict]):

        self._file = file  # file.stem
        self.data = data  # json.load(f)

    @classmethod
    def load(cls, file: Path | str) -> 'IgInfoFactory':  # type of IgInfoFactory
        file = Path(file)
        if file.exists() and file.is_file():
            with open(Path(file), 'rb') as f:
                return IgInfoFactory(file.stem, json.load(f))  # init class
        raise FileNotFoundError('')

    @property
    def hashtag(self) -> str:
        # split file name into two part, one is str, another part is int
        # extract the first one [0] as hashtag
        return re.split(r'\d', self._file, 1)[0]

    @property
    def extract_date(self) -> str:
        """
        1/1/2017 - 7/1/2023
        """
        pass

    @property
    def download_date(self) -> str:
        """TODO 檔名"""
        pass

    def collect_latest_posts(self) -> LatestPostInfo:
        ret = []
        for it in self.data:
            lps = it['latestPosts']
            if len(lps) != 0:
                for lp in lps:  # type: PostDict

                    ret.append(lp)

        return LatestPostInfo(self.hashtag, ret)


def load_from_directory(d: Path | str) -> list[IgInfoFactory]:
    if not Path(d).is_dir():
        raise ValueError('')
    return [IgInfoFactory.load(f) for f in Path(d).glob('*.json')]


def load_from_excel(f: Path) -> LatestPostInfo:
    """TODO 從有的excel 對應到latestpostinfo, 找資料"""
    pass


# ==== #

def printdf(df: pl.DataFrame,
            nrows: int | None = None,
            ncols: int | None = None) -> str:
    """print polars dataframe with given row numbers"""
    with pl.Config() as cfg:
        rows = df.shape[0] if nrows is None else nrows
        cols = df.shape[1] if ncols is None else ncols
        cfg.set_tbl_rows(rows)
        cfg.set_tbl_cols(cols)

        print(df)

        return df.__repr__()


if __name__ == '__main__':
    d = '/Users/wei/Documents/CARA Network/AMR /AMR Instagram data/json file'
    ify = load_from_directory(d)
    info = [it.collect_latest_posts() for it in ify]
    ret = LatestPostInfo.concat(info)  # concat 11 json files
    ret = ret.remove_unused_fields()
    ret.to_dataframe().write_excel('original_instagram_data.xlsx')
    ret.contain_duplicated()
    ret.remove_duplicate()
