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
from pathlib import Path
from typing import TypedDict, Any, NamedTuple
# Namedtuple is accessible like dict (key-value pairs) and is immutable(unchangeable)
import polars as pl


class PostDict(TypedDict):  # topPosts
    id: str
    type: str
    shortCode: str
    caption: str
    hashtags: list[str]
    mentions: list[str]  # who was @mentioned in a post
    url: str  # image url = url of a post
    commentsCount: int
    firstComment: str
    latestComments: list[[]]  # empty list in latestComments
    dimensionsHeight: int
    dimensionsWidth: int
    displayUrl: str
    images: list[[]]  # empty list in images
    alt: Any | None  # alternative text. text referring to the image
    likesCount: int
    timestamp: str
    childPosts: list[[]]  # empty list in childPosts
    ownerId: str

    def is_selected_image(self) -> bool:
        # is_selected: bool | None  # not yet implemented
        # todo
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
    # find file name in path, it sows str
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
        # print('Number of total id(images):', len(self.data), ', Number of unique id(images): ', len(validate))
        # print(f'These "{(len(self.data) - len(validate))}" duplicated id(images) should be deleted')
        return len(self.data) != len(validate)  # if it includes duplicate id, return True

    def remove_unused_fields(self) -> 'LatestPostInfo':
        # todo: if a column include all Null and empty list --> drop that column

        ret = []
        for it in self.data:
            new = deepcopy(it)

            for k, v in it.items():  # type: str, list  # key and value in data
                if isinstance(v, list) and len(v) == 0:  # if value and len of value ==0, add null in dataset
                    new[k] = pl.Null

            ret.append(new)  # dataset includes null in cell

        return self._replace(data=ret)

    def remove_duplicate(self) -> 'LatestPostInfo':
        # TODO
        pass

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

    def download_image(self, output_dir: Path | str):
        """download image with `ID` filename TODO"""  # use unique as filename
        pass

    def to_pickle(self):  # It can store image
        # TODO
        pass


class IgInfoFactory:
    # todo: how/when to init object

    def __init__(self, file: str,
                 data: list[DownloadDict]):

        self._file = file
        self.data = data

    @classmethod
    def load(cls, file: Path | str) -> 'IgInfoFactory':  # type of IgInfoFactory
        file = Path(file)
        if file.exists() and file.is_file():
            with open(Path(file), 'rb') as f:
                return IgInfoFactory(file.stem, json.load(f))
        raise FileNotFoundError('')

    @property
    def hashtag(self) -> str:
        # TODO check
        return re.split(r'\d', self._file, 1)[0]

    @property
    def extract_date(self) -> str:
        """
        1/1/2017 - 7/1/2023
        """
        pass

    @property
    def download_date(self) -> str:
        """TODO"""
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
    return [IgInfoFactory.load(f) for f in Path(d).glob('*.json')]


def load_from_excel(f: Path) -> LatestPostInfo:
    """TODO"""
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
    d = '/Users/wei/Job Application 2023/CARA Network/AMR /AMR Instagram data/json file'
    ify = load_from_directory(d)
    info = [it.collect_latest_posts() for it in ify]
    ret = LatestPostInfo.concat(info)  # concat 11 json files
    remove_ = ret.remove_unused_fields()
    # ret.contain_duplicated()
    df = ret.to_dataframe()

