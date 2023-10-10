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
import collections
import json
import pickle
import re
from copy import deepcopy  # copy an object which is completely independent of the original object
from pathlib import Path
from typing import TypedDict, Any, NamedTuple, Optional, List, Union
import pandas as pd
# Namedtuple is accessible like dict (key-value pairs) and is immutable(unchangeable)
import polars as pl
from PIL import UnidentifiedImageError
from matplotlib.image import imread

PathLike = Union[Path | str]


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

    def is_selected_image(self) -> 'LatestPostInfo':  # 原本是bool
        # todo: why this function is under PostDict
        p = '/Users/wei/Documents/cara_network/amr_igdata/output/final_test.xlsx'
        df = pd.read_excel(p, engine='openpyxl')
        selected_df = df[df['selected_images'].str.contains('1')]

        return self._replace(data=selected_df)

        # is_selected: bool | None  # not yet implemented

    def extract_date(self, start_date='2017/1/1', end_date='2023/7/1') -> 'LatestPostInfo':
        from datetime import datetime
        start_date = datetime.strptime(start_date, '%Y/%m/%d')
        end_date = datetime.strptime(end_date, '%Y/%m/%d')
        ret = []
        for it in self.data:
            date = it['timestamp']
            date = datetime.strptime(date, '%Y-%m-%dT%H:%M:%S.000Z')

            if start_date <= date <= end_date:
                ret.append(it)

        return self._replace(data=ret)


def to_pickle(images_path: PathLike):  # It can store image

    # todo:image is store as array, so save this array into pickle, why failed
    from matplotlib.image import imread  # imread return nparray
    jpg_files = sorted(images_path.glob("*.jpg"))
    images_list = []
    ret = collections.defaultdict(list)

    for i, jpg_file in enumerate(jpg_files, start=1):
        file_name = str(jpg_file).split('/')[-1]
        images = imread(jpg_file)
        images_list.append(images)

        try:
            with open('images_list.pkl', 'wb') as file:  # wb: write bite
                pickle.dump(images_list, file)
        except UnidentifiedImageError as e:
            print(repr(e))
            print(f'cannot identify {file_name}')
            ret['identified_error'].append('failed')
        error_identified = pl.DataFrame(ret)
        error_identified.write_csv('test_error.csv')


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
    def extract_date(self) -> str:  # moved to LatestPost
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


def load_from_excel(dir_path: PathLike, cara_out: PathLike = None) -> LatestPostInfo:  # ? pd.DataFrame
    """TODO 從有的excel 對應到latestpostinfo, 找資料"""
    cara_df = pd.read_csv(dir_path / 'Instagram data with category_29 Aug.csv')
    cara_df = pl.DataFrame(cara_df)
    original_df = pl.read_excel(dir_path / 'original_instagram_data.xlsx')
    merge_df = cara_df.join(original_df, on=['url'], how='inner')
    cara_df = merge_df.to_pandas()
    cara_df['date'] = pd.to_datetime(cara_df['timestamp'])
    cara_df['year'] = cara_df['date'].dt.year
    cara_df['month'] = cara_df['date'].dt.month
    cara_df['date'] = cara_df['date'].dt.tz_localize(None)
    if cara_out is not None:
        cara_df.to_csv(cara_out)

    return cara_df


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
    d = '/Users/wei/Documents/cara_network/amr_igdata/json_file'
    ify = load_from_directory(d)
    info = [it.collect_latest_posts() for it in ify]
    ret = LatestPostInfo.concat(info)  # concat 11 json files
    ret = ret.remove_unused_fields()
    # ret = ret.extract_date()
    # ret = ret.is_selected_image()

    images_path = Path('/Users/wei/Documents/cara_network/amr_igdata/instagram_images')
    to_pickle(images_path)

    with open('images_list.pkl', 'rb') as file:  # read bites mode
        load_image = pickle.load(file)
    print(load_image)
    # ret.to_dataframe().write_excel('original_instagram_data.xlsx')
    # ret.contain_duplicated()
    # ret.remove_duplicate()

    # dir_path = Path('/Users/wei/Documents/cara_network/amr_igdata/output')
    # cara_out = Path('/Users/wei/Documents/cara_network/amr_igdata/output/final_609posts_data.csv', index=False)
    # load_from_excel(dir_path, cara_out)
    # ret.is_selected_image()
