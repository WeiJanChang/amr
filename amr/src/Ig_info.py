import json
import re
from copy import deepcopy
from pathlib import Path
from typing import TypedDict, Any, NamedTuple
import polars as pl


class PostDict(TypedDict):
    id: str
    type: str
    shortCode: str
    caption: str
    hashtags: list[str]
    mentions: list[str]  # who was @mentioned in a post
    url: str  # image url = url of a post
    commentsCount: int
    firstComment: str
    latestComments: list[Any]  # TODO check
    dimensionsHeight: int
    dimensionsWidth: int
    displayUrl: str
    images: list[Any]  # TODO check
    alt: Any | None  # TODO check
    likesCount: int
    timestamp: str
    childPosts: list[Any]  # TODO check
    ownerId: str

    #
    is_selected: bool | None  # not yet implemented


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
    hashtag: str | list[str]
    data: list[PostDict]

    @property
    def n_posts(self) -> int:  # number of post
        return len(self.data)

    def contain_duplicated(self, field: str = 'id') -> bool:
        # check url, caption duplicated? or only id?
        validate = set()
        for it in self.data:
            validate.add(it[field])
        print(len(self.data), len(validate))
        return len(self.data) != len(validate)

    def remove_unused_fields(self) -> 'LatestPostInfo':

        ret = []
        for it in self.data:
            new = deepcopy(it)
            for k, v in it.items():
                if isinstance(v, list) and len(v) == 0:
                    new[k] = pl.Null

            ret.append(new)

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

    def __init__(self, file: str,
                 data: list[DownloadDict]):

        self._file = file
        self.data = data

    @classmethod
    def load(cls, file: Path | str) -> 'IgInfoFactory':  # type of iginfofactory
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
    ret = LatestPostInfo.concat(info)
    df = ret.to_dataframe()
    print(df.shape[0])

    # FALSE
    # ret.remove_duplicate().contain_duplicated()
