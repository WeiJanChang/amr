from pathlib import Path
from typing import Union

from amr.src.Ig_info_practice import load_from_directory, LatestPostInfo

PathLike = Union[Path | str]


def create_latestpost_info(directory: PathLike) -> LatestPostInfo:
    ify = load_from_directory(directory)
    info = [it.collect_latest_posts() for it in ify]
    ret = LatestPostInfo.concat(info)  # concat 11 json files
    return ret.remove_unused_fields().remove_duplicate()


def get_url_from_latestpost(info: LatestPostInfo) -> list[str]:
    df = info.to_dataframe()
    url = list(df['url'])
    return url


def download_image():
    pass


if __name__ == '__main__':
    d = '/Users/wei/Documents/CARA Network/AMR /AMR Instagram data/json file'
    info = create_latestpost_info(d)
    urls = get_url_from_latestpost(info=info)
    print(urls)

