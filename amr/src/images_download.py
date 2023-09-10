import datetime
from pathlib import Path
from typing import Union, Tuple, List, Any

import instaloader
from instaloader import InstaloaderContext, Instaloader

from amr.src.Ig_info_practice import load_from_directory, LatestPostInfo

PathLike = Union[Path | str]


def create_latestpost_info(directory: PathLike) -> LatestPostInfo:
    ify = load_from_directory(directory)
    info = [it.collect_latest_posts() for it in ify]
    ret = LatestPostInfo.concat(info)  # concat 11 json files
    return ret.remove_unused_fields().remove_duplicate()


def download_image(info: LatestPostInfo):
    df = info.to_dataframe()
    loader = instaloader.Instaloader(save_metadata=False)
    context: InstaloaderContext = loader.context
    for i in df.iter_rows(named=True):
        id = i['id']  # type: str
        url = i['url']
        _download(loader, context, id, url)
        print(id)
        break


def _download(loader: Instaloader, context: InstaloaderContext, post_id: str, post_url: str):
    """
    Download images and videos via url and save name based on id from each post
    """
    shortcode = post_url.split('/')[-2]
    post = instaloader.Post.from_shortcode(context, shortcode)
    f = f"ID_{post_id}"
    file_path = Path('/Users/wei/Documents/CARA Network/AMR /AMR Instagram data/Instagram images')
    loader.download_post(post, target=file_path / f)


if __name__ == '__main__':
    d = '/Users/wei/Documents/CARA Network/AMR /AMR Instagram data/json file'
    info = create_latestpost_info(d)
    urls_id = download_image(info)
