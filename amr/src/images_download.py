from pathlib import Path
from typing import Union

import instaloader

from amr.src.Ig_info_practice import load_from_directory, LatestPostInfo

PathLike = Union[Path | str]


def create_latestpost_info(directory: PathLike) -> LatestPostInfo:
    ify = load_from_directory(directory)
    info = [it.collect_latest_posts() for it in ify]
    ret = LatestPostInfo.concat(info)  # concat 11 json files
    return ret.remove_unused_fields().remove_duplicate()


def download_image(info: LatestPostInfo) -> bool:
    df = info.to_dataframe().to_pandas()  # polar df to pandas df
    post_url = df['url'].to_list()
    return post_url

    loader = instaloader.Instaloader()
    # post = instaloader.Post.from_shortcode(loader.context, post_url.split('/')[-2])

    image_name = f"{id}.jpg"
    # file_path = ''
    # loader.download_post(post, target=file_path)

    return post_url


if __name__ == '__main__':
    d = '/Users/wei/Documents/CARA Network/AMR /AMR Instagram data/json file'
    info = create_latestpost_info(d)
    urls = download_image(info)
    print(urls)
    print('https://www.instagram.com/p/CuroezmPzbb/'.split('/')[-2])
