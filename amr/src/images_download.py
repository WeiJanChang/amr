import re

import numpy as np
import pandas as pd
import polars as pl
import time
from pathlib import Path
from typing import Union
import instaloader
from instaloader import InstaloaderContext, Instaloader, InstaloaderException, ConnectionException
from amr.src.Ig_info import load_from_directory, LatestPostInfo

# todo: download images and video only with all enlgish Caption and hashtags,
#  preview jpgs in python,
PathLike = Union[Path | str]


def create_latestpost_info(directory: PathLike) -> LatestPostInfo:
    ify = load_from_directory(directory)
    info = [it.collect_latest_posts() for it in ify]
    ret = LatestPostInfo.concat(info)  # concat 11 json files
    return ret.remove_unused_fields().remove_duplicate()


def download_image(info: LatestPostInfo, output_path: PathLike):
    """
    convert LatestPostInfo to polars df. get all ids and urls from df.
    for each download using _download and save images and videos to output_path

    todo: new col name: number of images after to dataframe
    :param info: LatestPostInfo
    :param output_path: path
    :return:
    """
    df = info.to_dataframe()
    loader = instaloader.Instaloader(save_metadata=False)
    context: InstaloaderContext = loader.context
    for i in df.iter_rows(named=True):
        id = i['id']  # type: str
        url = i['url']
        try:
            _download(loader, context, id, url, output_path)
        except InstaloaderException as e:
            print(f'{id} download fail')  # todo
            print(repr(e))

        time.sleep(5)


def _download(loader: Instaloader, context: InstaloaderContext, post_id: str, post_url: str, output_path: PathLike):
    """
    Download images and videos via url and save name based on id from each post
    :param loader: Instaloader
    :param context: InstaloaderContext
    :param post_id: id
    :param post_url: url
    :param output_path: path
    :return:
    """
    shortcode = post_url.split('/')[-2]
    post = instaloader.Post.from_shortcode(context, shortcode)
    f = f"ID_{post_id}"
    loader.download_post(post, target=Path(output_path) / f)


def download_postprocess(output_path: PathLike, new_dir: PathLike, verbose: bool = True):
    id_list = []
    num_jpg_list = []
    num_video_list = []
    for path in Path(output_path).iterdir():  # select all folders in output_path
        if path.is_dir():  # whether path is directory
            jpg_files = sorted(path.glob("*.jpg"))  # Sort the files for consistent numbering
            video_files = sorted(path.glob("*.mp4"))
            txt_files = sorted(path.glob("*.txt"))

            for i, jpg_file in enumerate(jpg_files, start=1):
                new_jpg_file = new_dir / f"{path.name}_{i}{jpg_file.suffix}"
                jpg_file.rename(new_jpg_file)
                id = re.split(r'(\d+)', (str(new_jpg_file).split('/')[-1]), 1)[1]  # type: str # \d+ integer part
                id_list.append(id)
                num_jpg_list.append(len(jpg_files))
                if verbose:
                    print(f"{jpg_file} -> {new_jpg_file}")

            for j, video_file in enumerate(video_files, start=1):
                new_video_file = new_dir / f"{path.name}_{j}{video_file.suffix}"
                video_file.rename(new_video_file)
                id = re.split(r'(\d+)', (str(new_video_file).split('/')[-1]), 1)[1]
                id_list.append(id)
                num_video_list.append(len(video_files))

                if verbose:
                    print(f"{video_file} -> {new_video_file}")

            for k, txt_file in enumerate(txt_files, start=1):
                new_txt_file = new_dir / f"{path.name}_{k}{txt_file.suffix}"
                txt_file.rename(new_txt_file)
                if verbose:
                    print(f"{txt_file} -> {new_txt_file}")

    if len(num_video_list) == 0 in num_video_list:
        num_video_list.append('0')
        jpg_df = pd.DataFrame({'id': id_list, 'number of jpg': num_jpg_list})
        video_df = pd.DataFrame({'id': id_list, 'number of video': num_video_list})
        final_df = jpg_df.merge(video_df, on='id', how='outer')
    else:
        jpg_df = pd.DataFrame({'id': id_list, 'number of jpg': num_jpg_list})
        final_df = jpg_df.copy()
        final_df['number of video'] = '0'
    return final_df


if __name__ == '__main__':
    d = '/Users/wei/Documents/CARA Network/AMR/AMR Instagram data/json file'
    info = create_latestpost_info(d)
    output_path = Path('/Users/wei/Documents/CARA Network/AMR/AMR Instagram data/instagram_images_with_dir')
    download_image(info, output_path)
    new_dir = Path('/Users/wei/Documents/CARA Network/AMR/AMR Instagram data/all_instagram_images')
    df = download_postprocess(output_path, new_dir)
    df.to_excel('/Users/wei/Documents/CARA Network/AMR /AMR Instagram data/number_of_images_and_videos_with_id.xlsx')
