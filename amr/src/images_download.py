from pathlib import Path
from typing import Union, Tuple, List, Any
import instaloader
from instaloader import InstaloaderContext, Instaloader
from amr.src.Ig_info import load_from_directory, LatestPostInfo

# todo: download images and video only with all enlgish Caption and hashtags,
#  save all of them into one folder. preview jpgs in python,
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
        _download(loader, context, id, url, output_path)
        break


def _download(loader: Instaloader, context: InstaloaderContext, post_id: str, post_url: str, output_path: PathLike):
    """
    Download images and videos via url and save name based on id from each post

    todo: check path
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


def rename(output_path: PathLike):
    for path in Path(output_path).iterdir():
        if path.is_dir():
            jpg_files = sorted(path.glob("*.jpg"))  # Sort the files for consistent numbering
            video_files = sorted(path.glob("*.mp4"))
            txt_files = sorted(path.glob("*.txt"))
            for i, jpg_file in enumerate(jpg_files, start=1):
                new_name = f"{path.name}_{i}{jpg_file.suffix}"
                new_path = jpg_file.with_name(new_name)
                jpg_file.rename(new_path)
            for i, video_file in enumerate(video_files, start=1):
                new_name = f"{path.name}_{i}{video_file.suffix}"
                new_path = video_file.with_name(new_name)
                video_file.rename(new_path)
            for txt_file in txt_files:
                new_name = path.name + txt_file.suffix
                new_path = txt_file.with_name(new_name)
                txt_file.rename(new_path)

    pass
    # df['number of images'] = jpg_count
    # non_english = df['caption'].apply(contains_non_english)  # type: bool
    # df = df.filter(non_english)


if __name__ == '__main__':
    d = '/Users/wei/Documents/CARA Network/AMR /AMR Instagram data/json file'
    info = create_latestpost_info(d)
    output_path = Path('/Users/wei/Documents/CARA Network/AMR /AMR Instagram data/Instagram images')
