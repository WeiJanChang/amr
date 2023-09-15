import collections
import os
import polars as pl
import time
from pathlib import Path
from typing import Union, Callable
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


def download_image(info: LatestPostInfo, output_path: PathLike, error_out: PathLike = None):
    """
    convert LatestPostInfo to polars df. get all ids and urls from df.
    for each download using _download and save images and videos to output_path

    :param error_out: any error messages save to csv file
    :param info: LatestPostInfo
    :param output_path: path
    :return:
    """
    ret = collections.defaultdict(list)
    df = info.to_dataframe()
    loader = instaloader.Instaloader(save_metadata=False)
    context: InstaloaderContext = loader.context
    for i in df.iter_rows(named=True):
        id = i['id']  # type: str
        url = i['url']
        try:
            _download(loader, context, id, url, output_path)
            ret['download fail'].append(False)
        except ConnectionException as e:
            print(f'{id} fail connection')
            print(repr(e))  # repr:representation. return a string
            ret['download fail'].append(True)

        except InstaloaderException as e:
            print(f'{id} download fail')
            print(repr(e))
            ret['download fail'].append(True)
    error_df = pl.DataFrame(ret)
    if error_out is not None:
        error_df.write_csv(error_out)

    return error_df


def _download(loader: Instaloader, context: InstaloaderContext, post_id: str, post_url: str, output_path: PathLike,
              exist_out: PathLike = None):
    """
    Download images and videos via url and save name based on id from each post
    :param loader: Instaloader
    :param context: InstaloaderContext
    :param post_id: id
    :param post_url: url
    :param output_path: path
    :return:
    """
    ret = collections.defaultdict(list)
    shortcode = post_url.split('/')[-2]
    post = instaloader.Post.from_shortcode(context, shortcode)

    f = f"ID_{post_id}"
    file_path = Path(output_path) / f

    if not file_path.exists():
        loader.download_post(post, target=file_path)
    else:
        ret['exist'].append(True)

    exist_df = pl.DataFrame(ret)
    if exist_out is not None:
        exist_df.write_csv(exist_out)

    return exist_df


def download_postprocess(output_path: PathLike, new_dir: PathLike,
                         move: bool = False, verbose: bool = True,
                         out: PathLike = None) -> pl.DataFrame:
    ret = collections.defaultdict(list)
    # ret: return; defaultdict(list): it is easy to group a sequence of key-value pairs into a dictionary of lists

    if move:
        fn: Callable[[Path, Path], None] = os.rename  # fn: function
        v = 'MOVE'
    else:
        import shutil
        fn: Callable[[Path, Path], Path] = shutil.copy  # copies the file data
        v = 'COPY'

    for path in Path(output_path).iterdir():  # select all folders in output_path
        if path.is_dir():  # whether path is directory
            jpg_files = sorted(path.glob("*.jpg"))  # Sort the files for consistent numbering
            video_files = sorted(path.glob("*.mp4"))
            txt_files = sorted(path.glob("*.txt"))

            ret['id'].append(path.name.split('_')[1])
            ret['n_image'].append(len(jpg_files))
            ret['n_video'].append(len(video_files))

            for i, jpg_file in enumerate(jpg_files, start=1):
                new_jpg_file = new_dir / f"{path.name}_{i}{jpg_file.suffix}"
                fn(jpg_file, new_jpg_file)

                if verbose:
                    print(f"{v}:{jpg_file} -> {new_jpg_file}")

            for j, video_file in enumerate(video_files, start=1):
                new_video_file = new_dir / f"{path.name}_{j}{video_file.suffix}"
                fn(video_file, new_video_file)

                if verbose:
                    print(f"{v}:{video_file} -> {new_video_file}")

            for k, txt_file in enumerate(txt_files, start=1):
                new_txt_file = new_dir / f"{path.name}_{k}{txt_file.suffix}"
                fn(txt_file, new_txt_file)
                if verbose:
                    print(f"{v}:{txt_file} -> {new_txt_file}")
    df = pl.DataFrame(ret)
    if out is not None:
        df.write_csv(out)

    return df


if __name__ == '__main__':
    d = '/Users/wei/Documents/CARA Network/AMR/AMR Instagram data/json file'
    info = create_latestpost_info(d)
    output_path = Path('/Users/wei/Documents/CARA Network/AMR/AMR Instagram data/test')
    download_image(info, output_path)
    new_dir = Path('/Users/wei/Documents/CARA Network/AMR/AMR Instagram data/rename_test')
    save_path = '/Users/wei/Documents/CARA Network/AMR/AMR Instagram data/n_images_video_with_id.csv'
    download_postprocess(output_path, new_dir, out=save_path)
