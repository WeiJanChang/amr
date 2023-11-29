import collections
import os
import polars as pl
import time
from pathlib import Path
from typing import Union, Callable
import instaloader
from instaloader import InstaloaderContext, Instaloader, InstaloaderException, ConnectionException
from tqdm import tqdm

from Ig_info import LatestPostInfo, load_from_directory

PathLike = Union[Path | str]


def create_latestpost_info(directory: PathLike) -> LatestPostInfo:
    ify = load_from_directory(directory)
    info = [it.collect_latest_posts() for it in ify]
    ret = LatestPostInfo.concat(info)  # concat 11 json files

    return ret.remove_unused_fields().remove_duplicate()


def download_image(info: LatestPostInfo, output_path: PathLike, error_out: PathLike = None) -> pl.DataFrame:
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
    for path in Path('/Users/wei/Documents/cara_network/amr_igdata/instagram_images_with_dir').iterdir():
        if path.is_dir():
            downloaded_id = path.name.split('_')[1]
            df = df.filter(pl.col('id') != downloaded_id)

    loader = instaloader.Instaloader(save_metadata=False)
    context: InstaloaderContext = loader.context
    for i in tqdm(df.iter_rows(named=True), total=len(df), desc='Downloading'):
        id = i['id']  # type: str
        url = i['url']
        ret['id'].append(id)

        try:
            _download(loader, context, id, url, output_path)
            ret['download status'].append('successful')
        except ConnectionException as e:
            print(f'{id} fail connection')
            print(repr(e))  # repr:representation. return a string
            ret['download status'].append('connection_error')
        except InstaloaderException as e:
            print(f'{id} download fail')
            print(repr(e))
            ret['download status'].append('post_unavailable')
        time.sleep(3)
    error_df = pl.DataFrame(ret)
    if error_out is not None:
        error_df.write_csv(error_out)

    return error_df


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
    file_path = Path(output_path) / f

    if not file_path.exists():
        loader.download_post(post, target=file_path)
    else:
        print(f'id exists already')


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


def merge_all(dir_path: PathLike, final_out: PathLike = None) -> pl.DataFrame:
    messages = {1: 'Humour', 2: 'Shock/Disgust/Fear', 3: 'Educational/Informative',
                4: 'Personal Stories', 5: 'Opportunistic', 6: 'Advocacy', 9: 'Non-selected'}
    selected_keys = {key: value for key, value in messages.items() if key in [1, 2, 3, 4, 5, 6]}
    ret = collections.defaultdict(list)
    original_df = pl.read_excel(dir_path / 'original_instagram_data.xlsx')
    error_df = pl.read_csv(dir_path / 'error_out_test.csv')
    postprocess_df = pl.read_csv(dir_path / 'n_images_video_with_id.csv')
    merge_df = postprocess_df.join(error_df, on='id', how='outer')
    merge_df1 = original_df.join(merge_df, on='id')

    def select_images(dir_path: PathLike, label_out: PathLike = None) -> pl.DataFrame:
        label_df = pl.read_csv(dir_path / 'test2.csv')
        post_id = label_df['filename'].to_list()
        for ids in post_id:
            ret['id'].append(ids.split('_')[1])
        note = label_df['notes'].to_list()
        for notes in note:
            if notes in selected_keys:
                ret['selected_images'].append(str(1))  # selected
                ret['notes'].append(str(notes))
            else:
                ret['selected_images'].append(str(0))  # non-selected
                ret['notes'].append(str(notes))
        label_df = pl.DataFrame(ret)
        label_df = label_df.group_by('id').agg([pl.col('selected_images', 'notes')])
        label_df = label_df.with_columns(pl.col('id').cast(pl.Int64))

        if label_out is not None:
            label_df.write_excel(label_out)

        return label_df

    final_df = select_images(dir_path)
    final_df = merge_df1.join(final_df, on='id', how='inner')

    if final_out is not None:
        final_df.write_excel(final_out)

    return final_df


if __name__ == '__main__':
    d = '/Users/wei/Documents/cara_network/amr_igdata/json_file'
    info = create_latestpost_info(d)
    output_path = Path('/Users/wei/Documents/cara_network/amr_igdata/instagram_images_with_dir')
    error_out = Path('/Users/wei/Documents/cara_network/amr_igdata/output/error_out_test.csv')
    download_image(info, output_path, error_out)
    new_dir = Path('/Users/wei/Documents/cara_network/amr_igdata/instagram_images')
    save_path = '/Users/wei/Documents/cara_network/amr_igdata/output/n_images_video_with_id.csv'
    ret = download_postprocess(output_path, new_dir, out=save_path)

    dir_path = Path('/Users/wei/Documents/cara_network/amr_igdata/output')
    final_out = Path('/Users/wei/Documents/cara_network/amr_igdata/output/final_test.xlsx')
    merge_all(dir_path, final_out)
