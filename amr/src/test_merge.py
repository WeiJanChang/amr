import collections
from pathlib import Path
from typing import Union

import polars as pl

PathLike = Union[Path | str]


def merge_all(dir_path: PathLike, final_out: PathLike = None) -> pl.DataFrame:
    ret = collections.defaultdict(list)
    original_df = pl.read_excel(dir_path / 'original_instagram_data.xlsx')
    error_df = pl.read_csv(dir_path / 'error_out_test.csv')
    postprocess_df = pl.read_csv(dir_path / 'n_images_video_with_id.csv')
    merge_df = postprocess_df.join(error_df, on='id', how='outer')
    merge_df1 = original_df.join(merge_df, on='id')

    def select_images(dir_path: PathLike) -> pl.DataFrame:

        label_df = pl.read_csv(dir_path / 'test2.csv')

        post_id = label_df['filename'].to_list()
        for ids in post_id:
            ret['id'].append(ids.split('_')[1])
        note = label_df['notes'].to_list()
        for notes in note:
            if 1 <= notes <= 8:
                ret['selected_images'].append(True)
            else:
                ret['selected_images'].append(False)
        label_df = pl.DataFrame(ret)
        label_df = label_df.group_by('id').agg([pl.col('selected_images')])
        label_df = label_df.with_columns(pl.col('id').cast(pl.Int64))
        return label_df

    final_df = select_images(dir_path)
    final_df = merge_df1.join(final_df, on='id', how='outer')
    if final_out is not None:
        final_df.write_excel(final_out)


if __name__ == '__main__':
    dir_path = Path('/Users/wei/Documents/cara_network/amr_igdata/output')
    final_out = Path('/Users/wei/Documents/cara_network/amr_igdata/output/final_test.xlsx')
    merge_all(dir_path, final_out)
