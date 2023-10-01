"""
pipeline:
merge df from Simon's application (images) and original instagram data (11 json files)
"""
import collections
from pathlib import Path
from typing import Union
import pandas as pd
import polars as pl

PathLike = Union[Path | str]

messages = {1: 'Humour', 2: 'Shock/Disgust/Fear', 3: 'Educational/Informative',
            4: 'Personal Stories', 5: 'Opportunistic', 6: 'Advocacy', 9: 'Non-selected'}
selected_keys = {key: value for key, value in messages.items() if key in [1, 2, 3, 4, 5, 6]}


def merge_all(dir_path: PathLike, final_out: PathLike = None) -> pl.DataFrame:
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
        final_df.write_parquet(final_out)

    return final_df


def cara_messages(df: pd.DataFrame):
    from sklearn.metrics import cohen_kappa_score
    df['cat_1_message'] = df['Cat 1'].map({v: k for k, v in messages.items()})
    df['wei_message'] = df['Wei Cat 1'].map({v: k for k, v in messages.items()})
    df['sana_message'] = df['Sana Cat 1'].map({v: k for k, v in messages.items()})
    df['akke_message'] = df['Akke_cat1'].map({v: k for k, v in messages.items()})
    df['patricia_message'] = df['Patricia_cat1'].map({v: k for k, v in messages.items()})
    df.fillna(0, inplace=True)
    # Calculate Cohen's Kappa
    kappa_cat1_wei = cohen_kappa_score(df['cat_1_message'], df['wei_message'])
    print(f"Cohen's Kappa between cat_1_message and wei_message: {kappa_cat1_wei}")

    kappa_cat1_sana = cohen_kappa_score(df['cat_1_message'], df['sana_message'])
    print(f"Cohen's Kappa between cat_1_message and sana_message: {kappa_cat1_sana}")


if __name__ == '__main__':
    dir_path = Path('/Users/wei/Documents/cara_network/amr_igdata/output')
    final_out = Path('/Users/wei/Documents/cara_network/amr_igdata/output/final_test.parquet')
    merge_all(dir_path, final_out)
    # df = pd.read_excel("/Users/wei/Documents/cara_network/amr_igdata/output/kappa_test_file_27Sep.xlsx")
    # cara_messages(df)
