import collections
from pathlib import Path
import polars as pl
from Ig_info import PathLike

messages = {1: 'Humour', 2: 'Shock/disgust/fear', 3: 'Educational/informative',
            4: 'Personal stories', 5: 'Opportunistic', 6: 'Advocacy', 9: 'Non-selected'}
selected_keys = {key: value for key, value in messages.items() if key in [1, 2, 3, 4, 5, 6]}
ret = collections.defaultdict(list)


def select_images(dir_path: PathLike) -> pl.DataFrame:
    label_df = pl.read_csv(dir_path / 'test2.csv')
    post_id = label_df['filename'].to_list()
    for ids in post_id:
        ret['id'].append(ids.split('_')[1])
    note = label_df['notes'].to_list()
    for notes in note:
        if notes in selected_keys:
            ret['selected_images'].append(1)  # selected
            ret['notes'].append(notes)
        else:
            ret['selected_images'].append(0)  # non-selecte
            ret['notes'].append(notes)

    label_df = pl.DataFrame(ret)

    label_df = label_df.group_by('id').agg([pl.col('selected_images', 'notes')])
    label_df = label_df.with_columns(pl.col('id').cast(pl.Int64))
    label_df.write_excel('/Users/wei/Documents/cara_network/amr_igdata/output/label_test.xlsx')
    return label_df


if __name__ == '__main__':
    dir_path = Path('/Users/wei/Documents/cara_network/amr_igdata/output')
    select_images(dir_path)
