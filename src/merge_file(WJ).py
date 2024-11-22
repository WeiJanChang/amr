"""
pipeline

1. merge each csv file (11 keywords)
2. find duplicate ID
3. Remove duplicate ID and save to a new csv file
"""
import pandas as pd
from typing import Union, Optional
from pathlib import Path

def find_duplicated(file: Path | str,
                    save_path: Optional[Path] = None) -> bool:
    """
    read csv file, find duplicate row in Caption and add ****** in Index, save as new csv file
    """
    try:
        df = pd.read_csv(file)

        # find duplicate row
        duplicates = df[df.duplicated(subset='ID', keep=False)]
        duplicate_captions_index = duplicates.index

        # Group duplicate rows by 'Caption' and get the corresponding indices
        duplicate_indices = duplicates.groupby('ID').apply(lambda x: x.index.tolist())

        # Output duplicate indices
        print("Duplicate Indices:")
        for indices in duplicate_indices:
            print(indices)
            indices = pd.Series(indices)
            print(indices.to_csv())

        # add 'This is duplicate' at Index
        duplicate_data = df.rename(index={index: f'This is duplicate{index}' for index in duplicate_captions_index})

        # save
        duplicate_data.to_csv(save_path)
        duplicate_data.drop_duplicates(subset='ID', keep='first', inplace=True)
        duplicate_data.to_csv(
            '/Users/wei/Job Application 2023/CARA Network/AMR /AMR Instagram data/all_Instagram_data(non-English excluded)- remove duplicate id marked.csv')
        print("duplicate index：", duplicate_captions_index.tolist())
        print('row (After removing):', duplicate_data.shape[0])
        print('duplicate rows in total:', 3355 - duplicate_data.shape[0])

    except Exception as e:
        print(f"error：{e}")