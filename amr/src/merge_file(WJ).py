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


if __name__ == '__main__':
    df1 = pd.read_csv(
        '/Users/wei/Documents/cara_network/amr_igdata/AMR/AMR 01 Jan 2017 - 01 July 2023_specific hashtags (non-English excluded).csv')
    df2 = pd.read_csv(
        '/Users/wei/Documents/cara_network/amr_igdata/Antibiotic prescribing/Antibiotic prescribing 01 Jan 2017 - 01 July 2023_specific hashtags (non-English excluded).csv')
    df3 = pd.read_csv(
        '/Users/wei/Documents/cara_network/amr_igdata/Antibiotic resistance/Antibiotic resistance 01 Jan 2017 - 01 July 2023_specific hashtags (non-English excluded).csv')
    df4 = pd.read_csv(
        '/Users/wei/Documents/cara_network/amr_igdata/Antibiotics/Antibiotics 01 Jan 2017 - 01 July 2023_specific hashtags (non-English excluded).csv')
    df5 = pd.read_csv(
        '/Users/wei/Documents/cara_network/amr_igdata/Antimicrobial resistance/Antimicrobial resistance 01 Jan 2017 - 01 July 2023_specific hashtags (non-English excluded).csv')
    df6 = pd.read_csv(
        '/Users/wei/Documents/cara_network/amr_igdata/Antimicrobial stewardship/Antimicrobial stewardship 01 Jan 2017 - 01 July 2023_specific hashtags (non-English excluded).csv')
    df7 = pd.read_csv(
        '/Users/wei/Documents/cara_network/amr_igdata/Antimicrobials/Antimicrobials 01 Jan 2017 - 01 July 2023_specific hashtags (non-English excluded).csv')
    df8 = pd.read_csv(
        '/Users/wei/Documents/cara_network/amr_igdata/Bacterial infections/Bacterial infections 01 Jan 2017 - 01 July 2023_specific hashtags (non-English excluded).csv')
    df9 = pd.read_csv(
        '/Users/wei/Documents/cara_network/amr_igdata/Drug resistant/Drug resistant 01 Jan 2017 - 01 July 2023_specific hashtags (non-English excluded).csv')
    df10 = pd.read_csv(
        '/Users/wei/Documents/cara_network/amr_igdata/Infections/Infections 01 Jan 2017 - 01 July 2023_specific hashtags (non-English excluded).csv')
    df11 = pd.read_csv(
        '/Users/wei/Documents/cara_network/amr_igdata/Superbugs/Superbugs 01 Jan 2017 - 01 July 2023_specific hashtags (non-English excluded).csv')
    instagram_df = pd.concat([df1, df2, df3, df4, df5, df6, df7, df8, df9, df10, df11])
    instagram_df.to_csv(
        '/Users/wei/Documents/cara_network/amr_igdata/all_Instagram_data(non-English excluded).csv',
        index=False)

    num_rows = instagram_df.shape[0]
    num_cols = instagram_df.shape[1]

    print("all_Instagram_data(non-English excluded).csv row and col：", instagram_df.shape)
    #
    #
    # p = '/Users/wei/Job Application 2023/CARA Network/AMR /AMR Instagram data/all_Instagram_data(non-English excluded).csv'
    # save_path = '/Users/wei/Job Application 2023/CARA Network/AMR /AMR Instagram data/all_Instagram_data(non-English excluded)- duplicate id marked.csv'
    #
