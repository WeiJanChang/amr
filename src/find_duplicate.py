import pandas as pd
from pathlib import Path


def find_duplicated(file: Path | str):
    """
    1. merge each csv file (11 keywords)
    2. find duplicate ID
    3. Remove duplicate ID and save to a new csv file
    """
    try:
        df = pd.read_csv(file)
        # find duplicate rows by 'ID'
        duplicates = df[df.duplicated(subset='ID', keep=False)]
        duplicate_indices_grouped = duplicates.groupby('ID').apply(lambda x: x.index.tolist())
        duplicate_indices_flat = duplicates.index.tolist()

        # Mark duplicates
        df['Note'] = ''
        df.loc[duplicate_indices_flat, 'Note'] = 'This is duplicate'

        # Drop duplicates
        deduplicated_df = df.drop_duplicates(subset='ID', keep='first')
        deduplicated_df.to_csv('../test_file/removed_duplicate_id.csv', index=False)
        original_count = df.shape[0]
        print(f"Duplicate indices found for IDs:\n{duplicate_indices_grouped}")
        print(f"Removed {original_count - deduplicated_df.shape[0]} duplicates.")
        print(f"Remaining rows: {deduplicated_df.shape[0]}")

    except Exception as e:
        print(f"error：{e}")
    return


if __name__ == '__main__':
    find_duplicated('../test_file/igdata.csv')
