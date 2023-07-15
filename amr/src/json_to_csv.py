from pathlib import Path  # pathlib: module, Path: class. Checking if a path exist
from typing import Optional, List, Dict, Tuple  # typing: support for type hint
import pandas as pd
import numpy as np
import collections  # This module contains different datatype to process the data: dict, list, set, and tuple.

__all__ = ['select_df']  # only import 'select_df'


def select_df(df: pd.DataFrame,
              rename_mapping: Dict[str, str] = None,
              column_drop: Optional[List[str]] = None,
              save_path: Optional[Path] = None) -> pd.DataFrame:
    df = df.copy()

    # Find all JSON files in the specified folder
    json_folder = Path('/Users/wei/Google 雲端硬碟/Job Application 2023/CARA Network/AMR/')
    json_files = json_folder.glob('*.json')

    for json_file in json_files:
        try:
            # Load the JSON file as a Pandas DataFrame
            json_data = pd.read_json(json_file)
            # Perform any necessary operations on the DataFrame
            if rename_mapping is not None:
                json_data = json_data.rename(columns=rename_mapping)
            if column_drop is not None:
                json_data = json_data.drop(columns=column_drop)


        except (FileNotFoundError, IOError) as e:
            print(f"Error: Failed to load the JSON file {json_file}.")
            print(e)
            exit()
        except ValueError as e:
            print(f"Error: Failed to parse the JSON file {json_file}.")
            print(e)
            exit()


     # Get the save path for the CSV file
    if save_path is not None:
        csv_file = json_file.with_suffix('.csv')
        save_path = json_folder / csv_file.name
        json_data.to_csv(save_path)
    return df


# Extract captions and URLs
def extract_data(posts):
    captions = []
    urls = []
    for i, post in enumerate(posts, start=1):
        if 'caption' in post:
            caption = f"{i}. {post['caption']}"
            if caption not in captions:  # Check for duplicate captions
                captions.append(caption)
        if 'url' in post:
            url = f"{i}. {post['url']}"
            if url not in urls:  # Check for duplicate URLs
                urls.append(url)
    return captions, urls

    df['Caption'], df['URL'] = zip(*df['latestPosts'].apply(extract_captions))

    # Save the modified DataFrame to a CSV file
    if save_path is not None:
        df.to_csv(save_path)

    # Print the captions and URLs for easy reference
    for i, (captions, urls) in enumerate(zip(df['Caption'], df['URL']), start=1):
        print(f"Post {i}:")
        for caption in captions:
            print(f"  Caption: {caption}")
        for url in urls:
            print(f"  URL: {url}")
        print()

    # Print a success message
    print("Data successfully processed and saved to modified_test.csv.")


if __name__ == '__main__':
    df = pd.read_json('/Users/wei/Google 雲端硬碟/Job Application 2023/CARA Network/AMR')
select_df(df)
