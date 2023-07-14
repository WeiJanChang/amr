import pandas as pd
from typing import Optional, List, Dict
from pathlib import Path
import numpy as np

def cleandata(df: pd.DataFrame,
              column_drop: Optional[List[str]] = None,
              save_path: Optional[Path] = None) -> pd.DataFrame:
    df = df.copy()
    if column_drop is not None:
        df = df.drop(columns=column_drop)

    if save_path is not None:
        df.to_excel(save_path)

    return df


if __name__ == '__main__':
    df = pd.read_csv(
        "/Users/wei/Google 雲端硬碟/Job Application 2023/CARA Network/AMR /AMR Instagram data/Antimicrobial resistance/Antimicrobial resistance 01 Jan 2017 - 01 July 2023.csv")

    column_drop = ['id', 'topPostsOnly', 'profilePicUrl', 'postsCount', 'topPosts', 'latestPosts']
    save_path = Path('/Users/wei/Google 雲端硬碟/Job Application 2023/CARA Network/AMR /AMR Instagram data/Antimicrobial resistance/test.csv')

    cleaned_df = cleandata(df, column_drop=column_drop, save_path=save_path)
