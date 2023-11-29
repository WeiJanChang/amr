from pathlib import Path
from typing import Union
import pandas as pd

PathLike = Union[Path | str]

messages = {1: 'Humour', 2: 'Shock/Disgust/Fear', 3: 'Educational/Informative',
            4: 'Personal Stories', 5: 'Opportunistic', 6: 'Advocacy', 9: 'Non-selected'}


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
    df = pd.read_excel("/Users/wei/Documents/cara_network/amr_igdata/output/kappa_test_file_27Sep.xlsx")
    cara_messages(df)
