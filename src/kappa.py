from pathlib import Path
from typing import Union
import pandas as pd
from sklearn.metrics import cohen_kappa_score

PathLike = Union[Path | str]

messages = {1: 'Humour', 2: 'Shock/Disgust/Fear', 3: 'Educational/Informative',
            4: 'Personal Stories', 5: 'Opportunistic', 6: 'Advocacy', 9: 'Non-selected'}


def cal_kappa(df: pd.DataFrame, coder_1: str, coder_2: str) -> float:
    """
    Compute Cohen’s kappa to measure inter-annotator agreement.
    The kappa statistic, which is a number between -1 and 1.
    The maximum value means complete agreement; zero or lower means chance agreement.

    :param df: df
    :param coder_1: the first coder
    :param coder_2: the second coder
    :return: kappa score between coder 1 and coder 2
    """
    # df['cat_1_message'] = df['category'].map({v: k for k, v in messages.items()})
    # df['coder1_message'] = df['coder1'].map({v: k for k, v in messages.items()})
    # df['coder2_message'] = df['coder2'].map({v: k for k, v in messages.items()})
    # df['coder3_message'] = df['coder3'].map({v: k for k, v in messages.items()})
    # df['coder4_message'] = df['coder4'].map({v: k for k, v in messages.items()})

    # pre-processing data: remove na in both rows to calculate Cohen's kappa
    filter_df = df[[coder_1, coder_2]].dropna()
    # Calculate Cohen's Kappa
    kappa_value = cohen_kappa_score(filter_df[coder_1], filter_df[coder_2])

    print(f"Cohen's Kappa between {coder_1} and {coder_2}: {kappa_value}")

    return kappa_value


if __name__ == '__main__':
    df = pd.read_excel("~/code/amr/test_file/coders_messages.xlsx")
    cal_kappa(df, coder_1='coder1', coder_2='coder3')
