import pandas as pd
from typing import Optional, List

__all__ = [
    'descriptive_stats'
]


# Category counts
def descriptive_stats(df: pd.DataFrame, col_name: str, groupby_col: Optional[List[str]] = None):
    """
    Display and return descriptive statistics (count & percentage) for a column.
    If groupby_col is provided, also show group-wise summary.

    :param df: post_processed_data
    :param col_name: col for calculate
    :param groupby_col: group by column(s)
    :return:
    """
    results = {}
    if col_name not in df.columns:
        print(f"Column '{col_name}' not found in DataFrame.The columns are {df.columns}")
        return results
    # Main descriptive summary
    v_counts = df[col_name].value_counts()
    v_counts = v_counts.sort_values(ascending=False)
    cal_percentage = (v_counts.values / sum(v_counts)) * 100
    # with two decimals
    cal_percentage = cal_percentage.round(2)
    # build a table for it
    dscpt_table = pd.DataFrame(
        {col_name: v_counts.index,
         'Count': v_counts.values,
         'Percentage(%)': cal_percentage
         })
    print("=== Overall Summary Table ===")
    print(dscpt_table)

    if groupby_col:
        if isinstance(groupby_col, str):  # 支援單個字串輸入
            groupby_col = [groupby_col]

        missing_cols = [col for col in groupby_col if col not in df.columns]
        if missing_cols:
            print(f"Groupby columns {missing_cols} not found. The columns are {df.columns.tolist()}")
        else:
            if 'year' in groupby_col and len(groupby_col) > 1:
                grouped = df.groupby(groupby_col)[col_name].sum().unstack('year')
                grouping_table = grouped.reset_index()
            else:
                grouped = df.groupby(groupby_col)[col_name].sum().sort_values(ascending=False)
                group_percent = (grouped / grouped.sum() * 100).round(2)
                grouping_table = grouped.reset_index()
                grouping_table['Percentage'] = group_percent.values

            print(f"=== Grouped by {groupby_col} ===")
            print(grouping_table)
            results['grouping_table'] = grouping_table

        return results


if __name__ == '__main__':
    df = pd.read_excel('~/code/amr/test_file/post_processed_data.xlsx')
    descriptive_stats(df, 'likesCount', groupby_col=['cat', 'year'])
    descriptive_stats(df, col_name='cat')
