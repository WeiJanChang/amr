"""
Instagram 1 file:
    Number of video files: 14
    Number of JPG files: 499
    Number of TXT files: 334
Instagram 2 file:
    Number of video files: 30
    Number of JPG files: 360
    Number of TXT files: 251

Total video: 44
Total JPG: 859
Total TXT: 585

find these 585 txt in "all_Instagram_data(non-English excluded).csv, save to a new file"
"""
import os
import csv
import demoji
import pandas as pd


# 將 "Caption" 列中的特殊字符和大小寫規範化
def normalize_caption(caption) -> str:
    return caption.lower().strip()


folder_path = '/Users/wei/Library/CloudStorage/GoogleDrive-wei-jan.chang@ucdconnect.ie/.shortcut-targets-by-id/10hvZ9DULDsQxq93eg2SZ3tFyFCP37DnA/Wei-Jan/Image,Video with texts_Instagram 2'
output_csv_name = '/Users/wei/Job Application 2023/CARA Network/AMR /AMR Instagram data/Instagram 2.csv'

# get txt file
txt_files = [file for file in os.listdir(folder_path) if file.endswith('.txt')]

# read txt
data = []
for txt_file in txt_files:  # type: str
    txt_file_path = os.path.join(folder_path, txt_file)
    if os.path.isfile(txt_file_path):
        with open(txt_file_path, 'r', encoding='utf-8') as f:
            content = f.read()
            data.append(content)

# txt to csv
output_csv_path = os.path.join(folder_path, output_csv_name)
with open(output_csv_path, 'w', newline='', encoding='utf-8') as csvfile:
    csvwriter = csv.writer(csvfile)

    # add header
    csvwriter.writerow(['Caption'])
    for item in data:
        csvwriter.writerow([item])


def remove_emojis(text) -> str:

    """
    :param text: all text in files
    :return: modified text with any emojis removed
    """
    return demoji.replace(text, "")


instagram_1 = pd.read_csv('/Users/wei/Job Application 2023/CARA Network/AMR /AMR Instagram data/Instagram 1.csv')
instagram_2 = pd.read_csv('/Users/wei/Job Application 2023/CARA Network/AMR /AMR Instagram data/Instagram 2.csv')
combine_df = pd.concat([instagram_1, instagram_2])
combine_df["Caption"] = combine_df["Caption"].apply(remove_emojis)
combine_df['Caption'] = combine_df['Caption'].astype(str)
combine_df['Caption'] = combine_df['Caption'].apply(normalize_caption)
combine_df.to_csv(
    '/Users/wei/Job Application 2023/CARA Network/AMR /AMR Instagram data/Instagram 1 + 2 (only Caption).csv',
    index=False)
print('Instagram 1 + 2 (only Caption): ', combine_df.shape)
duplicated_combinedf_captions = combine_df[combine_df.duplicated('Caption')]
duplicated_combinedf_captions.to_excel(
    '/Users/wei/Job Application 2023/CARA Network/AMR /AMR Instagram data/IG 1+2 duplicated caption'
    '.xlsx', index=False)

Instagram_df = pd.read_csv(
    '/Users/wei/Job Application 2023/CARA Network/AMR /AMR Instagram data/all_Instagram_data(non-English excluded).csv')
Instagram_df['Caption'] = Instagram_df['Caption'].astype(str)
Instagram_df['Caption'] = Instagram_df['Caption'].apply(normalize_caption)

final_instagram = pd.merge(Instagram_df, combine_df, on='Caption', how='inner')
final_instagram.to_excel(
    '/Users/wei/Job Application 2023/CARA Network/AMR /AMR Instagram data/Instagram 1 + 2 with all information.xlsx',
    index=False)
print('Instagram 1 + 2 with all information: ', final_instagram.shape)
unique_final_instagram_url = final_instagram.drop_duplicates(subset=['URL'])

unique_final_instagram_url.to_excel(
    '/Users/wei/Job Application 2023/CARA Network/AMR /AMR Instagram data/Instagram 1 + 2 with all information (unique URL only).xlsx',
    index=False)
print('unique url in Instagram 1 + 2 with all information: ', unique_final_instagram_url.shape)

duplicated_url = final_instagram[final_instagram.duplicated('URL')]
print('duplicated url in final_instagram: ', duplicated_url.shape)
duplicated_url.to_excel(
    '/Users/wei/Job Application 2023/CARA Network/AMR /AMR Instagram data/duplicated_url in Instagram 1 + 2 with all information.xlsx',
    index=False)

duplicated_caption = unique_final_instagram_url.drop_duplicates(subset=['Caption'])
print('duplicated_caption in unique_final_instagram_url: ', duplicated_caption.shape)
duplicated_caption.to_excel(
    '/Users/wei/Job Application 2023/CARA Network/AMR /AMR Instagram data/duplicated_caption in Instagram 1 + 2 with all information.xlsx',
    index=False)
