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

"""
import os
import csv
import demoji
import pandas as pd

folder_path = '/Users/wei/Library/CloudStorage/GoogleDrive-wei-jan.chang@ucdconnect.ie/.shortcut-targets-by-id/10hvZ9DULDsQxq93eg2SZ3tFyFCP37DnA/Wei-Jan/Image,Video with texts_Instagram 1'
output_csv_name = '/Users/wei/Job Application 2023/CARA Network/AMR /AMR Instagram data/Instagram 1.csv'

# get txt file
txt_files = [file for file in os.listdir(folder_path) if file.endswith('.txt')]

# read txt
data = []
for txt_file in txt_files:
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

def remove_emojis(text):
    """
    :param text: all text in files
    :return: modified text with any emojis removed
    """
    return demoji.replace(text, "")


instagram_1 = pd.read_csv('/Users/wei/Job Application 2023/CARA Network/AMR /AMR Instagram data/Instagram 1.csv')
instagram_2 = pd.read_csv('/Users/wei/Job Application 2023/CARA Network/AMR /AMR Instagram data/Instagram 2.csv')
combine_df = pd.concat([instagram_1,instagram_2])
combine_df["Caption"] = combine_df["Caption"].apply(remove_emojis)
combine_df.to_csv(
    '/Users/wei/Job Application 2023/CARA Network/AMR /AMR Instagram data/Instagram 1 + 2.csv',
    index=False)


Instagram_df =pd.read_csv('/Users/wei/Job Application 2023/CARA Network/AMR /AMR Instagram data/all_Instagram_data(non-English excluded).csv')

merged_df = pd.merge(Instagram_df, combine_df, on='Caption', how='inner')


merged_df.to_csv('/Users/wei/Job Application 2023/CARA Network/AMR /AMR Instagram data/merged.csv', index=False)
