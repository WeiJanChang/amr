import os
import csv

import pandas as pd

folder_path = '/Users/wei/Library/CloudStorage/GoogleDrive-wei-jan.chang@ucdconnect.ie/.shortcut-targets-by-id/10hvZ9DULDsQxq93eg2SZ3tFyFCP37DnA/Wei-Jan/Image,Video with texts_Instagram 2'
output_csv_name = '/Users/wei/Job Application 2023/CARA Network/AMR /AMR Instagram data/Instagram 2.csv'

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


instagram_1 = pd.read_csv('/Users/wei/Job Application 2023/CARA Network/AMR /AMR Instagram data/Instagram 1.csv')
instagram_2 = pd.read_csv('/Users/wei/Job Application 2023/CARA Network/AMR /AMR Instagram data/Instagram 2.csv')
combine_df = pd.concat([instagram_1,instagram_2])
combine_df.to_csv(
    '/Users/wei/Job Application 2023/CARA Network/AMR /AMR Instagram data/Instagram 1 + 2.csv',
    index=False)

