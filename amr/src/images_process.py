import os
# Use OS to create, remove, change and list directories and files, run commands, get environment variables,
# manage processes
import pandas as pd
import glob
import shutil

# confirm how many number of video, jpg, and txt files
txt_folder1 = '/Users/wei/Library/CloudStorage/GoogleDrive-wei-jan.chang@ucdconnect.ie/.shortcut-targets-by-id/10hvZ9DULDsQxq93eg2SZ3tFyFCP37DnA/Wei-Jan/Image,Video with texts_Instagram 1'
txt_folder2 = '/Users/wei/Library/CloudStorage/GoogleDrive-wei-jan.chang@ucdconnect.ie/.shortcut-targets-by-id/10hvZ9DULDsQxq93eg2SZ3tFyFCP37DnA/Wei-Jan/Image,Video with texts_Instagram 2'

video_files = glob.glob(os.path.join(txt_folder1, '*.mp4')) + glob.glob(os.path.join(txt_folder1, '*.avi'))
jpg_files = glob.glob(os.path.join(txt_folder1, '*.jpg'))
txt_files = glob.glob(os.path.join(txt_folder1, '*.txt'))

# print(f"Number of video files: {len(video_files)}")
# print(f"Number of JPG files: {len(jpg_files)}")
# print(f"Number of TXT files: {len(txt_files)}")

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
# def read_file_with_encoding(file_path, encodings):
#     for encoding in encodings:
#         try:
#             with open(file_path, 'r', encoding=encoding) as file:
#                 return file.readlines()
#         except UnicodeDecodeError:
#             pass
#     return None
#
# instagram_df ='/Users/wei/Job Application 2023/CARA Network/AMR /AMR Instagram data/all_Instagram_data(non-English excluded).csv'
# output_folder = 'test'
#
# df = pd.read_csv(instagram_df)
# caption_column_index = df.columns.get_loc('Caption')
# # 建立輸出資料夾
# os.makedirs(output_folder, exist_ok=True)
# # 遍歷.txt檔案，刪除沒有對應'Caption'的檔案並移動到輸出資料夾
# for folder in [txt_folder1]:
#     for txt_file in os.listdir(folder):
#         if txt_file.endswith('.txt'):
#             txt_file_path = os.path.join(folder, txt_file)
#
#             lines = read_file_with_encoding(txt_file_path, ['utf-8', 'utf-16', 'gbk'])
#             if lines is not None:
#                 caption_found = any(df.iloc[:, caption_column_index] in line for line in lines)
#                 if not caption_found:
#                     output_file_path = os.path.join(output_folder, txt_file)
#                     shutil.move(txt_file_path, output_file_path)
#             else:
#                 os.remove(txt_file_path)
# output_excel_path = os.path.join(output_folder, 'modified_data.xlsx')
# df.to_excel(output_excel_path, index=False)
# print("Finish")
#
#


import os
import pandas as pd
import glob
import shutil

def read_file_with_encoding(file_path, encodings):
    for encoding in encodings:
        try:
            with open(file_path, 'r', encoding=encoding) as file:
                return file.readlines()
        except UnicodeDecodeError:
            pass
    return None

def main():
    txt_folder = '/Users/wei/Library/CloudStorage/GoogleDrive-wei-jan.chang@ucdconnect.ie/.shortcut-targets-by-id/10hvZ9DULDsQxq93eg2SZ3tFyFCP37DnA/Wei-Jan/Image,Video with texts_Instagram 1'
    output_folder = 'test'

    # 讀取CSV文件，取得'Caption'所在的列索引
    instagram_df = '/Users/wei/Job Application 2023/CARA Network/AMR /AMR Instagram data/all_Instagram_data(non-English excluded).csv'
    df = pd.read_csv(instagram_df)
    caption_column_index = df.columns.get_loc('Caption')

    # 建立輸出資料夾
    os.makedirs(output_folder, exist_ok=True)

    # 遍歷.txt檔案，刪除沒有對應'Caption'的檔案並移動到輸出資料夾
    for txt_file in os.listdir(txt_folder):
        if txt_file.endswith('.txt'):
            txt_file_path = os.path.join(txt_folder, txt_file)

            lines = read_file_with_encoding(txt_file_path, ['utf-8', 'utf-16', 'gbk'])
            if lines is not None:
                caption_found = any(df.iloc[:, caption_column_index] in line for line in lines)
                if not caption_found:
                    output_file_path = os.path.join(output_folder, txt_file)
                    shutil.move(txt_file_path, output_file_path)
            else:
                os.remove(txt_file_path)

    # 儲存新的 DataFrame 為 Excel 檔案
    output_excel_path = os.path.join(output_folder, 'modified_data.xlsx')
    df.to_excel(output_excel_path, index=False)

    print("檔案處理完成，並已儲存為 Excel 檔案")

if __name__ == "__main__":
    main()



# instagram_df = pd.read_csv(
#     '/Users/wei/Job Application 2023/CARA Network/AMR /AMR Instagram data/all_Instagram_data(non-English excluded).csv')
#
#
# # find files named .txt
# txt_files = [f for f in os.listdir(instagram_df) if f.endswith('.txt')]
#
# # 建立一個用來判斷是否保留的布林值的列表
# keep_row = [False] * len(instagram_df)
#
# # 建立一個用來儲存被刪除的資料列的資料框
# deleted_df = pd.DataFrame(columns=instagram_df.columns)
#
# # 迭代每個.txt檔案，檢查其內容是否存在於 df['Caption']，若是則將相應的布林值設為 True
# for idx, file_name in enumerate(txt_files):
#     file_path = os.path.join(instagram_df, file_name)
# with open(file_path, 'r', encoding='utf-8') as file:
#     file_content = file.read().strip()  # 讀取.txt檔案的內容並去除首尾空白
# if file_content in instagram_df['Caption'].values:
#     # 檔案內容存在於 df['Caption']，將對應的資料列設為保留
#     keep_row[instagram_df[instagram_df['Caption'] == file_content].index[0]] = True
# else:
#     # 將要刪除的資料列儲存到 deleted_df
#     deleted_df = deleted_df.append(instagram_df[instagram_df['Caption'] == file_content], ignore_index=True)
#
# # 刪除不符合條件的資料列
# df = instagram_df[keep_row]
#
# # 儲存修改後的資料框
# df.to_csv('updated_dataframe.csv', index=False)
#
# # 儲存被刪除的資料列
# deleted_df.to_csv('deleted_dataframe.csv', index=False)
#
# print("完成資料列刪除並儲存被刪除的資料列。")
