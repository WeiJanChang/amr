import os
import requests
from bs4 import BeautifulSoup
import pandas as pd
from PIL import Image
import instaloader

all_ig = pd.read_csv(
    '/Users/wei/Job Application 2023/CARA Network/AMR /AMR Instagram data/all_Instagram_data(non-English excluded).csv')

def download_instagram_image(post_url, name, ID, save_path):
    try:
        loader = instaloader.Instaloader()

        post = instaloader.Post.from_shortcode(loader.context, post_url.split('/')[-2])

        loader.download_post(post, target=save_path)
        print(f"Images {name}_{ID} saved as {save_path}")
    except instaloader.exceptions.InvalidArgumentException:
        print("無效的帖子URL")
    except instaloader.exceptions.ConnectionException as e:
        print("connect error:", e)
    except instaloader.exceptions.ProfileNotExistsException:
        print("user doesn't exist")
    except instaloader.exceptions.TwoFactorAuthRequiredException:
        print("Need authorized")
    except Exception as e:
        print("download error:", e)

# save path
output_folder = 'Instagram images'
if not os.path.exists(output_folder):
    os.makedirs(output_folder)

# 使用for迴圈遍歷每一行
for index, row in all_ig.iterrows():
    instagram_url = row['URL']
    image_name = f"{row['name']}_{row['ID']}.jpg"
    image_path = output_folder
    download_instagram_image(instagram_url, row['name'], row['ID'], image_path)


