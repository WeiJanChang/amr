import os
import requests
from bs4 import BeautifulSoup
import pandas as pd
from PIL import Image
import instaloader
import requests

all_ig = pd.read_csv(
    '/Users/wei/Job Application 2023/CARA Network/AMR /AMR Instagram data/all_Instagram_data(non-English excluded).csv')
"""

# can't download complete images
def download_instagram_image(post_url, name, ID, save_path):
    try:
        response = requests.get(post_url)
        response.raise_for_status()
        soup = BeautifulSoup(response.text, 'html.parser')

        # search URL
        image_url = soup.find('meta', property='og:image')['content']

        # download images
        image_response = requests.get(image_url)
        image_response.raise_for_status()
        with open(save_path, 'wb') as f:
            f.write(image_response.content)

        print(f"Images {name}_{ID} saved as {save_path}")
    except requests.exceptions.HTTPError as errh:
        print("Http Error:", errh)
    except requests.exceptions.ConnectionError as errc:
        print("Error Connecting:", errc)
    except requests.exceptions.Timeout as errt:
        print("Timeout Error:", errt)
    except requests.exceptions.RequestException as err:
        print("OOps: Something Else", err)
    except (IOError, ValueError, OSError) as err:
        print("images error:", err)
    except TypeError:
        print("can't find correct URL")


# save path
output_folder = 'Instagram images'
if not os.path.exists(output_folder):
    os.makedirs(output_folder)

for index, row in all_ig.iterrows():
    instagram_url = row['URL']
    image_name = f"{row['name']}_{row['ID']}.jpg"
    image_path = os.path.join(output_folder, image_name)
    download_instagram_image(instagram_url, row['name'], row['ID'], image_path)

"""
#NEED API
def download_instagram_image(post_url, name, ID, save_path):
    try:
        loader = instaloader.Instaloader()

        post = instaloader.Post.from_shortcode(loader.context, post_url.split('/')[-2])

        # Generate the filename using the provided name and ID
        image_name = f"{name}_{ID}.jpg"

        # Combine the save_path and image_name to get the full file path
        full_file_path = os.path.join(save_path, image_name)

        loader.download_post(post, target=full_file_path)
        print(f"Image {name}_{ID} saved as {full_file_path}")
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

for index, row in all_ig.iterrows():
    instagram_url = row['URL']
    image_name = f"{row['name']}_{row['ID']}.jpg"
    image_path = output_folder
    download_instagram_image(instagram_url, row['name'], row['ID'], image_path)


