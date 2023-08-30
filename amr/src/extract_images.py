import os
import pandas as pd
import instaloader
from pathlib import Path
from typing import Union, Tuple
import cv2
import numpy as np
import pytesseract


instagram_df = pd.read_csv(
    '/Users/wei/Job Application 2023/CARA Network/AMR /AMR Instagram data/all_Instagram_data(non-English excluded).csv')


# Step 1 Download images from all_Instagram_data(non-English excluded).csv

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
        print("can't open URL")
    except instaloader.exceptions.ConnectionException as e:
        print("connect error:", e)
    except instaloader.exceptions.ProfileNotExistsException:
        print("user doesn't exist")
    except instaloader.exceptions.TwoFactorAuthRequiredException:
        print("Need authorized")
    except Exception as e:
        print("download error:", e)


def main_instagram():
    # save path
    output_folder = 'Instagram images'
    if not os.path.exists(output_folder):
        os.makedirs(output_folder)

    for index, row in instagram_df.iterrows():
        instagram_url = row['URL']
        image_name = f"{row['name']}_{row['ID']}.jpg"
        image_path = output_folder
        download_instagram_image(instagram_url, row['name'], row['ID'], image_path)



def extract_text(file: Union[Path, str],
                 verify_plot=True,
                 **kwargs) -> Tuple[bool, str]:
    """
    extract text from image

    :param file:
    :param verify_plot: plot original & preprocessed images.
    :param kwargs:
    :return:
    """

    img = cv2.imread(str(file))[..., ::-1]
    img_proc = _preprocess_image(img)
    extract = pytesseract.image_to_string(img_proc, **kwargs)
    has_text = True if len(extract) else False
    print(f'EXTRACT: {extract}')
    if verify_plot:
        import matplotlib.pyplot as plt
        fig, ax = plt.subplots(1, 2)
        ax[0].imshow(img)
        ax[1].imshow(img_proc)
        plt.title(extract)
        plt.show()


    return has_text, extract


def _preprocess_image(image: np.ndarray) -> np.ndarray:
    img = cv2.cvtColor(image, cv2.COLOR_RGB2GRAY)
    img = cv2.adaptiveThreshold(img, 100,
                                cv2.ADAPTIVE_THRESH_MEAN_C,
                                cv2.THRESH_BINARY,
                                15, 16)

    return img


if __name__ == '__main__':
# run .jpg in files and return img
# todo: how to save them into csv and don't return each img. need run them all

    directory = '/Users/wei/Library/CloudStorage/GoogleDrive-wei-jan.chang@ucdconnect.ie/.shortcut-targets-by-id/10hvZ9DULDsQxq93eg2SZ3tFyFCP37DnA/Wei-Jan/Image,Video with texts_Instagram 2/selected'
    jpg_files = list(Path(directory).glob('*.jpg'))

    results = []
    for file_path in jpg_files:
        has_text, extract = extract_text(file_path, config="--psm 6")
        results.append({'Image_Path': str(file_path), 'Extracted_Text': extract})

    df = pd.DataFrame(results)
    df.to_csv('/Users/wei/Job Application 2023/CARA Network/AMR /AMR Instagram data/image_extracted_text.csv', index=False)