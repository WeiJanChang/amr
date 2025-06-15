"""
pipeline
Extract texts from images
"""
from pathlib import Path
from typing import Union, Tuple
import cv2
import numpy as np
import pandas as pd
import pytesseract
from tqdm import tqdm


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
    # print(f'EXTRACT: {extract}')
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
    # run .jpg in files, return img, and save extracted_text to csv file

    directory = 'PATH'
    jpg_files = list(Path(directory).glob('*.jpg'))

    results = []
    for file_path in tqdm(jpg_files, desc='Extracting text'):
        has_text, extract = extract_text(file_path, config="r'--psm 6'", verify_plot=False)
        results.append({'Image_Path': str(file_path), 'Extracted_Text': extract.strip()})

    df = pd.DataFrame(results)
    df.to_csv('~/code/amr/test_file/extracted_text.csv', index=False)
