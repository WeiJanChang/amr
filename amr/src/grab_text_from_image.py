from pathlib import Path
from typing import Union, Tuple, List
import cv2
import numpy as np
import pytesseract


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
    p = '/Users/wei/Documents/Image,Video with texts_Instagram_2 Aug/2017-10-11_22-53-04_UTC.jpg'
    ret = extract_text(p, config="r'--psm 6'")

