"""Image related processings"""

import os

from PIL import (
    Image
)


EXT_JPG = '.jpg'
EXT_PNG = '.png'

# default resolution if not provided
# for both dimensions
DEFAULT_DPI = (300, 300)


def get_imageinfo(path_img_dir):
    """Calculate image features"""

    mps = 0
    dpi = 0
    if os.path.exists(path_img_dir):
        imag = Image.open(path_img_dir)
        (width, height) = imag.size
        mps = (width * height) / 1000000
        if 'dpi' in imag.info:
            dpi = imag.info['dpi'][0]
    return mps, dpi


def sanitize_image(image_file_path, work_dir_sub):
    """Preprocess image data
    * store from source dir into future OCR-D workspace
    * sanitize DPI metadata
    * convert into PNG format
    """

    # sanitize file extension
    if not str(image_file_path).endswith(EXT_JPG):
        image_file_path = f"{image_file_path}{EXT_JPG}"

    input_image = Image.open(image_file_path)
    file_name = os.path.basename(image_file_path)
    # store image one level inside the workspace
    image_max_dir = os.path.join(work_dir_sub, 'MAX')
    if not os.path.isdir(image_max_dir):
        os.mkdir(image_max_dir)
    output_path = os.path.join(image_max_dir, file_name).replace(EXT_JPG, EXT_PNG)
    res_dpi = DEFAULT_DPI
    if 'dpi' in input_image.info:
        res_dpi = input_image.info['dpi']
    # store resolution for PNG image in both x,y dimensions
    input_image.save(output_path, format='png', dpi=res_dpi)
    return output_path


def is_jpg(a_file:str):
    """Check whether file extension
    indicates JPG-format"""
    _as_string = a_file.lower()
    return _as_string.endswith("jpg") or _as_string.endswith("jpeg")
