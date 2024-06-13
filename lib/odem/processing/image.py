"""Image related processings"""

import os

from pathlib import Path

from PIL import Image

# local file extension concerning image data
EXT_JPG = '.jpg'
EXT_JPEG = '.jpeg'
EXT_PNG = '.png'
EXT_TIF = '.tif'
IMAGE_EXTS = [EXT_JPG, EXT_JPEG, EXT_PNG, EXT_TIF]

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

    # sanitize file extension only if missing
    if not Path(image_file_path).suffix:
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


def has_image_ext(a_file:str):
    """Check whether file extension
    indicates JPG-format"""
    _as_string = a_file.lower()
    _suffix = Path(_as_string).suffix
    return len(_suffix) > 0 and _suffix in IMAGE_EXTS
