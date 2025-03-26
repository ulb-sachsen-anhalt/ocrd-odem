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
    """Calculate image features and avoid
    fraction values like 299.xxx via rounding
    """

    mps = 0
    dpi = 0
    if os.path.exists(path_img_dir):
        imag = Image.open(path_img_dir)
        (width, height) = imag.size
        mps = (width * height) / 1000000
        if 'dpi' in imag.info:
            dpi = round(imag.info['dpi'][0])
    return mps, dpi


def ensure_format_png(image_file_path ):
    """Preprocess image data
    * sanitze file extension if missing due download
    * enforce png format with DPI set in both dimensions
    """

    # sanitize
    if not isinstance(image_file_path, Path):
        image_file_path = Path(image_file_path)
    if not image_file_path.suffix:
        image_file_path = image_file_path.with_suffix(EXT_JPG)
    input_image = Image.open(image_file_path)
    res_dpi = DEFAULT_DPI
    if 'dpi' in input_image.info:
        res_dpi = tuple(int(d) for d in input_image.info["dpi"])
    output_path = image_file_path.with_suffix(EXT_PNG) #.parent / image_file_path.name
    input_image.save(output_path, format='png', dpi=res_dpi)
    return output_path


def has_image_ext(a_file:str):
    """Check whether file extension
    indicates JPG-format"""
    _as_string = a_file.lower()
    _suffix = Path(_as_string).suffix
    return len(_suffix) > 0 and _suffix in IMAGE_EXTS
