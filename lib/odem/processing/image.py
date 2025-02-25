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


def ensure_image_format(image_file_path, work_dir_sub):
    """Preprocess image data
    * sanitize types
    * sanitze file extension if missing due download
    * store from source dir into future OCR-D workspace
    * sanitize DPI metadata
    * write as PNG format into specific directory
    """

    # sanitize
    if not isinstance(image_file_path, Path):
        image_file_path = Path(image_file_path)
    if not image_file_path.suffix:
        image_file_path = image_file_path.with_suffix(EXT_JPG)
    if not isinstance(work_dir_sub, Path):
        work_dir_sub = Path(work_dir_sub)
    # enforce image format png with DPI set in both dimensions
    input_image = Image.open(image_file_path)
    res_dpi = DEFAULT_DPI
    if 'dpi' in input_image.info:
        res_dpi = input_image.info['dpi']
    # store image one level inside down in workspace
    image_max_dir = work_dir_sub / "MAX"
    if not image_max_dir.is_dir():
        image_max_dir.mkdir()
    output_path = image_max_dir / image_file_path.name
    if image_file_path.suffix != ".png":
        file_name = image_file_path.stem
        output_path = image_max_dir / f"{file_name}{EXT_PNG}"
    # store sanitized image as png
    input_image.save(output_path, format='png', dpi=res_dpi)
    return output_path


def has_image_ext(a_file:str):
    """Check whether file extension
    indicates JPG-format"""
    _as_string = a_file.lower()
    _suffix = Path(_as_string).suffix
    return len(_suffix) > 0 and _suffix in IMAGE_EXTS
