"""Specification for image processing"""

from pathlib import Path

import pytest

import lib.odem.processing.image as o_imgp

from .conftest import create_test_tif


@pytest.mark.parametrize("a_file,is_image",
                         [
                             ('/foo/bar', False),
                             ('file:///foo.jpeg', True),
                             ('/foo/bar.txt', False),
                             ('/data/ocr/foo/bar.tif', True),
                             ('http://host.domain/res/001.jpg', True),
                             ('http://host.domain/res/001', False),
                         ])
def test_odem_recognize_as_image(a_file, is_image):
    """Ensure image recognitions via path/url works"""

    assert o_imgp.has_image_ext(a_file) == is_image


def test_format_tif_imput(tmp_path: Path):
    """Ensure TIF-Data properly stored"""

    path_img = tmp_path / "my.tif"
    work_dir = tmp_path / "wrk-dir"
    work_dir.mkdir()
    create_test_tif(path_img)
    assert path_img.is_file()

    result = o_imgp.ensure_image_format(path_img, work_dir)
    if not isinstance(result, Path):
        result = Path(result)
    assert result.is_file()
    assert result.suffix == ".png"
