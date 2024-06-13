"""Specification for image processing"""

import pytest


from lib.odem.processing.image import (
    has_image_ext,
)


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

    assert has_image_ext(a_file) == is_image
