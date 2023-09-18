"""Specification for OCR-D related functionalities"""

import pytest

from lib.ocrd3_odem.processing_ocrd import (
    get_recognition_level
)


@pytest.mark.parametrize("model_configuration,recognotion_level",
                         [
                             ('ger', 'word'),
                             ('ara', 'glyph'),
                             ('ara+ger', 'glyph'),
                             ('ger+lat', 'word'),
                             ('fas', 'glyph'),
                             ('ger+lat', 'word'),
                             ('eng+fas', 'glyph')
                         ])
def test_odem_recognition_level(model_configuration, recognotion_level):
    """Check determined recognition level passed
    forth to tesserocr for common model configurations"""

    assert get_recognition_level(model_configuration) == recognotion_level
