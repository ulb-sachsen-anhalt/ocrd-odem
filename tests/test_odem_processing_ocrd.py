"""Specification for OCR-D related functionalities"""

import pytest

from lib.ocrd3_odem.odem_commons import (
    DEFAULT_RTL_MODELS as D_RTL,
)

from lib.ocrd3_odem.processing_ocrd import (
    get_recognition_level,
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

    assert get_recognition_level(model_configuration, rtl_models=D_RTL) == recognotion_level


@pytest.mark.parametrize("model_conf,rec_level",
                         [
                             ('ger', 'word'),
                             ('gt4ara', 'glyph'),
                             ('ulb-fas', 'glyph'),
                         ])
def test_odem_recognition_level_custom(model_conf, rec_level):
    """Ensure custom rtl configuration respected"""

    _custom_rtl = ['gt4ara', 'ulb-fas']
    assert get_recognition_level(model_conf, _custom_rtl) == rec_level
