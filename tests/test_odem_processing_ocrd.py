"""Specification for OCR-D related functionalities"""

import pytest
from digiflow import OAIRecord

from .conftest import fixture_configuration
from lib.ocrd3_odem import OCRDPageParallel
from lib.ocrd3_odem.odem_commons import (
    DEFAULT_RTL_MODELS as D_RTL, ODEMException,
)

from lib.ocrd3_odem.processing_ocrd import (
    get_recognition_level,
)


@pytest.mark.parametrize("model_configuration,recognotion_level",
                         [
                             ('ger.traineddata', 'word'),
                             ('ara.traineddata', 'glyph'),
                             ('ara.traineddata+ger.traineddata', 'glyph'),
                             ('ger.traineddata+lat.traineddata', 'word'),
                             ('fas.traineddata', 'glyph'),
                             ('ger.traineddata+lat.traineddata', 'word'),
                             ('eng.traineddata+fas.traineddata', 'glyph')
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
