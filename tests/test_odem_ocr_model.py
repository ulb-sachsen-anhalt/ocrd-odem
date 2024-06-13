"""Specification for OCR Model data"""

import os

import pytest

import lxml.etree as ET

import lib.odem.ocr.ocr_model as odem_model

from .conftest import (
    TEST_RES,
)


@pytest.mark.parametrize('ocr_res,expected_lines', [
    ('1667522809_J_0073_0512.xml', 510),        # ALTO V3 ULB ZD1
    ('288652.xml', 33),                         # PAGE 2013 FID GT 2021
    ('OCR-RESULT_0001.xml', 35),                # PAGE 2019 OCR-D
    ('ram110.xml', 24),                         # PAGE 2013 CITLab Rostock
    ('Lubab_alAlbab.pdf_000003.xml', 23)        # ALTO V4
])
def test_get_lines_defaults(ocr_res, expected_lines):
    """Parametrized test with different OCR-Formats

    Args:
        ocr_res (_type_): _description_
        expected_lines (_type_): _description_
    """

    # arrange
    res_alto = os.path.join(TEST_RES, ocr_res)
    xml_data = ET.parse(res_alto)

    # act
    lines = odem_model.get_lines(xml_data)

    # assert
    assert len(lines) == expected_lines


def test_get_lines_from_newspaper_alto_minlen():
    """How does a larger value for min_len affect the number of results?
    """

    # arrange
    res_alto = os.path.join(TEST_RES, '1667522809_J_0073_0512.xml')
    xml_data = ET.parse(res_alto)

    # act
    lines = odem_model.get_lines(xml_data, min_len=32)

    # assert
    assert len(lines) == 225


def test_get_lines_page_empty_lines_but_word_exception():
    """Strange case of PAGE date with words but not on line level
    """

    # arrange
    ocr_res = os.path.join(TEST_RES, '1123596.xml')
    xml_data = ET.parse(ocr_res)

    # act
    with pytest.raises(RuntimeError) as exc:
        odem_model.get_lines(xml_data)

    # assert
    assert "just words for line 'line_1617688885509_1198'" in str(
        exc.value)
