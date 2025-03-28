"""Specification for OCR-D related functionalities"""

from pathlib import Path

import digiflow as df
import lxml.etree as ET

import pytest

import lib.odem as odem
import lib.odem.ocr.ocr_d as o3o_ocrd

from .conftest import create_test_tif

# pylint:disable=c-extension-no-member

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

    the_lvl = o3o_ocrd.get_recognition_level(model_configuration, rtl_models=odem.DEFAULT_RTL_MODELS)
    assert the_lvl == recognotion_level


@pytest.mark.parametrize("model_conf,rec_level",
                         [
                             ('ger', 'word'),
                             ('gt4ara', 'glyph'),
                             ('ulb-fas', 'glyph'),
                         ])
def test_odem_recognition_level_custom(model_conf, rec_level):
    """Ensure custom rtl configuration respected"""

    _custom_rtl = ['gt4ara', 'ulb-fas']
    assert o3o_ocrd.get_recognition_level(model_conf, _custom_rtl) == rec_level


def test_create_workspace_mets(tmp_path):
    """Check what kind of workspace created
    and stored as METS/MODS for single added page

    cf. https://github.com/OCR-D/assets/tree/master/data/kant_aufklaerung_1784
    =>
    LOCTYPE="OTHER" OTHERLOCTYPE="FILE"

    Fix: move created png image to workspace, no copy
    """

    path_workspace = tmp_path / "00000001"
    path_image: Path = tmp_path / "00000001.tif"
    create_test_tif(path_image)
    o3o_ocrd.setup_workspace(path_workspace, path_image)
    path_ws_mets: Path = path_workspace / "mets.xml"

    # assert
    assert path_image.is_file()
    assert path_ws_mets.is_file()

    ws_mets_root = ET.parse(path_ws_mets).getroot()
    flocats = ws_mets_root.findall(".//mets:file/mets:FLocat", df.XMLNS)
    assert len(flocats) == 1
    assert flocats[0].attrib == {'{http://www.w3.org/1999/xlink}href': 'MAX/00000001.png',
                                 'LOCTYPE': 'OTHER',
                                 'OTHERLOCTYPE': 'FILE'}

    assert (path_workspace / "MAX" / "00000001.png").exists()
    assert not (tmp_path / "00000001.png").exists()
