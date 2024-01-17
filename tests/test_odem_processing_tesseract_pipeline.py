# -*- coding: utf-8 -*-
"""Tests OCR Pipeline API"""

import os
import shutil

from pathlib import (
    Path,
)

import pytest

from digiflow import (
    OAIRecord,
)

from lib.ocrd3_odem.odem_commons import (
    get_logger,
)
from lib.ocrd3_odem.ocrd3_odem import (
    ODEMTesseract,
)
from lib.ocrd3_odem.processing_ocr_pipeline import (
    StepPostReplaceChars,
    StepPostReplaceCharsRegex,
    StepTesseract,
    profile,
    init_steps,
)

from .conftest import (
    TEST_RES,
    PROD_RES,
)


RES_0001_TIF = "0001.tif"
RES_0002_PNG = "0002.png"
RES_0003_JPG = "0003.jpg"
RES_00041_XML = str(TEST_RES / '0041.xml')
RES_CFG = str(PROD_RES / 'tesseract_pipeline_config.ini')


@pytest.fixture(name="a_workspace")
def fixure_a_workspace(tmp_path):
    """create MWE workspace"""

    data_dir = tmp_path / "scandata"
    data_dir.mkdir()
    log_dir = tmp_path / "log"
    log_dir.mkdir()
    path_scan_0001 = data_dir / RES_0001_TIF
    path_scan_0002 = data_dir / RES_0002_PNG
    path_scan_0003 = data_dir / RES_0003_JPG
    path_mark_prev = data_dir / "ocr_pipeline_open"
    with open(path_mark_prev, 'w', encoding="UTF-8") as marker_file:
        marker_file.write("some previous state\n")
    shutil.copyfile(RES_00041_XML, path_scan_0001)
    shutil.copyfile(RES_00041_XML, path_scan_0002)
    shutil.copyfile(RES_00041_XML, path_scan_0003)
    return tmp_path


@pytest.fixture(name="my_pipeline")
def _fixture_default_pipeline(a_workspace: Path):
    _record = OAIRecord('oai:urn:mwe')
    _odem = ODEMTesseract(_record, a_workspace)
    _odem.read_pipeline_config(RES_CFG)
    _logger = get_logger(a_workspace / 'log')
    _odem.the_logger = _logger
    return _odem


def test_ocr_pipeline_default_config(my_pipeline: ODEMTesseract):
    """check default config options"""

    assert my_pipeline
    _cfg = my_pipeline.pipeline_config
    assert _cfg.get('pipeline', 'executors') == '8'
    assert _cfg.get('pipeline', 'logger_name') == 'ocr_pipeline'
    assert _cfg.get('pipeline', 'file_ext') == 'tif,jpg,png,jpeg'
    assert _cfg.get('step_03', 'language') == 'de-DE'
    assert _cfg.get('step_03', 'enabled_rules') == 'GERMAN_SPELLER_RULE'


def test_ocr_pipeline_profile():
    """check profiling"""

    # arrange
    # pylint: disable=missing-class-docstring,too-few-public-methods
    class InnerClass:

        # pylint: disable=missing-function-docstring,no-self-use
        def func(self):
            return [i * i for i in range(1, 2000000)]

    # act
    inner = InnerClass()
    result = profile(inner.func)
    assert "test_ocr_pipeline_profile run" in result


def test_ocr_pipeline_estimations(my_pipeline: ODEMTesseract):
    """check estimation data persisted"""

    # arrange
    estms = [('0001.tif', 21.476, 3143, 675, 506, 29, 24, 482),
             ('0002.png', 38.799, 1482, 575, 193, 11, 34, 159),
             ('0003.jpg', 39.519, 582, 230, 152, 2, 12, 140)]

    # act
    wtr_path = my_pipeline.store_estimations(estms)

    # assert
    assert os.path.exists(wtr_path)


@pytest.fixture(name="custom_pipe")
def _fixture_custom_config_pipeline(a_workspace):
    data_dir = a_workspace / "MAX"
    if not os.path.exists(data_dir):
        data_dir.mkdir()
    log_dir = a_workspace / "log"
    if not os.path.exists(log_dir):
        log_dir.mkdir()
    conf_dir = a_workspace / "conf"
    if not os.path.exists(conf_dir):
        conf_dir.mkdir()
    conf_file = TEST_RES / 'ocr_config_full.ini'
    assert os.path.isfile(conf_file)
    _odem = ODEMTesseract(OAIRecord('oai:urn_custom'), a_workspace)
    _odem.read_pipeline_config(conf_file)
    return _odem


def test_pipeline_step_tesseract(custom_pipe: ODEMTesseract, a_workspace):
    """Check proper tesseract cmd from full configuration"""

    steps = init_steps(custom_pipe.pipeline_config)
    steps[0].path_in = a_workspace / 'scandata' / RES_0001_TIF

    # assert
    assert len(steps) == 5
    assert isinstance(steps[0], StepTesseract)
    the_cmd = steps[0].cmd
    the_cmd_tokens = the_cmd.split()
    assert len(the_cmd_tokens) == 6
    assert the_cmd_tokens[0] == 'tesseract'
    assert the_cmd_tokens[1].endswith('scandata/0001.tif')
    assert the_cmd_tokens[2].endswith('scandata/0001')
    assert the_cmd_tokens[3] == '-l'
    assert the_cmd_tokens[4] == 'frk+deu'
    assert the_cmd_tokens[5] == 'alto'


def test_pipeline_step_replace(custom_pipe):
    """Check proper steps from full configuration"""

    # act
    steps = init_steps(custom_pipe.pipeline_config)

    # assert
    assert len(steps) == 5
    assert isinstance(steps[1], StepPostReplaceChars)
    assert isinstance(steps[1].dict_chars, dict)


def test_pipeline_step_replace_regex(custom_pipe):
    """Check proper steps from full configuration"""

    # act
    steps = init_steps(custom_pipe.pipeline_config)

    # assert
    assert len(steps) == 5
    assert isinstance(steps[2], StepPostReplaceCharsRegex)
    assert steps[2].pattern == 'r\'([aeioubcglnt]3[:-]*")\''
