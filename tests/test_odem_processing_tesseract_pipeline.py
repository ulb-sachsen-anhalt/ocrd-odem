# -*- coding: utf-8 -*-
"""Tests OCR Pipeline API"""

import os
import shutil

from pathlib import Path

import pytest

import digiflow as df

import lib.ocrd3_odem as o3o
import lib.ocrd3_odem.processing_ocr_pipeline as o3o_pop
from lib.ocrd3_odem.odem_commons import get_logger
from lib.ocrd3_odem.ocrd3_odem import ODEMTesseract

from .conftest import TEST_RES, PROD_RES


RES_0001_TIF = "0001.tif"
RES_0002_PNG = "0002.png"
RES_0003_JPG = "0003.jpg"
RES_00041_XML = str(TEST_RES / '0041.xml')
PATH_ODEM_CFG = PROD_RES / 'odem.ocr-pipeline.ini'
ODEM_CFG = o3o.get_configparser()
ODEM_CFG.read(PATH_ODEM_CFG)
OCR_PIPELINE_CFG_PATH = PROD_RES / 'odem.ocr-pipeline.steps.ini'


def test_ocr_pipeline_profile():
    """check profiling"""

    # arrange
    # pylint: disable=missing-class-docstring,too-few-public-methods
    class InnerClass:

        # pylint: disable=missing-function-docstring,no-self-use
        def func(self):
            return [i * i for i in range(1, 200000)]

    # act
    inner = InnerClass()
    result = o3o_pop.profile(inner.func)
    assert "test_ocr_pipeline_profile run" in result


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
    # create some stub tesseract config files
    res_cnt_mapping = ODEM_CFG.get(o3o.CFG_SEC_OCR, o3o.CFG_KEY_RES_VOL)
    tmp_tokens = res_cnt_mapping.split(':')
    model_dir = tmp_path / Path(tmp_tokens[0]).name
    model_dir.mkdir()
    configs = ['gt4hist_5000k.traineddata', 'lat_ocr.traineddata']
    for config in configs:
        modelconf_path = model_dir / config
        with open(modelconf_path, 'wb') as writer:
            writer.write(b'\x1234')
    ODEM_CFG.set(o3o.CFG_SEC_OCR, o3o.CFG_KEY_RES_VOL, f'{model_dir}:{tmp_tokens[1]}')
    return tmp_path


@pytest.fixture(name="my_pipeline")
def _fixture_default_pipeline(a_workspace: Path):
    _record = df.OAIRecord('oai:urn:mwe')
    odem_process = o3o.ODEMProcess(_record, a_workspace)
    odem_process.odem_configuration = ODEM_CFG
    odem_process._statistics_ocr['languages'] = ['ger']
    odem_process.the_logger = get_logger(a_workspace / 'log')
    odem_tess = ODEMTesseract(odem_process)
    return odem_tess


def test_ocr_pipeline_default_config(my_pipeline: ODEMTesseract):
    """check default config options"""

    _cfg = my_pipeline.read_pipeline_config(OCR_PIPELINE_CFG_PATH)
    assert 'pipeline' in _cfg.sections()
    assert _cfg.get('pipeline', 'logger_name') == 'ocr_pipeline'
    assert _cfg.get('pipeline', 'file_ext') == 'tif,jpg,png,jpeg'


@pytest.mark.skip('kept only for documentation')
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
    odem_process = o3o.ODEMProcess(df.OAIRecord('oai:urn_custom'), a_workspace)
    odem_process.odem_configuration = ODEM_CFG
    odem_process._statistics_ocr['languages'] = ['ger', 'lat']
    odem_process.the_logger = get_logger(a_workspace / 'log')
    odem_tess = o3o.ODEMTesseract(odem_process)
    odem_tess.read_pipeline_config(conf_file)
    return odem_tess


def test_pipeline_step_tesseract(custom_pipe: ODEMTesseract, a_workspace):
    """Check proper tesseract cmd from full configuration"""

    steps = o3o_pop.init_steps(custom_pipe.pipeline_configuration)
    steps[0].path_in = a_workspace / 'scandata' / RES_0001_TIF

    # assert
    assert len(steps) == 5
    assert isinstance(steps[0], o3o_pop.StepTesseract)
    the_cmd = steps[0].cmd
    the_cmd_tokens = the_cmd.split()
    assert len(the_cmd_tokens) == 6
    assert the_cmd_tokens[0] == 'tesseract'
    assert the_cmd_tokens[1].endswith('scandata/0001.tif')
    assert the_cmd_tokens[2].endswith('scandata/0001')
    assert the_cmd_tokens[3] == '-l'
    assert the_cmd_tokens[4] == 'gt4hist_5000k+lat_ocr'
    assert the_cmd_tokens[5] == 'alto'


def test_pipeline_step_replace(custom_pipe: ODEMTesseract):
    """Check proper steps from full configuration"""

    # act
    steps = o3o_pop.init_steps(custom_pipe.pipeline_configuration)

    # assert
    assert len(steps) == 5
    assert isinstance(steps[1], o3o_pop.StepPostReplaceChars)
    assert isinstance(steps[1].dict_chars, dict)


def test_pipeline_step_replace_regex(custom_pipe: ODEMTesseract):
    """Check proper steps from full configuration"""

    # act
    steps = o3o_pop.init_steps(custom_pipe.pipeline_configuration)

    # assert
    assert len(steps) == 5
    assert isinstance(steps[2], o3o_pop.StepPostReplaceCharsRegex)
    assert steps[2].pattern == 'r\'([aeioubcglnt]3[:-]*")\''
