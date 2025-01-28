# -*- coding: utf-8 -*-
"""Tests OCR Pipeline API"""

import json
import os
import shutil
import unittest
import unittest.mock

from pathlib import Path

import requests
import pytest

import lxml.etree as ET
import digiflow.record as df_r

from lib import odem
import lib.odem.ocr.ocr_pipeline as o3o_pop

from .conftest import TEST_RES, PROD_RES

# pylint: disable=c-extension-no-member

RES_0001_TIF = "0001.tif"
RES_0002_PNG = "0002.png"
RES_0003_JPG = "0003.jpg"
RES_00041_XML = str(TEST_RES / '0041.xml')
PATH_ODEM_CFG = PROD_RES / 'odem.example.ini'
ODEM_CFG = odem.get_configparser()
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
    res_cnt_mapping = ODEM_CFG.get(odem.CFG_SEC_OCR, odem.CFG_SEC_OCR_OPT_RES_VOL)
    tmp_tokens = res_cnt_mapping.split(':')
    model_dir = tmp_path / Path(tmp_tokens[0]).name
    model_dir.mkdir()
    configs = ['gt4hist_5000k.traineddata', 'lat_ocr.traineddata']
    for config in configs:
        modelconf_path = model_dir / config
        with open(modelconf_path, 'wb') as writer:
            writer.write(b'\x1234')
    ODEM_CFG.set(odem.CFG_SEC_OCR, odem.CFG_SEC_OCR_OPT_RES_VOL, f'{model_dir}:{tmp_tokens[1]}')
    return tmp_path


@pytest.fixture(name="my_pipeline")
def _fixture_default_pipeline(a_workspace: Path):
    _record = df_r.Record('oai:urn:mwe')
    odem_process = odem.ODEMProcessImpl(_record, a_workspace)
    odem_process.configuration = ODEM_CFG
    odem_process.process_statistics[odem.ARG_L_LANGUAGES] = ['ger']
    odem_process.logger = odem.get_logger(a_workspace / 'log')
    odem_tess = odem.ODEMTesseract(odem_process)
    return odem_tess


@pytest.mark.skip('kept only for documentation')
def test_ocr_pipeline_estimations(my_pipeline: odem.ODEMTesseract):
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
    odem_process = odem.ODEMProcessImpl(df_r.Record('oai:urn_custom'), a_workspace)
    odem_process.configuration = ODEM_CFG
    odem_process.process_statistics[odem.ARG_L_LANGUAGES] = ['ger', 'lat']
    odem_process.logger = odem.get_logger(a_workspace / 'log')
    odem_tess = odem.ODEMTesseract(odem_process)
    odem_tess.read_pipeline_config(conf_file)
    return odem_tess


def test_pipeline_step_tesseract(custom_pipe: odem.ODEMTesseract, a_workspace):
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


def test_pipeline_step_replace(custom_pipe: odem.ODEMTesseract):
    """Check proper steps from full configuration"""

    # act
    steps = o3o_pop.init_steps(custom_pipe.pipeline_configuration)

    # assert
    assert len(steps) == 5
    assert isinstance(steps[1], o3o_pop.StepPostReplaceChars)
    assert isinstance(steps[1].dict_chars, dict)


def test_pipeline_step_replace_regex(custom_pipe: odem.ODEMTesseract):
    """Check proper steps from full configuration"""

    # act
    steps = o3o_pop.init_steps(custom_pipe.pipeline_configuration)

    # assert
    assert len(steps) == 5
    assert isinstance(steps[2], o3o_pop.StepPostReplaceCharsRegex)
    assert steps[2].pattern == 'r\'([aeioubcglnt]3[:-]*")\''


def test_stepio_not_initable():
    """StepIO cant be instantiated"""

    with pytest.raises(TypeError) as exec_info:
        o3o_pop.StepIO()    # pylint: disable=abstract-class-instantiated
    assert "Can't instantiate" in str(exec_info.value)


TIF_001 = '001.tif'
TIF_002 = '002.tif'


@pytest.fixture(name='max_dir')
def fixture_path_existing(tmp_path):
    """supply valid path"""

    max_dir = tmp_path / 'MAX'
    max_dir.mkdir()
    path1 = max_dir / TIF_001
    path1.write_bytes(bytearray([120, 3, 255, 0, 100]))
    path2 = max_dir / TIF_002
    path2.write_bytes(bytearray([120, 3, 255, 0, 100]))
    return max_dir


def test_step_tesseract_list_langs(max_dir: Path):
    """Tesseract list-langs"""

    # arrange
    args = {'--list-langs': None}

    # act
    step = o3o_pop.StepTesseract(args)
    step.path_in = max_dir / TIF_001

    # assert
    assert ' --list-langs' in step.cmd


def test_step_tesseract_path_out_folder(max_dir):
    """Tesseract path to write result"""

    # arrange
    args = {'-l': 'deu', 'alto': None}

    # act
    step = o3o_pop.StepTesseract(args)
    step.path_in = os.path.join(max_dir, TIF_001)

    # assert
    assert step.path_next.name == '001.xml'


def test_step_tesseract_change_input(max_dir):
    """Tesseract path to write result"""

    # arrange
    args = {'-l': 'deu', 'alto': None}

    # act
    step = o3o_pop.StepTesseract(args)
    step.path_in = os.path.join(max_dir, TIF_001)

    # assert
    assert 'MAX/001.tif ' in step.cmd
    assert 'MAX/001.xml ' not in step.cmd
    assert 'MAX/001 ' in step.cmd

    # re-act
    step.path_in = os.path.join(max_dir, TIF_002)

    # re-assert
    assert 'MAX/001.tif ' not in step.cmd
    assert 'MAX/002.tif ' in step.cmd
    assert 'MAX/002 ' in step.cmd


def test_step_tesseract_change_input_with_dir(max_dir):
    """Tesseract path to write result"""

    # arrange
    args = {'-l': 'deu', 'alto': None}

    # act
    step = o3o_pop.StepTesseract(args)
    step.path_in = os.path.join(max_dir, TIF_001)

    # assert
    assert 'MAX/001.tif ' in step.cmd
    assert 'MAX/001 ' in step.cmd

    # re-act
    step.path_in = os.path.join(max_dir, TIF_002)

    # re-assert
    assert 'MAX/001.tif ' not in step.cmd
    assert 'MAX/002.tif ' in step.cmd
    assert 'MAX/002 ' in step.cmd


def test_step_tesseract_invalid_params(max_dir):
    """Check nature of params"""

    # act
    with pytest.raises(o3o_pop.StepException) as excinfo:
        o3o_pop.StepTesseract(max_dir)

    # assert
    actual_exc_text = str(excinfo.value)
    assert 'Invalid params' in actual_exc_text


def test_step_tesseract_full_args(max_dir):
    """Tesseract check cmd from args from following schema:
    'tesseract --dpi 500 <read_path> <out_path> -l <DEFAULT_CHARSET> alto'
    """

    # arrange
    # some args are computed later on
    args = {'--dpi': 470, '-l': 'ulbfrk', 'alto': None}

    # act
    step = o3o_pop.StepTesseract(args)
    step.path_in = os.path.join(max_dir, TIF_001)

    # assert
    input_tif = os.path.join(max_dir, TIF_001)
    output_xml = os.path.splitext(os.path.join(max_dir, TIF_001))[0]
    cmd = f'tesseract {input_tif} {output_xml} --dpi 470 -l ulbfrk alto'
    assert cmd == step.cmd
    assert step.path_next.name == '001.xml'


def test_step_tesseract_different_configurations(max_dir):
    """Check cmd from args use different lang config"""

    # arrange
    args = {'-l': 'frk_ulbzd1', 'alto': None, 'txt': None}

    # act
    step = o3o_pop.StepTesseract(args)
    step.path_in = os.path.join(max_dir, TIF_001)

    # assert
    input_tif = os.path.join(max_dir, TIF_001)
    output_xml = os.path.splitext(os.path.join(max_dir, TIF_001))[0]
    tesseract_cmd = f'tesseract {input_tif} {output_xml} -l frk_ulbzd1 alto txt'
    assert tesseract_cmd == step.cmd


def test_step_copy_alto_back(max_dir: Path):
    """
    Move ALTO file back to where we started
    Preserve filename, only switch directory
    """

    # arrange
    path_target = max_dir.parent / 'FULLTEXT'
    step = o3o_pop.StepPostMoveAlto({'path_target': path_target})

    # act
    step.path_in = max_dir / TIF_001
    step.execute()

    # assert
    assert step.path_next == path_target / TIF_001
    assert os.path.exists(step.path_next)


def test_step_replace():
    """unittest replace func"""

    # arrange
    src = str(TEST_RES / '500_gray00003.xml')
    dict_chars = {'ſ': 's', 'ic)': 'ich'}
    params = {'dict_chars': dict_chars, 'must_backup': True}
    step = o3o_pop.StepPostReplaceChars(params)
    step.path_in = src

    lines = ['<String ID="string_405" WC="0.96" CONTENT="geweſen"/>']
    lines.append('<String ID="string_406" WC="0.95" CONTENT="iſt."/>')
    lines.append('<String ID="string_407" WC="0.96" CONTENT="Beſtätigt"/>')

    # act
    step._replace(lines)

    # assert
    assert len(step.lines_new) == 3
    assert 'iſt.' not in step.lines_new[1]
    assert 'ist.' in step.lines_new[1]
    assert step.must_backup()


@pytest.fixture(name='empty_ocr')
def fixture_empty_ocr(tmpdir):
    """create tmp data empty ALTO XML"""

    path = tmpdir.mkdir("xml").join("0041.xml")
    shutil.copyfile(TEST_RES / '0041.xml', path)
    return str(path)


def test_step_replace_with_empty_alto(empty_ocr):
    """Determine behavior for invalid input data"""

    step = o3o_pop.StepPostReplaceChars({'dict_chars': {'ſ': 's'}})
    step.path_in = empty_ocr

    # act
    step.execute()

    # assert
    assert not step.statistics


@pytest.fixture(name='tmp_500_gray')
def fixture_create_tmp_500_gray(tmpdir):
    """create tmp data from file 500_gray00003.xml"""

    path = tmpdir.mkdir("xml").join("input.xml")
    shutil.copyfile('./tests/resources/500_gray00003.xml', path)
    return path


def _provide_replace_params():
    dict_chars = {'ſ': 's', 'ic)': 'ich'}
    params = {'dict_chars': dict_chars, 'must_backup': True}
    return params


def test_replaced_file_written(tmp_500_gray):
    """test replaced file written"""

    # arrange
    params = _provide_replace_params()
    step = o3o_pop.StepPostReplaceChars(params)

    # act
    step.path_in = tmp_500_gray
    step.execute()

    # assert
    check_handle = open(tmp_500_gray, 'r', encoding="UTF-8")
    lines = check_handle.readlines()
    for line in lines:
        for (k, _) in params['dict_chars'].items():
            assert k not in line
    check_handle.close()

    assert os.path.exists(os.path.join(os.path.dirname(tmp_500_gray),
                                       'input_before_StepPostReplaceChars.xml'))
    assert not os.path.exists(os.path.join(os.path.dirname(tmp_500_gray),
                                           'input_before_StepPostReplaceCharsRegex.xml'))


def test_replaced_file_statistics(tmp_500_gray):
    """test statistics available"""

    # arrange
    step = o3o_pop.StepPostReplaceChars(_provide_replace_params())
    step.path_in = tmp_500_gray

    # act
    step.execute()

    # assert
    expected = ['ſ:392', 'ic):6']
    assert expected == step.statistics
    assert os.path.exists(os.path.join(os.path.dirname(tmp_500_gray),
                                       'input_before_StepPostReplaceChars.xml'))


def test_regex_replacements(tmp_500_gray):
    """check regex replacements in total"""

    # arrange
    params = {'pattern': r'([aeioubcglnt]3[:-]*")', 'old': '3', 'new': 's'}
    step = o3o_pop.StepPostReplaceCharsRegex(params)

    # act
    step.path_in = tmp_500_gray
    step.execute()

    # assert
    assert not os.path.exists(os.path.join(os.path.dirname(str(tmp_500_gray)),
                                           'input_before_StepPostReplaceChars.xml'))
    with open(str(tmp_500_gray), encoding='utf-8') as test_handle:
        lines = test_handle.readlines()
        for line in lines:
            assert 'u3"' not in line, 'detected trailing "3" in ' + line

    expected = ['a3"=>as":5',
                'u3"=>us":1',
                'l3"=>ls":2',
                'e3"=>es":4',
                't3"=>ts":4',
                'c3"=>cs":1',
                'b3"=>bs":1',
                'i3"=>is":2',
                'g3"=>gs":1',
                'n3"=>ns":1']
    assert expected == step.statistics


def test_remove_failed():
    """Test remove failed since file is missing"""

    # arrange
    step = o3o_pop.StepPostRemoveFile({'file_suffix': 'tif'})

    # act
    with pytest.raises(o3o_pop.StepException) as step_err:
        step.path_in = 'qwerrwe.tif'

    # assert
    assert "qwerrwe.tif' invalid!" in step_err.value.args[0]


def test_remove_succeeded(max_dir):
    """Test remove success"""

    # arrange
    step = o3o_pop.StepPostRemoveFile({'file_suffix': 'tif'})

    # act
    step.path_in = os.path.join(max_dir, TIF_001)
    step.execute()

    # assert
    assert step.is_removed()


def test_stepestimateocr_analyze():
    """Analyse estimation results"""

    # arrange
    results = [
        ('0001.tif', 14.123),
        ('0002.tif', 18.123),
        ('0003.tif', 28.123),
        ('0004.tif', 38.123),
        ('0005.tif', 40.123),
        ('0006.tif', 41.123),
        ('0007.tif', 51.123),
        ('0008.tif', 60.123),
        ('0009.tif', 68.123),
        ('0010.tif', 68.123),
    ]

    # act
    actual = o3o_pop.analyze(results)

    # assert
    assert actual[0] == 42.723
    assert len(actual[1]) == 5
    assert len(actual[1][0]) == 1
    assert len(actual[1][1]) == 2
    assert len(actual[1][2]) == 3
    assert len(actual[1][3]) == 1
    assert len(actual[1][4]) == 3


def test_estimate_handle_large_wtr():
    """Test handle border cases and large real wtr from 1667524704_J_0116/0936.tif"""

    # arrange
    results = [
        ('0001.tif', 0),
        ('0002.tif', 28.123),
        ('0003.tif', 41.123),
        ('0004.tif', 50.123),
        ('0936.tif', 78.571),
        ('0005.tif', 100.123),
    ]

    # act
    actual = o3o_pop.analyze(results)

    # assert
    assert actual[0] == 49.677
    assert len(actual[1]) == 5
    assert len(actual[1][0]) == 1
    assert len(actual[1][1]) == 1
    assert len(actual[1][2]) == 1
    assert len(actual[1][3]) == 1
    assert len(actual[1][4]) == 2


def test_step_estimateocr_empty_alto(empty_ocr):
    """
    Determine bahavior of stepestimator when confronted with empty alto file
    Modified: in this (rare) case, just do nothing, do *not* raise any Exception
    """

    step = o3o_pop.StepEstimateOCR({})
    step.path_in = empty_ocr

    # act
    step.execute()

    # assert
    assert step.statistics[0] == -1


@unittest.mock.patch("requests.head")
def test_service_down(mock_requests):
    """Determine Behavior when url not accessible"""

    # arrange
    params = {'service_url': 'http://localhost:8010/v2/check'}
    step = o3o_pop.StepEstimateOCR(params)
    mock_requests.side_effect = requests.ConnectionError

    # assert
    assert not step.is_available()
    assert mock_requests.called == 1


def test_step_estimateocr_textline_conversions():
    """Test functional behavior for valid ALTO-output"""

    test_data = os.path.join('tests', 'resources', '500_gray00003.xml')

    # pylint: disable=protected-access
    xml_data = ET.parse(test_data)
    lines = o3o_pop.get_lines(xml_data)
    (_, n_lines, _, _, n_lines_out) = o3o_pop.textlines2data(lines)

    assert n_lines == 360
    assert n_lines_out == 346

# pylint: disable=unused-argument


def _fixture_languagetool(*args, **kwargs):
    result = unittest.mock.Mock()
    result.status_code = 200
    response_path = os.path.join(TEST_RES / 'languagetool_response_500_gray00003.json')
    with open(response_path, encoding="UTF-8") as the_json_file:
        result.json.return_value = json.load(the_json_file)
    return result


@unittest.mock.patch("requests.post")
def test_step_estimateocr_lines_and_tokens_err_ratio(mock_requests):
    """Test behavior of for valid ALTO-output"""

    # arrange
    test_data = os.path.join(TEST_RES / '500_gray00003.xml')
    mock_requests.side_effect = _fixture_languagetool
    params = {'service_url': 'http://localhost:8010/v2/check',
              'language': 'de-DE',
              'enabled_rules': 'GERMAN_SPELLER_RULE'
              }
    step = o3o_pop.StepEstimateOCR(params)
    step.path_in = test_data

    # act
    step.execute()

    assert step.statistics
    assert mock_requests.called == 1
    assert step.n_errs == 548
    assert step.n_words == 2636
    assert step.statistics[0] == pytest.approx(79.211, rel=1e-3)


@unittest.mock.patch("requests.post")
def test_step_estimateocr_lines_and_tokens_hit_ratio(mock_requests):
    """Test behavior of for valid ALTO-output"""

    # arrange
    test_data = os.path.join(TEST_RES / '500_gray00003.xml')
    mock_requests.side_effect = _fixture_languagetool
    params = {'service_url': 'http://localhost:8010/v2/check',
              'language': 'de-DE',
              'enabled_rules': 'GERMAN_SPELLER_RULE'
              }
    step = o3o_pop.StepEstimateOCR(params)
    step.path_in = test_data

    # act
    step.execute()

    assert mock_requests.called == 1
    err_ratio = (step.n_errs / step.n_words) * 100
    assert err_ratio == pytest.approx(20.789, rel=1e-3)

    # revert metric to represent hits
    # to hit into positive compliance
    hits = (step.n_words - step.n_errs) / step.n_words * 100
    assert hits == pytest.approx(79.21, rel=1e-3)

    # holds this condition?
    assert hits == pytest.approx(100 - err_ratio, rel=1e-9)


@unittest.mock.patch("requests.get")
def test_stepestimate_invalid_data(mock_request):
    """
    Check that in case of *really empty* data,
    language-tool is not called after all
    """

    # arrange
    data_path = os.path.join(TEST_RES / '1667524704_J_0173_0173.xml')
    params = {'service_url': 'http://localhost:8010/v2/check',
              'language': 'de-DE',
              'enabled_rules': 'GERMAN_SPELLER_RULE'
              }
    step = o3o_pop.StepEstimateOCR(params)
    step.path_in = data_path

    # act
    step.execute()

    # assert
    assert step.statistics
    assert not mock_request.called


@pytest.fixture(name="altov4_xml")
def _fixture_altov4(tmp_path):
    test_data = os.path.join(TEST_RES / '16331011.xml')
    prev_root = ET.parse(test_data).getroot()
    prev_strings = prev_root.findall('.//alto:String', o3o_pop.NAMESPACES)
    assert len(prev_strings) == 275
    dst_path = tmp_path / "16331011.xml"
    shutil.copy(test_data, dst_path)

    # act within a fixture
    step = o3o_pop.StepPostprocessALTO()
    step.path_in = str(dst_path)
    step.execute()

    yield ET.parse(dst_path).getroot()


def test_clear_empty_content(altov4_xml):
    """Ensure no more empty Strings exist"""

    all_strings = altov4_xml.findall('.//alto:String', o3o_pop.NAMESPACES)
    # assert about 20 Strings (from 275, cf. fixture)
    # have been dropped due emptyness
    assert len(all_strings) == 254


def test_process_alto_file_identifier_set(altov4_xml):
    """Ensure expected fileIdentifier present
    """
    assert altov4_xml.find('.//alto:fileIdentifier', o3o_pop.NAMESPACES).text == '16331011'


def test_process_alto_filename_set(altov4_xml):
    """Ensure expected fileName present
    """
    assert altov4_xml.find('.//alto:fileName', o3o_pop.NAMESPACES).text == '16331011.xml'


def test_clear_empty_lines_with_spatiums(tmp_path):
    """Ensure no more empty Strings exist"""

    test_data = os.path.join(TEST_RES / '16331001.xml')
    prev_root = ET.parse(test_data).getroot()
    prev_strings = prev_root.findall('.//alto:String', o3o_pop.NAMESPACES)
    # original ALTO output
    assert len(prev_strings) == 1854
    dst_path = tmp_path / "16331001.xml"
    shutil.copy(test_data, dst_path)
    step = o3o_pop.StepPostprocessALTO()
    step.path_in = dst_path

    # act
    step.execute()

    # assert
    xml_root = ET.parse(dst_path).getroot()
    all_strings = xml_root.findall('.//alto:String', o3o_pop.NAMESPACES)
    # line with 2 empty strings and SP in between
    line_with_sps = xml_root.findall(
        './/alto:TextLine[@ID="line_2"]', o3o_pop.NAMESPACES)
    assert not line_with_sps
    # assert many Strings have been dropped due emptyness
    assert len(all_strings) == 1673
    assert xml_root.find(
        './/alto:fileIdentifier',
        o3o_pop.NAMESPACES).text == '16331001'
    assert xml_root.find('.//alto:fileName', o3o_pop.NAMESPACES).text == '16331001.xml'


@pytest.fixture(name="pipeline_odem_xml")
def _fixture_pipeline_odem_xml(tmp_path):
    test_data = os.path.join(TEST_RES / 'urn+nbn+de+gbv+3+1-121915-p0159-6_ger.xml')
    dst_path = tmp_path / "urn+nbn+de+gbv+3+1-121915-p0159-6_ger.xml"
    shutil.copy(test_data, dst_path)

    # act within a fixture
    step = o3o_pop.StepPostprocessALTO({'page_prefix': ''})
    step.path_in = dst_path
    step.execute()

    yield ET.parse(dst_path).getroot()


def test_process_odem_result_identifier_set(pipeline_odem_xml):
    """Ensure expected fileIdentifier present
    """
    file_ident = pipeline_odem_xml.find('.//alto:fileIdentifier', o3o_pop.NAMESPACES)
    assert file_ident is not None
    assert file_ident.text == 'urn+nbn+de+gbv+3+1-121915-p0159-6_ger'


def test_process_odem_filename_set(pipeline_odem_xml):
    """Ensure expected fileName present
    """
    txt_filename = pipeline_odem_xml.find('.//alto:fileName', o3o_pop.NAMESPACES)
    assert txt_filename is not None
    assert txt_filename.text == 'urn+nbn+de+gbv+3+1-121915-p0159-6_ger.xml'


def test_process_odem_page_id(pipeline_odem_xml):
    """Ensure expected fileName present
    """
    page_id = pipeline_odem_xml.find('.//alto:Page', o3o_pop.NAMESPACES).attrib['ID']
    assert page_id == 'urn+nbn+de+gbv+3+1-121915-p0159-6_ger'


def test_step_replace_regex_literal(tmp_path):
    """Ensure 'J's have reduced"""

    # arrange
    alto_in = TEST_RES / '1516514412012_175762_00000003.xml'
    tmp_file = shutil.copyfile(alto_in, tmp_path / alto_in.name)
    assert tmp_file.exists()
    with open(tmp_file, encoding='utf-8') as reader:
        text_in = reader.readlines()
    J_in = sum((1 for l in text_in if 'J' in l))
    assert J_in == 185
    params = {
        'pattern': r'(J[cdhmn]\w*)', 'old': 'J', 'new': 'I'
    }

    step = o3o_pop.StepPostReplaceCharsRegex(params)
    step.path_in = tmp_file

    # act
    step.execute()

    # assert
    assert hasattr(step, 'statistics')
    assert len(step.statistics) == 9
    assert len(step._replacements) == 9
    with open(step.path_next, encoding='utf-8') as reader:
        text_out = reader.readlines()
    j_out = sum((1 for l in text_out if 'J' in l))
    assert j_out == 172


def test_step_replace_regex_from_configuration(tmp_path):
    """Ensure 'J's have reduced"""

    # arrange
    alto_in = TEST_RES / '1516514412012_175762_00000003.xml'
    tmp_file = shutil.copyfile(alto_in, tmp_path / alto_in.name)
    assert tmp_file.exists()
    with open(tmp_file, encoding='utf-8') as reader:
        text_in = reader.readlines()
    j_in = sum((1 for l in text_in if 'J' in l))
    assert j_in == 185
    cfg_parser = odem.get_configparser()
    cfg_parser.read(OCR_PIPELINE_CFG_PATH)
    step_keys = cfg_parser['step_02'].keys()
    params = {k: cfg_parser['step_02'][k] for k in step_keys}
    step = o3o_pop.StepPostReplaceCharsRegex(params)
    step.path_in = tmp_file

    # act
    step.execute()

    # assert
    assert hasattr(step, 'statistics')
    assert len(step.statistics) == 9
    assert len(step._replacements) == 9
    with open(step.path_next, encoding='utf-8') as reader:
        text_out = reader.readlines()
    J_out = sum((1 for l in text_out if 'J' in l))
    assert J_out == 172
