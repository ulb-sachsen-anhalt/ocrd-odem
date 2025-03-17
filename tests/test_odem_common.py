"""Specification for OCR-D related functionalities"""

import os

import pytest

import digiflow.record as df_r

from lib import odem

from .conftest import PROJECT_ROOT_DIR, fixture_configuration


# silence linter warning for custum converter
# pylint: disable=no-member
def test_merge_args_exchange_model_mappings():
    """Behavior when CLI args replace
    configured options"""

    # arrange
    _conf_parser = odem.get_configparser()
    _default_ini_file = os.path.join(PROJECT_ROOT_DIR, 'resources', 'odem.example.ini')
    assert os.path.isfile(_default_ini_file)
    assert _conf_parser.read(_default_ini_file) == [_default_ini_file]
    _prev_mapping = _conf_parser.getdict(odem.CFG_SEC_OCR, odem.KEY_MODEL_MAP)
    assert 'lat' in _prev_mapping
    assert 'per' in _prev_mapping
    assert _prev_mapping['per'] == 'fas.traineddata'

    # act
    _args = {odem.KEY_MODEL_MAP: 'fas: ulb-fas-123'}
    _n_merged = odem.merge_args(_conf_parser, _args)

    # assert
    assert _n_merged == [(odem.CFG_SEC_OCR, odem.KEY_MODEL_MAP, 'fas: ulb-fas-123')]
    assert 'lat' not in _conf_parser.get(odem.CFG_SEC_OCR, odem.KEY_MODEL_MAP)


def test_merge_args_from_cli():
    """Behavior when CLI arg for only a
    specific, single language is set via CLI
    """

    # arrange
    _conf_parser = odem.get_configparser()
    _default_ini_file = os.path.join(PROJECT_ROOT_DIR, 'resources', 'odem.example.ini')
    assert os.path.isfile(_default_ini_file)
    assert _conf_parser.read(_default_ini_file) == [_default_ini_file]
    _prev_mapping = _conf_parser.getdict(odem.CFG_SEC_OCR, odem.KEY_MODEL_MAP)
    assert 'lat' in _prev_mapping
    assert 'per' in _prev_mapping
    assert _prev_mapping['per'] == 'fas.traineddata'

    # act
    _args = {odem.KEY_LANGUAGES: 'ulb-fas-123'}
    _n_merged = odem.merge_args(_conf_parser, _args)

    # assert
    assert _n_merged == [(odem.CFG_SEC_OCR, odem.KEY_LANGUAGES, 'ulb-fas-123')]
    # lat still present, only persian mapping was affected
    assert 'lat' in _conf_parser.get(odem.CFG_SEC_OCR, odem.KEY_MODEL_MAP)


def test_merge_model_mappings_with_subsequent_calls():
    """Behavior when CLI arg for specific model is called
    more than once => ensure nothing is duplicated
    """

    # arrange
    _conf_parser = odem.get_configparser()
    _default_ini_file = os.path.join(PROJECT_ROOT_DIR, 'resources', 'odem.example.ini')
    assert os.path.isfile(_default_ini_file)
    assert _conf_parser.read(_default_ini_file) == [_default_ini_file]
    _prev_mapping = _conf_parser.getdict(odem.CFG_SEC_OCR, odem.KEY_MODEL_MAP)
    assert 'per' in _prev_mapping
    assert _prev_mapping['per'] == 'fas.traineddata'

    # act
    _args = {odem.KEY_MODEL_MAP: 'fas: ulb-fas-123'}
    odem.merge_args(_conf_parser, _args)
    odem.merge_args(_conf_parser, _args)

    # assert
    assert _conf_parser.get(odem.CFG_SEC_OCR, odem.KEY_MODEL_MAP) == 'fas: ulb-fas-123'


def test_work_dir_mode_local(tmp_path):
    """Behavior with workflow mode set to local"""

    # arrange
    some_dir = tmp_path / "some_dir"
    some_dir.mkdir()
    the_cfg = fixture_configuration()

    # act
    proc = odem.ODEMProcessImpl(configuration=the_cfg, work_dir=some_dir, record=None)

    # assert
    assert proc.local_mode


def test_work_dir_mode_local_alerta():
    """Behavior with workflow mode set to local"""

    # arrange
    the_cfg = fixture_configuration()

    # act
    with pytest.raises(odem.ODEMException) as odem_alarma:
        odem.ODEMProcessImpl(configuration=the_cfg, work_dir="some_dir", record=None)

    # assert
    assert "Invalid work_dir" in odem_alarma.value.args[0]


def test_work_dir_record_input(tmp_path):
    """Behavior with workflow mode set to local"""

    # arrange
    some_dir = tmp_path / "some_dir"
    some_dir.mkdir()
    the_cfg = fixture_configuration()
    the_rec = df_r.Record('oai:opendata.uni-halle.de:1981185920/37167')

    # act
    proc = odem.ODEMProcessImpl(configuration=the_cfg, work_dir=some_dir, record=the_rec)

    # assert
    assert proc.local_mode is False
