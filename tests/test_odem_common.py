"""Specification for OCR-D related functionalities"""

import os

from lib.ocrd3_odem.odem_commons import (
    KEY_LANGUAGES,
    KEY_MODEL_MAP,
    CFG_SEC_OCR,
    get_configparser,
    merge_args,
)

from .conftest import (
    PROJECT_ROOT_DIR,
)


# silence linter warning for custum converter
# pylint: disable=no-member
def test_merge_args_exchange_model_mappings():
    """Behavior when CLI args replace
    configured options"""

    # arrange
    _conf_parser = get_configparser()
    _default_ini_file = os.path.join(PROJECT_ROOT_DIR, 'resources', 'odem.ini')
    assert os.path.isfile(_default_ini_file)
    assert _conf_parser.read(_default_ini_file) == [_default_ini_file]
    _prev_mapping = _conf_parser.getdict(CFG_SEC_OCR, KEY_MODEL_MAP)
    assert 'lat' in _prev_mapping
    assert 'per' in _prev_mapping
    assert _prev_mapping['per'] == 'fas'

    # act
    _args = {KEY_MODEL_MAP: 'fas: ulb-fas-123'}
    _n_merged = merge_args(_conf_parser, _args)

    # assert
    assert _n_merged == [(CFG_SEC_OCR, KEY_MODEL_MAP, 'fas: ulb-fas-123')]
    assert 'lat' not in _conf_parser.get(CFG_SEC_OCR, KEY_MODEL_MAP)


def test_merge_args_from_cli():
    """Behavior when CLI arg for only a
    specific, single language is set via CLI
    """

    # arrange
    _conf_parser = get_configparser()
    _default_ini_file = os.path.join(PROJECT_ROOT_DIR, 'resources', 'odem.ini')
    assert os.path.isfile(_default_ini_file)
    assert _conf_parser.read(_default_ini_file) == [_default_ini_file]
    _prev_mapping = _conf_parser.getdict(CFG_SEC_OCR, KEY_MODEL_MAP)
    assert 'lat' in _prev_mapping
    assert 'per' in _prev_mapping
    assert _prev_mapping['per'] == 'fas'

    # act
    _args = {KEY_LANGUAGES: 'ulb-fas-123'}
    _n_merged = merge_args(_conf_parser, _args)

    # assert
    assert _n_merged == [(CFG_SEC_OCR, KEY_LANGUAGES, 'ulb-fas-123')]
    # lat still present, only persian mapping was affected
    assert 'lat' in _conf_parser.get(CFG_SEC_OCR, KEY_MODEL_MAP)
