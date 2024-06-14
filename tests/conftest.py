# -*- coding: utf-8 -*-
"""Shared test functionalities"""

import os
import pathlib
import shutil

from pathlib import Path

import pytest

import digiflow as df

import lib.odem as odem


PROJECT_ROOT_DIR = pathlib.Path(__file__).resolve().parents[1]
PROD_RES = PROJECT_ROOT_DIR / 'resources'
TEST_RES = pathlib.Path(__file__).parents[0] / 'resources'


def fixture_configuration():
    """
    pwc: productive working configuration 
         => dc (say: 'default configuration')
    """

    config = odem.get_configparser()
    config.read(os.path.join(PROJECT_ROOT_DIR, 'resources', 'odem.ocrd.tesseract.ini'))
    config.set('global', 'data_fields', 'IDENTIFIER, SETSPEC, CREATED, INFO, STATE, STATE_TIME')
    config.set(odem.CFG_SEC_METS, 'blacklist_file_groups', 'DEFAULT, THUMB, THUMBS, MIN, FULLTEXT, DOWNLOAD')
    config.set(odem.CFG_SEC_METS, 'blacklist_logical_containers', 'cover_front,cover_back')
    config.set(odem.CFG_SEC_METS, 'blacklist_physical_container_labels',
               'Auftragszettel,Colorchecker,Leerseite,Rückdeckel,Deckblatt,Vorderdeckel,Illustration')
    config.set(odem.CFG_SEC_METS, 'agents', 'DFG-OCRD3-ODEM_ocrd/all:2022-08-15')
    config.set(odem.CFG_SEC_OCR, 'strip_tags', 'alto:Shape,alto:Processing,alto:Illustration,alto:GraphicalElement')
    config.set(odem.CFG_SEC_OCR, 'ocrd_baseimage', 'ocrd/all:2022-08-15')
    return config


def prepare_tessdata_dir(tmp_path: Path) -> str:
    """Generate MWE model data"""
    model_dir = tmp_path / 'tessdata'
    model_dir.mkdir()
    models = ['gt4hist_5000k', 'lat_ocr', 'grc', 'ger', 'fas']
    for _m in models:
        with open(str(model_dir / f'{_m}.traineddata'), 'wb') as writer:
            writer.write(b'abc')
    return str(model_dir)


def prepare_kraken_dir(tmp_path: Path) -> str:
    """Generate MWE model data"""
    model_dir = tmp_path / 'kraken'
    model_dir.mkdir()
    models = ['arabic_best']
    for _m in models:
        with open(str(model_dir / f'{_m}.mlmodel'), 'wb') as writer:
            writer.write(b'abc')
    return str(model_dir)


@pytest.fixture(name="fixture_27949", scope='module')
def _module_fixture_123456789_27949(tmp_path_factory):
    path_workdir = tmp_path_factory.mktemp('workdir')
    orig_file = TEST_RES / '123456789_27949.xml'
    trgt_mets = path_workdir / 'test.xml'
    orig_alto = TEST_RES / '123456789_27949_FULLTEXT'
    trgt_alto = path_workdir / 'FULLTEXT'
    shutil.copyfile(orig_file, trgt_mets)
    shutil.copytree(orig_alto, trgt_alto)
    (path_workdir / 'log').mkdir()
    _model_dir = prepare_tessdata_dir(path_workdir)
    record = df.OAIRecord('oai:dev.opendata.uni-halle.de:123456789/27949')
    _oproc = odem.ODEMProcessImpl(record, work_dir=path_workdir, log_dir=path_workdir / 'log')
    _oproc.odem_configuration = fixture_configuration()
    _oproc.odem_configuration.set(odem.CFG_SEC_OCR, odem.CFG_SEC_OCR_OPT_RES_VOL, f'{_model_dir}:/usr/local/share/ocrd-resources/ocrd-tesserocr-recognize')
    _oproc.ocr_files = [os.path.join(trgt_alto, a)
                        for a in os.listdir(trgt_alto)]
    _oproc.mets_file = str(trgt_mets)
    _oproc.inspect_metadata()
    _oproc.clear_existing_entries()
    n_integrated = _oproc.link_ocr_files()
    assert n_integrated == 4
    yield _oproc
