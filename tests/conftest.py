# -*- coding: utf-8 -*-
"""Shared test functionalities"""

import configparser
import os
import pathlib
import shutil
import typing

from pathlib import Path

import PIL.Image
import PIL.TiffImagePlugin as pil_tif
import numpy as np

import pytest

import digiflow.record as df_r

from lib import odem


PROJECT_ROOT_DIR = pathlib.Path(__file__).resolve().parents[1]
PROD_RES = PROJECT_ROOT_DIR / 'resources'
TEST_RES = pathlib.Path(__file__).parents[0] / 'resources'


def fixture_configuration():
    """
    pwc: productive working configuration 
         => dc (say: 'default configuration')
    """

    config = odem.get_configparser()
    config.read(os.path.join(PROJECT_ROOT_DIR, 'resources', 'odem.example.ini'))
    config.set(odem.CFG_SEC_FLOW, 'data_fields',
               'IDENTIFIER, SETSPEC, CREATED, INFO, STATE, STATE_TIME')
    config.set(odem.CFG_SEC_FLOW, odem.CFG_SEC_FLOW_OPT_URL,
               'https://opendata.uni-halle.de/oai/dd')
    config.set(odem.CFG_SEC_METS, 'blacklist_file_groups',
               'DEFAULT, THUMB, THUMBS, MIN, FULLTEXT, DOWNLOAD')
    config.set(odem.CFG_SEC_METS, 'blacklist_logical_containers', 'cover_front,cover_back')
    config.set(odem.CFG_SEC_METS, 'blacklist_physical_container_labels',
               'Auftragszettel,Colorchecker,Leerseite,Rückdeckel,Deckblatt,Vorderdeckel,Illustration')
    config.set(odem.CFG_SEC_METS, 'agents', 'DFG-OCRD3-ODEM_ocrd/all:2022-08-15')
    config.set(odem.CFG_SEC_OCR, 'strip_tags',
               'alto:Shape,alto:Processing,alto:Illustration,alto:GraphicalElement')
    config.set(odem.CFG_SEC_OCR, 'ocrd_baseimage', 'ocrd/all:2022-08-15')
    return config


def prepare_tessdata_dir(tmp_path: Path) -> str:
    """Generate MWE model data"""
    model_dir = tmp_path / 'tessdata'
    model_dir.mkdir()
    models = ['gt4hist_5000k', 'lat_ocr', 'grc', 'ger', 'fas']
    for _m in models:
        with open(str(model_dir / f'{_m}.traineddata'), 'wb') as tmp_model_writer:
            tmp_model_writer.write(b'abc')
    return str(model_dir)


def prepare_kraken_dir(tmp_path: Path) -> str:
    """Generate MWE model data"""
    model_dir = tmp_path / 'kraken'
    model_dir.mkdir()
    models = ['arabic_best']
    for _m in models:
        with open(str(model_dir / f'{_m}.mlmodel'), 'wb') as tmp_model_writer:
            tmp_model_writer.write(b'abc')
    return str(model_dir)


@pytest.fixture(name="fixture_27949", scope='module')
def _module_fixture_123456789_27949(tmp_path_factory):
    identifier = '123456789_27949'
    work_dir_root = tmp_path_factory.mktemp('work_dir')
    (work_dir_root / 'log').mkdir()
    path_work_dir = work_dir_root / identifier
    path_work_dir.mkdir()
    orig_file = TEST_RES / f'{identifier}.xml'
    trgt_mets = path_work_dir / f'{identifier}.xml'
    orig_alto = TEST_RES / '123456789_27949_FULLTEXT'
    trgt_alto = path_work_dir / 'FULLTEXT'
    shutil.copyfile(orig_file, trgt_mets)
    shutil.copytree(orig_alto, trgt_alto)
    _model_dir = prepare_tessdata_dir(path_work_dir)
    record = df_r.Record('oai:dev.opendata.uni-halle.de:123456789/27949')
    mock_cfg: configparser.ConfigParser = fixture_configuration()
    mock_cfg.set(odem.CFG_SEC_OCR, odem.CFG_SEC_OCR_OPT_RES_VOL,
                 f'{_model_dir}:/usr/local/share/ocrd-resources/ocrd-tesserocr-recognize')
    odem_proc = odem.ODEMProcessImpl(mock_cfg, work_dir=path_work_dir,
                                     log_dir=work_dir_root / 'log',
                                     record=record)
    odem_proc.ocr_files = [os.path.join(trgt_alto, a)
                           for a in os.listdir(trgt_alto)]
    odem_proc.mets_file_path = str(trgt_mets)
    odem_proc.inspect_metadata()
    odem_proc.modify_mets_groups()
    n_integrated = odem_proc.link_ocr_files()
    assert n_integrated == 4
    yield odem_proc


def create_test_tif(path_image: Path, widht=60, height=100):
    """Create random image with proper metadata"""

    arr = np.random.randint(0, 256, (height, widht), np.uint8)
    the_img: PIL.Image = PIL.Image.fromarray(arr)
    tiff_tags: typing.Dict = {
        pil_tif.RESOLUTION_UNIT: 2,
        pil_tif.SAMPLESPERPIXEL: 1,
        pil_tif.X_RESOLUTION: 300,
        pil_tif.Y_RESOLUTION: 300,
    }
    the_img.save(path_image, tiffinfo=tiff_tags)
