# -*- coding: utf-8 -*-
"""Specification ODEM API"""

import os
import shutil
import unittest
import unittest.mock

from pathlib import Path

import digiflow as df
import digiflow.record as df_r
import lxml.etree as ET

import pytest

from lib import odem
import lib.odem.odem_commons as odem_c

from .conftest import (
    PROJECT_ROOT_DIR,
    TEST_RES,
    fixture_configuration,
    prepare_tessdata_dir, prepare_kraken_dir,
)


@pytest.mark.parametrize("img_path,lang_str", [
    ('resources/urn+nbn+de+gbv+3+1-116899-p0062-3_ger.jpg', 'gt4hist_5000k.traineddata'),
    ('resources/urn+nbn+de+gbv+3+1-116299-p0107-6_lat+ger.jpg',
     'lat_ocr.traineddata+gt4hist_5000k.traineddata'),
    ('resources/urn+nbn+de+gbv+3+1-118702-p0055-9_gre+lat.jpg',
     'grc.traineddata+lat_ocr.traineddata'),
    ('resources/urn+nbn+de+gbv+3+1-116899-p0062-3_ger.jpg', 'gt4hist_5000k.traineddata'),
    ('resources/urn+nbn+de+gbv+3+1-116299-p0107-6_lat.jpg', 'lat_ocr.traineddata'),
    ('resources/urn+nbn+de+gbv+3+1-118702-p0055-9_ger+lat.jpg',
     'gt4hist_5000k.traineddata+lat_ocr.traineddata')
])
def test_mapping_from_imagefilename(img_path, lang_str, tmp_path):
    """Ensure ODEM Object picks 
    proper project language mappings
    """

    work_dir = tmp_path / 'work_dir'
    work_dir.mkdir()
    work_2 = work_dir / 'test2'
    work_2.mkdir()
    log_dir = tmp_path / 'log'
    log_dir.mkdir()
    odem_processor = odem.ODEMProcessImpl(None, work_dir=str(work_2))
    odem_processor.configuration = fixture_configuration()
    _tess_dir = prepare_tessdata_dir(tmp_path)
    odem_processor.configuration.set(odem.CFG_SEC_OCR, odem.CFG_SEC_OCR_OPT_RES_VOL,
                                     f'{_tess_dir}:/usr/local/share/ocrd-resources/ocrd-tesserocr-recognize')
    odem_processor.logger = odem.get_logger(str(log_dir))
    odem_processor.local_mode = True

    # act
    assert odem_processor.map_language_to_modelconfig(img_path) == lang_str


@pytest.mark.parametrize("img_path,langs,models", [
    ('resources/urn+nbn+de+gbv+3+1-116899-p0062-3_fre.jpg', 'lat', 'lat_ocr.traineddata'),
    ('resources/urn+nbn+de+gbv+3+1-116299-p0107-6_lat+ger.jpg', 'ger+lat',
     'gt4hist_5000k.traineddata+lat_ocr.traineddata'),
    ('resources/urn+nbn+de+gbv+3+1-116299-p0107-6_lat.jpg', 'ger', 'gt4hist_5000k.traineddata'),
])
def test_exchange_language(img_path, langs, models, tmp_path):
    """Ensure: ODEM can be forced to use different
    languages than are present via file name
    """

    # arrange
    work_dir = tmp_path / 'work_dir'
    work_dir.mkdir()
    work_2 = work_dir / 'test2'
    work_2.mkdir()
    log_dir = tmp_path / 'log'
    log_dir.mkdir()
    odem_processor = odem.ODEMProcessImpl(None, work_dir=str(work_2))
    odem_processor.configuration = fixture_configuration()
    _tess_dir = prepare_tessdata_dir(tmp_path)
    odem_processor.configuration.set(
        odem.CFG_SEC_OCR,
        odem.CFG_SEC_OCR_OPT_RES_VOL,
        f"{_tess_dir}:/dummy"
    )
    odem_processor.configuration.set(odem.CFG_SEC_OCR, odem.KEY_LANGUAGES, langs)
    odem_processor.logger = odem.get_logger(str(log_dir))
    odem_processor.local_mode = True

    # act
    assert odem_processor.map_language_to_modelconfig(img_path) == models


def test_enforce_language_and_model_mapping(tmp_path):
    """Behavior when both language and model
    mapping are provided via configuration
    for several local files

    Rationale: prevent subsequent language
    additions like 'fas+fas+fas'
    """

    # arrange
    work_dir = tmp_path / 'work_dir'
    work_dir.mkdir()
    work_2 = work_dir / 'test2'
    work_2.mkdir()
    log_dir = tmp_path / 'log'
    log_dir.mkdir()
    odem_processor = odem.ODEMProcessImpl(None, work_dir=str(work_2))
    odem_processor.configuration = fixture_configuration()
    _tess_dir = prepare_tessdata_dir(tmp_path)
    _kraken_dir = prepare_kraken_dir(tmp_path)
    odem_processor.configuration.set(
        odem.CFG_SEC_OCR,
        odem.CFG_SEC_OCR_OPT_RES_VOL,
        f'{_tess_dir}:/dummy,{_kraken_dir}:/dummy'
    )
    odem_processor.configuration.set(odem.CFG_SEC_OCR, odem.KEY_LANGUAGES, 'ara+fas')
    odem_processor.configuration.set(
        odem.CFG_SEC_OCR,
        odem.KEY_MODEL_MAP,
        'fas: fas.traineddata, ara:arabic_best.mlmodel'
    )
    odem_processor.logger = odem.get_logger(str(log_dir))
    odem_processor.local_mode = True

    # act 1st
    odem_processor.configuration.set(odem.CFG_SEC_OCR, odem.CFG_SEC_OCR_OPT_MODEL_COMBINABLE, 'False')
    assert odem_processor.map_language_to_modelconfig('/data/img/0001.tif') == 'arabic_best.mlmodel'
    # act 2nd
    odem_processor.configuration.set(odem.CFG_SEC_OCR, odem.CFG_SEC_OCR_OPT_MODEL_COMBINABLE, 'True')
    assert odem_processor.map_language_to_modelconfig('/data/img/0002.tif') == 'arabic_best.mlmodel+fas.traineddata'
    # act 3rd call. still only fas:fas
    odem_processor.configuration.set(odem.CFG_SEC_OCR, odem.CFG_SEC_OCR_OPT_MODEL_COMBINABLE, 'False')
    odem_processor.configuration.set(odem.CFG_SEC_OCR, odem.KEY_LANGUAGES, 'fas')
    assert odem_processor.map_language_to_modelconfig('/data/img/0003.tif') == 'fas.traineddata'


def test_load_mock_called(tmp_path_factory):
    """Initial ODEM fixture before doing any OCR"""

    def _side_effect(*args, **kwargs):
        """just copy the local test resource
        to the expected workdir instead of
        doing *real* requests with storage"""

        orig_file = TEST_RES / '1981185920_44046.xml'
        trgt_mets = workdir / '1981185920_44046.xml'
        shutil.copyfile(orig_file, trgt_mets)

    root_workdir = tmp_path_factory.mktemp('workdir')
    workdir = root_workdir / '1981185920_44046'
    workdir.mkdir()
    log_dir = root_workdir / 'log'
    log_dir.mkdir()
    record = df_r.Record('oai:opendata.uni-halle.de:1981185920/44046')
    odem_proc = odem.ODEMProcessImpl(fixture_configuration(), workdir,
                                     odem.get_logger(str(log_dir)),
                                     str(log_dir), record)
    model_dir = prepare_tessdata_dir(workdir)
    odem_proc.configuration.set(odem.CFG_SEC_OCR, odem.CFG_SEC_OCR_OPT_RES_VOL,
                                f'{model_dir}:/usr/local/share/ocrd-resources/ocrd-tesserocr-recognize')
    odem_proc.logger = odem.get_logger(str(log_dir))

    # mock loading of OAI Record
    # actual load attempt *must* be done
    # in scope of mocked object to work!
    with unittest.mock.patch('digiflow.OAILoader.load') as request_mock:
        request_mock.side_effect = _side_effect
        odem_proc.load()

    # act
    odem_proc.inspect_metadata()

    # assert
    assert request_mock.call_count == 1
    assert os.path.exists(odem_proc.mets_file_path)


def test_odem_process_identifier_local_workdir(tmp_path):
    """Ensure expected identifier calculated
    if no OAI record present at all"""

    # arrange
    _workdir = tmp_path / 'workdir' / 'foo_bar'
    _workdir.mkdir(parents=True, exist_ok=True)

    # act
    odem_proc = odem.ODEMProcessImpl(None, _workdir)

    # assert
    assert odem_proc.process_identifier == 'foo_bar'


@pytest.fixture(name='odem_processor')
def _fixture_odem_setup(tmp_path):
    work_dir = tmp_path / 'work_dir'
    work_dir.mkdir()
    work_2 = work_dir / 'test2'
    work_2.mkdir()
    log_dir = tmp_path / 'log'
    log_dir.mkdir()
    odem_processor = odem.ODEMProcessImpl(None, work_dir=str(work_2))
    cfg = odem.get_configparser()
    cfg.read(os.path.join(PROJECT_ROOT_DIR, 'resources', 'odem.example.ini'))
    odem_processor.configuration = cfg
    model_dir = prepare_tessdata_dir(work_dir)
    model_dir_dst = f'{model_dir}:/usr/local/share/ocrd-resources/ocrd-tesserocr-recognize'
    odem_processor.configuration.set(odem.CFG_SEC_OCR, odem.CFG_SEC_OCR_OPT_RES_VOL, model_dir_dst)
    odem_processor.local_mode = True
    odem_processor.logger = odem.get_logger(log_dir)
    return odem_processor


def test_lang_mapping_missing_conf_error(odem_processor: odem.ODEMProcessImpl):
    """Ensure unknown language mapping caught properly"""

    # arrange
    img_path = 'resources/urn+nbn+de+gbv+3+1-116899-p0062-3_gop.jpg'

    # act
    with pytest.raises(odem.ODEMException) as err:
        odem_processor.map_language_to_modelconfig(img_path)

    # assert
    assert "'gop' mapping not found (languages: ['gop'])!" in err.value.args[0]


def test_lang_mapping_missing_lang_error(odem_processor: odem.ODEMProcessImpl):
    """Ensure cannot map dummy language 'yyy.traineddata'"""

    # arrange
    img_path = 'resources/urn+nbn+de+gbv+3+1-116899-p0062-3_xxx.jpg'

    # act
    with pytest.raises(odem.ODEMException) as err:
        odem_processor.map_language_to_modelconfig(img_path)

    # assert
    assert "'yyy.traineddata' model config not found !" in err.value.args[0]


def test_module_fixture_one_integrated_ocr_in_mets(fixture_27949: odem.ODEMProcessImpl):
    """Ensure, that generated final OCR files
    * are properly linked into original METS
    * contain required link data to images
    """

    # arrange
    assert len(fixture_27949.ocr_files) == 4

    _root = ET.parse(fixture_27949.mets_file_path).getroot()
    _phys_links = _root.xpath('//mets:div[@TYPE="physSequence"]/mets:div', namespaces=df.XMLNS)
    # at most 2: one MAX-Image plus according optional FULLTEXT
    assert len(_phys_links[1].getchildren()) == 1
    assert len(_phys_links[2].getchildren()) == 2
    assert len(_phys_links[3].getchildren()) == 2
    assert len(_phys_links[4].getchildren()) == 2
    assert len(_phys_links[5].getchildren()) == 2
    assert len(_phys_links[6].getchildren()) == 1


def test_module_fixture_one_images_4_ocr_by_metadata(fixture_27949: odem.ODEMProcessImpl):
    """Ensure setting and filtering of images behavior.

    Record oai:dev.opendata.uni-halle.de:123456789/27949
    just 4 out of total 9 images picked because of
    their physical presens and according METS metadata
    """

    assert len(fixture_27949.ocr_candidates) == 4


def test_fixture_one_postprocess_ocr_create_text_bundle(fixture_27949: odem.ODEMProcessImpl):
    """Ensure text bundle data created
    and present with expected number of text rows
    Please note:
    according to workflow modifications the ocr-output
    is no longer postprocessed, and lots of to short
    non-alphabetical lines will remain
    therefore line number increased from 77 => 111
    """

    # arrange
    tmp_path = fixture_27949.work_dir_root

    # act
    fixture_27949.link_ocr_files()
    fixture_27949.create_text_bundle_data()

    # assert
    _txt_bundle_file = Path(tmp_path) / '198114125.pdf.txt'
    assert os.path.exists(_txt_bundle_file)
    assert 111 == fixture_27949.statistics['n_text_lines']
    with open(_txt_bundle_file, encoding='utf-8') as bundle_handle:
        assert 111 == len(bundle_handle.readlines())


def test_images_4_ocr_properly_filtered(tmp_path):
    """Ensure behavior links selected
    from images to images_4_ocr as expected

    1981185920_44046 => 5 images, 4 ocr-able
    (last one's container is labled '[Colorchecker]'
    which shall not be passed for ocr-ing)

    """

    record = df_r.Record('oai:opendata.uni-halle.de:1981185920/44046')
    work_dir = tmp_path / '1981185920_44046'
    work_dir.mkdir()
    log_dir = tmp_path / 'log'
    log_dir.mkdir()
    max_dir = work_dir / 'MAX'
    max_dir.mkdir()
    for i in range(1, 6):
        _file_path = f"{max_dir}/{i:08d}.jpg"
        with open(_file_path, 'wb') as tmp_img_writer:
            tmp_img_writer.write(b'0x00')
    orig_mets = TEST_RES / '1981185920_44046.xml'
    shutil.copyfile(orig_mets, work_dir / '1981185920_44046.xml')
    odem_processor = odem.ODEMProcessImpl(fixture_configuration(), work_dir,
                                          odem.get_logger(str(log_dir)), record=record)
    # act
    odem_processor.inspect_metadata()
    odem_processor.set_local_images()

    # assert
    assert len(odem_processor.ocr_candidates) == 4
    assert odem_processor.ocr_candidates[0][0].endswith('1981185920_44046/MAX/00000001.jpg')


@unittest.mock.patch('digiflow.OAILoader.load', side_effect=df.LoadException("url '{}' returned '{}'"))
def test_no_catch_when_load_exc(mock_load, tmp_path):
    """Ensure df.OAILoadException is raised for internal server errors (#9992)
    """

    # arrange
    record = df_r.Record('oai:opendata.uni-halle.de:1981185920/44046')
    work_dir = tmp_path / '1981185920_44046'
    work_dir.mkdir()
    log_dir = tmp_path / 'log'
    log_dir.mkdir()
    odem_proc = odem.ODEMProcessImpl(fixture_configuration(),
                                     work_dir,
                                     logger=None,
                                     log_dir=log_dir, record=record)
    odem_proc.configuration.read(os.path.join(PROJECT_ROOT_DIR, 'resources', 'odem.ocrd.tesseract.ini'))
    odem_proc.logger = odem.get_logger(str(log_dir))

    # act
    with pytest.raises(df.LoadException) as err:
        odem_proc.load()

    # assert
    assert "url" in err.value.args[0] and "returned" in err.value.args[0]
    assert mock_load.called == 1


def test_record_with_unknown_language(tmp_path):
    """Fix behavior when opendata record has one
    of total 3 languages which is actually
    not unknown (gmh == German, Middle High 1050-1500)
    """

    identifier = '1981185920_72977'
    path_workdir = tmp_path / identifier
    path_workdir.mkdir()
    orig_file = TEST_RES / f'{identifier}.xml'
    trgt_mets = path_workdir / f'{identifier}.xml'
    shutil.copyfile(orig_file, trgt_mets)
    log_dir = path_workdir / 'log'
    log_dir.mkdir()
    record = df_r.Record('oai:opendata.uni-halle.de:1981185920/72977')
    oproc = odem.ODEMProcessImpl(fixture_configuration(),
                                     path_workdir,
                                     logger=None,
                                     log_dir=log_dir, record=record)
    model_dir = prepare_tessdata_dir(tmp_path)
    oproc.configuration.set(odem.CFG_SEC_OCR, odem.CFG_SEC_OCR_OPT_RES_VOL,
                            f'{model_dir}:/usr/local/share/ocrd-resources/ocrd-tesserocr-recognize')
    oproc.mets_file_path = str(trgt_mets)
    oproc.inspect_metadata()
    langs = oproc.statistics.get(odem.STATS_KEY_LANGS)

    # act
    with pytest.raises(odem.ODEMModelMissingException) as odem_exc:
        oproc.language_modelconfig(langs)

    # assert
    assert "'gmh' mapping not found (languages: ['lat', 'ger', 'gmh'])!" == odem_exc.value.args[0]


def test_export_flat_zip(tmp_path):
    """Test export workflow for a
    flat archive layout like was
    use of old with the legacy
    semantics VLS systems
    """

    identifier = '1981185920_44046'
    path_workdir = tmp_path / identifier
    path_workdir.mkdir()
    path_tmp_export_dir = tmp_path / 'tmp_export'
    path_tmp_export_dir.mkdir()
    path_export_dir = tmp_path / 'export'
    path_export_dir.mkdir()

    orig_file = TEST_RES / '1981185920_44046.xml'
    trgt_mets = path_workdir / f'{identifier}.xml'
    shutil.copyfile(orig_file, trgt_mets)

    orig_files = TEST_RES / 'vd18-1180329' / 'FULLTEXT'
    trgt_files = path_workdir / 'FULLTEXT'
    shutil.copytree(orig_files, trgt_files)
    log_dir = path_workdir / 'log'
    log_dir.mkdir()
    record = df_r.Record('oai:opendata.uni-halle.de:1981185920/44046')
    oproc = odem.ODEMProcessImpl(fixture_configuration(),
                                 path_workdir,
                                 logger=None, log_dir=log_dir, record=record)
    model_dir = prepare_tessdata_dir(tmp_path)
    oproc.configuration.set(odem_c.CFG_SEC_EXP,
                            odem_c.CFG_SEC_EXP_OPT_FORMAT,
                            odem.ExportFormat.FLAT_ZIP)
    oproc.configuration.set(odem_c.CFG_SEC_EXP,
                            odem_c.CFG_SEC_EXP_OPT_TMP,
                            str(path_tmp_export_dir))
    oproc.configuration.set(odem_c.CFG_SEC_EXP,
                            odem_c.CFG_SEC_EXP_OPT_DST,
                            str(path_export_dir))
    oproc.configuration.set(
        odem.CFG_SEC_OCR,
        odem.CFG_SEC_OCR_OPT_RES_VOL,
        f'{model_dir}:/usr/local/share/ocrd-resources/ocrd-tesserocr-recognize'
    )

    oproc.mets_file_path = str(trgt_mets)
    oproc.inspect_metadata()

    # act
    zipfilepath, _ = oproc.export_data()

    # assert
    assert os.path.exists(zipfilepath) and os.path.getsize(zipfilepath) == 58552

_PATH_38841 = '/odem-wrk-dir/1981185920_38841/PAGE/'
def test_odem_common_ocr_statistics(tmp_path):
    """Fix behavor for common data"""

    # arrange
    identifier = '1981185920_44046'
    path_workdir = tmp_path / identifier
    path_workdir.mkdir()
    path_log_dir = path_workdir / 'log'
    path_log_dir.mkdir()
    record = df_r.Record('oai:opendata.uni-halle.de:1981185920/44046')
    oproc = odem.ODEMProcessImpl(record, work_dir=path_workdir, log_dir=path_log_dir)
    oproc.ocr_candidates = [('/MAX/00000002.jpg', 'PHYS_02'),
                            ('/MAX/00000003.jpg', 'PHYS_03'),
                            ('/MAX/00000004.jpg', 'PHYS_04'),
                            ('/MAX/00000005.jpg', 'PHYS_05'),
                            ('/MAX/00000006.jpg', 'PHYS_06'),
                            ]
    ocr_outcomes = [odem_c.OCRResult(f'{_PATH_38841}/PAGE/00000002.xml', 0.5577, 3.893415),
                    odem_c.OCRResult(f'{_PATH_38841}/PAGE/00000003.xml', 0.6628, 3.893415),
                    odem_c.OCRResult(f'{_PATH_38841}/PAGE/00000004.xml', 0.6748, 3.893415),
                    odem_c.OCRResult(f'{_PATH_38841}/PAGE/00000005.xml', 0.6669, 3.893415),
                    odem_c.OCRResult(f'{_PATH_38841}/PAGE/00000006.xml', 0.6753, 3.893415)]

    # act
    oproc.calculate_statistics_ocr(ocr_outcomes)

    # assert
    assert oproc.statistics.get(odem.STATS_KEY_MB) == 3.24
    assert oproc.statistics.get(odem.STATS_KEY_N_OCR) == 5
    assert odem.STATS_KEY_OCR_LOSS not in oproc.statistics


def test_odem_ocr_statistics_some_loss(tmp_path):
    """Fix behavor if single data set missing
    Actually we miss OCR data for image 00000005.jpg
    """

    # arrange
    identifier = '1981185920_44046'
    path_workdir = tmp_path / identifier
    path_workdir.mkdir()
    path_log_dir = path_workdir / 'log'
    path_log_dir.mkdir()
    record = df_r.Record('oai:opendata.uni-halle.de:1981185920/44046')
    oproc = odem.ODEMProcessImpl(record, work_dir=path_workdir, log_dir=path_log_dir)
    oproc.ocr_candidates = [('/MAX/00000002.jpg', 'PHYS_02'),
                            ('/MAX/00000003.jpg', 'PHYS_03'),
                            ('/MAX/00000004.jpg', 'PHYS_04'),
                            ('/MAX/00000005.jpg', 'PHYS_05'),
                            ('/MAX/00000006.jpg', 'PHYS_06'),
                            ]
    ocr_outcomes = [odem_c.OCRResult(f'{_PATH_38841}/PAGE/00000002.xml', 0.5577, 3.893415),
                    odem_c.OCRResult(f'{_PATH_38841}/PAGE/00000003.xml', 0.6628, 3.893415),
                    odem_c.OCRResult(f'{_PATH_38841}/PAGE/00000004.xml', 0.6748, 3.893415),
                    odem_c.OCRResult(f'{_PATH_38841}/PAGE/00000006.xml', 0.6753, 3.893415)
                    ]

    # act
    oproc.calculate_statistics_ocr(ocr_outcomes)

    # assert
    assert oproc.statistics.get(odem.STATS_KEY_N_OCR) == 4
    assert odem.STATS_KEY_OCR_LOSS in oproc.statistics
    assert oproc.statistics.get(odem.STATS_KEY_OCR_LOSS) == ['00000005']
    assert oproc.statistics.get(odem.STATS_KEY_MPS) == [(3.9, 4)]
