# -*- coding: utf-8 -*-
"""Specification ODEM API"""

import os
import shutil
from unittest import (
    mock
)

import lxml.etree as ET
import pytest
from digiflow import (
    OAIRecord,
    OAILoadException
)

from lib.ocrd3_odem import (
    CFG_SEC_OCR,
    KEY_LANGUAGES,
    KEY_MODEL_MAP,
    STATS_KEY_LANGS,
    XMLNS,
    ODEMProcess,
    ODEMException,
    get_configparser,
    get_logger, CFG_KEY_RES_VOL, CFG_KEY_MODEL_COMBINABLE, ExportFormat,
)
from .conftest import (
    PROJECT_ROOT_DIR,
    TEST_RES,
    fixture_configuration,
    prepare_tessdata_dir, prepare_kraken_dir,
)


@pytest.mark.parametrize("img_path,lang_str", [
    ('resources/urn+nbn+de+gbv+3+1-116899-p0062-3_ger.jpg', 'gt4hist_5000k.traineddata'),
    ('resources/urn+nbn+de+gbv+3+1-116299-p0107-6_lat+ger.jpg', 'lat_ocr.traineddata+gt4hist_5000k.traineddata'),
    ('resources/urn+nbn+de+gbv+3+1-118702-p0055-9_gre+lat.jpg', 'grc.traineddata+lat_ocr.traineddata'),
    ('resources/urn+nbn+de+gbv+3+1-116899-p0062-3_ger.jpg', 'gt4hist_5000k.traineddata'),
    ('resources/urn+nbn+de+gbv+3+1-116299-p0107-6_lat.jpg', 'lat_ocr.traineddata'),
    ('resources/urn+nbn+de+gbv+3+1-118702-p0055-9_ger+lat.jpg', 'gt4hist_5000k.traineddata+lat_ocr.traineddata')
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
    odem_processor = ODEMProcess(None, work_dir=str(work_2))
    odem_processor.cfg = fixture_configuration()
    _tess_dir = prepare_tessdata_dir(tmp_path)
    odem_processor.cfg.set(CFG_SEC_OCR, CFG_KEY_RES_VOL,
                           f'{_tess_dir}:/usr/local/share/ocrd-resources/ocrd-tesserocr-recognize')
    odem_processor.the_logger = get_logger(str(log_dir))
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
    odem_processor = ODEMProcess(None, work_dir=str(work_2))
    odem_processor.cfg = fixture_configuration()
    _tess_dir = prepare_tessdata_dir(tmp_path)
    odem_processor.cfg.set(
        CFG_SEC_OCR,
        CFG_KEY_RES_VOL,
        f"{_tess_dir}:/dummy"
    )
    odem_processor.cfg.set(CFG_SEC_OCR, KEY_LANGUAGES, langs)
    odem_processor.the_logger = get_logger(str(log_dir))
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
    odem_processor = ODEMProcess(None, work_dir=str(work_2))
    odem_processor.cfg = fixture_configuration()
    _tess_dir = prepare_tessdata_dir(tmp_path)
    _kraken_dir = prepare_kraken_dir(tmp_path)
    odem_processor.cfg.set(
        CFG_SEC_OCR,
        CFG_KEY_RES_VOL,
        f'{_tess_dir}:/dummy,{_kraken_dir}:/dummy'
    )
    odem_processor.cfg.set(CFG_SEC_OCR, KEY_LANGUAGES, 'ara+fas')
    odem_processor.cfg.set(
        CFG_SEC_OCR,
        KEY_MODEL_MAP,
        'fas: fas.traineddata, ara:arabic_best.mlmodel'
    )
    odem_processor.the_logger = get_logger(str(log_dir))
    odem_processor.local_mode = True

    # act 1st
    odem_processor.cfg.set(CFG_SEC_OCR, CFG_KEY_MODEL_COMBINABLE, 'False')
    assert odem_processor.map_language_to_modelconfig('/data/img/0001.tif') == 'arabic_best.mlmodel'
    # act 2nd
    odem_processor.cfg.set(CFG_SEC_OCR, CFG_KEY_MODEL_COMBINABLE, 'True')
    assert odem_processor.map_language_to_modelconfig('/data/img/0002.tif') == 'arabic_best.mlmodel+fas.traineddata'
    # act 3rd call. still only fas:fas
    odem_processor.cfg.set(CFG_SEC_OCR, CFG_KEY_MODEL_COMBINABLE, 'False')
    odem_processor.cfg.set(CFG_SEC_OCR, KEY_LANGUAGES, 'fas')
    assert odem_processor.map_language_to_modelconfig('/data/img/0003.tif') == 'fas.traineddata'


def test_load_mock_called(tmp_path_factory):
    """Initial ODEM fixture before doing any OCR"""

    def _side_effect(*args, **kwargs):
        """just copy the local test resource
        to the expected workdir instead of
        doing *real* requests with storage"""

        orig_file = TEST_RES / '1981185920_44046.xml'
        trgt_mets = _workdir / '1981185920_44046.xml'
        shutil.copyfile(orig_file, trgt_mets)

    _root_workdir = tmp_path_factory.mktemp('workdir')
    _workdir = _root_workdir / '1981185920_44046'
    _workdir.mkdir()
    _log_dir = _root_workdir / 'log'
    _log_dir.mkdir()
    _record = OAIRecord('oai:opendata.uni-halle.de:1981185920/44046')
    odem = ODEMProcess(_record, _workdir)
    odem.cfg = fixture_configuration()
    _model_dir = prepare_tessdata_dir(_workdir)
    odem.cfg.set(CFG_SEC_OCR, CFG_KEY_RES_VOL, f'{_model_dir}:/usr/local/share/ocrd-resources/ocrd-tesserocr-recognize')
    odem.the_logger = get_logger(str(_log_dir))

    # mock loading of OAI Record
    # actual load attempt *must* be done
    # in scope of mocked object to work!
    with mock.patch('digiflow.OAILoader.load') as request_mock:
        request_mock.side_effect = _side_effect
        odem.load()

    # act
    odem.inspect_metadata()

    # assert
    assert request_mock.call_count == 1
    assert os.path.exists(odem.mets_file)


def test_odem_process_identifier_local_workdir(tmp_path):
    """Ensure expected identifier calculated
    if no OAI record present at all"""

    # arrange
    _workdir = tmp_path / 'workdir' / 'foo_bar'
    _workdir.mkdir(parents=True, exist_ok=True)

    # act
    odem = ODEMProcess(None, _workdir)

    # assert
    assert odem.process_identifier == 'foo_bar'


@pytest.fixture(name='odem_processor')
def _fixture_odem_setup(tmp_path):
    work_dir = tmp_path / 'work_dir'
    work_dir.mkdir()
    work_2 = work_dir / 'test2'
    work_2.mkdir()
    log_dir = tmp_path / 'log'
    log_dir.mkdir()
    odem_processor = ODEMProcess(None, work_dir=str(work_2))
    cfg = get_configparser()
    cfg.read(os.path.join(PROJECT_ROOT_DIR, 'resources', 'odem.ocrd.tesseract.ini'))
    odem_processor.cfg = cfg
    _model_dir = prepare_tessdata_dir(work_dir)
    odem_processor.cfg.set(CFG_SEC_OCR, CFG_KEY_RES_VOL,
                           f'{_model_dir}:/usr/local/share/ocrd-resources/ocrd-tesserocr-recognize')
    odem_processor.local_mode = True
    odem_processor.the_logger = get_logger(log_dir)
    return odem_processor


def test_lang_mapping_missing_conf_error(odem_processor: ODEMProcess):
    """Ensure unknown language mapping caught properly"""

    # arrange
    img_path = 'resources/urn+nbn+de+gbv+3+1-116899-p0062-3_gop.jpg'

    # act
    with pytest.raises(ODEMException) as err:
        odem_processor.map_language_to_modelconfig(img_path)

    # assert
    assert "'gop' mapping not found (languages: ['gop'])!" in err.value.args[0]


def test_lang_mapping_missing_lang_error(odem_processor: ODEMProcess):
    """Ensure cannot map dummy language 'yyy.traineddata'"""

    # arrange
    img_path = 'resources/urn+nbn+de+gbv+3+1-116899-p0062-3_xxx.jpg'

    # act
    with pytest.raises(ODEMException) as err:
        odem_processor.map_language_to_modelconfig(img_path)

    # assert
    assert "'yyy.traineddata' model config not found !" in err.value.args[0]


def test_module_fixture_one_integrated_ocr_in_mets(fixture_27949: ODEMProcess):
    """Ensure, that generated final OCR files
    * are properly linked into original METS
    * contain required link data to images
    """

    # arrange
    assert len(fixture_27949.ocr_files) == 4

    _root = ET.parse(fixture_27949.mets_file).getroot()
    _phys_links = _root.xpath('//mets:div[@TYPE="physSequence"]/mets:div', namespaces=XMLNS)
    # at most 2: one MAX-Image plus according optional FULLTEXT
    assert len(_phys_links[1].getchildren()) == 1
    assert len(_phys_links[2].getchildren()) == 2
    assert len(_phys_links[3].getchildren()) == 2
    assert len(_phys_links[4].getchildren()) == 2
    assert len(_phys_links[5].getchildren()) == 2
    assert len(_phys_links[6].getchildren()) == 1


def test_module_fixture_one_images_4_ocr_by_metadata(fixture_27949: ODEMProcess):
    """Ensure setting and filtering of images behavior.

    Record oai:dev.opendata.uni-halle.de:123456789/27949
    just 4 out of total 9 images picked because of
    their physical presens and according METS metadata
    """

    assert len(fixture_27949.images_4_ocr) == 4


def test_fixture_one_postprocess_ocr_create_text_bundle(fixture_27949: ODEMProcess):
    """Ensure text bundle data created
    and present with expected number of text rows
    """

    # arrange
    tmp_path = fixture_27949.work_dir_main

    # act
    fixture_27949.link_ocr()
    fixture_27949.postprocess_ocr()
    fixture_27949.create_text_bundle_data()

    # assert
    _txt_bundle_file = tmp_path / '198114125.pdf.txt'
    assert os.path.exists(_txt_bundle_file)
    assert 77 == fixture_27949.statistics['n_text_lines']
    with open(_txt_bundle_file, encoding='utf-8') as bundle_handle:
        assert 77 == len(bundle_handle.readlines())


def test_validate_mets(tmp_path):
    """
    if is invalid mets file, throw according exception
    """
    _record = OAIRecord('oai:opendata.uni-halle.de:1981185920/105054')
    _work_dir = tmp_path / '1981185920_105054'
    _work_dir.mkdir()
    _max_dir = _work_dir / 'MAX'
    _max_dir.mkdir()
    for i in range(1, 6):
        _file_path = f"{_max_dir}/{i:08d}.jpg"
        with open(_file_path, 'wb') as _writer:
            _writer.write(b'0x00')
    _orig_mets = TEST_RES / '1981185920_105054.xml'
    shutil.copyfile(_orig_mets, _work_dir / '1981185920_105054.xml')
    odem_processor = ODEMProcess(_record, work_dir=_work_dir)
    with pytest.raises(ODEMException):
        odem_processor.validate_mets()


def test_images_4_ocr_properly_filtered(tmp_path):
    """Ensure behavior links selected
    from images to images_4_ocr as expected

    1981185920_44046 => 5 images, 4 ocr-able
    (last one's container is labled '[Colorchecker]'
    which shall not be passed for ocr-ing)

    """

    _record = OAIRecord('oai:opendata.uni-halle.de:1981185920/44046')
    _work_dir = tmp_path / '1981185920_44046'
    _work_dir.mkdir()
    _max_dir = _work_dir / 'MAX'
    _max_dir.mkdir()
    for i in range(1, 6):
        _file_path = f"{_max_dir}/{i:08d}.jpg"
        with open(_file_path, 'wb') as _writer:
            _writer.write(b'0x00')
    _orig_mets = TEST_RES / '1981185920_44046.xml'
    shutil.copyfile(_orig_mets, _work_dir / '1981185920_44046.xml')
    odem_processor = ODEMProcess(_record, work_dir=_work_dir)
    cfg = get_configparser()
    cfg.read(os.path.join(PROJECT_ROOT_DIR, 'resources', 'odem.ocrd.tesseract.ini'))
    odem_processor.cfg = cfg
    _log_dir = tmp_path / 'log'
    _log_dir.mkdir()
    odem_processor.the_logger = get_logger(str(_log_dir))

    # act
    odem_processor.inspect_metadata()
    odem_processor.set_local_images()

    # assert
    assert len(odem_processor.images_4_ocr) == 4
    assert odem_processor.images_4_ocr[0][0].endswith('1981185920_44046/MAX/00000001.jpg')


@mock.patch('digiflow.OAILoader.load', side_effect=OAILoadException("url '{}' returned '{}'"))
def test_no_catch_when_load_exc(mock_load, tmp_path):
    """Ensure OAILoadException is raised for internal server errors (#9992)
    """

    # arrange
    _record = OAIRecord('oai:opendata.uni-halle.de:1981185920/44046')
    _work_dir = tmp_path / '1981185920_44046'
    _work_dir.mkdir()
    odem_processor = ODEMProcess(_record, work_dir=_work_dir)
    cfg = get_configparser()
    cfg.read(os.path.join(PROJECT_ROOT_DIR, 'resources', 'odem.ocrd.tesseract.ini'))
    odem_processor.cfg = cfg
    _log_dir = tmp_path / 'log'
    _log_dir.mkdir()
    odem_processor.the_logger = get_logger(str(_log_dir))

    # act
    with pytest.raises(OAILoadException) as err:
        odem_processor.load()

    # assert
    assert "url" in err.value.args[0] and "returned" in err.value.args[0]
    assert mock_load.called == 1


def test_record_with_unknown_language(tmp_path):
    """Fix behavior when opendata record has one
    of total 3 languages which is actually
    not unknown (gmh == German, Middle High 1050-1500)
    """

    path_workdir = tmp_path / 'workdir'
    path_workdir.mkdir()
    orig_file = TEST_RES / '1981185920_72977.xml'
    trgt_mets = path_workdir / 'test.xml'
    shutil.copyfile(orig_file, trgt_mets)
    (path_workdir / 'log').mkdir()
    record = OAIRecord('oai:opendata.uni-halle.de:1981185920/72977')
    oproc = ODEMProcess(record, work_dir=path_workdir, log_dir=path_workdir / 'log')
    oproc.cfg = fixture_configuration()
    _model_dir = prepare_tessdata_dir(tmp_path)
    oproc.cfg.set(CFG_SEC_OCR, CFG_KEY_RES_VOL,
                  f'{_model_dir}:/usr/local/share/ocrd-resources/ocrd-tesserocr-recognize')
    oproc.mets_file = str(trgt_mets)
    oproc.inspect_metadata()
    _langs = oproc.statistics.get(STATS_KEY_LANGS)

    # act
    with pytest.raises(ODEMException) as odem_exc:
        oproc.language_modelconfig(_langs)

    # assert
    assert "'gmh' mapping not found (languages: ['lat', 'ger', 'gmh'])!" == odem_exc.value.args[0]


def test_export_flat_zip(tmp_path):
    """Test export workflow for a
    flat archive layout like was
    use of old with the legacy
    semantics VLS systems
    """

    path_workdir = tmp_path / 'workdir'
    path_workdir.mkdir()
    path_tmp_export_dir = tmp_path / 'tmp_export'
    path_tmp_export_dir.mkdir()
    path_export_dir = tmp_path / 'export'
    path_export_dir.mkdir()

    orig_file = TEST_RES / '1981185920_44046.xml'
    trgt_mets = path_workdir / 'test.xml'
    shutil.copyfile(orig_file, trgt_mets)

    orig_files = TEST_RES / 'vd18-1180329' / 'FULLTEXT'
    trgt_files = path_workdir / 'FULLTEXT'
    shutil.copytree(orig_files, trgt_files)

    (path_workdir / 'log').mkdir()
    record = OAIRecord('oai:opendata.uni-halle.de:1981185920/44046')
    oproc = ODEMProcess(record, work_dir=path_workdir, log_dir=path_workdir / 'log')
    oproc.cfg = fixture_configuration()
    _model_dir = prepare_tessdata_dir(tmp_path)

    oproc.cfg.set('export', 'export_format', ExportFormat.FLAT_ZIP)
    oproc.cfg.set('global', 'local_export_tmp', str(path_tmp_export_dir))
    oproc.cfg.set('global', 'local_export_dir', str(path_export_dir))
    oproc.cfg.set(
        CFG_SEC_OCR,
        CFG_KEY_RES_VOL,
        f'{_model_dir}:/usr/local/share/ocrd-resources/ocrd-tesserocr-recognize'
    )

    oproc.mets_file = str(trgt_mets)
    oproc.inspect_metadata()
    _langs = oproc.statistics.get(STATS_KEY_LANGS)

    # act
    zipfilepath, _ = oproc.export_data()

    # assert
    assert os.path.exists(zipfilepath) and os.path.getsize(zipfilepath) == 58552
