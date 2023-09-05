# -*- coding: utf-8 -*-
"""Specification ODEM API"""

import datetime
import os
import shutil
from pathlib import (
    Path
)
from unittest import (
    mock
)

import lxml.etree as ET
import pytest
from digiflow import (
    OAIRecord,
    ResourceGenerator,
    OAILoadException
)

from lib.ocrd3_odem import (
    get_config,
    get_modelconf_from,
    get_odem_logger,
    postprocess_ocr_file,
    ODEMProcess,
    ODEMException,
    ODEMNoImagesForOCRException,
    ODEMNoTypeForOCRException,
    XMLNS,
    PUNCTUATIONS,
    IDENTIFIER_CATALOGUE,
)
from .conftest import (
    PROJECT_ROOT_DIR,
    TEST_RES,
    fixture_configuration,
)


@pytest.mark.parametrize("file_path,model_conf",
                         [
                             ('resources/urn+nbn+de+gbv+3+1-116899-p0062-3_ger.jpg', ['ger']),
                             ('resources/urn+nbn+de+gbv+3+1-116299-p0107-6_lat.jpg', ['lat']),
                             ('resources/urn+nbn+de+gbv+3+1-118702-p0055-9_ger+lat.jpg', ['ger','lat'])
                         ])
def test_odem_local_file_modelconf(file_path, model_conf):
    """Ensure that expected models picked
    from image name language suffix
    """

    # act
    assert get_modelconf_from(file_path) == model_conf


def _prepare_tessdata_dir(tmp_path: Path) -> str:
    model_dir = tmp_path / 'tessdata'
    model_dir.mkdir()
    models = ['gt4hist_5000k', 'lat_ocr', 'grc', 'ger']
    for _m in models:
        with open(str(model_dir / f'{_m}.traineddata'), 'wb') as writer:
            writer.write(b'abc')
    return str(model_dir)


@pytest.mark.parametrize("img_path,lang_str",
                         [
                             ('resources/urn+nbn+de+gbv+3+1-116899-p0062-3_ger.jpg', 'gt4hist_5000k'),
                             ('resources/urn+nbn+de+gbv+3+1-116299-p0107-6_lat+ger.jpg', 'lat_ocr+gt4hist_5000k'),
                             ('resources/urn+nbn+de+gbv+3+1-118702-p0055-9_gre+lat.jpg', 'grc+lat_ocr')
                         ])
def test_lang_mapping(img_path, lang_str, tmp_path):
    """Ensure ODEM Object picks 
    proper project language mappings"""

    work_dir = tmp_path / 'work_dir'
    work_dir.mkdir()
    work_2 = work_dir / 'test2'
    work_2.mkdir()
    log_dir = tmp_path / 'log'
    log_dir.mkdir()
    odem_processor = ODEMProcess(None, work_dir=str(work_2))
    odem_processor.cfg = fixture_configuration()
    _tess_dir = _prepare_tessdata_dir(tmp_path)
    odem_processor.cfg.set('ocr', 'tessdir_host', _tess_dir)
    odem_processor.the_logger = get_odem_logger(str(log_dir))
    odem_processor.local_mode = True

    # act
    assert odem_processor.map_language_to_modelconfig(img_path) == lang_str


@pytest.fixture(name="init_odem", scope='module')
def _fixture_1981185920_44043(tmp_path_factory):
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
    _model_dir = _prepare_tessdata_dir(_workdir)
    odem.cfg.set('ocr', 'tessdir_host', _model_dir)
    odem.the_logger = get_odem_logger(str(_log_dir))

    # mock loading of OAI Record
    # actual load attempt *must* be done
    # in scope of mocked object to work!
    with mock.patch('digiflow.OAILoader.load') as request_mock:
        request_mock.side_effect = _side_effect
        odem.load()
        yield odem


def test_odem_process_internal_identifier(init_odem: ODEMProcess):
    """Ensure proper internal identifier calculated
    for say, logging"""

    assert init_odem.process_identifier == '1981185920_44046'


def test_odem_process_catalog_identifier(init_odem: ODEMProcess):
    """Ensure proper external identifier present
    which will be used finally to name the export SAF
    """

    # act
    init_odem.inspect_metadata()

    # assert
    assert init_odem.identifiers[IDENTIFIER_CATALOGUE] == '265982944'


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
    cfg = get_config()
    cfg.read(os.path.join(PROJECT_ROOT_DIR, 'resources', 'odem.ini'))
    odem_processor.cfg = cfg
    _model_dir = _prepare_tessdata_dir(work_dir)
    odem_processor.cfg.set('ocr', 'tessdir_host', _model_dir)
    odem_processor.local_mode = True
    odem_processor.the_logger = get_odem_logger(log_dir)
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
    """Ensure cannot map dummy language 'yyy'"""

    # arrange
    img_path = 'resources/urn+nbn+de+gbv+3+1-116899-p0062-3_xxx.jpg'

    # act
    with pytest.raises(ODEMException) as err:
        odem_processor.map_language_to_modelconfig(img_path)

    # assert
    assert "'yyy' model config not found !" in err.value.args[0]


@pytest.fixture(name="module_fixture_one", scope='module')
def _module_fixture_123456789_27949(tmp_path_factory):
    path_workdir = tmp_path_factory.mktemp('workdir')
    orig_file = TEST_RES / '123456789_27949.xml'
    trgt_mets = path_workdir / 'test.xml'
    orig_alto = TEST_RES / '123456789_27949_FULLTEXT'
    trgt_alto = path_workdir / 'FULLTEXT'
    shutil.copyfile(orig_file, trgt_mets)
    shutil.copytree(orig_alto, trgt_alto)
    (path_workdir / 'log').mkdir()
    _model_dir = _prepare_tessdata_dir(path_workdir)
    record = OAIRecord('oai:dev.opendata.uni-halle.de:123456789/27949')
    oproc = ODEMProcess(record, work_dir=path_workdir, log_dir=path_workdir / 'log')
    oproc.cfg = fixture_configuration()
    oproc.cfg.set('ocr', 'tessdir_host', _model_dir)
    oproc.ocr_files = [os.path.join(trgt_alto, a)
                       for a in os.listdir(trgt_alto)]
    oproc.mets_file = str(trgt_mets)
    oproc.inspect_metadata()
    yield (oproc, path_workdir)


def test_module_fixture_one_integrated_ocr_in_mets(module_fixture_one):
    """Ensure, that generated final OCR files
    * are properly linked into original METS
    * contain required link data to images
    """

    # arrange
    record_123456789_27949, _ = module_fixture_one
    assert len(record_123456789_27949.ocr_files) == 4

    # act
    n_integrated = record_123456789_27949.integrate_ocr()

    # assertions about METS structure
    assert n_integrated == 4
    _root = ET.parse(record_123456789_27949.mets_file).getroot()
    _phys_links = _root.xpath('//mets:div[@TYPE="physSequence"]/mets:div', namespaces=XMLNS)
    # at most 2: one MAX-Image plus according optional FULLTEXT
    assert len(_phys_links[1].getchildren()) == 1
    assert len(_phys_links[2].getchildren()) == 2
    assert len(_phys_links[3].getchildren()) == 2
    assert len(_phys_links[4].getchildren()) == 2
    assert len(_phys_links[5].getchildren()) == 2
    assert len(_phys_links[6].getchildren()) == 1


def test_module_fixture_one_integrated_ocr_files_fit_identifier(module_fixture_one):
    """Ensure ocr-file elements fit syntactically
    * proper fileName
    * proper PageId set
    """

    # arrange
    record_123456789_27949, tmp_path = module_fixture_one

    # act
    record_123456789_27949.integrate_ocr()

    # assert
    assert not os.path.exists(tmp_path / 'FULLTEXT' / '00000002.xml')
    assert os.path.exists(tmp_path / 'FULLTEXT' / '00000003.xml')
    ocr_file_03 = ET.parse(str(tmp_path / 'FULLTEXT' / '00000003.xml')).getroot()
    assert len(ocr_file_03.xpath('//alto:Page[@ID="p00000003"]', namespaces=XMLNS)) == 1
    assert ocr_file_03.xpath('//alto:fileName', namespaces=XMLNS)[0].text == '00000003.jpg'
    ocr_file_06 = ET.parse(str(tmp_path / 'FULLTEXT' / '00000006.xml')).getroot()
    assert len(ocr_file_06.xpath('//alto:Page[@ID="p00000006"]', namespaces=XMLNS)) == 1
    assert not os.path.exists(tmp_path / 'FULLTEXT' / '00000007.xml')


def test_module_fixture_one_images_4_ocr_by_metadata(module_fixture_one):
    """Ensure setting and filtering of images behavior.

    Record oai:dev.opendata.uni-halle.de:123456789/27949
    just 4 out of total 9 images picked because of
    their physical presens and according METS metadata
    """

    # arrange
    odem_123456789_27949, tmp_path = module_fixture_one
    # generate total 9 small but physical
    # jpg-images in sub-dir MAX
    ResourceGenerator(tmp_path / 'MAX', number=9).get_batch()

    # act
    odem_123456789_27949.inspect_metadata_images()

    # assert
    assert len(odem_123456789_27949.images_4_ocr) == 4


@pytest.fixture(name="fixture_one")
def _fixture_123456789_27949(tmp_path):
    path_workdir = tmp_path / 'workdir'
    path_workdir.mkdir()
    orig_file = TEST_RES / '123456789_27949.xml'
    trgt_mets = path_workdir / 'test.xml'
    orig_alto = TEST_RES / '123456789_27949_FULLTEXT'
    trgt_alto = path_workdir / 'FULLTEXT'
    shutil.copyfile(orig_file, trgt_mets)
    shutil.copytree(orig_alto, trgt_alto)
    (path_workdir / 'log').mkdir()
    record = OAIRecord('oai:dev.opendata.uni-halle.de:123456789/27949')
    oproc = ODEMProcess(record, work_dir=path_workdir, log_dir=path_workdir / 'log')
    oproc.cfg = fixture_configuration()
    _model_dir = _prepare_tessdata_dir(path_workdir)
    oproc.cfg.set('ocr', 'tessdir_host', _model_dir)
    oproc.ocr_files = [os.path.join(trgt_alto, a)
                       for a in os.listdir(trgt_alto)]
    oproc.mets_file = str(trgt_mets)
    oproc.inspect_metadata()
    yield (oproc, path_workdir)


def test_fixture_one_postprocessed_ocr_files_elements(fixture_one):
    """Ensure ocr-file unwanted elements dropped as expected
    """

    # arrange
    record_123456789_27949, tmp_path = fixture_one

    # act
    record_123456789_27949.postprocess_ocr()

    # assert
    ocr_file_03 = ET.parse(str(tmp_path / 'FULLTEXT' / '00000003.xml')).getroot()
    assert not ocr_file_03.xpath('//alto:Shape', namespaces=XMLNS)


def test_fixture_one_postprocess_ocr_files(fixture_one):
    """Ensure expected replacements done *even* when
    diacritics occour more several times in single word"""

    # arrange
    _, tmp_path = fixture_one
    path_file = tmp_path / 'FULLTEXT' / '00000003.xml'
    strip_tags = fixture_configuration().getlist('ocr', 'strip_tags') # pylint: disable=no-member

    # act
    postprocess_ocr_file(path_file, strip_tags)

    # assert
    _raw_lines = [l.strip() for l in open(path_file, encoding='utf-8').readlines()]
    # these lines must be dropped, since they are empty save the SP-element afterwards
    # changed due different punctuation interpretation
    # 'region0012_line0002' is no okay
    _droppeds = ['region0001_line0002', 'region0012_line0001']
    for _line in _raw_lines:
        for _dropped in _droppeds:
            assert _dropped not in _line

    _contents = [ET.fromstring(l.strip()).attrib['CONTENT']
                 for l in _raw_lines
                 if 'CONTENT' in l]
    for _content in _contents:
        for _punc in PUNCTUATIONS:
            # adapted like semantics did
            # each trailing punctuation
            # is now in it's own STRING element
            if _punc in _content[-1]:
                assert len(_content) == 1


def test_fixture_one_postprocess_ocr_create_text_bundle(fixture_one):
    """Ensure text bundle data created
    and present with expected number of text rows
    """

    # arrange
    odem, tmp_path = fixture_one

    # act
    odem.postprocess_ocr()
    odem.create_text_bundle_data()

    # assert
    _txt_bundle_file = tmp_path / '198114125.pdf.txt'
    assert os.path.exists(_txt_bundle_file)
    assert 77 == odem.statistics['lines']
    with open(_txt_bundle_file, encoding='utf-8') as bundle_handle:
        assert 77 == len(bundle_handle.readlines())


@pytest.fixture(name='post_mets', scope='module')
def _fixture_postprocessing_mets(tmp_path_factory):
    """Fixture for checking postprocessing"""
    _workdir = tmp_path_factory.mktemp('workdir')
    orig_file = TEST_RES / '198114125_part_mets.xml'
    trgt_mets = _workdir / 'test.xml'
    shutil.copyfile(orig_file, trgt_mets)
    (_workdir / 'log').mkdir()
    _proc = ODEMProcess(None, work_dir=_workdir, log_dir=_workdir / 'log')
    _proc.mets_file = str(trgt_mets)
    _proc.cfg = fixture_configuration()
    _proc.postprocess_mets()
    _root = ET.parse(_proc.mets_file).getroot()
    yield (_proc, _root)


def test_postprocess_mets_agent_entries_number_fits(post_mets):
    """Ensure METS metadata agents has expected number"""

    (_, xml_root) = post_mets
    assert len(xml_root.xpath('//mets:agent', namespaces=XMLNS)) == 4


def test_postprocess_mets_agent_odem_fits(post_mets):
    """Ensure METS agent odem has used OCR-D 
    baseimage annotated"""

    (proc, xml_root) = post_mets
    _agent_odem = xml_root.xpath('//mets:agent', namespaces=XMLNS)[3]
    _xp_agent_note = 'mets:note/text()'
    _xp_agent_name = 'mets:name/text()'
    _curr_image = proc.cfg.get('ocr', 'ocrd_baseimage')
    assert _agent_odem.xpath(_xp_agent_name, namespaces=XMLNS)[0] == f'DFG-OCRD3-ODEM_{_curr_image}'
    _today = datetime.datetime.strftime(datetime.datetime.now(), '%Y-%m-%d')
    assert _today in _agent_odem.xpath(_xp_agent_note, namespaces=XMLNS)[0]


def test_postprocess_mets_agent_derivans_fits(post_mets):
    """Ensure METS agent derivans was re-done"""

    (_, xml_root) = post_mets
    _agent_derivans = xml_root.xpath('//mets:agent', namespaces=XMLNS)[2]
    _xp_agent_note = 'mets:note/text()'
    _xp_agent_name = 'mets:name/text()'
    assert _agent_derivans.xpath(_xp_agent_name, namespaces=XMLNS)[0] == 'DigitalDerivans V1.6.0-SNAPSHOT'
    assert _agent_derivans.xpath(_xp_agent_note, namespaces=XMLNS)[0].endswith('2022-05-17T11:27:16')
    assert not xml_root.xpath('//dv:iiif', namespaces=XMLNS)
    assert not xml_root.xpath('//dv:sru', namespaces=XMLNS)


def test_postprocess_mets_provenance_removed(post_mets):
    """Ensure METS entries for digital provenance removed"""

    (_, xml_root) = post_mets
    assert not xml_root.xpath('//dv:iiif', namespaces=XMLNS)
    assert not xml_root.xpath('//dv:sru', namespaces=XMLNS)


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
    cfg = get_config()
    cfg.read(os.path.join(PROJECT_ROOT_DIR, 'resources', 'odem.ini'))
    odem_processor.cfg = cfg
    _log_dir = tmp_path / 'log'
    _log_dir.mkdir()
    odem_processor.the_logger = get_odem_logger(str(_log_dir))

    # act
    odem_processor.inspect_metadata_images()
    odem_processor.filter_images()

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
    cfg = get_config()
    cfg.read(os.path.join(PROJECT_ROOT_DIR, 'resources', 'odem.ini'))
    odem_processor.cfg = cfg
    _log_dir = tmp_path / 'log'
    _log_dir.mkdir()
    odem_processor.the_logger = get_odem_logger(str(_log_dir))

    # act
    with pytest.raises(OAILoadException) as err:
        odem_processor.load()

    # assert
    assert "url" in err.value.args[0] and "returned" in err.value.args[0]
    assert mock_load.called == 1


@pytest.mark.parametrize("model_configuration,recognotion_level",
                         [
                             ('ger', 'word'),
                             ('ara', 'glyph'),
                             ('ara+ger', 'glyph'),
                             ('ger+lat', 'word'),
                             ('fas', 'glyph'),
                             ('ger+lat', 'word'),
                             ('eng+fas', 'glyph')
                         ])
def test_odem_recognition_level(model_configuration, recognotion_level):
    """Check determined recognition level passed
    forth to tesserocr for common model configurations"""

    assert ODEMProcess.get_recognition_level(model_configuration) == recognotion_level


def test_opendata_record_unknown_language(tmp_path):
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
    _model_dir = _prepare_tessdata_dir(tmp_path)
    oproc.cfg.set('ocr', 'tessdir_host', _model_dir)
    oproc.mets_file = str(trgt_mets)

    # act
    with pytest.raises(ODEMException) as odem_exc:
        oproc.inspect_metadata()

    # assert
    assert "'gmh' mapping not found (languages: ['lat', 'ger', 'gmh'])!" ==  odem_exc.value.args[0]


def test_opendata_record_no_images_for_ocr(tmp_path):
    """Fix behavior when opendata record contains
    only cover pages or illustrations
    """

    path_workdir = tmp_path / 'workdir'
    path_workdir.mkdir()
    orig_file = TEST_RES / '1981185920_74357.xml'
    trgt_mets = path_workdir / 'test.xml'
    shutil.copyfile(orig_file, trgt_mets)
    (path_workdir / 'log').mkdir()
    record = OAIRecord('oai:opendata.uni-halle.de:1981185920/74357')
    oproc = ODEMProcess(record, work_dir=path_workdir, log_dir=path_workdir / 'log')
    oproc.cfg = fixture_configuration()
    _model_dir = _prepare_tessdata_dir(path_workdir)
    oproc.cfg.set('ocr', 'tessdir_host', _model_dir)
    oproc.mets_file = str(trgt_mets)

    # act
    with pytest.raises(ODEMNoImagesForOCRException) as odem_exc:
        oproc.inspect_metadata()

    # assert
    assert "1981185920_74357 contains no images for OCR (total: 15)!" ==  odem_exc.value.args[0]


def test_opendata_record_no_printwork(tmp_path):
    """Fix behavior when opendata record is a parent
    struct (c-stage) without any pages/images
    """

    path_workdir = tmp_path / 'workdir'
    path_workdir.mkdir()
    orig_file = TEST_RES / '1981185920_79080.xml'
    trgt_mets = path_workdir / 'test.xml'
    shutil.copyfile(orig_file, trgt_mets)
    (path_workdir / 'log').mkdir()
    record = OAIRecord('oai:opendata.uni-halle.de:1981185920/79080')
    oproc = ODEMProcess(record, work_dir=path_workdir, log_dir=path_workdir / 'log')
    oproc.cfg = fixture_configuration()
    _model_dir = _prepare_tessdata_dir(path_workdir)
    oproc.cfg.set('ocr', 'tessdir_host', _model_dir)
    oproc.mets_file = str(trgt_mets)

    # act
    with pytest.raises(ODEMNoTypeForOCRException) as odem_exc:
        oproc.inspect_metadata()

    # assert
    assert "1981185920_79080 is no print: Ac!" ==  odem_exc.value.args[0]
