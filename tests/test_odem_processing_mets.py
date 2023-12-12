"""Specification for METS/MODS handling"""

import datetime
import os
import shutil

import pytest

import lxml.etree as ET

from lib.ocrd3_odem import (
    XMLNS,
    ODEMMetadataMetsException,
    ODEMNoImagesForOCRException,
	ODEMMetadataInspecteur,
    postprocess_mets,
)

from .conftest import (
    TEST_RES,
    fixture_configuration,
)


@pytest.fixture(name="inspecteur_44043", scope='module')
def _fixture_1981185920_44043():
    """Initial ODEM fixture before doing any OCR"""

    # arrange
    _ident = '1981185920_44046'
    file = TEST_RES / '1981185920_44046.xml'
    inspc = ODEMMetadataInspecteur(file,
                                   process_identifier=_ident,
                                   cfg=fixture_configuration())
    yield inspc


def test_odem_process_internal_identifier(inspecteur_44043: ODEMMetadataInspecteur):
    """Ensure proper internal identifier calculated
    for say, logging"""

    assert inspecteur_44043.process_identifier == '1981185920_44046'


def test_odem_process_catalog_identifier(inspecteur_44043: ODEMMetadataInspecteur):
    """Ensure proper external identifier present
    which will be used finally to name the export SAF
    """

    # act
    # init_odem.inspect_metadata()

    # assert
    assert inspecteur_44043.record_identifier == '265982944'


@pytest.fixture(name='post_mets', scope='module')
def _fixture_postprocessing_mets(tmp_path_factory):
    """Fixture for checking postprocessing"""
    _workdir = tmp_path_factory.mktemp('workdir')
    orig_file = TEST_RES / '198114125_part_mets.xml'
    trgt_mets = _workdir / 'test.xml'
    shutil.copyfile(orig_file, trgt_mets)
    _cfg = fixture_configuration()
    _cnt_base_image = _cfg.get('ocr', 'ocrd_baseimage')
    postprocess_mets(trgt_mets, _cnt_base_image)
    _root = ET.parse(trgt_mets).getroot()
    yield _root


def test_postprocess_mets_agent_entries_number_fits(post_mets):
    """Ensure METS metadata agents has expected number"""

    assert len(post_mets.xpath('//mets:agent', namespaces=XMLNS)) == 4


def test_postprocess_mets_agent_odem_fits(post_mets):
    """Ensure METS agent odem has used OCR-D 
    baseimage annotated"""

    _agent_odem = post_mets.xpath('//mets:agent', namespaces=XMLNS)[3]
    _xp_agent_note = 'mets:note/text()'
    _xp_agent_name = 'mets:name/text()'
    _curr_image = fixture_configuration().get('ocr', 'ocrd_baseimage')
    assert _agent_odem.xpath(_xp_agent_name, namespaces=XMLNS)[0] == f'DFG-OCRD3-ODEM_{_curr_image}'
    _today = datetime.datetime.strftime(datetime.datetime.now(), '%Y-%m-%d')
    assert _today in _agent_odem.xpath(_xp_agent_note, namespaces=XMLNS)[0]


def test_postprocess_mets_agent_derivans_fits(post_mets):
    """Ensure METS agent derivans was re-done"""

    _agent_derivans = post_mets.xpath('//mets:agent', namespaces=XMLNS)[2]
    _xp_agent_note = 'mets:note/text()'
    _xp_agent_name = 'mets:name/text()'
    assert _agent_derivans.xpath(_xp_agent_name, namespaces=XMLNS)[0] == 'DigitalDerivans V1.6.0-SNAPSHOT'
    assert _agent_derivans.xpath(_xp_agent_note, namespaces=XMLNS)[0].endswith('2022-05-17T11:27:16')
    assert not post_mets.xpath('//dv:iiif', namespaces=XMLNS)
    assert not post_mets.xpath('//dv:sru', namespaces=XMLNS)


def test_postprocess_mets_provenance_removed(post_mets):
    """Ensure METS entries for digital provenance removed"""

    assert not post_mets.xpath('//dv:iiif', namespaces=XMLNS)
    assert not post_mets.xpath('//dv:sru', namespaces=XMLNS)


def test_opendata_record_no_images_for_ocr():
    """Behavior when opendata record contains
    only cover pages or illustrations and no 
    *real* printed pages
    """

    orig_file = TEST_RES / '1981185920_74357.xml'
    _oai_urn = 'oai:opendata.uni-halle.de:1981185920/74357'
    cfg = fixture_configuration()
    inspc = ODEMMetadataInspecteur(orig_file, _oai_urn, cfg)

    # act
    with pytest.raises(ODEMNoImagesForOCRException) as odem_exc:
        inspc.inspect()

    # assert
    _alert = "oai:opendata.uni-halle.de:1981185920/74357 contains no images for OCR (total: 15)!"
    assert _alert == odem_exc.value.args[0]


def test_opendata_record_no_printwork():
    """Behavior when opendata record is a parent
    struct (c-stage) without any pages/images
    """

    _oai_urn = 'oai:opendata.uni-halle.de:1981185920/79080'
    orig_file = TEST_RES / '1981185920_79080.xml'
    cfg = fixture_configuration()
    inspc = ODEMMetadataInspecteur(orig_file, _oai_urn, cfg)

    # act
    with pytest.raises(ODEMMetadataMetsException) as odem_exc:
        inspc.inspect()

    # assert
    assert f"{_oai_urn} invalid PICA type for OCR: Ac" ==  odem_exc.value.args[0]


def test_opendata_record_no_granular_urn_present():
    """Fix behavior when opendata record is legacy
    kitodo2 with zedExporter creation
    or any other kind of digital object missing
    granular urn at all
    """

    _oai_urn = 'oai:opendata.uni-halle.de:1981185920/88132'
    orig_file = TEST_RES / '1981185920_88132.xml'
    cfg = fixture_configuration()
    inspc = ODEMMetadataInspecteur(orig_file, _oai_urn, cfg)

    # act
    inspc.inspect()

    # assert
    for img_entry in inspc.image_pairs:
        assert img_entry[1].startswith('PHYS_00')


def test_opendata_record_type_error():
    """Fix behavior when opendata record is legacy
    kitodo2 with zedExporter creation
    or any other kind of digital object missing
    granular urn at all
    """

    _oai_urn = 'oai:opendata.uni-halle.de:1981185920/105290'
    orig_file = TEST_RES / '1981185920_105290.xml'
    cfg = fixture_configuration()
    inspc = ODEMMetadataInspecteur(orig_file, _oai_urn, cfg)

    # act
    with pytest.raises(ODEMMetadataMetsException) as odem_exc:
        inspc.inspect()

    # assert
    assert "2x: Page PHYS_0112 not linked,Page PHYS_0113 not linked" ==  odem_exc.value.args[0]


def test_mets_mods_sbb_vol01_with_ulb_defaults():
    """Check result outcome for SBB digital object from
    OCR-D METS-server https://github.com/kba/ocrd-demo-mets-server
    with default ULB configuration settings
    """
    # sbb-PPN891267093
    _oai_urn = 'oai:digital.staatsbibliothek-berlin.de:PPN891267093'
    orig_file = TEST_RES / 'sbb-PPN891267093.xml'
    assert os.path.isfile(orig_file)
    cfg = fixture_configuration()
    inspc = ODEMMetadataInspecteur(orig_file, _oai_urn, cfg)

    # act
    inspc.inspect()

    # assert
    assert inspc.process_identifier == _oai_urn
    assert inspc.record_identifier == 'PPN891267093'
