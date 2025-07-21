"""Specification for METS/MODS handling"""

import configparser
import datetime
import os
import shutil

import pytest

import lxml.etree as ET
import digiflow as df
import digiflow.record as df_r

from lib import odem
import lib.odem.commons as oc
import lib.odem.processing.mets as odem_pm

from .conftest import (
    TEST_RES,
    fixture_configuration,
)


# please linter for lxml.etree contains no-member message
# pylint:disable=I1101

@pytest.fixture(name="inspecteur_44046", scope='module')
def _fixture_1981185920_44046():
    """Initial ODEM fixture before doing any OCR"""

    # arrange
    an_ident = '1981185920_44046'
    file = TEST_RES / '1981185920_44046.xml'
    inspc = odem.ODEMMetadataInspecteur(file,
                                        process_identifier=an_ident,
                                        cfg=fixture_configuration())
    inspc.read()
    yield inspc


def test_odem_process_internal_identifier(inspecteur_44046: odem.ODEMMetadataInspecteur):
    """Ensure proper internal identifier calculated
    for say, logging"""

    assert inspecteur_44046.process_identifier == '1981185920_44046'


def test_odem_process_catalog_identifier(inspecteur_44046: odem.ODEMMetadataInspecteur):
    """Ensure proper external identifier present
    which will be used finally to name the export SAF
    """

    # assert
    assert inspecteur_44046.record_identifier == "urn+nbn+de+gbv+3+1-146566"


@pytest.fixture(name='post_mets', scope='module')
def _fixture_postprocessing_mets(tmp_path_factory):
    """Fixture for checking postprocessing"""
    the_workdir = tmp_path_factory.mktemp('workdir')
    orig_file = TEST_RES / '198114125_part_mets.xml'
    dst_mets = the_workdir / 'test.xml'
    shutil.copyfile(orig_file, dst_mets)
    odem_cfg = fixture_configuration()
    odem_cfg.set(odem.CFG_SEC_METS, odem.CFG_SEC_METS_OPT_CLEAN, 'True')
    odem_cfg.set(odem.CFG_SEC_METS, odem.CFG_SEC_METS_OPT_AGENTS,
                 'DFG-OCRD3-ODEM_ocrd/all:2022-08-15')
    odem.postprocess_mets(dst_mets, odem_cfg)
    odem_pm.process_mets_derivans_agents(dst_mets, odem_cfg)
    the_root = ET.parse(dst_mets).getroot()
    yield the_root


def test_postprocess_mets_agent_entries_number_fits(post_mets):
    """Ensure METS metadata agents has expected number
    """

    assert len(post_mets.xpath('//mets:agent', namespaces=df.XMLNS)) == 4


def test_postprocess_mets_agent_odem_fits(post_mets):
    """Ensure METS agent odem wrote OCR-D baseimage note

    changed 2024-09-04
        because division of post_processing now OCR agent
        is inserted as last element [-1]
    """

    agent_odem = post_mets.xpath('//mets:agent', namespaces=df.XMLNS)[-1]
    xp_note = 'mets:note/text()'
    xp_name = 'mets:name/text()'
    curr_image = fixture_configuration().get(odem.CFG_SEC_OCR, 'ocrd_baseimage')
    assert agent_odem.xpath(xp_name, namespaces=df.XMLNS)[0] == f'DFG-OCRD3-ODEM_{curr_image}'
    today = datetime.datetime.strftime(datetime.datetime.now(), '%Y-%m-%d')
    assert today in agent_odem.xpath(xp_note, namespaces=df.XMLNS)[0]


def test_postprocess_mets_agent_derivans_fits(post_mets):
    """Ensure METS agent derivans was re-done"""

    agent_derivans = post_mets.xpath('//mets:agent', namespaces=df.XMLNS)[2]
    xp_note = 'mets:note/text()'
    xp_name = 'mets:name/text()'
    name_agent_zeros = agent_derivans.xpath(xp_name, namespaces=df.XMLNS)
    assert name_agent_zeros[0] == 'DigitalDerivans V1.6.0-SNAPSHOT'
    assert agent_derivans.xpath(xp_note, namespaces=df.XMLNS)[0].endswith('2022-05-17T11:27:16')
    assert not post_mets.xpath('//dv:iiif', namespaces=df.XMLNS)
    assert not post_mets.xpath('//dv:sru', namespaces=df.XMLNS)


def test_postprocess_mets_provenance_removed(post_mets):
    """Ensure METS entries for digital provenance removed"""

    assert not post_mets.xpath('//dv:iiif', namespaces=df.XMLNS)
    assert not post_mets.xpath('//dv:sru', namespaces=df.XMLNS)


def test_opendata_record_no_images_for_ocr():
    """Behavior when opendata record contains
    only cover pages or illustrations and no 
    *real* printed pages
    """

    orig_file = TEST_RES / '1981185920_74357.xml'
    oai_urn = 'oai:opendata.uni-halle.de:1981185920/74357'
    cfg = fixture_configuration()
    inspc = odem.ODEMMetadataInspecteur(orig_file, oai_urn, cfg)

    # act
    with pytest.raises(odem.ODEMNoImagesForOCRException) as odem_exc:
        inspc.read()

    # assert
    an_alert = "oai:opendata.uni-halle.de:1981185920/74357 contains no images for OCR (total: 15)!"
    assert an_alert == odem_exc.value.args[0]


def test_opendata_record_no_printwork():
    """Behavior when opendata record is a parent
    struct (c-stage) without any pages/images
    """

    oai_urn = 'oai:opendata.uni-halle.de:1981185920/79080'
    orig_file = TEST_RES / '1981185920_79080.xml'
    cfg = fixture_configuration()
    inspc = odem.ODEMMetadataInspecteur(orig_file, oai_urn, cfg)

    # act
    with pytest.raises(odem.ODEMNoTypeForOCRException) as odem_exc:
        inspc.read()

    # assert
    assert f"{oai_urn} no PICA type for OCR: Ac" == odem_exc.value.args[0]


def test_opendata_record_no_granular_urn_present():
    """Fix behavior when opendata record is legacy
    kitodo2 with zedExporter creation
    or any other kind of digital object missing
    granular urn at all
    """

    oai_urn = 'oai:opendata.uni-halle.de:1981185920/88132'
    orig_file = TEST_RES / '1981185920_88132.xml'
    cfg = fixture_configuration()
    inspc = odem.ODEMMetadataInspecteur(orig_file, oai_urn, cfg)

    # act
    inspc.read()

    # assert
    for img_entry in inspc.image_pairs:
        assert img_entry[1].startswith('PHYS_00')


def test_opendata_record_type_error():
    """Fix behavior when opendata record is legacy
    kitodo2 with zedExporter creation
    or any other kind of digital object missing
    granular urn at all
    """

    oai_urn = 'oai:opendata.uni-halle.de:1981185920/105290'
    orig_file = TEST_RES / '1981185920_105290.xml'
    cfg = fixture_configuration()
    inspc = odem.ODEMMetadataInspecteur(orig_file, oai_urn, cfg)

    # act
    with pytest.raises(odem.ODEMMetadataMetsException) as odem_exc:
        inspc.read()

    # assert
    assert "2x: Page PHYS_0112 not linked,Page PHYS_0113 not linked" == odem_exc.value.args[0]


def test_mets_mods_sbb_vol01_with_ulb_defaults():
    """Check result outcome for SBB digital object from
    OCR-D METS-server https://github.com/kba/ocrd-demo-mets-server
    with special configuration settings
    """
    oai_urn = 'oai:digital.staatsbibliothek-berlin.de:PPN891267093'
    orig_file = TEST_RES / 'sbb-PPN891267093.xml'
    assert os.path.isfile(orig_file)
    cfg: configparser.ConfigParser = fixture_configuration()
    xpr_id = "//mods:mods/mods:recordInfo/mods:recordIdentifier[@source='gbv-ppn']/text()"
    cfg.set(oc.CFG_SEC_METS, oc.CFG_SEC_METS_OPT_ID_XPR, xpr_id)
    cfg.set(oc.CFG_SEC_METS, oc.CFG_SEC_METS_FGROUP, "DEFAULT")

    # act
    inspc = odem.ODEMMetadataInspecteur(orig_file, oai_urn, cfg)
    inspc.read()

    # assert
    assert inspc.process_identifier == oai_urn
    assert inspc.record_identifier == 'PPN891267093'


def test_mets_filter_logical_structs_by_type():
    """Check filter mechanics for Kitodo2 record
    which consists of 21 pages
    * cover_front       : PHYS_0001,PHYS_0002
    * cover_back        : PHYS_0019,PHYS_0020,PHYS_0021
    * "[Leerseite]"     : PHYS_0004,PHYS_0017,PHYS_0018
    * "[Colorchecker]"  : PHYS_0021 (already due cover_back)
    => use exact 13 of total 21 pairs
    """
    oai_urn = 'oai:opendata.uni-halle.de:1981185920/33908'
    orig_file = TEST_RES / '1981185920_33908.xml'
    assert os.path.isfile(orig_file)
    cfg = fixture_configuration()
    inspc = odem.ODEMMetadataInspecteur(orig_file, oai_urn, cfg)

    # act
    inspc.read()

    # assert
    assert inspc.process_identifier == oai_urn
    assert inspc.record_identifier == "urn+nbn+de+gbv+3+1-1192015415-058134433-12"
    image_page_pairs = inspc.image_pairs
    assert not any('PHYS_0001' in p[1] for p in image_page_pairs)
    assert not any('PHYS_0002' in p[1] for p in image_page_pairs)
    assert any('PHYS_0003' in p[1] for p in image_page_pairs)
    assert not any('PHYS_0004' in p[1] for p in image_page_pairs)
    assert any('PHYS_0016' in p[1] for p in image_page_pairs)
    assert not any('PHYS_0017' in p[1] for p in image_page_pairs)
    assert len(image_page_pairs) == 13


def test_mets_mods_sbb_vol01_filtering():
    """Check filtering for SBB digital object from
    OCR-D METS-server https://github.com/kba/ocrd-demo-mets-server
    with default ULB configuration settings
    """
    oai_urn = 'oai:digital.staatsbibliothek-berlin.de:PPN891267093'
    orig_file = TEST_RES / 'sbb-PPN891267093.xml'
    assert os.path.isfile(orig_file)
    cfg = fixture_configuration()
    cfg.set(oc.CFG_SEC_METS, oc.CFG_SEC_METS_FGROUP, "DEFAULT")
    inspc = odem.ODEMMetadataInspecteur(orig_file, oai_urn, cfg)

    # act
    inspc.read()

    # assert
    image_page_pairs = inspc.image_pairs
    assert not any('PHYS_0001' in p[1] for p in image_page_pairs)
    assert len(image_page_pairs) == 136


def test_mets_mods_sbb_vol01_filtering_custom():
    """Check filtering for SBB digital object from
    OCR-D METS-server https://github.com/kba/ocrd-demo-mets-server
    now also remove logical type 'binding'
    """
    oai_urn = 'oai:digital.staatsbibliothek-berlin.de:PPN891267093'
    orig_file = TEST_RES / 'sbb-PPN891267093.xml'
    assert os.path.isfile(orig_file)
    cfg = fixture_configuration()
    cfg.set('mets', 'blacklist_logical_containers', 'cover_front,cover_back,binding')
    cfg.set(oc.CFG_SEC_METS, oc.CFG_SEC_METS_FGROUP, "DEFAULT")
    inspc = odem.ODEMMetadataInspecteur(orig_file, oai_urn, cfg)

    # act
    inspc.read()

    # assert
    image_page_pairs = inspc.image_pairs
    assert not any('PHYS_0001' in p[1] for p in image_page_pairs)
    assert len(image_page_pairs) == 129


def test_validate_mets_105054_schema_fails(tmp_path):
    """
    If Schema validation is required, then throw according exception
    in this case: alert invalid order data format
    """
    record = df_r.Record('oai:opendata.uni-halle.de:1981185920/105054')
    work_dir = tmp_path / '1981185920_105054'
    work_dir.mkdir()
    orig_mets = TEST_RES / '1981185920_105054.xml'
    shutil.copyfile(orig_mets, work_dir / '1981185920_105054.xml')
    odem_processor = odem.ODEMProcessImpl(record=record, work_dir=work_dir)
    odem_processor.configuration = fixture_configuration()
    odem_processor.inspect_metadata()
    with pytest.raises(odem.ODEMException) as odem_exec:
        odem_processor.validate_metadata()

    assert "'order': '1.1979' is not a valid value" in odem_exec.value.args[0]
    assert " of the atomic type 'xs:integer'" in odem_exec.value.args[0]


def test_validate_mets_37167_schema_fails(tmp_path):
    """
    if is invalid mets file, throw according exception
    """
    rec = df_r.Record('oai:opendata.uni-halle.de:1981185920/37167')
    work_dir = tmp_path / '1981185920_37167'
    work_dir.mkdir()
    original_mets = TEST_RES / '1981185920_37167_01.xml'
    shutil.copyfile(original_mets, work_dir / '1981185920_37167.xml')
    odem_processor = odem.ODEMProcessImpl(record=rec, work_dir=work_dir)
    odem_processor.configuration = fixture_configuration()
    odem_processor.inspect_metadata()
    with pytest.raises(odem.ODEMException) as odem_exc:
        odem_processor.validate_metadata()

    assert "recordIdentifier': This element is not expected" in odem_exc.value.args[0]


def test_validate_mets_37167_ddb_fails(tmp_path):
    """
    This time METS/MODS is valid but DDB validation is
    requested which fails of 2024-06-10 with 4 errors:
    * relatedItem missing type attribute
    * extra mets:dmdSec not linked to LOGICAL MAP with 
      only shelfLocator and also missing titleInfo
      (these are all related to each other)

      => this we had already at Rahbar
    """
    rec = df_r.Record('oai:opendata.uni-halle.de:1981185920/37167')
    work_dir = tmp_path / '1981185920_37167'
    work_dir.mkdir()
    original_mets = TEST_RES / '1981185920_37167_02.xml'
    shutil.copyfile(original_mets, work_dir / '1981185920_37167.xml')
    odem_processor = odem.ODEMProcessImpl(record=rec, work_dir=work_dir)
    odem_processor.configuration = fixture_configuration()
    odem_processor.configuration.set('mets', 'validate', 'True')
    odem_processor.configuration.set('mets', 'ddb_min_level', 'warn')
    odem_processor.inspect_metadata()
    with pytest.raises(odem.ODEMException) as odem_exec:
        odem_processor.validate_metadata()

    ddb_complains = odem_exec.value.args[0]
    assert len(ddb_complains) > 0
    assert 'titleInfo_02' in ddb_complains
    assert 'relatedItem_04' in ddb_complains
    assert 'location_01' in ddb_complains
    assert 'dmdSec_04' in ddb_complains


def test_validate_mets_37167_finally_succeeds(tmp_path):
    """
    This time METS/MODS and also DDB-validation are both pleased,
    therefore a plain 'True' shall be returned
    """

    rec = df_r.Record('oai:opendata.uni-halle.de:1981185920/37167')
    work_dir = tmp_path / '1981185920_37167'
    work_dir.mkdir()
    original_mets = TEST_RES / '1981185920_37167_03.xml'
    shutil.copyfile(original_mets, work_dir / '1981185920_37167.xml')
    odem_processor = odem.ODEMProcessImpl(record=rec, work_dir=work_dir)
    odem_processor.configuration = fixture_configuration()
    odem_processor.configuration.set('mets', 'validate', 'True')
    odem_processor.inspect_metadata()

    assert odem_processor.validate_metadata()


def test_integrate_alto_from_ocr_pipeline(tmp_path):
    """Ensure we can handle ALTO output straight from Tesseract
    OCR-Pipeline workflows
    """

    # arrange
    mets_file = TEST_RES / '1981185920_42296.xml'
    fulltext_dir = TEST_RES / '1981185920_42296_FULLTEXT'
    assert mets_file.exists()
    assert fulltext_dir.exists()
    tmp_mets = shutil.copy(mets_file, tmp_path)

    mets_tree = ET.parse(tmp_mets)
    ocr_files = [os.path.join(fulltext_dir, f) for f in os.listdir(fulltext_dir)]
    assert len(ocr_files) == 4

    # actsert
    assert (4, 0) == odem_pm.integrate_ocr_file(mets_tree, ocr_files)


def test_extract_text_content_from_alto_file():
    """Ensure we can read ALTO output and get its contents
    """

    # arrange
    fulltext_dir = TEST_RES / '1981185920_42296_FULLTEXT'
    ocr_files = [os.path.join(fulltext_dir, f) for f in os.listdir(fulltext_dir)]
    assert len(ocr_files) == 4

    # act
    text = odem_pm.extract_text_content(ocr_files)

    # assert
    assert text is not None
    assert len(text) == 126


def test_extract_identifiers():
    """What can we expect for identification
    when feeding newspapers? Expect the
    custom kvx-ppn value
    16691561019210131
    """

    # arrange
    mets_file = TEST_RES / '1516514412012_175762.xml'
    inspecteur = odem_pm.ODEMMetadataInspecteur(mets_file,
                                                '1516514412012_175762',
                                                fixture_configuration())
    # act
    report = inspecteur.read()

    # assert
    assert report is not None
    assert inspecteur.record_identifier == "urn+nbn+de+gbv+3+1-171133730-16691561019210131-18"


def test_process_bad_input_data():
    """What can we expect for identification
    when feeding newspapers? Expect the
    custom kvx-ppn value
    16691561019210131
    """

    # arrange
    mets_file = TEST_RES / '1516514412012_175762.xml'
    ocr_file = TEST_RES / '117470_00000006.lt.xml'
    mets_tree = ET.parse(str(mets_file)).getroot()

    # act
    outcome = odem_pm.integrate_ocr_file(mets_tree, [str(ocr_file)])

    # assert
    assert outcome is not None


def test_record_identifier_ulb_vd17():
    """Check result outcome for ULB VD17 from
    legacy migration with special configuration setting
    """
    oai_urn = "oai:opendata2.uni-halle.de:1516514412012/32294"
    orig_file = TEST_RES / "1516514412012_32294.xml"
    assert os.path.isfile(orig_file)
    cfg: configparser.ConfigParser = fixture_configuration()
    xpr_id = "//mods:mods/mods:recordInfo/mods:recordIdentifier[@source='vd17-ppn']/text()"
    cfg.set(oc.CFG_SEC_METS, oc.CFG_SEC_METS_OPT_ID_XPR, xpr_id)
    inspc = odem.ODEMMetadataInspecteur(orig_file, oai_urn, cfg)

    # act
    inspc.read()

    # assert
    assert inspc.process_identifier == oai_urn
    assert inspc.record_identifier == "004952871"


def test_record_identifier_type_urn():
    """Check result outcome for ULB VD17 from recent
    digitalisation output with special configuration setting
    """
    oai_urn = "oai:opendata2.uni-halle.de:1516514412012/32294"
    orig_file = TEST_RES / "1516514412012_32294.xml"
    assert os.path.isfile(orig_file)
    cfg: configparser.ConfigParser = fixture_configuration()
    xpr_id = "//mods:mods/mods:identifier[@type='urn']/text()"
    cfg.set(oc.CFG_SEC_METS, oc.CFG_SEC_METS_OPT_ID_XPR, xpr_id)
    inspc = odem.ODEMMetadataInspecteur(orig_file, oai_urn, cfg)

    # act
    inspc.read()

    # assert
    assert inspc.process_identifier == oai_urn
    assert inspc.record_identifier == "urn+nbn+de+gbv+3+1-52939"


def test_record_identifier_provoke_exception():
    """Provoke exception if expression provided
    without match"""
    
    oai_urn = "oai:opendata2.uni-halle.de:1516514412012/32294"
    orig_file = TEST_RES / "1516514412012_32294.xml"
    assert os.path.isfile(orig_file)
    cfg: configparser.ConfigParser = fixture_configuration()
    xpr_id = "//mods:mods/mods:recordInfo/mods:recordIdentifier[@source='ulb-ppn']/text()"
    cfg.set(oc.CFG_SEC_METS, oc.CFG_SEC_METS_OPT_ID_XPR, xpr_id)
    inspc = odem.ODEMMetadataInspecteur(orig_file, oai_urn, cfg)
    inspc.read()

    # act
    with pytest.raises(odem_pm.ODEMMetadataMetsException) as mets_ex:
        inspc.record_identifier

    # assert
    assert "Invalid match [] " in mets_ex.value.args[0]


def test_metadata_newspaper_year_provoke_exception():
    """Provoke exception if try to OCR irrelvant
    newspaper year without any images"""

    oai_urn = "oai:opendata2.uni-halle.de:1516514412012/175735"
    orig_file = TEST_RES / "1516514412012_175735_year_1921.xml"
    assert os.path.isfile(orig_file)
    cfg: configparser.ConfigParser = fixture_configuration()
    inspc = odem.ODEMMetadataInspecteur(orig_file, oai_urn, cfg)

    # act
    with pytest.raises(odem_pm.ODEMNoTypeForOCRException) as mets_ex:
        inspc.read()

    # assert
    assert "no PICA type for OCR: Az" in mets_ex.value.args[0]
