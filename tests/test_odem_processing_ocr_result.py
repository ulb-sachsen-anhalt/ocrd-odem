"""Specification for OCR Postprocessings"""

import os
import shutil

import lxml.etree as ET
from digiflow import MetsProcessor

from lib.ocrd3_odem import (
    PUNCTUATIONS,
    XMLNS,
    ODEMProcess,
    postprocess_ocrd_file, integrate_ocr_file,
)
from .conftest import (
    fixture_configuration, TEST_RES,
)


def test_module_fixture_one_integrated_ocr_files_fit_identifier(fixture_27949: ODEMProcess):
    """Ensure ocr-file elements fit syntactically
    * proper fileName
    * proper PageId set
    """

    # arrange
    tmp_path = fixture_27949.work_dir_main

    # assert
    assert not os.path.exists(tmp_path / 'FULLTEXT' / '00000002.xml')
    assert os.path.exists(tmp_path / 'FULLTEXT' / '00000003.xml')
    ocr_file_03 = ET.parse(str(tmp_path / 'FULLTEXT' / '00000003.xml')).getroot()
    assert len(ocr_file_03.xpath('//alto:Page[@ID="p00000003"]', namespaces=XMLNS)) == 1
    assert ocr_file_03.xpath('//alto:fileName', namespaces=XMLNS)[0].text == '00000003.jpg'
    ocr_file_06 = ET.parse(str(tmp_path / 'FULLTEXT' / '00000006.xml')).getroot()
    assert len(ocr_file_06.xpath('//alto:Page[@ID="p00000006"]', namespaces=XMLNS)) == 1
    assert not os.path.exists(tmp_path / 'FULLTEXT' / '00000007.xml')


def test_fixture_one_postprocessed_ocr_files_elements(fixture_27949: ODEMProcess):
    """Ensure ocr-file unwanted elements dropped as expected
    """

    # arrange
    tmp_path = fixture_27949.work_dir_main

    # act
    # fixture_27949.link_ocr()
    fixture_27949.postprocess_ocr()

    # assert
    ocr_file_03 = ET.parse(str(tmp_path / 'FULLTEXT' / '00000003.xml')).getroot()
    assert not ocr_file_03.xpath('//alto:Shape', namespaces=XMLNS)


def test_fixture_one_postprocess_ocr_files(fixture_27949: ODEMProcess):
    """Ensure expected replacements done *even* when
    diacritics occour more several times in single word"""

    # arrange
    tmp_path = fixture_27949.work_dir_main
    path_file = tmp_path / 'FULLTEXT' / '00000003.xml'
    strip_tags = fixture_configuration().getlist('ocr', 'strip_tags')  # pylint: disable=no-member

    # act
    postprocess_ocrd_file(path_file, strip_tags)

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


def test_link_ocr_alto_v3_compat(tmp_path):
    orig_mets = TEST_RES / '1981185920_42296.xml'
    trgt_mets = tmp_path / '1981185920_42296.xml'
    orig_alto_dir = TEST_RES / '1981185920_42296_FULLTEXT'
    trgt_alto_dir = tmp_path / 'FULLTEXT'
    shutil.copyfile(orig_mets, trgt_mets)
    shutil.copytree(orig_alto_dir, trgt_alto_dir)

    proc = MetsProcessor(trgt_mets)
    _n_linked_ocr = integrate_ocr_file(proc.tree, [str(trgt_alto_dir / a) for a in os.listdir(trgt_alto_dir)])
    proc.write()

    assert _n_linked_ocr == 4
