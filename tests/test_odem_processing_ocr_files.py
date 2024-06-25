"""Specification for OCR Postprocessings"""

import os

from pathlib import Path

import lxml.etree as ET
import digiflow as df
import digiflow.validate as df_v

import pytest

from lib import odem

from .conftest import TEST_RES, fixture_configuration


# please linter for lxml.etree contains no-member message
# pylint:disable=I1101


def test_integrated_ocr_files_fit_identifier(fixture_27949: odem.ODEMProcessImpl):
    """Ensure ocr-file elements fit syntactically
    * proper fileName
    * proper PageId set
    """

    # arrange
    tmp_path = Path(fixture_27949.work_dir_root)

    # assert
    assert not os.path.exists(tmp_path / 'FULLTEXT' / '00000002.xml')
    assert os.path.exists(tmp_path / 'FULLTEXT' / '00000003.xml')
    ocr_file_03 = ET.parse(str(tmp_path / 'FULLTEXT' / '00000003.xml')).getroot()
    assert len(ocr_file_03.xpath('//alto:Page[@ID="p00000003"]', namespaces=df.XMLNS)) == 1
    assert ocr_file_03.xpath('//alto:fileName', namespaces=df.XMLNS)[0].text == '00000003.jpg'
    ocr_file_06 = ET.parse(str(tmp_path / 'FULLTEXT' / '00000006.xml')).getroot()
    assert len(ocr_file_06.xpath('//alto:Page[@ID="p00000006"]', namespaces=df.XMLNS)) == 1
    assert not os.path.exists(tmp_path / 'FULLTEXT' / '00000007.xml')


def test_fixture_one_postprocess_ocr_files(fixture_27949: odem.ODEMProcessImpl):
    """Ensure expected replacements done *even* when
    diacritics occour more several times in single word"""

    # arrange
    tmp_path = Path(fixture_27949.work_dir_root)
    path_file = tmp_path / 'FULLTEXT' / '00000003.xml'
    strip_tags = fixture_configuration().getlist(odem.CFG_SEC_OCR, 'strip_tags')  # pylint: disable=no-member

    # act
    odem.postprocess_ocr_file(path_file, strip_tags)

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
        for _punc in odem.PUNCTUATIONS:
            # adapted like semantics did
            # each trailing punctuation
            # is now in it's own STRING element
            if _punc in _content[-1]:
                assert len(_content) == 1


def test_postprocess_empty_alto():
    """What happens with empty input file?"""

    # arrange
    res_path = TEST_RES / '117470_00000006.lt.xml'
    strip_tags = ['alto:Shape','alto:Processing',
                  'alto:Illustration','alto:GraphicalElement']

    # act
    with pytest.raises(df_v.InvalidXMLException) as inv_exc:
        df_v.validate_xml(res_path)

    assert ' Missing child element(s)' in str(inv_exc)
    assert 'Expected is ( {http://www.loc.gov/standards/alto/ns-v4#}Layout' in str(inv_exc)
    odem.postprocess_ocr_file(res_path, strip_tags)
