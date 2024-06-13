"""Implementation related to OCR data handling"""

import math
import os
import string
import typing
import unicodedata

import lxml.etree as ET

import digiflow as df
import ocrd_page_to_alto.convert as opta_c

import lib.odem.odem_commons as odem_c


# define propably difficult characters
# very common separator '⸗'
DOUBLE_OBLIQUE_HYPHEN = '\u2E17'
# rare Geviertstrich '—'
EM_DASH = '\u2014'
ODEM_PUNCTUATIONS = string.punctuation + EM_DASH + DOUBLE_OBLIQUE_HYPHEN
# create module-wide translator
PUNCT_TRANSLATOR = str.maketrans('', '', ODEM_PUNCTUATIONS)
# we want all words to be at least 2 chars
MINIMUM_WORD_LEN = 2

# diacritica to take care of
COMBINING_SMALL_E = '\u0364'
# # we want all words to be at least 2 chars
# MINIMUM_WORD_LEN = 2
# punctuations to take into account
# includes
#   * regular ASCII-punctuations
#   * Dashes        \u2012-2017
#   * Quotations    \u2018-201F
PUNCTUATIONS = string.punctuation + '\u2012' + '\u2013' + '\u2014' + '\u2015' + '\u2016' + '\u2017' + '\u2018' + '\u2019' + '\u201A' + '\u201B' + '\u201C' + '\u201D' + '\u201E' + '\u201F'

DROP_ALTO_ELEMENTS = [
    'alto:Shape',
    'alto:Illustration',
    'alto:GraphicalElement']

LOCAL_DIR_RESULT = 'PAGE'


class ODEMMetadataOcrException(Exception):
    """Mark any problems related to OCR
    Metadata processing
    """


def postprocess_ocr_file(ocr_file, strip_tags):
    """
    Correct data in actual ocr_file
    * sourceImage file_name (ensure ends with '.jpg')
    * page ID using pattern "p0000000n"
    * strip non-alphabetial chars and if this clears
      String-Elements completely, drop them all
    * drop interpunctuations
    """

    # the xml cleanup
    mproc = df.MetsProcessor(str(ocr_file))
    if strip_tags:
        mproc.remove(strip_tags)

    # inspect transformation artifacts
    _all_text_blocks = mproc.tree.xpath('//alto:TextBlock', namespaces=df.XMLNS)
    for _block in _all_text_blocks:
        if 'IDNEXT' in _block.attrib:
            del _block.attrib['IDNEXT']

    # inspect textual content
    # _all_strings = mproc.tree.xpath('//alto:String', namespaces=XMLNS)
    _all_strings = mproc.tree.findall('.//alto:String', df.XMLNS)
    for _string_el in _all_strings:
        _content = _string_el.attrib['CONTENT'].strip()
        if _is_completely_punctuated(_content):
            # only common punctuations, nothing else
            _uplete(_string_el, _string_el.getparent())
            continue
        if len(_content) > 0:
            try:
                _handle_trailing_puncts(_string_el)
                _content = _string_el.attrib['CONTENT']
            except Exception as _exc:
                raise ODEMMetadataOcrException(f"{_exc.args[0]} from {ocr_file}!") from _exc
        if len(_content) < MINIMUM_WORD_LEN:
            # too few content, remove element bottom-up
            _uplete(_string_el, _string_el.getparent())
    mproc.write()


def list_files(dir_root, sub_dir) -> typing.List:
    _curr_dir = os.path.join(dir_root, sub_dir)
    return [
        os.path.join(_curr_dir, _file)
        for _file in os.listdir(_curr_dir)
        if str(_file).endswith('.xml')
    ]


def convert_to_output_format(work_dir_root):
    """Convert created OCR-Files to required presentation
    format (i.e. ALTO)
    """

    _converted = []
    _fulltext_dir = os.path.join(work_dir_root, odem_c.FILEGROUP_OCR)
    if not os.path.isdir(_fulltext_dir):
        os.makedirs(_fulltext_dir, exist_ok=True)
    _results = list_files(work_dir_root, LOCAL_DIR_RESULT)
    for _file in _results:
        the_id = os.path.basename(_file)
        output_file = os.path.join(_fulltext_dir, the_id)
        converter = opta_c.OcrdPageAltoConverter(page_filename=_file).convert()
        with open(output_file, 'w', encoding='utf-8') as output:
            output.write(str(converter))
        _converted.append(output_file)
    return _converted


def _is_completely_punctuated(a_string):
    """Check if only punctuations are contained
    but nothing else"""

    return len(a_string.translate(PUNCT_TRANSLATOR)) == 0


def _handle_trailing_puncts(string_element):
    """Split off final character if considered to be
    ODEM punctuation and not the only content
    """

    _content = string_element.attrib['CONTENT']
    if _content[-1] in ODEM_PUNCTUATIONS and len(_content) > 1:
        # gather information
        _id = string_element.attrib['ID']
        _left = int(string_element.attrib['HPOS'])
        _top = int(string_element.attrib['VPOS'])
        _width = int(string_element.attrib['WIDTH'])
        _height = int(string_element.attrib['HEIGHT'])
        _w_per_char = math.ceil(_width / len(_content))

        # cut off last punctuation char
        # shrink by calculated char width
        _new_width = (len(_content) - 1) * _w_per_char
        _new_content = _content[:-1]
        string_element.attrib['WIDTH'] = str(_new_width)
        string_element.attrib['CONTENT'] = _new_content

        # create new string element with final char
        _tag = string_element.tag
        _attr = {'ID': f'{_id}_p1',
                 'HPOS': str(_left + _new_width),
                 'VPOS': str(_top),
                 'WIDTH': str(_w_per_char),
                 'HEIGHT': str(_height),
                 'CONTENT': _content[-1]
                 }
        _new_string = ET.Element(_tag, _attr)
        string_element.addnext(_new_string)


def _uplete(curr_el: ET._Element, parent: ET._Element):
    """delete empty elements up-the-tree"""

    parent.remove(curr_el)
    _content_childs = [kid
                       for kid in parent.getchildren()
                       if kid is not None and 'SP' not in kid.tag]
    if len(_content_childs) == 0 and parent.getparent() is not None:
        _uplete(parent, parent.getparent())


def _normalize_string_content(the_content):
    """normalize textual content
    * -try to normalize vocal ligatures via unicode-
      currently disabled
    * if contains at least one non-alphabetical char
      remove digits and punctuation chars
      => also remove the "Geviertstrich": u2014 (UTF-8)
    Args:
        the_content (str): text as is from alto:String@CONTENT
    """

    if not str(the_content).isalpha():
        punct_translator = str.maketrans('', '', PUNCTUATIONS)
        the_content = the_content.translate(punct_translator)
        # maybe someone searches for years - won't be possible
        # if digits are completely dropped
        # digit_translator = str.maketrans('','',string.digits)
        # the_content = the_content.translate(digit_translator)
    return the_content


def _normalize_vocal_ligatures(a_string):
    """Replace vocal ligatures, which otherwise
    may confuse the index component workflow,
    especially COMBINING SMALL LETTER E : \u0364
    a^e, o^e, u^e => (u0364) => ä, ö, ü
    """

    _out = []
    for i, _c in enumerate(a_string):
        if _c == COMBINING_SMALL_E:
            _preceeding_vocal = _out[i - 1]
            _vocal_name = unicodedata.name(_preceeding_vocal)
            _replacement = ''
            if 'LETTER A' in _vocal_name:
                _replacement = 'ä'
            elif 'LETTER O' in _vocal_name:
                _replacement = 'ö'
            elif 'LETTER U' in _vocal_name:
                _replacement = 'ü'
            else:
                _msg = f"No conversion for {_preceeding_vocal} ('{a_string}')!"
                raise ODEMMetadataOcrException(f"normalize vocal ligatures: {_msg}")
            _out[i - 1] = _replacement
        _out.append(_c)

    # strip all combining e's anyway
    return ''.join(_out).replace(COMBINING_SMALL_E, '')
