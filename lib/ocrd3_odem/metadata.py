"""Encapsulate Implementations concerning METS/MODS handling"""

from typing import (
    List,
)

import lxml.etree as ET

from digiflow import (
    MetsReader,
    XMLNS,
)

PRINT_WORKS = ['a', 'f', 'F', 'Z', 'B']
IDENTIFIER_CATALOGUE = 'gvk-ppn'
Q_XLINK_HREF = '{http://www.w3.org/1999/xlink}href'


class ODEMMetadataException(Exception):
    """Mark state when inconsistencies exist
    between linkings of physical and logical
    print sections, like logical sections
    without phyiscal pages or pages that
    refer to no logical section
    """


class ODEMNoTypeForOCRException(ODEMMetadataException):
    """Mark custom ODEM Workflow Exception
    when print because of metadata is *not* to be
    considered to be ocr-able because it
    contains no pages at all
    """

class ODEMNoImagesForOCRException(ODEMMetadataException):
    """Mark custom ODEM Workflow Exception
    when print metadata contains no images-of-interest
    for OCR, i.e. only maps/illustrations
    """


class ODEMMetadataInspecteur:
    """Take a look into print's metadata"""

    def __init__(self, input_data, process_identifier: str,
                 cfg, workdir):
        self.process_identifier = process_identifier
        self._data = input_data
        self._cfg = cfg
        self._work_dir_main = workdir
        self._report = None
        self.image_pairs = []
        self.n_images_pages = 0
        self.n_images_ocrable = 0

    def _get_report(self):
        if self._report is None:
            try:
                self._report = MetsReader(self._data).report
            except RuntimeError as _err:
                raise ODEMMetadataException(_err) from _err
        return self._report

    def inspect(self):
        """Gather knowledge about digital object's
        actual metadata from METS/MODS
        """
        try:
            report = self._get_report()
            # what kind of retro digi is it?
            # inspect final type mark
            if not report.type or report.type[-1] not in PRINT_WORKS:
                raise ODEMNoTypeForOCRException(f"{self.process_identifier} no type for OCR: {report.type}")
            # detailed inspection of languages
            # self.type = report.type
            # self.identifiers = report.identifiers
            # apply additional metadata checks
            # stop if data corrupt, ill or bad
            reader = MetsReader(self._data)
            reader.check()
            # detailed inspection of image group
            self.inspect_metadata_images()
        except RuntimeError as _err:
            raise ODEMMetadataException(_err.args[0]) from _err
        if IDENTIFIER_CATALOGUE not in report.identifiers:
            raise ODEMMetadataException(f"No {IDENTIFIER_CATALOGUE} in {self.process_identifier}")

    @property
    def identifiers(self):
        """Get language information"""
        return self._get_report().identifiers

    @property
    def languages(self):
        """Get language information"""
        return self._get_report().languages

    @property
    def type(self):
        """Get type information"""
        return self._get_report().type

    def inspect_metadata_images(self):
        """Reduce amount of Images passed on to
        OCR by utilizing metadata knowledge.
        Drop images which belong to
        * physical containers (named "Colorchecker")
        * logical structures (type "cover_front")

        This can obviously only apply if metadata
        is present at all and structured in-depth
        """

        blacklist_log = self._cfg.getlist('mets', 'blacklist_logical_containers')
        blacklist_lab = self._cfg.getlist('mets', 'blacklist_physical_container_labels')
        mets_root = ET.parse(self._data).getroot()
        _max_images = mets_root.findall('.//mets:fileGrp[@USE="MAX"]/mets:file', XMLNS)
        _n_max_images = len(_max_images)
        if _n_max_images < 1:
            _msg = f"{self.process_identifier} contains absolutly no images for OCR!"
            raise ODEMNoImagesForOCRException(_msg)
        # gather present images via generator
        pairs_img_id = fname_ident_pairs_from_metadata(mets_root, blacklist_log, blacklist_lab)
        n_images_ocrable = len(pairs_img_id)
        if n_images_ocrable < 1:
            _msg = f"{self.process_identifier} contains no images for OCR (total: {_n_max_images})!"
            raise ODEMNoImagesForOCRException(_msg)
        # else, journey onwards with image name only
        self.image_pairs = pairs_img_id
        self.n_images_pages = _n_max_images
        self.n_images_ocrable = len(self.image_pairs)


def fname_ident_pairs_from_metadata(mets_root, blacklist_structs, blacklist_page_labels):
    """Generate pairs of image label and URN
    that respect defined blacklisted physical
    and logical structures.
    
    * first, get all required linking groups
    * second, start with file image final part and gather
      from this group all required informations on the way
      from file location => physical container => structMap
      => logical structure
    """
    _pairs = []
    _problems = []
    _phys_conts = mets_root.findall('.//mets:structMap[@TYPE="PHYSICAL"]/mets:div/mets:div/mets:fptr', XMLNS)
    _structmap_links = mets_root.findall('.//mets:structLink/mets:smLink', XMLNS)
    _log_conts = mets_root.findall('.//mets:structMap[@TYPE="LOGICAL"]//mets:div', XMLNS)
    _max_images = mets_root.findall('.//mets:fileGrp[@USE="MAX"]/mets:file', XMLNS)
    for _max_file in _max_images:
        _local_file_name = _max_file[0].get(Q_XLINK_HREF).split('/')[-1]
        _file_id = _max_file.get('ID')
        _phys_dict = _phys_container_for_id(_phys_conts, _file_id)
        try:
            log_type = _log_type_for_id(_phys_dict['ID'], _structmap_links, _log_conts)
        except ODEMMetadataException as ome:
            _problems.append(ome.args[0])
        if not is_in(blacklist_structs, log_type):
            if not is_in(blacklist_page_labels, _phys_dict['LABEL']):
                _pairs.append((_local_file_name, _phys_dict['ID']))
    # re-raise on error
    if len(_problems) > 0:
        _n_probs = len(_problems)
        raise ODEMMetadataException(f"{_n_probs}x: {','.join(_problems)}")
    return _pairs

def _phys_container_for_id(_phys_conts, _id):
    """Collect and prepare all required 
    data from matching physical container 
    for later analyzis or processing"""

    for _cnt in _phys_conts:
        _file_id = _cnt.attrib['FILEID']
        if _file_id == _id:
            parent = _cnt.getparent()
            _cnt_id = parent.attrib['ID']
            _label = None
            if 'LABEL' in parent.attrib:
                _label = parent.attrib['LABEL']
            elif 'ORDERLABEL' in parent.attrib:
                _label = parent.attrib['ORDERLABEL']
            else:
                raise ODEMMetadataException(f"Cant handle label: {_label} of '{parent}'")
            return {'ID': _cnt_id, 'LABEL': _label}


def _log_type_for_id(phys_id, structmap_links, log_conts):
    """Follow link from physical container ('to') 
    via  strucmap link to the corresponding logical 
    structure ('from') and grab it's type

    Alert if no type found => indicates inconsistend data
    """

    for _link in structmap_links:
        _physical_target_id = _link.attrib['{http://www.w3.org/1999/xlink}to']
        if _physical_target_id == phys_id:
            for _logical_section in log_conts:
                _logical_section_id = _logical_section.attrib['ID']
                _logical_target_id = _link.attrib['{http://www.w3.org/1999/xlink}from']
                if _logical_section_id == _logical_target_id:
                    return _logical_section.attrib['TYPE']
    raise ODEMMetadataException(f"Page {phys_id} not linked")


def is_in(tokens: List[str], label):
    """label contained somewhere in a list of tokens?"""

    return any(t in label for t in tokens)