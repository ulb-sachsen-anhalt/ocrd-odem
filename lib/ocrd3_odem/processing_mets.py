"""Encapsulate Implementations concerning METS/MODS handling"""

import os

from typing import (
    List,
)

import lxml.etree as ET

from digiflow import (
    MetsReader,
    MetsProcessor,
    XMLNS,
    post_oai_extract_metsdata,
    validate_xml,
    write_xml_file,
)

from .odem_commons import (
    FILEGROUP_IMG,
    FILEGROUP_OCR,
)

PRINT_WORKS = ['a', 'f', 'F', 'Z', 'B']
IDENTIFIER_CATALOGUE = 'gvk-ppn'
Q_XLINK_HREF = '{http://www.w3.org/1999/xlink}href'
METS_AGENT_ODEM = 'DFG-OCRD3-ODEM'


def extract_mets_data(the_self, the_data):
    """
    Migration Post-recive OAI METS/MODS callback
    """

    xml_root = ET.fromstring(the_data)
    mets_tree = post_oai_extract_metsdata(xml_root)
    write_xml_file(mets_tree, the_self.path_mets)


class ODEMMetadataMetsException(Exception):
    """Mark state when inconsistencies exist
    between linkings of physical and logical
    print sections, like logical sections
    without phyiscal pages or pages that
    refer to no logical section
    """


class ODEMNoTypeForOCRException(ODEMMetadataMetsException):
    """Mark custom ODEM Workflow Exception
    when print because of metadata is *not* to be
    considered to be ocr-able because it
    contains no pages at all
    """


class ODEMNoImagesForOCRException(ODEMMetadataMetsException):
    """Mark custom ODEM Workflow Exception
    when print metadata contains no images-of-interest
    for OCR, i.e. only maps/illustrations
    """


class ODEMMetadataInspecteur:
    """Take a look into print's metadata"""

    def __init__(self, input_data, process_identifier: str,
                 cfg):
        self.process_identifier = process_identifier
        self._data = input_data
        self._cfg = cfg
        self._report = None
        self.image_pairs = []
        self.n_images_pages = 0
        self.n_images_ocrable = 0

    def _get_report(self):
        if self._report is None:
            try:
                self._report = MetsReader(self._data).report
            except RuntimeError as _err:
                raise ODEMMetadataMetsException(_err) from _err
        return self._report

    def inspect(self):
        """Gather knowledge about digital object's.
        First, try to determin what kind of retro-digit
        we are handling by inspecting it's final PICA mark
        actual metadata from METS/MODS
        Stop if data corrupt, ill or bad
        """
        try:
            report = self._get_report()
            if not report.type or report.type[-1] not in PRINT_WORKS:
                raise ODEMNoTypeForOCRException(f"{self.process_identifier} no type for OCR: {report.type}")
            reader = MetsReader(self._data)
            reader.check()
            self.inspect_metadata_images()
        except RuntimeError as _err:
            raise ODEMMetadataMetsException(_err.args[0]) from _err
        if IDENTIFIER_CATALOGUE not in report.identifiers:
            raise ODEMMetadataMetsException(f"No {IDENTIFIER_CATALOGUE} in {self.process_identifier}")

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
        except ODEMMetadataMetsException as ome:
            _problems.append(ome.args[0])
        if not is_in(blacklist_structs, log_type):
            if not is_in(blacklist_page_labels, _phys_dict['LABEL']):
                _pairs.append((_local_file_name, _phys_dict['ID']))
    # re-raise on error
    if len(_problems) > 0:
        _n_probs = len(_problems)
        raise ODEMMetadataMetsException(f"{_n_probs}x: {','.join(_problems)}")
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
                raise ODEMMetadataMetsException(f"Cant handle label: {_label} of '{parent}'")
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
    raise ODEMMetadataMetsException(f"Page {phys_id} not linked")


def clear_filegroups(xml_file, removals):
    """Drop existing file group entries
    and unlink them properly like
    * DOWNLOAD (created within common ODEM workflow)
    * THUMBS (created by Share_it)
    * DEFAULT (created by Share_it)
    """

    proc = MetsProcessor(xml_file)
    proc.clear_filegroups(black_list=removals)
    proc.write()


def integrate_ocr_file(xml_tree, ocr_files: List) -> int:
    """Enrich given OCR-Files into XML tree
    
    Returns number of linked files
    """

    _n_linked_ocr = 0
    file_sec = xml_tree.find('.//mets:fileSec', XMLNS)
    tag_file_group = f'{{{XMLNS["mets"]}}}fileGrp'
    tag_file = f'{{{XMLNS["mets"]}}}file'
    tag_flocat = f'{{{XMLNS["mets"]}}}FLocat'

    file_grp_fulltext = ET.Element(tag_file_group, USE=FILEGROUP_OCR)
    for _ocr_file in ocr_files:
        _file_name = os.path.basename(_ocr_file).split('.')[0]
        new_id = FILEGROUP_OCR + '_' + _file_name
        file_ocr = ET.Element(
            tag_file, MIMETYPE="application/alto+xml", ID=new_id)
        flocat_href = ET.Element(tag_flocat, LOCTYPE="URL")
        flocat_href.set(Q_XLINK_HREF, _ocr_file)
        file_ocr.append(flocat_href)
        file_grp_fulltext.append(file_ocr)

        # Referencing / linking the ALTO data as a file pointer in
        # the sequence container of the physical structMap
        # Assignment takes place via the name of the corresponding
        # image (= name ALTO file)
        _mproc = MetsProcessor(_ocr_file)
        src_info = _mproc.tree.xpath('//alto:sourceImageInformation/alto:fileName', namespaces=XMLNS)[0]
        src_info.text = f'{_file_name}.jpg'
        first_page_el = _mproc.tree.xpath('//alto:Page', namespaces=XMLNS)[0]
        first_page_el.attrib['ID'] = f'p{_file_name}'
        _mproc.write()
        _n_linked_ocr += _link_fulltext(new_id, xml_tree)
    file_sec.append(file_grp_fulltext)
    return _n_linked_ocr


def _link_fulltext(file_ident, xml_tree):
    file_name = file_ident.split('_')[-1]
    xp_files = f'.//mets:fileGrp[@USE="{FILEGROUP_IMG}"]/mets:file'
    file_grp_max_files = xml_tree.findall(xp_files, XMLNS)
    for file_grp_max_file in file_grp_max_files:
        _file_link = file_grp_max_file[0].attrib['{http://www.w3.org/1999/xlink}href']
        _file_label = _file_link.split('/')[-1]
        if file_name in _file_label:
            max_file_id = file_grp_max_file.attrib['ID']
            xp_phys = f'//mets:div/mets:fptr[@FILEID="{max_file_id}"]/..'
            parents = xml_tree.xpath(xp_phys, namespaces=XMLNS)
            if len(parents) == 1:
                ET.SubElement(parents[0], f"{{{XMLNS['mets']}}}fptr", {
                    "FILEID": file_ident})
                # add only once, therefore return
                return 1
    # if not linked, return zero
    return 0


def is_in(tokens: List[str], label):
    """label contained somewhere in a list of tokens?"""

    return any(t in label for t in tokens)


def postprocess_mets(mets_file, label_base_image):
    """wrap work related to processing METS/MODS"""

    mproc = MetsProcessor(mets_file)
    _process_agents(mproc, label_base_image)
    _clear_provenance_links(mproc)
    mproc.write()

def _process_agents(mproc, label_base_image):
    # drop existing ODEM marks
    # enrich *only* latest run
    xp_txt_odem = f'//mets:agent[contains(mets:name,"{METS_AGENT_ODEM}")]'
    agents_odem = mproc.tree.xpath(xp_txt_odem, namespaces=XMLNS)
    for old_odem in agents_odem:
        parent = old_odem.getparent()
        parent.remove(old_odem)
    mproc.enrich_agent(f"{METS_AGENT_ODEM}_{label_base_image}")

    # ensure only very recent derivans agent entry exists
    xp_txt_derivans = '//mets:agent[contains(mets:name,"DigitalDerivans")]'
    derivanses = mproc.tree.xpath(xp_txt_derivans, namespaces=XMLNS)
    if len(derivanses) < 1:
        raise RuntimeError(f"Missing METS agent entry for {xp_txt_derivans}!")
    # sort by latest token in agent note ascending
    # note is assumed to be a date
    # like: "PDF FileGroup for PDF_198114125 created at 2022-04-29T12:40:30"
    _sorts = sorted(derivanses, key=lambda e: e[1].text.split()[-1])
    _sorts.pop()
    for i, _retired_agent in enumerate(_sorts):
        _parent = _retired_agent.getparent()
        _parent.remove(_sorts[i])


def _clear_provenance_links(mproc):
    xp_dv_iif_or_sru = '//dv:links/*[local-name()="iiif" or local-name()="sru"]'
    old_dvs = mproc.tree.xpath(xp_dv_iif_or_sru, namespaces=XMLNS)
    for old_dv in old_dvs:
        parent = old_dv.getparent()
        parent.remove(old_dv)


def validate_mets(mets_file:str):
    """Forward METS-schema validation"""

    xml_root = ET.parse(mets_file).getroot()
    validate_xml(xml_root)
