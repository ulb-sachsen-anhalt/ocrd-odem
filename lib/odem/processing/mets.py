"""Encapsulate Implementations concerning METS/MODS handling"""

import configparser
import typing

from pathlib import Path

import lxml.etree as ET
import digiflow as df
import digiflow.validate as dfv

import lib.odem.odem_commons as odem_c


TYPE_PRINTS_PICA = ['a', 'f', 'F', 'Z', 'B']
TYPE_PRINTS_LOGICAL = ['monograph', 'volume', 'issue', 'additional']
CATALOG_ULB = 'gvk-ppn'
CATALOG_ULB2 = 'kxp-ppn'  # ULB ZD related
CATALOG_OTH = 'gbv-ppn'
CATALOG_SWB = 'swb-ppn'  # SLUB OAI related
CATALOGUE_IDENTIFIERS = [CATALOG_ULB, CATALOG_ULB2, CATALOG_OTH, CATALOG_SWB]
RECORD_IDENTIFIER = 'recordIdentifier'
Q_XLINK_HREF = '{http://www.w3.org/1999/xlink}href'
METS_AGENT_ODEM = 'DFG-OCRD3-ODEM'

IMAGE_GROUP_ULB = 'MAX'
IMAGE_GROUP_DEFAULT = 'DEFAULT'


# please linter for lxml.etree contains no-member message
# pylint:disable=I1101

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
                self._report = df.MetsReader(self._data).report
            except RuntimeError as _err:
                raise ODEMMetadataMetsException(_err) from _err
        return self._report

    def metadata_report(self) -> df.MetsReaderReport:
        """Gather knowledge about digital object's.
        First, try to determin what kind of retro-digit
        we are handling by inspecting it's final PICA mark
        actual metadata from METS/MODS
        Stop if data corrupt, ill or bad
        """
        report = self._get_report()
        if not report.type:
            raise ODEMNoTypeForOCRException(f"{self.process_identifier} found no type")
        _type = report.type
        # PICA types *might* contain trailing 'u' or 'v' = 'Afu'
        if len(_type) in range(2, 4) and _type[1] not in TYPE_PRINTS_PICA:
            raise ODEMNoTypeForOCRException(f"{self.process_identifier} no PICA type for OCR: {report.type}")
        if len(_type) > 4 and _type not in TYPE_PRINTS_LOGICAL:
            raise ODEMNoTypeForOCRException(f"{self.process_identifier} unknown type: {_type}")
        reader = df.MetsReader(self._data)
        reader.check()
        self.inspect_metadata_images()
        if not any(ident in CATALOGUE_IDENTIFIERS for ident in report.identifiers):
            raise ODEMMetadataMetsException(f"No {CATALOGUE_IDENTIFIERS} in {self.process_identifier}")
        return report

    @property
    def identifiers(self):
        """Get *all* identifiers"""
        return self._get_report().identifiers

    @property
    def mods_record_identifier(self):
        """Get main MODS recordIdentifier if present
        guess if more than 1 ppn-like entry exist
        """
        idents = dict(self._get_report().identifiers)
        if 'urn' in idents:
            del idents['urn']
        if len(idents) == 1:
            return list(idents.values())[0]
        if CATALOG_ULB in idents:
            return idents[CATALOG_ULB]
        elif CATALOG_OTH in idents:
            return idents[CATALOG_OTH]
        else:
            _proc_in = self.process_identifier
            if ':' in _proc_in:
                return _proc_in.replace(':', '+')
            return _proc_in

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
        _image_res = mets_root.findall(f'.//mets:fileGrp[@USE="{IMAGE_GROUP_ULB}"]/mets:file', df.XMLNS)
        _n_image_res = len(_image_res)
        if _n_image_res == 0:
            _image_res = mets_root.findall(f'.//mets:fileGrp[@USE="{IMAGE_GROUP_DEFAULT}"]/mets:file', df.XMLNS)
            _n_image_res = len(_image_res)
        if _n_image_res < 1:
            _msg = f"{self.process_identifier} contains absolutly no images for OCR!"
            raise ODEMNoImagesForOCRException(_msg)
        # gather present images via generator
        pairs_img_id = fname_ident_pairs_from_metadata(mets_root, _image_res, blacklist_log, blacklist_lab)
        n_images_ocrable = len(pairs_img_id)
        if n_images_ocrable < 1:
            _msg = f"{self.process_identifier} contains no images for OCR (total: {_n_image_res})!"
            raise ODEMNoImagesForOCRException(_msg)
        # else, journey onwards with image name only
        self.image_pairs = pairs_img_id
        self.n_images_pages = _n_image_res
        self.n_images_ocrable = len(self.image_pairs)


def fname_ident_pairs_from_metadata(mets_root, image_res, blacklist_structs, blacklist_page_labels):
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
    _phys_conts = mets_root.findall('.//mets:structMap[@TYPE="PHYSICAL"]/mets:div/mets:div/mets:fptr', df.XMLNS)
    _structmap_links = mets_root.findall('.//mets:structLink/mets:smLink', df.XMLNS)
    _log_conts = mets_root.findall('.//mets:structMap[@TYPE="LOGICAL"]//mets:div', df.XMLNS)
    for img_cnt in image_res:
        _local_file_name = img_cnt[0].get(Q_XLINK_HREF).split('/')[-1]
        _file_id = img_cnt.get('ID')
        _phys_dict = _phys_container_for_id(_phys_conts, _file_id)
        try:
            log_types = _log_types_for_page(_phys_dict['ID'], _structmap_links, _log_conts)
        except ODEMMetadataMetsException as ome:
            _problems.append(ome.args[0])
        if not is_in(blacklist_structs, log_types):
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


def _log_types_for_page(phys_id, structmap_links, log_conts):
    """Follow link from physical container ('to') 
    via  strucmap link to any corresponding logical 
    structure ('from') and grab it's type

    Alert if no link found => indicates inconsistend data
    """

    _log_linked_types = []
    for _link in structmap_links:
        _physical_target_id = _link.attrib['{http://www.w3.org/1999/xlink}to']
        if _physical_target_id == phys_id:
            for _logical_section in log_conts:
                _logical_section_id = _logical_section.attrib['ID']
                _logical_target_id = _link.attrib['{http://www.w3.org/1999/xlink}from']
                if _logical_section_id == _logical_target_id:
                    _log_linked_types.append(_logical_section.attrib['TYPE'])
    if len(_log_linked_types) == 0:
        raise ODEMMetadataMetsException(f"Page {phys_id} not linked")
    return _log_linked_types


def clear_filegroups(xml_file, removals):
    """Drop existing file group entries
    and unlink them properly like
    * DOWNLOAD (created within common ODEM workflow)
    * THUMBS (created by Share_it)
    * DEFAULT (created by Share_it)
    """

    proc = df.MetsProcessor(xml_file)
    proc.clear_filegroups(black_list=removals)
    proc.write()


def integrate_ocr_file(xml_tree, ocr_files: typing.List) -> int:
    """Enrich given OCR-Files into XML tree

    Returns number of linked files
    """

    n_linked_ocr = 0
    n_passed_ocr = 0
    file_sec = xml_tree.find('.//mets:fileSec', df.XMLNS)
    tag_file_group = f'{{{df.XMLNS["mets"]}}}fileGrp'
    tag_file = f'{{{df.XMLNS["mets"]}}}file'
    tag_flocat = f'{{{df.XMLNS["mets"]}}}FLocat'

    file_grp_fulltext = ET.Element(tag_file_group, USE=odem_c.FILEGROUP_FULLTEXT)
    for ocr_file in ocr_files:
        file_name = df.UNSET_LABEL
        try:
            file_name = Path(ocr_file).stem
            new_id = odem_c.FILEGROUP_FULLTEXT + '_' + file_name
            file_ocr = ET.Element(
                tag_file, MIMETYPE="application/alto+xml", ID=new_id)
            flocat_href = ET.Element(tag_flocat, LOCTYPE="URL")
            flocat_href.set(Q_XLINK_HREF, ocr_file)
            file_ocr.append(flocat_href)
            file_grp_fulltext.append(file_ocr)

            # Referencing / linking the ALTO data as a file pointer in
            # the sequence container of the physical structMap
            # Assignment takes place via the name of the corresponding
            # image (= name ALTO file)
            mproc = df.MetsProcessor(ocr_file)
            ns_map = _sanitize_namespaces(mproc.tree)
            xpr_file_name = '//alto:sourceImageInformation/alto:fileName'
            src_info = mproc.tree.xpath(xpr_file_name, namespaces=ns_map)[0]
            src_info.text = f'{file_name}.jpg'
            page_elements = mproc.tree.xpath('//alto:Page', namespaces=ns_map)
            if len(page_elements) == 0:
                n_passed_ocr += 1
                continue
            first_page_el = page_elements[0]
            first_page_el.attrib['ID'] = f'p{file_name}'
            mproc.write()
            n_linked_ocr += _link_fulltext(new_id, xml_tree)
        except IndexError as idx_exc:
            note = f"{ocr_file}({file_name}):{idx_exc.args[0]}"
            raise ODEMMetadataMetsException(note) from idx_exc

    file_sec.append(file_grp_fulltext)
    return n_linked_ocr, n_passed_ocr


def _sanitize_namespaces(tree):
    ns_map = tree.nsmap
    if None in ns_map and '/alto/' in ns_map[None]:
        mapping = ns_map[None]
        ns_map = {'alto': mapping}
    return ns_map


def _link_fulltext(file_ident, xml_tree):
    file_name = file_ident.split('_')[-1]
    xp_files = f'.//mets:fileGrp[@USE="{odem_c.FILEGROUP_IMG}"]/mets:file'
    file_grp_max_files = xml_tree.findall(xp_files, df.XMLNS)
    for file_grp_max_file in file_grp_max_files:
        _file_link = file_grp_max_file[0].attrib['{http://www.w3.org/1999/xlink}href']
        _file_label = _file_link.split('/')[-1]
        if file_name in _file_label:
            max_file_id = file_grp_max_file.attrib['ID']
            xp_phys = f'//mets:div/mets:fptr[@FILEID="{max_file_id}"]/..'
            parents = xml_tree.xpath(xp_phys, namespaces=df.XMLNS)
            if len(parents) == 1:
                ET.SubElement(parents[0], f"{{{df.XMLNS['mets']}}}fptr", {
                    "FILEID": file_ident})
                # add only once, therefore return
                return 1
    # if not linked, return zero
    return 0


def is_in(tokens: typing.List[str], label):
    """label contained somewhere in a list of tokens?"""

    return any(t in label for t in tokens)


def postprocess_mets(mets_file, odem_config: configparser.ConfigParser):
    """wrap work related to processing METS/MODS
    Must be called *after* new PDF is enriched because
    it clears all DErivans agents but the most recent
    """

    mproc = df.MetsProcessor(mets_file)
    if odem_config.has_option(odem_c.CFG_SEC_METS, odem_c.CFG_SEC_METS_OPT_AGENTS):
        agent_entries = odem_config.get(odem_c.CFG_SEC_METS, odem_c.CFG_SEC_METS_OPT_AGENTS).split(',')
        for agent_entry in agent_entries:
            if '##' in agent_entry:
                agent_parts = agent_entry.split('##')
                agent_name = agent_parts[0]
                agent_note = agent_parts[1]
                mproc.enrich_agent(agent_name, agent_note)
            else:
                mproc.enrich_agent(agent_entry)
    _process_derivans_agents(mproc)
    _clear_provenance_links(mproc)
    mproc.write()


def _process_derivans_agents(mproc):
    # ensure only very recent derivans agent entry exists
    xp_txt_derivans = '//mets:agent[contains(mets:name,"DigitalDerivans")]'
    derivanses = mproc.tree.xpath(xp_txt_derivans, namespaces=df.XMLNS)
    if len(derivanses) < 1:
        # no previous derivans agent can happen
        # for data from other institutions
        # like SLUB or SBB records
        return
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
    old_dvs = mproc.tree.xpath(xp_dv_iif_or_sru, namespaces=df.XMLNS)
    for old_dv in old_dvs:
        parent = old_dv.getparent()
        parent.remove(old_dv)


def validate(mets_file: str, ddb_ignores,
             validate_ddb=False, digi_type='Aa'):
    """Forward METS-schema validation"""

    xml_root = ET.parse(mets_file).getroot()
    try:
        dfv.validate_xml(xml_root)
        if validate_ddb:
            df.ddb_validation(path_mets=mets_file, digi_type=digi_type,
                              ignore_rules=ddb_ignores)
    except dfv.InvalidXMLException as err:
        if len(err.args) > 0 and ('SCHEMASV' in str(err.args[0])):
            raise odem_c.ODEMException(str(err.args[0])) from err
        raise err
    except df.DigiflowDDBException as ddb_err:
        raise odem_c.ODEMException(ddb_err.args[0]) from ddb_err
    return True


def extract_text_content(ocr_files: typing.List) -> str:
    """Extract textual content from ALTO files' String element
    """
    sorted_files = sorted(ocr_files)
    txt_contents = []
    for ocr_file in sorted_files:
        with open(ocr_file, mode='r', encoding='UTF-8') as _ocr_file:
            ocr_root = ET.parse(_ocr_file).getroot()
            ns_map = _sanitize_namespaces(ocr_root)
            all_lines = ocr_root.findall('.//alto:TextLine', ns_map)
            for single_line in all_lines:
                line_strs = [s.attrib['CONTENT'] for s in single_line.findall('.//alto:String', ns_map)]
                txt_contents.append(' '.join(line_strs))
    return txt_contents