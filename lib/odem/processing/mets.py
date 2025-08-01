"""Encapsulate Implementations concerning METS/MODS handling"""

import configparser
import typing

from pathlib import Path

import lxml.etree as ET
import digiflow as df
import digiflow.validate as dfv

import lib.odem.commons as oc


# contains PICA types like
# Aa, AZ, AF ...
PICA_PRINT_MARKS = ['a', 'f', 'F', 'Z', 'B']
TYPE_PRINTS_LOGICAL = ['monograph', 'volume', 'issue', 'additional']
PPN_GVK = 'gvk-ppn'
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
        self.digital_object_identifier = ""
        self._data = input_data
        self._cfg: configparser.ConfigParser = cfg
        self._reader: typing.Optional[df.MetsReader]
        self._report: typing.Optional[df.DmdReport]
        self.image_pairs = []
        self.n_images_pages = 0
        self.n_images_ocrable = 0

    def __set_reader(self):
        if not hasattr(self, "_report") or self._report is None:
            try:
                reader = df.MetsReader(self._data)
                if reader is None:
                    raise ODEMMetadataMetsException("Invalid METS report None")
                self._reader = reader
            except (RuntimeError, df.DigiflowMetadataException) as exc:
                raise ODEMMetadataMetsException(exc) from exc

    def read(self) -> df.MetsReport:
        """Gather knowledge about digital object's.
        First, try to determin what kind of retro-digit
        we are handling by inspecting it's final PICA mark
        actual metadata from METS/MODS
        Stop if data corrupt, ill or bad
        """
        self.__set_reader()
        report = self._reader.report
        if report is None:
            raise ODEMMetadataMetsException("Invalid METS report None")
        if not report.type:
            raise ODEMNoTypeForOCRException(f"{self.process_identifier} found no logical type")
        prime_report = report.prime_report
        if prime_report is None:
            raise ODEMMetadataMetsException("Invalid MODS prime report None")
        self._report = prime_report
        if not self.__is_relevant():
            raise ODEMNoTypeForOCRException(f"{self.process_identifier} not relevant")
        try:
            self._reader.inspect_logical_struct_links()
        except df.DigiflowMetadataException as dfmd_exc:
            raise ODEMMetadataMetsException(dfmd_exc.args[0]) from dfmd_exc
        self.__inspect_metadata_images()
        self.__metadata_export_name()
        return report

    def __is_relevant(self) -> bool:
        """Determine whether this digital object shall be processed by
        inspect PICA type: considered to have length between 2-4 chars
        which  *might* contain trailing chars 'u' or 'v' ('Afu')
        with 2nd char being most important && respect logical 
        DFG-structset type annotation
        """
        prime_type = self.types[1]
        if prime_type is not None and len(prime_type) in range(2, 4) \
            and prime_type[1] not in PICA_PRINT_MARKS:
            no_pica_today = f"{self.process_identifier} no PICA type for OCR: {prime_type}"
            raise ODEMNoTypeForOCRException(no_pica_today)
        mets_type = self.types[0]
        if mets_type is None or mets_type not in TYPE_PRINTS_LOGICAL:
            raise ODEMNoTypeForOCRException(f"{self.process_identifier} unknown: {prime_type}")
        return True

    @property
    def identifiers(self):
        """Get *all* identifiers"""
        if self._report is None:
            raise ODEMMetadataMetsException
        return self._report.identifiers

    def set_metadata_identifier(self):
        """Fix Identifiers-of-Interest
        A) If dedicated identifier xpr exists to calculate it from metadata,
           use this
        B) If ULB default exists, use gvk-ppn
        C) If exactly 1 identifier was found, use this (*should* be URN)
        D) raise Exception
        """

        identifier = None
        # call first to set the reader in place
        if self._report is None or self._report.identifiers is None:
            raise ODEMMetadataMetsException
        ident_map = dict(self._report.identifiers)
        ident_xpr = self._cfg.get(oc.CFG_SEC_METS,
                                  oc.CFG_SEC_METS_OPT_ID_XPR,
                                  fallback=None)
        if ident_xpr is not None:
            idents = self._reader.xpath(ident_xpr)
            if len(idents) != 1:
                the_msg = f"Invalid match {idents} for {ident_xpr} in {self.process_identifier}"
                raise ODEMMetadataMetsException(the_msg)
            tmp_ident = idents[0]
            identifier = tmp_ident
            if ":" in tmp_ident:
                identifier = tmp_ident.replace(":","+")
        if identifier is None and PPN_GVK in ident_map:
            identifier = ident_map[PPN_GVK]
        if identifier is None and len(ident_map) == 1:
            identifier = list(ident_map.values())[0]
        if identifier is None:
            the_msg = f"found no record identifier in {self.process_identifier}"
            raise ODEMMetadataMetsException(the_msg)
        self.digital_object_identifier = identifier
        return identifier

    @property
    def languages(self):
        """Get language information"""
        if self._report is None:
            raise RuntimeError
        return self._report.languages

    @property
    def types(self):
        """Get type information"""
        if self._reader is None:
            raise ODEMMetadataMetsException
        log_type = self._reader.report.type
        if self._report is None:
            raise ODEMMetadataMetsException
        prime_type = self._report.type
        return (log_type, prime_type)

    def __inspect_metadata_images(self):
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
        use_fgroup = self._cfg.get(oc.CFG_SEC_METS, oc.CFG_SEC_METS_FGROUP,
                                   fallback=oc.DEFAULT_FGROUP)
        mets_root = ET.parse(self._data).getroot()
        image_files = mets_root.findall(f'.//mets:fileGrp[@USE="{use_fgroup}"]/mets:file', df.XMLNS)
        n_images = len(image_files)
        if n_images < 1:
            the_msg = f"{self.process_identifier} contains no images!"
            raise ODEMMetadataMetsException(the_msg)
        # gather present images via generator
        use_id = self._cfg.getboolean(oc.CFG_SEC_FLOW, oc.CFG_SEC_FLOW_USE_FILEID,
                                      fallback=False)
        pairs_img_id = fname_ident_pairs_from_metadata(mets_root, image_files, blacklist_log,
                                                       blacklist_lab, use_file_id=use_id)
        n_images_ocrable = len(pairs_img_id)
        if n_images_ocrable < 1:
            the_msg = f"{self.process_identifier} contains no images for OCR (total: {n_images})!"
            raise ODEMNoImagesForOCRException(the_msg)
        # else, journey onwards with image name only
        self.image_pairs = pairs_img_id
        self.n_images_pages = n_images
        self.n_images_ocrable = len(self.image_pairs)

    def __metadata_export_name(self):
        """Evaluate optional export artefact name
        which assumes some kind of URN (OAI, NBN, DOI)
        If present, replace colon by plus sign for
        sake of ULB-ITZ convention
        """
        export_name_xpr = self._cfg.get(oc.CFG_SEC_EXP,
                                  oc.CFG_SEC_EXP_OPT_NAME,
                                  fallback=None)
        if export_name_xpr is not None:
            export_names = self._reader.xpath(export_name_xpr)
            if len(export_names) != 1:
                the_msg = f"Invalid {export_names} for {export_name_xpr} " \
                          f"in {self.process_identifier}"
                raise ODEMMetadataMetsException(the_msg)
            if ":" in export_names[0]:
                export_names[0] = export_names[0].replace(":","+")
            self._cfg.set(oc.CFG_SEC_EXP, oc.CFG_SEC_EXP_OPT_NAME, export_names[0])


def fname_ident_pairs_from_metadata(mets_root, images, blacklist_structs,
                                    blacklist_page_labels, use_file_id):
    """Generate pairs of image label and URN
    that respect defined blacklisted physical
    and logical structures.

    * first, get all required linking groups
    * second, start with file image final part and gather
      from this group all required informations on the way
      from file location => physical container => structMap
      => logical structure
    """
    the_pairs = []
    problems = []
    phys_structs = mets_root.findall('.//mets:structMap[@TYPE="PHYSICAL"]/mets:div/mets:div/mets:fptr', df.XMLNS)
    structmap_links = mets_root.findall('.//mets:structLink/mets:smLink', df.XMLNS)
    log_structs = mets_root.findall('.//mets:structMap[@TYPE="LOGICAL"]//mets:div', df.XMLNS)
    for img_cnt in images:
        file_id = img_cnt.get('ID')
        final_res_name = img_cnt[0].get(Q_XLINK_HREF).split('/')[-1]
        if use_file_id:
            final_res_name = file_id
        if "." not in final_res_name: # sanitize jpg ext
            final_res_name += ".jpg"
        phys_cnt = _phys_container_for_id(phys_structs, file_id)
        try:
            log_types = _log_types_for_page(phys_cnt['ID'], structmap_links, log_structs)
        except ODEMMetadataMetsException as ome:
            problems.append(ome.args[0])
        if not is_in(blacklist_structs, log_types):
            if not is_in(blacklist_page_labels, phys_cnt['LABEL']):
                the_pairs.append((final_res_name, phys_cnt['ID']))
    # re-raise on error
    if len(problems) > 0:
        n_probs = len(problems)
        raise ODEMMetadataMetsException(f"{n_probs}x: {','.join(problems)}")
    return the_pairs


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


def integrate_ocr_file(xml_tree, ocr_files: typing.List):
    """Enrich given OCR-Files
    Reference / link ALTO files as file pointer in METS/MODS
    fileGrp, if final transformed output contains content and a page element 
    Assignment done by name: image file == name ALTO file

    Returns number of linked files
    """

    n_linked_ocr = 0
    n_passed_ocr = 0
    file_sec = xml_tree.find('.//mets:fileSec', df.XMLNS)
    tag_file_group = f'{{{df.XMLNS["mets"]}}}fileGrp'
    tag_file = f'{{{df.XMLNS["mets"]}}}file'
    tag_flocat = f'{{{df.XMLNS["mets"]}}}FLocat'

    file_grp_fulltext = ET.Element(tag_file_group, USE=oc.FILEGROUP_FULLTEXT)
    for ocr_file in ocr_files:
        file_name = df.UNSET_LABEL
        try:
            file_name = Path(ocr_file).stem
            mproc = df.MetsProcessor(ocr_file)
            ns_map = _sanitize_namespaces(mproc.root)
            xpr_file_name = '//alto:sourceImageInformation/alto:fileName'
            src_info = mproc.root.xpath(xpr_file_name, namespaces=ns_map)[0]
            src_info.text = f'{file_name}.jpg'
            page_elements = mproc.root.xpath('//alto:Page', namespaces=ns_map)
            if len(page_elements) == 0:
                n_passed_ocr += 1
                continue

            # only enrich ocr-file if Page present!
            # prevent invalid METS/MODS:
            # fileSec_09]  id:FULLTEXT_00000820
            # (Das Element mets:file muss über sein Attribut ID mit
            # einem mets:fptr-Element im Element mets:structMap[@TYPE='PHYSICAL']
            # über dessen Attribut FILEID referenziert werden.
            new_id = oc.FILEGROUP_FULLTEXT + '_' + file_name
            file_ocr = ET.Element(
                tag_file, MIMETYPE="text/xml", ID=new_id)
            flocat_href = ET.Element(tag_flocat, LOCTYPE="URL")
            flocat_href.set(Q_XLINK_HREF, ocr_file)
            file_ocr.append(flocat_href)
            file_grp_fulltext.append(file_ocr)
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
    xp_files = f'.//mets:fileGrp[@USE="{oc.FILEGROUP_IMG}"]/mets:file'
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
    * optional clear some ULB-DSpace entries which will otherwise lead
      to import artefacts
    * optional enrich ODEM agent
       here use schema <agent-label>##<agent-note> to insert both elements

    Please note:
        If not properly configured, skip executiom
    """

    if odem_config.getboolean(oc.CFG_SEC_METS, oc.CFG_SEC_METS_OPT_CLEAN,
                              fallback=False):
        mproc = df.MetsProcessor(mets_file)
        xp_dv_iif_or_sru = '//dv:links/*[local-name()="iiif" or local-name()="sru"]'
        old_dvs = mproc.root.xpath(xp_dv_iif_or_sru, namespaces=df.XMLNS)
        for old_dv in old_dvs:
            parent = old_dv.getparent()
            parent.remove(old_dv)
        mproc.write()

    if odem_config.has_option(oc.CFG_SEC_METS, oc.CFG_SEC_METS_OPT_AGENTS):
        agent_entries = odem_config.get(oc.CFG_SEC_METS,
                                        oc.CFG_SEC_METS_OPT_AGENTS).split(',')
        if len(agent_entries) > 0:
            mproc = df.MetsProcessor(mets_file)
            for agent_entry in agent_entries:
                if '##' in agent_entry:
                    agent_parts = agent_entry.split('##')
                    agent_name = agent_parts[0]
                    agent_note = agent_parts[1]
                    mproc.enrich_agent(agent_name, agent_note)
                else:
                    mproc.enrich_agent(agent_entry)
            mproc.write()


def process_mets_derivans_agents(mets_file, odem_config: configparser.ConfigParser):
    """Ensure only very recent derivans agent entry exists
    by removing probably existing elder Derivans agent marks

    Plese note:
        Must *only* be called *if* new PDF is enriched because
        it clears all Derivans agenten entries but this latest
    """

    if not odem_config.getboolean(oc.CFG_SEC_METS, oc.CFG_SEC_METS_OPT_CLEAN,
                              fallback=False):
        return
    mproc = df.MetsProcessor(mets_file)
    xp_txt_derivans = '//mets:agent[contains(mets:name,"DigitalDerivans")]'
    derivanses = mproc.root.xpath(xp_txt_derivans, namespaces=df.XMLNS)
    if len(derivanses) < 1:
        # no previous derivans agent can happen
        # for data from other institutions
        # like SLUB or SBB records
        return
    # sort by latest token in agent note ascending
    # note is assumed to be a date
    # like: "PDF FileGroup for PDF_198114125 created at 2022-04-29T12:40:30"
    sorted_ones = sorted(derivanses, key=lambda e: e[1].text.split()[-1])
    sorted_ones.pop()
    drops = 0
    for i, retired_agent in enumerate(sorted_ones):
        the_parent = retired_agent.getparent()
        the_parent.remove(sorted_ones[i])
        drops +=1
    if drops > 0:
        mproc.write()

# def _clear_provenance_links(mproc):
#     xp_dv_iif_or_sru = '//dv:links/*[local-name()="iiif" or local-name()="sru"]'
#     old_dvs = mproc.tree.xpath(xp_dv_iif_or_sru, namespaces=df.XMLNS)
#     for old_dv in old_dvs:
#         parent = old_dv.getparent()
#         parent.remove(old_dv)


def validate_mets(mets_file: str, digi_type, ddb_ignores, ddb_min_level):
    """Forward METS-schema validation"""

    try:
        reporter = dfv.Reporter(mets_file, digi_type=digi_type)
        report: dfv.Report = reporter.get(ignore_ddb_rule_ids=ddb_ignores,
                                          min_ddb_level=ddb_min_level)
        if report.alert(min_ddb_role_label=ddb_min_level):
            xsd_msg = report.xsd_errors if report.xsd_errors else ''
            ddb_msg = report.read()
            raise oc.ODEMException(f"{xsd_msg}{ddb_msg}")
        return True
    except ET.XMLSchemaError as lxml_err:
        msg = f"fail to parse {mets_file}: {lxml_err.args}"
        raise oc.ODEMDataException(msg) from lxml_err
    except df.DigiflowTransformException as df_err:
        msg = f"fail to process {mets_file}: {df_err.args}"
        raise oc.ODEMDataException(msg) from df_err


def extract_text_content(ocr_files: typing.List) -> typing.List:
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
                line_strs = [s.attrib['CONTENT']
                             for s in single_line.findall('.//alto:String', ns_map)]
                txt_contents.append(' '.join(line_strs))
    return txt_contents
