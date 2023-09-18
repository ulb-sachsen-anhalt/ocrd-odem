# -*- coding: utf-8 -*-
"""OCR-Generation for OAI-Records"""

import concurrent.futures
import configparser
import datetime
import logging
import os
import pathlib
import shutil
import socket
import subprocess
import time
from typing import (
    Dict,
    List,
    Optional,
)
import lxml.etree as ET
import numpy as np
from ocrd.resolver import (
    Resolver
)
from ocrd_page_to_alto.convert import (
    OcrdPageAltoConverter
)
from digiflow import (
    OAILoader,
    OAIRecord,
    run_profiled,
    post_oai_extract_metsdata,
    export_data_from,
    write_xml_file,
    MetsProcessor,
    BaseDerivansManager,
    DerivansResult
)
from .processing_mets import (
    IDENTIFIER_CATALOGUE,
    ODEMMetadataInspecteur,
    ODEMMetadataMetsException,
    integrate_ocr_file,
    postprocess_mets,
    validate_mets,
)
from .processing_ocr import (
    postprocess_ocr_file,
)
from .processing_image import (
    is_jpg,
    sanitize_image,
    get_imageinfo,
)

#
# Module constants
#
# python process-wrapper limit
os.environ['OMP_THREAD_LIMIT'] = '1'
XMLNS = {'alto': 'http://www.loc.gov/standards/alto/ns-v4#',
         'dv': 'http://dfg-viewer.de/',
         'mets': 'http://www.loc.gov/METS/',
         'ulb': 'https://bibliothek.uni-halle.de',
         'zvdd': 'https:/zvdd'
         }
FILEGROUP_OCR = 'FULLTEXT'
DROP_ALTO_ELEMENTS = [
    'alto:Shape',
    'alto:Illustration',
    'alto:GraphicalElement']
# default language for fallback
# when processing local images
DEFAULT_LANG = 'ger'

# estimated ocr-d runtime
# for a regular page (A4, 1MB)
DEFAULT_RUNTIME_PAGE = 1.0

# process duration format
LOG_STORE_FORMAT = '%Y-%m-%d_%H-%m-%S'

# how will be allow a single page
# to be processed?
DEFAULT_DOCKER_CONTAINER_TIMEOUT = 600

# recognition level for tesserocr
# must switch otherwise glyphs are reverted
# for each word
RTL_LANGUAGES = ['ara', 'fas', 'heb']

#
# States
#
MARK_OCR_OPEN = 'n.a.'
MARK_OCR_BUSY = 'ocr_busy'
MARK_OCR_FAIL = 'ocr_fail'
MARK_OCR_DONE = 'ocr_done'
MARK_OCR_SKIP = 'ocr_skip'

PROJECT_ROOT = pathlib.Path(__file__).resolve().parents[2]


def get_config():
    """init plain configparser"""

    def _parse_dict(row):
        """
        Custom config converter to create a dictionary represented as string
        lambda s: {e[0]:e[1] for p in s.split(',') for e in zip(*p.strip().split(':'))}
        """
        a_dict = {}
        for pairs in row.split(','):
            pair = pairs.split(':')
            a_dict[pair[0].strip()] = pair[1].strip()
        return a_dict

    return configparser.ConfigParser(
        interpolation=configparser.ExtendedInterpolation(),
        converters={
            'list': lambda s: [e.strip() for e in s.split(',')],
            'dict': _parse_dict
        })


def post_download(the_self, the_data):
    """
    Migration Post-recive OAI METS/MODS callback
    """

    xml_root = ET.fromstring(the_data)

    # extract OAI response body
    mets_tree = post_oai_extract_metsdata(xml_root)

    # exchange MAX image URL
    write_xml_file(mets_tree, the_self.path_mets)


def ocrd_workspace_setup(path_workspace, image_path):
    """Wrap ocrd workspace init and add single file"""

    # init clean workspace
    the_dir = os.path.abspath(path_workspace)
    resolver = Resolver()
    workspace = resolver.workspace_from_nothing(
        directory=the_dir
    )
    workspace.save_mets()

    # add the one image which resides
    # already inside 'MAX' directory
    image_name = os.path.basename(image_path)
    resolver.download_to_directory(
        the_dir,
        image_path,
        subdir='MAX')
    kwargs = {
        'fileGrp': 'MAX',
        'ID': 'MAX_01',
        'mimetype': 'image/png',
        'pageId': 'PHYS_01',
        'url': f"MAX/{image_name}"}
    workspace.mets.add_file(**kwargs)
    workspace.save_mets()
    return image_path


def get_odem_logger(log_dir, logfile_name=None):
    """Create logger using log_dir"""

    _today = time.strftime('%Y-%m-%d', time.localtime())
    _host = socket.gethostname()
    _label = logfile_name if logfile_name is not None else ''
    _logfile_name = os.path.join(
        log_dir, f"odem_{_host}{_label}_{_today}.log")
    conf_logname = {'logname': _logfile_name}
    conf_file_location = os.path.join(
        PROJECT_ROOT, 'resources', 'odem_logging.ini')
    logging.config.fileConfig(conf_file_location, defaults=conf_logname)
    return logging.getLogger('odem')


class ODEMException(Exception):
    """Mark custom ODEM Workflow Exceptions"""


class ODEMProcess:
    """Create OCR for OAI Records.

        Runs both wiht OAIRecord or local path as input.
        process_identifier may represent a local directory
        or the local part of an OAI-URN.

        Languages for ocr-ing are assumed to be enriched in
        OAI-Record-Metadata (MODS) or be part of local
        paths. They will be applied by a custom mapping
        for the underlying OCR-Engine Tesseract-OCR.
    """

    def __init__(self, record: OAIRecord, work_dir, executors=2, log_dir=None, logger=None):
        """Create new ODEM Process.
        Args:
            record (OAIRecord): OAI Record dataset
            work_dir (_type_): required local work path
            executors (int, optional): Process pooling when running parallel. 
                Defaults to 2.
            log_dir (_type_, optional): Path to store log file.
                Defaults to None.
        """

        self.identifiers: Optional[Dict]
        self.record = record
        self.n_executors = executors
        self.work_dir_main = work_dir
        self.digi_type = None
        self.languages = []
        self.local_mode = record is None
        self.process_identifier = None
        if self.local_mode:
            self.process_identifier = os.path.basename(work_dir)
        if record is not None and record.local_identifier is not None:
            self.process_identifier = record.local_identifier
        self.export_dir = None
        self.the_logger = None
        self.cfg = None
        self.store = None
        self.images_4_ocr: List = []  # List[str] | List[Tuple[str, str]]
        self.ocr_files = []
        self._statistics = {'execs': executors}
        self._process_start = time.time()
        if logger is not None:
            self.the_logger = logger
        elif log_dir is not None and os.path.exists(log_dir):
            self._init_logger(log_dir)
        self.mets_file = os.path.join(
            work_dir, os.path.basename(work_dir) + '.xml')

    def _init_logger(self, log_dir):
        today = time.strftime('%Y-%m-%d', time.localtime())
        if not log_dir:
            log_parent = os.path.dirname(os.path.dirname(self.work_dir_main))
            if not os.access(log_parent, os.W_OK):
                raise RuntimeError(f"cant store log files at invalid {log_dir}")
            log_dir = os.path.join(log_parent, 'log')
            os.makedirs(log_dir, exist_ok=True)
        logfile_name = os.path.join(
            log_dir, f"odem_{today}.log")
        conf_logname = {'logname': logfile_name}
        conf_file_location = os.path.join(
            PROJECT_ROOT, 'resources', 'odem_logging.ini')
        logging.config.fileConfig(conf_file_location, defaults=conf_logname)
        self.the_logger = logging.getLogger('odem')

    def load(self):
        """Load Data via OAI-PMH-API very LAZY
        i.e. if not metadata file exists already in
        configured workspace directory"""

        request_identifier = self.record.identifier
        local_identifier = self.record.local_identifier
        req_dst_dir = os.path.join(
            os.path.dirname(self.work_dir_main), local_identifier)
        if not os.path.exists(req_dst_dir):
            os.makedirs(req_dst_dir, exist_ok=True)
        req_dst = os.path.join(req_dst_dir, local_identifier + '.xml')
        self.the_logger.debug("[%s] download %s to %s",
                              self.process_identifier, request_identifier, req_dst)
        base_url = self.cfg.get('global', 'base_url')
        try:
            loader = OAILoader(req_dst_dir, base_url=base_url, post_oai=post_download)
            loader.store = self.store
            loader.load(request_identifier, local_dst=req_dst)
        except RuntimeError as _err:
            raise ODEMException(_err.args[0]) from _err

    def inspect_metadata(self):
        """Inspected record data and try to
        make sense (or go nuts if invalid)

        Invalid means:
        * no print work type (i.e. C-stage, newspaper year)
        * no language
        * missing links between physical and logical structs
          (otherwise viewer navigation and PDF outline
           will be corrupt at this segment)
        * no page images for OCR
        """

        insp = ODEMMetadataInspecteur(self.mets_file,
                                      self.record.identifier,
                                      cfg=self.cfg,
                                      workdir=self.work_dir_main)
        try:
            insp.inspect()
            self.images_4_ocr = insp.image_pairs
        except ODEMMetadataMetsException as mde:
            raise ODEMException(f"{mde.args[0]}") from mde
        self.identifiers = insp.identifiers
        self._statistics[IDENTIFIER_CATALOGUE] = insp.identifiers[IDENTIFIER_CATALOGUE]
        self._statistics['type'] = insp.type
        self._statistics['langs'] = insp.languages
        self._statistics['n_images_pages'] = insp.n_images_pages
        self._statistics['n_images_ocrable'] = insp.n_images_ocrable
        _ratio = insp.n_images_ocrable / insp.n_images_pages * 100
        self.the_logger.info("[%s] %04d (%.2f%%) images used for OCR (total: %04d)",
                        self.process_identifier, insp.n_images_ocrable, _ratio, insp.n_images_pages)
        self.search_model_for(insp.languages)
        self._statistics['host'] = socket.gethostname()

    def search_model_for(self, languages:List[str]):
        """compose tesseract-ocr model config
        from metadata language entries.
        
        Please note: Configured model mappings
        might also contain compositions, therefore
        the additional inner loop
        """

        # disable warning since converter got added
        model_mappings: dict = self.cfg.getdict(  # pylint: disable=no-member
            'ocr', 'model_mapping')
        self.the_logger.info("[%s] inspect languages '%s'",
                             self.process_identifier, languages)
        for lang in languages:
            model_entry = model_mappings.get(lang)
            if not model_entry:
                raise ODEMException(f"'{lang}' mapping not found (languages: {languages})!")
            for model in model_entry.split('+'):
                if self._is_lang_available(model):
                    self.languages.append(model)
                else:
                    raise ODEMException(f"'{model}' model config not found !")
        self.the_logger.info("[%s] map languages '%s' => '%s'",
                             self.process_identifier, languages, self.languages)

    def map_language_to_modelconfig(self, image_path) -> str:
        """Determine Tesseract config from forehead
        processed print metadata or file name suffix
        if run in local mode
        """
        if self.local_mode:
            try:
                _file_lang_suffix = get_modelconf_from(image_path)
            except ODEMException as oxc:
                self.the_logger.warning("[%s] language mapping err '%s' for '%s', fallback to %s",
                                        self.process_identifier, oxc.args[0],
                                        image_path, DEFAULT_LANG)
                _file_lang_suffix = DEFAULT_LANG
            self.search_model_for(_file_lang_suffix)
        return '+'.join(self.languages)

    def _is_lang_available(self, lang) -> bool:
        """Determine whether model is available"""

        tess_host = self.cfg.get('ocr', 'tessdir_host')
        training_file = tess_host + '/' + lang + '.traineddata'
        if os.path.exists(training_file):
            return True
        return False

    def get_local_image_paths(self, image_local_dir=None) -> List[str]:
        """Build dataset from two different scenarios
        (-therefore setting images is divided from filtering):

        A) all images from sub_directory "MAX"
           created by preceding download stage
        B) all images within a local root directory
           i.e., pre-existing evaluation image data
        """

        image_dir = os.path.join(self.work_dir_main, 'MAX')
        if image_local_dir:
            if not os.path.isdir(image_local_dir):
                raise RuntimeError(f"invalid path: {image_local_dir}!")
            image_dir = image_local_dir

        # gather images, propably recursive
        images: List[str] = sorted([
            os.path.join(curr, the_file)
            for curr, _, the_files in os.walk(image_dir)
            for the_file in the_files
            if is_jpg(the_file)
        ])

        # this shouldn't happen
        if len(images) < 1:
            raise ODEMException(f"{self.record.identifier} contains no images!")

        self.the_logger.info("[%s] %d images total",
                             self.process_identifier, len(images))
        return images

    def filter_images(self):
        """Pick only those (local) images which
        match the filtered metadata output so far.
        Please note: that we pass a pair in,
            inspect only the label and pass the
            whole pair out, if file exists
        """
        _images_of_interest = []
        _local_max_dir = os.path.join(self.work_dir_main, 'MAX')
        for _img, _urn in self.images_4_ocr:
            _the_file = os.path.join(_local_max_dir, _img)
            if not os.path.exists(_the_file):
                raise ODEMException(f"[{self.process_identifier}] missing {_the_file}!")
            _images_of_interest.append((_the_file, _urn))
        self.images_4_ocr = _images_of_interest

    def run_parallel(self):
        """Run workflow parallel given poolsize"""

        self.the_logger.info("[%s] %d images run_parallel by %d executors",
                             self.process_identifier, len(self.images_4_ocr), self.n_executors)
        try:
            with concurrent.futures.ThreadPoolExecutor(
                    max_workers=self.n_executors,
                    thread_name_prefix='odem'
            ) as executor:
                outcomes = list(executor.map(self.create_single_ocr, self.images_4_ocr))
                # self._calculate_statistics(outcomes)
                return outcomes
        except (OSError, AttributeError) as err:
            self.the_logger.error(err)
            raise RuntimeError(f"OCR-D parallel: {err.args[0]}") from err

    def run_sequential(self):
        """run complete workflow plain sequential
        For debugging or small machines
        """

        _len_img = len(self.images_4_ocr)
        _estm_min = _len_img * DEFAULT_RUNTIME_PAGE
        self.the_logger.info("[%s] %d images run_sequential, estm. %dmin",
                             self.process_identifier, _len_img, _estm_min)
        try:
            outcomes = [self.create_single_ocr(_img)
                        for _img in self.images_4_ocr]
            # self._calculate_statistics(outcomes)
            return outcomes
        except (OSError, AttributeError) as err:
            self.the_logger.error(err)
            raise RuntimeError(f"OCR-D sequential: {err.args[0]}") from err

    def calculate_statistics(self, outcomes):
        n_ocr = sum([e[0] for e in outcomes if e[0] == 1])
        _total_mps = [round(e[2], 1) for e in outcomes if e[0] == 1]
        _mod_val_counts = np.unique(_total_mps, return_counts=True)
        mps = list(zip(*_mod_val_counts))
        total_mb = sum([e[3] for e in outcomes if e[0] == 1])
        self._statistics['n_ocr'] = n_ocr
        self._statistics['mb'] = round(total_mb, 2)
        self._statistics['mps'] = mps

    @run_profiled
    def run_ocr_page(self, *args):
        """wrap ocr container process
        *Please note*
        Trailing dot (".") is cruical, since it means "this directory"
        and is mandatory since 2022 again
        """

        ocr_dir = args[0]
        model = args[1]
        base_image = args[2]
        makefile = args[3]
        tess_host = args[4]
        tess_cntn = args[5]
        tess_level = args[6]
        docker_container_memory_limit: str = args[7]
        docker_container_timeout: int = args[8]
        os.chdir(ocr_dir)
        user_id = os.getuid()
        container_name: str = f'{self.process_identifier}_{os.path.basename(ocr_dir)}'
        if self.local_mode:
            container_name = os.path.basename(ocr_dir)
        # replace not allowed chars
        container_name = container_name.replace('+', '-')
        cmd: str = f"docker run --rm -u {user_id}"
        cmd += f" --name {container_name}"
        if docker_container_memory_limit is not None:
            cmd += f" --memory {docker_container_memory_limit}"
            cmd += f" --memory-swap {docker_container_memory_limit}"  # disables swap, because of same value
        cmd += f" -w /data -v {ocr_dir}:/data"
        cmd += f" -v {tess_host}:{tess_cntn} {base_image}"
        cmd += f" ocrd-make TESSERACT_CONFIG={model} TESSERACT_LEVEL={tess_level} -f {makefile} . "
        self.the_logger.info("[%s] run ocrd/all with '%s'",
                             self.process_identifier, cmd)
        subprocess.run(cmd, shell=True, check=True, timeout=docker_container_timeout)

    @staticmethod
    def get_recognition_level(model_config:str) -> str:
        """Determine tesseract recognition level
        with respect to language order by model
        configuration"""

        if any((m for m in model_config.split('+') if m in RTL_LANGUAGES)):
            return 'glyph'
        return 'word'

    def create_single_ocr(self, image_4_ocr):
        """Create OCR Data"""

        ocr_log_conf = os.path.join(
            PROJECT_ROOT, self.cfg.get('ocr', 'ocrd_logging'))
        ocr_makefile = os.path.join(
            PROJECT_ROOT, self.cfg.get('ocr', 'ocrd_makefile'))

        # Preprare workspace with makefile
        (image_path, ident) = image_4_ocr
        os.chdir(self.work_dir_main)
        file_name = os.path.basename(image_path)
        file_id = file_name.split('.')[0]
        page_workdir = os.path.join(self.work_dir_main, file_id)

        if os.path.exists(page_workdir):
            shutil.rmtree(page_workdir, ignore_errors=True)
        os.mkdir(page_workdir)

        shutil.copy(ocr_log_conf, page_workdir)
        shutil.copy(ocr_makefile, page_workdir)
        os.chdir(page_workdir)

        # move and convert image data at once
        processed_image_path = sanitize_image(image_path, page_workdir)

        # init ocr-d workspace
        ocrd_workspace_setup(page_workdir, processed_image_path)

        # find out the needed model config for tesseract
        model_config = self.map_language_to_modelconfig(image_path)
        self.the_logger.info("[%s] use '%s' for '%s'",
                             self.process_identifier, model_config, self.languages)

        # if one of the languages is RTL use glyph level for ocr
        ocr_level = ODEMProcess.get_recognition_level(model_config)

        stored = 0
        mps = 0
        filesize_mb = 0
        # use the original image rather than
        # transformed one since the PNG is
        # usually 2-5 times larger than JPG
        filestat = os.stat(image_path)
        if filestat:
            filesize_mb = filestat.st_size / 1048576
        (mps, dpi) = get_imageinfo(image_path)

        # how to identify data set?
        if self.record:
            _ident = self.process_identifier
        else:
            _ident = os.path.basename(self.work_dir_main)
        # OCR Generation
        profiling = ('n.a.', 0)
        base_image = self.cfg.get('ocr', 'ocrd_baseimage')
        makefile = self.cfg.get('ocr', 'ocrd_makefile').split('/')[-1]
        tess_host = self.cfg.get('ocr', 'tessdir_host')
        tess_cntn = self.cfg.get('ocr', 'tessdir_cntr')
        docker_container_memory_limit: str = self.cfg.get('ocr', 'docker_container_memory_limit', fallback=None)
        docker_container_timeout: str = self.cfg.getint(
            'ocr',
            'docker_container_timeout',
            fallback=DEFAULT_DOCKER_CONTAINER_TIMEOUT
        )
        try:
            profiling = self.run_ocr_page(
                page_workdir,
                model_config,
                base_image,
                makefile,
                tess_host,
                tess_cntn,
                ocr_level,
                docker_container_memory_limit,
                docker_container_timeout,
            )
            # will be unset in case of magic mocking for test
            if profiling:
                self.the_logger.info("[%s] '%s' in %s (%.1fMP, %dDPI, %.1fMB)",
                                     _ident, profiling[1], profiling[0], mps, dpi, filesize_mb)
            self.the_logger.info("[%s] run ocr creation in '%s'",
                                 _ident, page_workdir)
            stored = self._store_fulltext(page_workdir, image_path)
            if stored:
                self._preserve_log(page_workdir, ident)
        except (subprocess.CalledProcessError, subprocess.TimeoutExpired) as exc:
            self.the_logger.error("[%s] image '%s' failed due to subprocess timeout: %s",
                                  _ident, base_image, exc)
        except Exception as plain_exc:
            self.the_logger.error("[%s] generic exc '%s' for image '%s'",
                                  _ident, plain_exc, base_image)

        os.chdir(self.work_dir_main)
        if self.cfg.getboolean('ocr', 'keep_temp_orcd_data', fallback=False) is False:
            shutil.rmtree(page_workdir, ignore_errors=True)
        return stored, 1, mps, filesize_mb

    def _preserve_log(self, work_subdir, image_ident):
        """preserve ocrd.log for later analyzis as
        sub directory identified by adopted local
        identifier (local section of system OAI handle)"""

        _root_log = self.cfg.get('global', 'local_log_dir')
        _local_ident = self.process_identifier.replace('/', '_')
        _local_ocr_log = os.path.join(_root_log, _local_ident)
        if not os.path.exists(_local_ocr_log):
            os.makedirs(_local_ocr_log, exist_ok=True)

        _org_log = os.path.join(work_subdir, 'ocrd.log')
        if os.path.exists(_org_log):
            _ts = time.strftime(LOG_STORE_FORMAT, time.localtime())
            _log_label = f'ocrd_odem_{self.process_identifier}_{image_ident}_{_ts}.log'
            _rebranded = os.path.join(work_subdir, _log_label)
            os.rename(_org_log, _rebranded)
            shutil.copy(_rebranded, _local_ocr_log)
        else:
            self.the_logger.warning("[%s] No ocrd.log in %s",
                                    self.process_identifier, work_subdir)

    def _store_fulltext(self, image_subdir, original_image_path) -> int:
        """Move OCR Result from Workspace Subdir to export folder if exists"""

        # inspect possible ocr result dirs from within
        # the OCR-D subordinate workspaces for each image
        old_id = os.path.basename(image_subdir)
        ocr_result_dir = os.path.join(image_subdir, 'PAGE')
        if not os.path.isdir(ocr_result_dir):
            self.the_logger.info("[%s] no ocr results for '%s'",
                                 self.process_identifier, ocr_result_dir)
            return 0
        ocrs = [os.path.join(ocr_result_dir, ocr)
                for ocr in os.listdir(ocr_result_dir)
                if str(ocr).endswith('.xml')]
        self.the_logger.debug("[%s] %s ocr files",
                              self.process_identifier, ocrs)
        if ocrs and len(ocrs) == 1:

            # propably need to rename
            # since file now is like 'PAGE_01.xml'
            renamed = os.path.join(ocr_result_dir, old_id + '.xml')
            os.rename(ocrs[0], renamed)

            # regular case: OAI Workflow
            if not self.local_mode:
                # export to 'PAGE' dir
                wd_fulltext = os.path.join(self.work_dir_main, 'PAGE')
                if not os.path.exists(wd_fulltext):
                    os.mkdir(wd_fulltext)

            # special case: local runnings for straight evaluations
            else:
                wd_fulltext = os.path.dirname(original_image_path)

            # final storage
            target_path = os.path.join(wd_fulltext, old_id + '.xml')
            shutil.copy(renamed, target_path)
        return 1

    def has_already_any_ocr(self) -> bool:
        """
        Forward OCR data to store management
        * if FULLTEXT dir not in store, return False
        * if FULLTEXT dir doesn't contain any *.xml files, return False
        *No* check whether all required pages contain ocr data!
        """

        ocr_dir = os.path.join(self.work_dir_main, FILEGROUP_OCR)
        if self.store is None:
            return False
        store_dir_ocr = self.store.get(ocr_dir)
        if store_dir_ocr is None:
            return False

        ocr_dir = os.path.join(self.work_dir_main, 'FULLTEXT')
        ocr_files = [o for o in os.listdir(ocr_dir) if str(o).endswith('.xml')]
        if len(ocr_files) > 0:
            return True

        return False

    def to_alto(self) -> int:
        """Forward OCR data storage to data management
        feat. check for results"""

        ocr_dir = os.path.join(self.work_dir_main, 'PAGE')
        page_files = [
            os.path.join(curr_dir, page_file)
            for curr_dir, _, files in os.walk(ocr_dir)
            for page_file in files
            if str(page_file).endswith('.xml')
        ]
        n_candidates = len(self.images_4_ocr)
        if len(page_files) == 0 and n_candidates > 0:
            raise ODEMException(f"No OCR result for {n_candidates} candidates created!")

        # check output path
        alto_dir = os.path.join(self.work_dir_main, 'FULLTEXT')
        if not os.path.isdir(alto_dir):
            os.makedirs(alto_dir, exist_ok=True)

        for page_file in page_files:
            the_id = os.path.basename(page_file)
            output_file = os.path.join(alto_dir, the_id)
            converter = OcrdPageAltoConverter(page_filename=page_file).convert()
            with open(output_file, 'w', encoding='utf-8') as output:
                output.write(str(converter))
            self.ocr_files.append(output_file)
            self.the_logger.info("[%s] page-to-alto '%s'",
                                 self.process_identifier, output_file)

    def integrate_ocr(self):
        """Prepare and link OCR-data"""

        if not self.ocr_files:
            return 0
        _n_linked_ocr = 0

        proc = MetsProcessor(self.mets_file)
        if self.cfg:
            blacklisted = self.cfg.getlist('mets', 'blacklist_file_groups')
            proc.clear_filegroups(black_list=blacklisted)
        _n_linked_ocr = integrate_ocr_file(proc.tree, self.ocr_files)
        proc.write()
        return _n_linked_ocr

    def postprocess_ocr(self):
        """Apply additional postprocessing to OCR data"""

        # inspect each single created ocr file
        # drop unwanted elements
        # clear punctual regions
        strip_tags = self.cfg.getlist('ocr', 'strip_tags')
        for _ocr_file in self.ocr_files:
            postprocess_ocr_file(_ocr_file, strip_tags)

    def create_text_bundle_data(self):
        """create additional dspace bundle for indexing ocr text
        read ocr-file sequential according to their number label
        and extract every row into additional text file"""

        _ocrs = sorted(self.ocr_files)
        _txts = []
        for _o in _ocrs:
            with open(_o, mode='r', encoding='UTF-8') as _ocr_file:
                _alto_root = ET.parse(_ocr_file)
                _lines = _alto_root.findall('.//alto:TextLine', XMLNS)
                for _l in _lines:
                    _l_strs = [s.attrib['CONTENT'] for s in _l.findall('.//alto:String', XMLNS)]
                    _txts.append(' '.join(_l_strs))
        txt_content = '\n'.join(_txts)
        _out_path = os.path.join(self.work_dir_main, f'{self.identifiers[IDENTIFIER_CATALOGUE]}.pdf.txt')
        with open(_out_path, mode='w', encoding='UTF-8') as _writer:
            _writer.write(txt_content)
        self.the_logger.info("[%s] harvested %d lines from %d ocr files to %s",
                             self.process_identifier, len(_txts), len(_ocrs), _out_path)
        self._statistics['n_text_lines'] = len(_txts)

    def create_pdf(self):
        """Forward PDF-creation to Derivans"""

        _cfg_path_dir_bin = self.cfg.get('derivans', 'derivans_dir_bin', fallback=None)
        path_bin = None
        if _cfg_path_dir_bin is not None:
            path_bin = os.path.join(PROJECT_ROOT, _cfg_path_dir_bin)
        _cfg_path_dir_project = self.cfg.get('derivans', 'derivans_dir_project', fallback=None)
        path_prj = None
        if _cfg_path_dir_project is not None:
            path_prj = os.path.join(PROJECT_ROOT, _cfg_path_dir_project)
        path_cfg = os.path.join(
            PROJECT_ROOT,
            self.cfg.get('derivans', 'derivans_config')
        )
        derivans_image = self.cfg.get('derivans', 'derivans_image', fallback=None)
        derivans: BaseDerivansManager = BaseDerivansManager.create(
            self.mets_file,
            container_image_name=derivans_image,
            path_binary=path_bin,
            path_configuration=path_cfg,
            path_mvn_project=path_prj
        )
        derivans.init()
        # be cautious
        try:
            dresult:DerivansResult = derivans.start()
            self.the_logger.info("[%s] create derivates in %s",
                                 self.process_identifier, dresult.duration)
        except subprocess.CalledProcessError as _sub_err:
            _err_msg = _sub_err.stdout.decode().split(os.linesep)[0].replace("'", "\"")
            _args = [_err_msg]
            _args.extend(_sub_err.args)
            raise ODEMException(_args) from _sub_err

    def delete_before_export(self, folders):
        """delete folders given by list"""

        work = self.work_dir_main
        self.the_logger.info(
            "[%s] delete folders: %s", self.process_identifier, folders)
        for folder in folders:
            delete_me = os.path.join(work, folder)
            if os.path.exists(delete_me):
                self.the_logger.info(
                    "[%s] delete folder: %s", self.process_identifier, delete_me)
                shutil.rmtree(delete_me)

    def postprocess_mets(self):
        """wrap work related to processing METS/MODS"""

        postprocess_mets(self.mets_file, self.cfg.get('ocr', 'ocrd_baseimage'))
 
    def validate_mets(self):
        """Forward METS-schema validation"""

        validate_mets(self.mets_file)

    def export_data(self):
        """re-do metadata and transform into output format"""

        exp_dst = self.cfg.get('global', 'local_export_dir')
        exp_tmp = self.cfg.get('global', 'local_export_tmp')
        exp_col = self.cfg.get('global', 'export_collection')
        exp_map = self.cfg.getdict('global', 'export_mappings')
        # overwrite default mapping *.xml => 'mets.xml'
        # since we will have currently many more XML-files
        # created due OCR and do more specific mapping, though
        exp_map = {k: v for k, v in exp_map.items() if v != 'mets.xml'}
        exp_map[os.path.basename(self.mets_file)] = 'mets.xml'
        saf_name = self.identifiers[IDENTIFIER_CATALOGUE]
        export_result = export_data_from(self.mets_file, exp_col,
                                         saf_final_name=saf_name, export_dst=exp_dst,
                                         export_map=exp_map, tmp_saf_dir=exp_tmp)
        self.the_logger.info("[%s] exported data: %s",
                             self.process_identifier, export_result)
        if export_result:
            pth, size = export_result
            self.the_logger.info("[%s] create %s (%s)",
                                 self.process_identifier, pth, size)
            # final re-move at export destination
            if '.processing' in str(pth):
                final_path = pth.replace('.processing', '')
                self.the_logger.debug('[%s] rename %s to %s',
                                      self.process_identifier, pth, final_path)
                shutil.move(pth, final_path)

    @property
    def duration(self):
        """Get current duration of ODEMProcess.
        Most likely at the final end to get an idea
        how much the whole process takes."""

        return datetime.timedelta(seconds=round(time.time() - self._process_start))

    @property
    def statistics(self):
        """Get some statistics as dictionary
        with execution duration updated each call by
        requesting it's string representation"""

        self._statistics['timedelta'] = f'{self.duration}'
        return self._statistics


def get_modelconf_from(file_path) -> List[str]:
    """Determine model from file name extension
    marked with'_' at the end"""

    file_name = os.path.basename(file_path)
    if '_' not in file_name:
        raise ODEMException(f"Miss language in '{file_path}'!")
    return file_name.split('.')[0].split('_')[-1].split('+')
