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
import string
import subprocess
import time
import unicodedata
from math import (
    ceil
)
from typing import (
    List
)

import lxml.etree as ET
import numpy as np
from PIL import (
    Image
)
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
    validate_xml,
    MetsProcessor,
    MetsReader,
    BaseDerivansManager,
    DerivansResult
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
Q_XLINK_HREF = '{http://www.w3.org/1999/xlink}href'
FILEGROUP_OCR = 'FULLTEXT'
FILEGROUP_IMG = 'MAX'
IDENTIFIER_CATALOGUE = 'gvk-ppn'
DROP_ALTO_ELEMENTS = [
    'alto:Shape',
    'alto:Illustration',
    'alto:GraphicalElement']
METS_AGENT_ODEM = 'DFG-OCRD3-ODEM'
EXT_JPG = '.jpg'
EXT_PNG = '.png'
# default language for fallback
# when processing local images
DEFAULT_LANG = 'ger'

# default resolution if not provided
# for both dimensions
DEFAULT_DPI = (300, 300)

# estimated ocr-d runtime
# for a regular page (A4, 1MB)
DEFAULT_RUNTIME_PAGE = 1.0

# process duration format
LOG_STORE_FORMAT = '%Y-%m-%d_%H-%m-%S'

# how will be allow a single page
# to be processed?
DEFAULT_DOCKER_CONTAINER_TIMEOUT = 600

# diacritica to take care of
COMBINING_SMALL_E = '\u0364'
# we want all words to be at least 2 chars
MINIMUM_WORD_LEN = 2
# punctuations to take into account
# includes
#   * regular ASCII-punctuations
#   * Dashes        \u2012-2017
#   * Quotations    \u2018-201F
PUNCTUATIONS = string.punctuation + '\u2012' + '\u2013' + '\u2014' + '\u2015' + '\u2016' + '\u2017' + '\u2018' + '\u2019' + '\u201A' + '\u201B' + '\u201C' + '\u201D' + '\u201E' + '\u201F'

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


def is_in(tokens: List[str], label):
    """label contained somewhere in a list of tokens?"""

    return any(t in label for t in tokens)


def get_imageinfo(path_img_dir):
    """Calculate image features"""

    mps = 0
    dpi = 0
    if os.path.exists(path_img_dir):
        imag = Image.open(path_img_dir)
        (width, height) = imag.size
        mps = (width * height) / 1000000
        if 'dpi' in imag.info:
            dpi = imag.info['dpi'][0]
    return mps, dpi


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

        self.identifiers = None
        self.record = record
        self.n_executors = executors
        self.work_dir_main = work_dir
        self.digi_type = None
        self.languages = None
        self.local_mode = record is None
        self.identifier = None
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

    def evaluate_record_data(self):
        """Inspected loaded data and try to
        make some sense or go nuts if invalid
        record data detected"""

        try:
            reader = MetsReader(self.mets_file)
            report = reader.analyze()
            # share report results with self
            self.languages = report.languages
            self.digi_type = report.type
            self.identifiers = report.identifiers
            # apply some metadata checks forehand
            # stop if data corrupted, ill or bad
            reader.check()
        except RuntimeError as _err:
            raise ODEMException(_err.args[0]) from _err

        if IDENTIFIER_CATALOGUE not in report.identifiers:
            raise ODEMException(f"No {IDENTIFIER_CATALOGUE} in {self.record.identifier}")

        # keep track of info we know by now
        self._statistics[IDENTIFIER_CATALOGUE] = self.identifiers[IDENTIFIER_CATALOGUE]
        self._statistics['type'] = self.digi_type
        self._statistics['host'] = socket.gethostname()
        self._statistics['langs'] = self.languages

    def set_images_from_directory(self, image_local_dir=None):
        """Build dataset from two different scenarios
        (-therefore setting images is divided from filtering):

        A) all images from sub_directory "MAX"
           created by preceding download stage
        B) all images within a local root directory
           i.e., pre-existing evaluation image data
        """

        def is_jpg(f):
            return str(f).endswith("jpg") or str(f).endswith("jpeg")

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
        self._statistics['n_images_total'] = len(images)
        self._statistics['n_images_ocrable'] = 0
        self.images_4_ocr = images

    def filter_images(self):
        """
        Reduce the amount of Images passed to OCR.

        Only applied to Imagedata belonging to 
        proper OAI-Records, not single local dirs.

        Drop images which belong to a certain useless
        * physical containers (like "Colorchecker")
        * logical structures (like "cover_front")
        """

        blacklist_log = self.cfg.getlist('mets', 'blacklist_logical_containers')
        blacklist_lab = self.cfg.getlist('mets', 'blacklist_physical_container_labels')

        # gather images via generator
        img_for_ocr_generator = images_4_ocr(self.mets_file, self.images_4_ocr, blacklist_log, blacklist_lab)
        images_2_care_4 = [i for i in img_for_ocr_generator]
        n_img = len(self.images_4_ocr)
        n_use = len(images_2_care_4)
        _ratio = n_use / n_img * 100
        self.the_logger.info("[%s] %04d (%.2f%%) images used for OCR (total: %04d)",
                             self.process_identifier, n_use, _ratio, n_img)
        if len(images_2_care_4) > 0:
            self.images_4_ocr = images_2_care_4
            self._statistics['n_images_ocrable'] = len(self.images_4_ocr)

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
                self._calculate_statistics(outcomes)
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
            self._calculate_statistics(outcomes)
        except (OSError, AttributeError) as err:
            self.the_logger.error(err)
            raise RuntimeError(f"OCR-D sequential: {err.args[0]}") from err

    def _calculate_statistics(self, outcomes):
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
        """wrap ocr container
        *please note*
        The trailing dot (".") is cruical, since it means "this directory"
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

    def get_model_config(self, image_path) -> str:
        """Determine Tesseract config from
        model mapping or file name suffix
        """

        # disable warning since converter got added
        model_mappings: dict = self.cfg.getdict(  # pylint: disable=no-member
            'ocr', 'model_mapping')

        if self.local_mode:
            try:
                _file_lang_suffix = get_modelconf_from(image_path)
            except ODEMException as oxc:
                self.the_logger.warning("[%s] language mapping err '%s', fallback to %s",
                                        self.process_identifier, oxc.args[0], DEFAULT_LANG)
                _file_lang_suffix = DEFAULT_LANG
            self.languages = _file_lang_suffix.split('+')

        # compose tesseract model config
        _languages = []
        for lang in self.languages:
            models = model_mappings.get(lang)
            if not models:
                raise ODEMException(f"'{lang}' not found in language mappings!")
            # modelmappings *might* also contain composition
            _sub_langs = models.split('+')
            for _lang in _sub_langs:
                if self.is_lang_available(_lang):
                    _languages.append(_lang)
                else:
                    raise ODEMException(f"Cant find language {_lang} file")

        return '+'.join(_languages)

    def is_lang_available(self, lang) -> bool:
        """Determine whether model is available"""

        tess_host = self.cfg.get('ocr', 'tessdir_host')
        training_file = tess_host + '/' + lang + '.traineddata'
        if os.path.exists(training_file):
            return True
        return False

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
        (image_path, urn) = image_4_ocr
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
        processed_image_path = self._preprocess_image(image_path, page_workdir)

        # init ocr-d workspace
        ocrd_workspace_setup(page_workdir, processed_image_path)

        # find out the needed model config for tesseract
        model_config = self.get_model_config(image_path)
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
                self._preserve_process_log(page_workdir, urn)
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

    def _preprocess_image(self, image_file_path, work_dir_sub):
        """Preprocess image data
        * store from source dir into future OCR-D workspace
        * convert into PNG format
        """

        # sanitize file extension
        if not str(image_file_path).endswith(EXT_JPG):
            image_file_path = f"{image_file_path}{EXT_JPG}"

        input_image = Image.open(image_file_path)
        file_name = os.path.basename(image_file_path)
        # store image one level inside the workspace
        image_max_dir = os.path.join(work_dir_sub, 'MAX')
        if not os.path.isdir(image_max_dir):
            os.mkdir(image_max_dir)
        output_path = os.path.join(image_max_dir, file_name).replace(EXT_JPG, EXT_PNG)
        res_dpi = DEFAULT_DPI
        if 'dpi' in input_image.info:
            res_dpi = input_image.info['dpi']
        # store resolution for PNG image in both x,y dimensions
        input_image.save(output_path, format='png', dpi=res_dpi)
        return output_path

    def _preserve_process_log(self, work_subdir, urn):
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
            _rebranded = os.path.join(work_subdir, f'ocrd_odem_{urn}_{_ts}.log')
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
        xml_tree = proc.tree
        file_sec = xml_tree.find('.//mets:fileSec', XMLNS)
        tag_file_group = f'{{{XMLNS["mets"]}}}fileGrp'
        tag_file = f'{{{XMLNS["mets"]}}}file'
        tag_flocat = f'{{{XMLNS["mets"]}}}FLocat'

        file_grp_fulltext = ET.Element(tag_file_group, USE=FILEGROUP_OCR)
        for _ocr_file in self.ocr_files:
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
            _n_linked_ocr += self._link_fulltext(new_id, xml_tree)

        file_sec.append(file_grp_fulltext)
        proc.write()
        return _n_linked_ocr

    def _link_fulltext(self, file_ident, xml_tree):
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
        self._statistics['lines'] = len(_txts)

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

        mproc = MetsProcessor(self.mets_file)
        self._process_agents(mproc)
        self._clear_provenance_links(mproc)
        mproc.write()

    def _process_agents(self, mproc):
        # drop existing ODEM marks
        # enrich *only* latest run
        xp_txt_odem = f'//mets:agent[contains(mets:name,"{METS_AGENT_ODEM}")]'
        agents_odem = mproc.tree.xpath(xp_txt_odem, namespaces=XMLNS)
        for old_odem in agents_odem:
            parent = old_odem.getparent()
            parent.remove(old_odem)
        _cnt_img = self.cfg.get('ocr', 'ocrd_baseimage')
        mproc.enrich_agent(f"{METS_AGENT_ODEM}_{_cnt_img}")

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

    def _clear_provenance_links(self, mproc):
        xp_dv_iif_or_sru = '//dv:links/*[local-name()="iiif" or local-name()="sru"]'
        old_dvs = mproc.tree.xpath(xp_dv_iif_or_sru, namespaces=XMLNS)
        for old_dv in old_dvs:
            parent = old_dv.getparent()
            parent.remove(old_dv)

    def validate_mets(self):
        """Forward METS-schema validation"""

        xml_root = ET.parse(self.mets_file).getroot()
        validate_xml(xml_root)

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


def images_4_ocr(mets_path, image_paths: List[str], blacklist_structs, blacklist_page_labels):
    """Generate images that comply to defined rules
    for blacklisted physical and logical structures
    * first, parse METS and get all required linking groups
    * second, start with file image final part and gather
      from this group all required informations on the way
      from file location => physical container => structMap
      => logical structure
    """

    mets_root = ET.parse(mets_path).getroot()
    _max_images = mets_root.findall('.//mets:fileGrp[@USE="MAX"]/mets:file', XMLNS)
    _phys_conts = mets_root.findall('.//mets:structMap[@TYPE="PHYSICAL"]/mets:div/mets:div/mets:fptr', XMLNS)
    _structmap_links = mets_root.findall('.//mets:structLink/mets:smLink', XMLNS)
    _log_conts = mets_root.findall('.//mets:structMap[@TYPE="LOGICAL"]//mets:div', XMLNS)
    for _image_path in image_paths:
        _image_id = os.path.basename(_image_path).split('.')[0]
        _file_id = _id_for_image(_max_images, _image_id)
        _phys_dict = _phys_container_for_id(_phys_conts, _file_id)
        log_type = _log_type_for_id(_phys_dict['ID'], _structmap_links, _log_conts)
        if not is_in(blacklist_structs, log_type):
            if not is_in(blacklist_page_labels, _phys_dict['LABEL']):
                yield _image_path, _phys_dict['URN']


def _id_for_image(_image_group, image_id):
    """Get proper linking ID from file to 
    physical container"""

    for _image in _image_group:
        _loc = _image.find('mets:FLocat', XMLNS)
        _file_link = _loc.attrib[Q_XLINK_HREF]
        if image_id in _file_link:
            return _loc.getparent().attrib['ID']


def _phys_container_for_id(_phys_conts, _id):
    """Collect and prepare all required 
    data from matching physical container 
    for later analyzis or processing"""

    for _cnt in _phys_conts:
        _file_id = _cnt.attrib['FILEID']
        if _file_id == _id:
            parent = _cnt.getparent()
            _cnt_id = parent.attrib['ID']
            # mask ":" with "+" since first
            # is not welcome in local filenames
            _cnt_urn = parent.attrib['CONTENTIDS'].replace(':', '+')
            _label = None
            if 'LABEL' in parent.attrib:
                _label = parent.attrib['LABEL']
            elif 'ORDERLABEL' in parent.attrib:
                _label = parent.attrib['ORDERLABEL']
            else:
                raise ODEMException(f"Cant handle label: {_label} of '{parent}'")
            return {'ID': _cnt_id, 'URN': _cnt_urn, 'LABEL': _label}


def _log_type_for_id(phys_id, structmap_links, log_conts):
    """Follow link from physical container via 
    strucmap link to the corresponding logical 
    structure and grab it's type"""

    for _link in structmap_links:
        _from_id = _link.attrib['{http://www.w3.org/1999/xlink}from']
        _to_id = _link.attrib['{http://www.w3.org/1999/xlink}to']
        if _to_id == phys_id:
            for _log in log_conts:
                _log_id = _log.attrib['ID']
                if _log_id == _from_id:
                    return _log.attrib['TYPE']


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
    mproc = MetsProcessor(str(ocr_file))
    if strip_tags:
        mproc.remove(strip_tags)

    # inspect transformation artifacts
    _all_text_blocks = mproc.tree.xpath('//alto:TextBlock', namespaces=XMLNS)
    for _block in _all_text_blocks:
        if 'IDNEXT' in _block.attrib:
            del _block.attrib['IDNEXT']

    # inspect textual content
    # _all_strings = mproc.tree.xpath('//alto:String', namespaces=XMLNS)
    _all_strings = mproc.tree.findall('.//alto:String', XMLNS)
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
            except ODEMException as oexc:
                raise ODEMException(f"ocr postproc: {oexc.args[0]} from {ocr_file}!") from oexc
        if len(_content) < MINIMUM_WORD_LEN:
            # too few content, remove element bottom-up
            _uplete(_string_el, _string_el.getparent())
    mproc.write()


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


# define propably difficult characters
# very common separator '⸗'
DOUBLE_OBLIQUE_HYPHEN = '\u2E17'
# rare Geviertstrich '—'
EM_DASH = '\u2014'
ODEM_PUNCTUATIONS = string.punctuation + EM_DASH + DOUBLE_OBLIQUE_HYPHEN


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
        _w_per_char = ceil(_width / len(_content))

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


# create module-wide translator
PUNCT_TRANSLATOR = str.maketrans('', '', ODEM_PUNCTUATIONS)


def _is_completely_punctuated(a_string):
    """Check if only punctuations are contained
    but nothing else"""

    return len(a_string.translate(PUNCT_TRANSLATOR)) == 0


def _uplete(curr_el: ET._Element, parent: ET._Element):
    """delete empty elements up-the-tree"""

    parent.remove(curr_el)
    _content_childs = [kid
                       for kid in parent.getchildren()
                       if kid is not None and 'SP' not in kid.tag]
    if len(_content_childs) == 0 and parent.getparent() is not None:
        _uplete(parent, parent.getparent())


def get_modelconf_from(file_path) -> str:
    """Determine model from file name extension
    marked with'_' at the end"""

    file_name = os.path.basename(file_path)
    if '_' not in file_name:
        raise ODEMException(f"Miss language in '{file_path}'!")
    return file_name.split('.')[0].split('_')[-1]


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
                raise ODEMException(f"normalize vocal ligatures: {_msg}")
            _out[i - 1] = _replacement
        _out.append(_c)

    # strip all combining e's anyway
    return ''.join(_out).replace(COMBINING_SMALL_E, '')
