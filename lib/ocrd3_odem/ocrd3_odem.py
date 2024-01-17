# -*- coding: utf-8 -*-
"""OCR-Generation for OAI-Records"""

import concurrent.futures
import configparser
import datetime
import logging
import os
import shutil
import socket
import subprocess
import tempfile
import time
from pathlib import (
    Path
)
from typing import (
    Dict,
    List,
    Optional,
)
import lxml.etree as ET
import numpy as np
from digiflow import (
    OAILoader,
    OAIRecord,
    MetsProcessor,
    BaseDerivansManager,
    DerivansResult,
    export_data_from,
    map_contents,
)
from digiflow.digiflow_export import (
    _move_to_tmp_file
)

from .odem_commons import (
    CFG_SEC_OCR,
    CFG_KEY_RES_VOL,
    DEFAULT_RTL_MODELS,
    KEY_LANGUAGES,
    STATS_KEY_LANGS,
    PROJECT_ROOT,
    ExportFormat,
    ODEMException,
)
from .processing_mets import (
    CATALOG_ULB,
    XMLNS,
    ODEMMetadataInspecteur,
    ODEMMetadataMetsException,
    extract_mets_data,
    integrate_ocr_file,
    postprocess_mets,
    validate_mets,
)
from .processing_ocrd import (
    run_ocr_page,
)
from .processing_ocr_pipeline import (
    analyze,
    run_pipeline,
)
from .processing_ocr_results import (
    FILEGROUP_OCR,
    convert_to_output_format,
    list_files,
    postprocess_ocrd_file,
)
from .processing_image import (
    has_image_ext,
    sanitize_image,
    get_imageinfo,
)
from .processing_ocrd import (
    ocrd_workspace_setup,
)

# python process-wrapper limit
os.environ['OMP_THREAD_LIMIT'] = '1'
# default language fallback
# (only when processing local images)
DEFAULT_LANG = 'ger'
# estimated ocr-d runtime
# for a regular page (A4, 1MB)
DEFAULT_RUNTIME_PAGE = 1.0
# process duration format
ODEM_PAGE_TIME_FORMAT = '%Y-%m-%d_%H-%m-%S'
# how long to process single page?
DEFAULT_DOCKER_CONTAINER_TIMEOUT = 600


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
        self.local_mode = record is None
        self.process_identifier = None
        if self.local_mode:
            self.process_identifier = os.path.basename(work_dir)
        if record is not None and record.local_identifier is not None:
            self.process_identifier = record.local_identifier
        self.export_dir = None
        self.the_logger: logging.Logger = None
        self.cfg: configparser.ConfigParser = None
        self.store = None
        self.images_4_ocr: List = []  # List[str] | List[Tuple[str, str]]
        self.ocr_files = []
        self.ocr_function = None
        self.ocr_input_paths = []
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
            loader = OAILoader(req_dst_dir, base_url=base_url, post_oai=extract_mets_data)
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
                                      cfg=self.cfg)
        try:
            insp.inspect()
            self.images_4_ocr = insp.image_pairs
        except ODEMMetadataMetsException as mde:
            raise ODEMException(f"{mde.args[0]}") from mde
        self.identifiers = insp.identifiers
        self._statistics[CATALOG_ULB] = insp.record_identifier
        self._statistics['type'] = insp.type
        self._statistics[STATS_KEY_LANGS] = insp.languages
        self._statistics['n_images_pages'] = insp.n_images_pages
        self._statistics['n_images_ocrable'] = insp.n_images_ocrable
        _ratio = insp.n_images_ocrable / insp.n_images_pages * 100
        self.the_logger.info("[%s] %04d (%.2f%%) images used for OCR (total: %04d)",
                             self.process_identifier, insp.n_images_ocrable, _ratio,
                             insp.n_images_pages)
        self._statistics['host'] = socket.gethostname()

    def clear_existing_entries(self):
        """Clear METS/MODS of configured file groups"""

        if self.cfg:
            _blacklisted = self.cfg.getlist('mets', 'blacklist_file_groups')
            _ident = self.process_identifier
            self.the_logger.info("[%s] remove %s", _ident, _blacklisted)
            _proc = MetsProcessor(self.mets_file)
            _proc.clear_filegroups(_blacklisted)
            _proc.write()

    def language_modelconfig(self, languages=None) -> str:
        """resolve model configuration from
        * provided "languages" parameter
        * else use metadata language entries.

        Please note: Configured model mappings
        might contain compositions, therefore
        the additional inner loop
        """

        _models = []
        model_mappings: dict = self.cfg.getdict(  # pylint: disable=no-member
            'ocr', 'model_mapping')
        self.the_logger.info("[%s] inspect languages '%s'",
                             self.process_identifier, languages)
        if languages is None:
            languages = self._statistics.get(STATS_KEY_LANGS)
        for lang in languages:
            model_entry = model_mappings.get(lang)
            if not model_entry:
                raise ODEMException(f"'{lang}' mapping not found (languages: {languages})!")
            for model in model_entry.split('+'):
                if self._is_model_available(model):
                    _models.append(model)
                else:
                    raise ODEMException(f"'{model}' model config not found !")
        _model_conf = '+'.join(_models) if self.cfg.getboolean('ocr', "model_combinable", fallback=True) else _models[0]
        self.the_logger.info("[%s] map languages '%s' => '%s'",
                             self.process_identifier, languages, _model_conf)
        return _model_conf

    def map_language_to_modelconfig(self, image_path) -> str:
        """Determine Tesseract config from forehead
        processed print metadata or file name suffix.

        Please note, that more than one config
        can be required, each glued with a '+' sign.
        (Therefore the splitting.)

        Resolving order
        #1: inspect language flag
        #2: inspect local filenames
        #3: inspect metadata
        """

        _file_lang_suffixes = DEFAULT_LANG
        # inspect language arg
        if self.cfg.has_option(CFG_SEC_OCR, KEY_LANGUAGES):
            _file_lang_suffixes = self.cfg.get(CFG_SEC_OCR, KEY_LANGUAGES).split('+')
            return self.language_modelconfig(_file_lang_suffixes)
        # inspect final '_' segment of local file names
        if self.local_mode:
            try:
                _image_name = Path(image_path).stem
                if '_' not in _image_name:
                    raise ODEMException(f"Miss language mark for '{_image_name}'!")
                _file_lang_suffixes = _image_name.split('_')[-1].split('+')
            except ODEMException as oxc:
                self.the_logger.warning("[%s] language mapping err '%s' for '%s', fallback to %s",
                                        self.process_identifier, oxc.args[0],
                                        image_path, DEFAULT_LANG)
            return self.language_modelconfig(_file_lang_suffixes)
        # inspect language information from MODS metadata
        return self.language_modelconfig()

    def _is_model_available(self, model) -> bool:
        """Determine whether model is available"""

        resource_dir_mappings = self.cfg.getdict(CFG_SEC_OCR, CFG_KEY_RES_VOL, fallback={})
        for host_dir, _ in resource_dir_mappings.items():
            training_file = host_dir + '/' + model
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

        # gather local images, propably recursive
        images: List[str] = sorted([
            os.path.join(curr, the_file)
            for curr, _, the_files in os.walk(image_dir)
            for the_file in the_files
            if has_image_ext(the_file)
        ])

        # this shouldn't happen
        if len(images) < 1:
            raise ODEMException(f"{self.record.identifier} contains no images!")

        self.the_logger.info("[%s] %d images total",
                             self.process_identifier, len(images))
        return images

    def set_local_images(self):
        """Construct pairs of local paths for 
        (optional previously filtered by object metadata)
        images and original page urn
        """
        _images_of_interest = []
        _local_max_dir = os.path.join(self.work_dir_main, 'MAX')
        for _img, _urn in self.images_4_ocr:
            _the_file = os.path.join(_local_max_dir, _img)
            if not os.path.exists(_the_file):
                raise ODEMException(f"[{self.process_identifier}] missing {_the_file}!")
            _images_of_interest.append((_the_file, _urn))
        self.images_4_ocr = _images_of_interest

    def run(self) -> List:
        """Execute OCR workflow
        Subject to actual ODEM flavor
        """
        _outcomes = [(0, 0, 0, 0)]
        if self.n_executors > 1:
            _outcomes = self.run_parallel()
        else:
            _outcomes = self.run_sequential()
        if _outcomes:
            self._statistics['outcomes'] = _outcomes
        return _outcomes

    def run_parallel(self):
        """Run workflow parallel given poolsize"""

        self.the_logger.info("[%s] %d images run_parallel by %d executors",
                             self.process_identifier, len(self._pipeline_input), self.n_executors)
        try:
            with concurrent.futures.ThreadPoolExecutor(
                    max_workers=self.n_executors,
                    thread_name_prefix='odem'
            ) as executor:
                outcomes = list(executor.map(self.ocr_function, self.ocr_input_paths))
                return outcomes
        except (OSError, AttributeError) as err:
            self.the_logger.error(err)
            raise RuntimeError(f"OCR-D parallel: {err.args[0]}") from err

    def run_sequential(self):
        """run complete workflow plain sequential
        For debugging or small machines
        """

        _len_img = len(self.ocr_input_paths)
        _estm_min = _len_img * DEFAULT_RUNTIME_PAGE
        self.the_logger.info("[%s] %d images run_sequential, estm. %dmin",
                             self.process_identifier, _len_img, _estm_min)
        try:
            outcomes = [self.ocr_function(_img)
                        for _img in self.ocr_input_paths]
            return outcomes
        except (OSError, AttributeError) as err:
            self.the_logger.error(err)
            raise RuntimeError(f"OCR-D sequential: {err.args[0]}") from err

    def calculate_statistics(self, outcomes: List):
        """Calculate and aggregate runtime stats"""
        n_ocr = sum([e[0] for e in outcomes if e[0] == 1])
        _total_mps = [round(e[2], 1) for e in outcomes if e[0] == 1]
        _mod_val_counts = np.unique(_total_mps, return_counts=True)
        mps = list(zip(*_mod_val_counts))
        total_mb = sum([e[3] for e in outcomes if e[0] == 1])
        self._statistics['n_ocr'] = n_ocr
        self._statistics['mb'] = round(total_mb, 2)
        self._statistics['mps'] = mps

    def link_ocr(self) -> int:
        """Prepare and link OCR-data"""

        self.ocr_files = list_files(self.work_dir_main, FILEGROUP_OCR)
        if not self.ocr_files:
            return 0
        proc = MetsProcessor(self.mets_file)
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
            postprocess_ocrd_file(_ocr_file, strip_tags)

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
        _out_path = os.path.join(self.work_dir_main, f'{self._statistics[CATALOG_ULB]}.pdf.txt')
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
        path_logging = self.cfg.get('derivans', 'derivans_logdir', fallback=None)
        derivans: BaseDerivansManager = BaseDerivansManager.create(
            self.mets_file,
            container_image_name=derivans_image,
            path_binary=path_bin,
            path_configuration=path_cfg,
            path_mvn_project=path_prj,
            path_logging=path_logging,
        )
        derivans.init()
        # be cautious
        try:
            dresult: DerivansResult = derivans.start()
            self.the_logger.info("[%s] create derivates in %.1fs",
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
        try:
            validate_mets(self.mets_file)
        except RuntimeError as err:
            if len(err.args) > 0 and str(err.args[0]).startswith('invalid schema'):
                raise ODEMException(str(err.args[0])) from err
            raise err

    def export_data(self):
        """re-do metadata and transform into output format"""

        export_format: str = self.cfg.get('export', 'export_format', fallback=ExportFormat.SAF)
        export_mets: bool = self.cfg.getboolean('export', 'export_mets', fallback=True)

        exp_dst = self.cfg.get('global', 'local_export_dir')
        exp_tmp = self.cfg.get('global', 'local_export_tmp')
        exp_col = self.cfg.get('global', 'export_collection')
        exp_map = self.cfg.getdict('global', 'export_mappings')
        # overwrite default mapping *.xml => 'mets.xml'
        # since we will have currently many more XML-files
        # created due OCR and do more specific mapping, though
        exp_map = {k: v for k, v in exp_map.items() if v != 'mets.xml'}
        if export_mets:
            exp_map[os.path.basename(self.mets_file)] = 'mets.xml'
        saf_name = self.identifiers.get(CATALOG_ULB)
        if export_format == ExportFormat.SAF:
            export_result = export_data_from(
                self.mets_file,
                exp_col,
                saf_final_name=saf_name,
                export_dst=exp_dst,
                export_map=exp_map,
                tmp_saf_dir=exp_tmp,
            )
        elif export_format == ExportFormat.FLAT_ZIP:
            prefix = 'opendata-working-'
            source_path_dir = os.path.dirname(self.mets_file)
            tmp_dir = tempfile.gettempdir()
            if exp_tmp:
                tmp_dir = exp_tmp
            with tempfile.TemporaryDirectory(prefix=prefix, dir=tmp_dir) as tmp_dir:
                work_dir = os.path.join(tmp_dir, saf_name)
                export_mappings = map_contents(source_path_dir, work_dir, exp_map)
                for mapping in export_mappings:
                    mapping.copy()
                tmp_zip_path, size = self._compress(os.path.dirname(work_dir), saf_name)
                path_export_processing = _move_to_tmp_file(tmp_zip_path, exp_dst)
                export_result = path_export_processing, size

        else:
            raise ODEMException(f'Unsupported export format: {export_format}')

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
                return final_path, size

        return None

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

    def _compress(self, work_dir, archive_name):
        zip_file_path = os.path.join(os.path.dirname(work_dir), archive_name) + '.zip'

        previous_dir = os.getcwd()
        os.chdir(os.path.join(work_dir, archive_name))
        cmd = f'zip -q -r {zip_file_path} ./*'
        subprocess.run(cmd, shell=True, check=True)
        os.chmod(zip_file_path, 0o666)
        zip_size = int(os.path.getsize(zip_file_path) / 1024 / 1024)
        os.chdir(previous_dir)
        return zip_file_path, f"{zip_size}MiB"


class OCRDPageParallel(ODEMProcess):
    """Use page parallel workflow"""

    def run(self):
        """Wrap specific OCR execution with
        respect to number of executors"""

        _outcomes = [(0, 0, 0, 0)]
        if self.n_executors > 1:
            _outcomes = self.run_parallel()
        else:
            _outcomes = self.run_sequential()
        if _outcomes:
            self._statistics['outcomes'] = _outcomes
        self.to_alto()
        return _outcomes

    def run_parallel(self):
        """Run workflow parallel given poolsize"""

        self.the_logger.info("[%s] %d images run_parallel by %d executors",
                             self.process_identifier, len(self.images_4_ocr), self.n_executors)
        try:
            with concurrent.futures.ThreadPoolExecutor(
                    max_workers=self.n_executors,
                    thread_name_prefix='odem'
            ) as executor:
                outcomes = list(executor.map(self.ocrd_page, self.images_4_ocr))
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
            outcomes = [self.ocrd_page(_img)
                        for _img in self.images_4_ocr]
            return outcomes
        except (OSError, AttributeError) as err:
            self.the_logger.error(err)
            raise RuntimeError(f"OCR-D sequential: {err.args[0]}") from err

    def ocrd_page(self, image_4_ocr):
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

        # # find out the needed model config for tesseract
        model_config = self.map_language_to_modelconfig(image_path)

        stored = 0
        mps = 0
        filesize_mb = 0
        # use original image rather than
        # transformed one since PNG is
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

        container_name: str = f'{self.process_identifier}_{os.path.basename(page_workdir)}'
        container_memory_limit: str = self.cfg.get('ocr', 'docker_container_memory_limit', fallback=None)
        container_user = self.cfg.get('ocr', 'docker_container_user', fallback=os.getuid())
        container_timeout: int = self.cfg.getint(
            'ocr',
            'docker_container_timeout',
            fallback=DEFAULT_DOCKER_CONTAINER_TIMEOUT
        )
        base_image = self.cfg.get('ocr', 'ocrd_baseimage')
        makefile = self.cfg.get('ocr', 'ocrd_makefile').split('/')[-1]
        tesseract_model_rtl: List[str] = self.cfg.getlist('ocr', 'tesseract_model_rtl', fallback=DEFAULT_RTL_MODELS)
        ocrd_resources_volumes: Dict[str, str] = self.cfg.getdict('ocr', CFG_KEY_RES_VOL, fallback={})

        if self.local_mode:
            container_name = os.path.basename(page_workdir)
        try:
            profiling = run_ocr_page(
                page_workdir,
                base_image,
                container_memory_limit,
                container_timeout,
                container_name,
                container_user,
                model_config,
                makefile,
                ocrd_resources_volumes,
                tesseract_model_rtl,
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
            _ts = time.strftime(ODEM_PAGE_TIME_FORMAT, time.localtime())
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

    def to_alto(self) -> int:
        """Forward OCR format conversion"""

        _cnv = convert_to_output_format(self.work_dir_main)
        n_candidates = len(self.images_4_ocr)
        if len(_cnv) == 0 and n_candidates > 0:
            raise ODEMException(f"No OCR result for {n_candidates} candidates created!")
        self.ocr_files = _cnv
        self.the_logger.info("[%s] converted '%d' files page-to-alto",
                             self.process_identifier, len(_cnv))


class ODEMTesseract(ODEMProcess):
    """Tesseract Runner"""

    def __init__(self, record, workspace, n_execs):
        super().__init__(record, workspace, executors=n_execs)
        self.ocr_function = run_pipeline
        self.pipeline_config = None

    def run(self):
        """Wrap specific OCR execution with
        respect to number of executors"""

        _cfg = self.read_pipeline_config()
        self._prepare_workdir_tmp()
        _n_total = len(self.images_4_ocr)
        self.ocr_input_paths = [(img, i, _n_total, self.the_logger, _cfg)
                                for i, img in enumerate(self.images_4_ocr, start=1)]
        return super().run()

    def read_pipeline_config(self, path_cfg=None) -> configparser:
        """Read and process additional pipeline configuration"""

        _path_cfg = path_cfg
        if path_cfg is None:
            if self.cfg.has_option('ocr', 'ocr_pipeline_config'):
                _path_cfg = os.path.abspath(self.cfg.get('ocr', 'ocr_pipeline_config'))
        if not os.path.isfile(_path_cfg):
            raise ODEMException(f"Invalid ocr-pipeline conf {_path_cfg}")
        _cfg = configparser.ConfigParser()
        _cfg.read(_path_cfg)
        self.pipeline_config = _cfg
        return _cfg

    def _prepare_workdir_tmp(self):
        workdir_tmp = self.cfg.get('ocr', 'ocr_pipeline_workdir_tmp')
        self.the_logger.warning("no workdir set, use '%s'", workdir_tmp)
        if not os.path.isdir(workdir_tmp):
            if os.access(workdir_tmp, os.W_OK):
                os.makedirs(workdir_tmp)
            else:
                self.the_logger.warning("tmp workdir '%s' not writable, use /tmp",
                                        workdir_tmp)
                workdir_tmp = '/tmp/ocr-pipeline-workdir'
                if os.path.exists(workdir_tmp):
                    self._clean_workdir(workdir_tmp)
                os.makedirs(workdir_tmp, exist_ok=True)
        else:
            self._clean_workdir(workdir_tmp)
        return workdir_tmp

    def _clean_workdir(self, the_dir):
        self.the_logger.info("clean existing workdir '%s'", the_dir)
        for file_ in os.listdir(the_dir):
            fpath = os.path.join(the_dir, file_)
            if os.path.isfile(fpath):
                os.unlink(fpath)

    # def run_parallel(self):
    #     """Run workflow parallel given poolsize"""

    #     self.the_logger.info("[%s] %d images run_parallel by %d executors",
    #                          self.process_identifier, len(self._pipeline_input), self.n_executors)
    #     try:
    #         with concurrent.futures.ThreadPoolExecutor(
    #                 max_workers=self.n_executors,
    #                 thread_name_prefix='odem'
    #         ) as executor:
    #             outcomes = list(executor.map(run_pipeline, self._pipeline_input))
    #             return outcomes
    #     except (OSError, AttributeError) as err:
    #         self.the_logger.error(err)
    #         raise RuntimeError(f"OCR-D parallel: {err.args[0]}") from err

    # def run_sequential(self):
    #     """run complete workflow plain sequential
    #     For debugging or small machines
    #     """

    #     _len_img = len(self._pipeline_input)
    #     _estm_min = _len_img * DEFAULT_RUNTIME_PAGE
    #     self.the_logger.info("[%s] %d images run_sequential, estm. %dmin",
    #                          self.process_identifier, _len_img, _estm_min)
    #     try:
    #         outcomes = [run_pipeline(_img)
    #                     for _img in self._pipeline_input]
    #         return outcomes
    #     except (OSError, AttributeError) as err:
    #         self.the_logger.error(err)
    #         raise RuntimeError(f"OCR-D sequential: {err.args[0]}") from err

    def store_estimations(self, estms):
        """Postprocessing of OCR-Quality Estimation Data"""

        valids = [r for r in estms if r[1] != -1]
        invalids = [r for r in estms if r[1] == -1]
        sorteds = sorted(valids, key=lambda r: r[1])
        aggregations = analyze(sorteds)
        end_time = time.strftime('%Y-%m-%d_%H-%M', time.localtime())
        if not os.path.isdir(self.work_dir_main):
            self.the_logger.warning('unable to choose store for estm data: %s',
                                    str(self.work_dir_main))
            return

        file_name = os.path.basename(self.work_dir_main)
        file_path = os.path.join(
            self.work_dir_main, f"{file_name}_{end_time}.wtr")
        self.the_logger.info("store mean '%.3f' in '%s'",
                             aggregations[0], file_path)
        if aggregations:
            (mean, bins) = aggregations
            b_1 = len(bins[0])
            b_2 = len(bins[1])
            b_3 = len(bins[2])
            b_4 = len(bins[3])
            b_5 = len(bins[4])
            n_v = len(valids)
            n_i = len(invalids)
            self.the_logger.info("WTE (Mean): '%.1f' (1: %d/%d, ... 5: %d/%d)",
                                 mean, b_1, n_v, b_5, n_v)
            with open(file_path, 'w', encoding="UTF-8") as outfile:
                outfile.write(
                    f"{mean},{b_1},{b_2},{b_3},{b_4},{b_5},{len(estms)},{n_i}\n")
                for s in sorteds:
                    outfile.write(
                        f"{s[0]},{s[1]:.3f},{s[2]},{s[3]},{s[4]},{s[5]},{s[6]},{s[7]}\n")
                outfile.write("\n")
                return file_path
