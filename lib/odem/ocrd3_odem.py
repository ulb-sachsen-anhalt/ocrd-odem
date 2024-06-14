# -*- coding: utf-8 -*-
"""OCR-Generation for OAI-Records"""

from __future__ import annotations

# import concurrent.futures
import configparser
import datetime
import typing
import logging
import os
import shutil
import socket
import subprocess
import tempfile
import time
import typing

# from enum import Enum
from pathlib import Path

import numpy as np
import digiflow as df
import digiflow.digiflow_export as dfx
import digiflow.digiflow_metadata as dfm

import lib.odem.odem_commons as odem_c
import lib.odem.processing.image as odem_image

from .processing.mets import (
    ODEMMetadataInspecteur,
    extract_text_content,
    integrate_ocr_file,
    postprocess_mets,
    validate,
)
# from lib.odem.ocr.ocrd import (
#     run_ocr_page,
# )
# from .ocr.ocr_pipeline import (
#     STEP_MOVE_PATH_TARGET,
#     run_pipeline,
# )
# from .processing.ocr_files import (
#     convert_to_output_format,
#     postprocess_ocr_file,
# )
# from .processing.image import (
#     has_image_ext,
#     sanitize_image,
#     get_imageinfo,
# )
# from .ocr.ocrd import (
#     ocrd_workspace_setup,
# )

# python process-wrapper limit
os.environ['OMP_THREAD_LIMIT'] = '1'
# default language fallback
# (only when processing local images)
DEFAULT_LANG = 'ger'
# # estimated ocr-d runtime
# # for a regular page (A4, 1MB)
# _DEFAULT_RUNTIME_PAGE = 1.0
# # process duration format
# _ODEM_PAGE_TIME_FORMAT = '%Y-%m-%d_%H-%m-%S'
# # how long to process single page?
# _DEFAULT_DOCKER_CONTAINER_TIMEOUT = 600

# _LOCAL_OCRD_RESULT_DIR = 'PAGE'


# class OdemWorkflowProcessType(str, Enum):
#     OCRD_PAGE_PARALLEL = "OCRD_PAGE_PARALLEL"
#     ODEM_TESSERACT = "ODEM_TESSERACT"


class ODEMProcessImpl(odem_c.OdemProcess):
    """Create OCR for OAI Records.

        Runs both wiht OAIRecord or local path as input.
        process_identifier may represent a local directory
        or the local part of an OAI-URN.

        Languages for ocr-ing are assumed to be enriched in
        OAI-Record-Metadata (MODS) or be part of local
        paths. They will be applied by a custom mapping
        for the underlying OCR-Engine Tesseract-OCR.
    """

    def __init__(self, record: df.OAIRecord, work_dir, executors=2, log_dir=None, logger=None):
        """Create new ODEM Process.
        Args:
            record (OAIRecord): OAI Record dataset
            work_dir (_type_): required local work path
            executors (int, optional): Process pooling when running parallel.
                Defaults to 2.
            log_dir (_type_, optional): Path to store log file.
                Defaults to None.
        """

        self.record = record
        self.work_dir_main = work_dir
        self.digi_type = None
        self.mods_identifier = None
        self.local_mode = record is None
        self.process_identifier = None
        if self.local_mode:
            self.process_identifier = os.path.basename(work_dir)
        if record is not None and record.local_identifier is not None:
            self.process_identifier = record.local_identifier
        self.export_dir = None
        self.the_logger: logging.Logger = None
        self.odem_configuration: configparser.ConfigParser = None
        self.store: df.LocalStore = None
        self.images_4_ocr: typing.List = []  # List[str] | List[Tuple[str, str]]
        self.ocr_files = []
        self.ocr_function = None
        self.ocr_input: typing.List = []
        self._statistics_ocr = {'execs': executors}
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
            odem_c.PROJECT_ROOT, 'resources', 'odem_logging.ini')
        logging.config.fileConfig(conf_file_location, defaults=conf_logname)
        self.the_logger = logging.getLogger('odem')

    def load(self):
        request_identifier = self.record.identifier
        local_identifier = self.record.local_identifier
        req_dst_dir = os.path.join(
            os.path.dirname(self.work_dir_main), local_identifier)
        if not os.path.exists(req_dst_dir):
            os.makedirs(req_dst_dir, exist_ok=True)
        req_dst = os.path.join(req_dst_dir, local_identifier + '.xml')
        self.the_logger.debug("[%s] download %s to %s",
                              self.process_identifier, request_identifier, req_dst)
        base_url = self.odem_configuration.get('global', 'base_url')
        try:
            loader = df.OAILoader(req_dst_dir, base_url=base_url, post_oai=dfm.extract_mets)
            loader.store = self.store
            loader.load(request_identifier, local_dst=req_dst)
        except df.OAILoadClientError as load_err:
            raise odem_c.ODEMException(load_err.args[0]) from load_err
        except RuntimeError as _err:
            raise odem_c.ODEMException(_err.args[0]) from _err

    def clear_resources(self, remove_all=False):
        """Remove OAI-Resources from store or even
        anything related to current process
        """

        if self.store is not None:
            sweeper = df.OAIFileSweeper(self.store.dir_store_root, '.xml')
            sweeper.sweep()
            if remove_all:
                shutil.rmtree(self.store.dir_store_root)
        if os.path.exists(self.work_dir_main):
            shutil.rmtree(self.work_dir_main)

    def inspect_metadata(self):
        insp = ODEMMetadataInspecteur(self.mets_file,
                                      self.record.identifier,
                                      cfg=self.odem_configuration)
        try:
            the_report = insp.metadata_report()
            self.digi_type = the_report.type
            self.images_4_ocr = insp.image_pairs
        except RuntimeError as mde:
            raise odem_c.ODEMException(f"{mde.args[0]}") from mde
        self.mods_identifier = insp.mods_record_identifier
        for t, ident in insp.identifiers.items():
            self._statistics_ocr[t] = ident
        self._statistics_ocr['type'] = insp.type
        self._statistics_ocr[odem_c.STATS_KEY_LANGS] = insp.languages
        self._statistics_ocr['n_images_pages'] = insp.n_images_pages
        self._statistics_ocr['n_images_ocrable'] = insp.n_images_ocrable
        _ratio = insp.n_images_ocrable / insp.n_images_pages * 100
        self.the_logger.info("[%s] %04d (%.2f%%) images used for OCR (total: %04d)",
                             self.process_identifier, insp.n_images_ocrable, _ratio,
                             insp.n_images_pages)
        self._statistics_ocr['host'] = socket.gethostname()

    def clear_existing_entries(self):
        """Clear METS/MODS of configured file groups"""

        if self.odem_configuration:
            _blacklisted = self.odem_configuration.getlist('mets', 'blacklist_file_groups')
            _ident = self.process_identifier
            self.the_logger.info("[%s] remove %s", _ident, _blacklisted)
            _proc = df.MetsProcessor(self.mets_file)
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
        model_mappings: dict = self.odem_configuration.getdict(  # pylint: disable=no-member
            odem_c.CFG_SEC_OCR, 'model_mapping')
        self.the_logger.info("[%s] inspect languages '%s'",
                             self.process_identifier, languages)
        if languages is None:
            languages = self._statistics_ocr.get(odem_c.STATS_KEY_LANGS)
        for lang in languages:
            model_entry = model_mappings.get(lang)
            if not model_entry:
                raise odem_c.ODEMException(f"'{lang}' mapping not found (languages: {languages})!")
            for model in model_entry.split('+'):
                if self._is_model_available(model):
                    _models.append(model)
                else:
                    raise odem_c.ODEMException(f"'{model}' model config not found !")
        _model_conf = '+'.join(_models) if self.odem_configuration.getboolean(odem_c.CFG_SEC_OCR, "model_combinable", fallback=True) else _models[0]
        self._statistics_ocr[odem_c.STATS_KEY_MODELS] = _model_conf
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
        if self.odem_configuration.has_option(odem_c.CFG_SEC_OCR, odem_c.KEY_LANGUAGES):
            _file_lang_suffixes = self.odem_configuration.get(odem_c.CFG_SEC_OCR, odem_c.KEY_LANGUAGES).split('+')
            return self.language_modelconfig(_file_lang_suffixes)
        # inspect final '_' segment of local file names
        if self.local_mode:
            try:
                _image_name = Path(image_path).stem
                if '_' not in _image_name:
                    raise odem_c.ODEMException(f"Miss language mark for '{_image_name}'!")
                _file_lang_suffixes = _image_name.split('_')[-1].split('+')
            except odem_c.ODEMException as oxc:
                self.the_logger.warning("[%s] language mapping err '%s' for '%s', fallback to %s",
                                        self.process_identifier, oxc.args[0],
                                        image_path, DEFAULT_LANG)
            return self.language_modelconfig(_file_lang_suffixes)
        # inspect language information from MODS metadata
        return self.language_modelconfig()

    def _is_model_available(self, model) -> bool:
        """Determine whether model is available"""

        resource_dir_mappings = self.odem_configuration.getdict(odem_c.CFG_SEC_OCR, odem_c.CFG_SEC_OCR_OPT_RES_VOL, fallback={})
        for host_dir, _ in resource_dir_mappings.items():
            training_file = host_dir + '/' + model
            if os.path.exists(training_file):
                return True
        return False

    def get_local_image_paths(self, image_local_dir=None) -> typing.List[str]:
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
        images: typing.List[str] = sorted([
            os.path.join(curr, the_file)
            for curr, _, the_files in os.walk(image_dir)
            for the_file in the_files
            if odem_image.has_image_ext(the_file)
        ])

        # this shouldn't happen
        if len(images) < 1:
            raise odem_c.ODEMException(f"{self.record.identifier} contains no images!")

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
                raise odem_c.ODEMException(f"[{self.process_identifier}] missing {_the_file}!")
            _images_of_interest.append((_the_file, _urn))
        self.images_4_ocr = _images_of_interest

    def calculate_statistics_ocr(self, outcomes: typing.List):
        """Calculate and aggregate runtime stats"""
        n_ocr = sum([e[0] for e in outcomes if e[0] == 1])
        _total_mps = [round(e[2], 1) for e in outcomes if e[0] == 1]
        _mod_val_counts = np.unique(_total_mps, return_counts=True)
        mps = list(zip(*_mod_val_counts))
        total_mb = sum([e[3] for e in outcomes if e[0] == 1])
        self._statistics_ocr[odem_c.STATS_KEY_N_OCR] = n_ocr
        self._statistics_ocr[odem_c.STATS_KEY_MB] = round(total_mb, 2)
        self._statistics_ocr[odem_c.STATS_KEY_MPS] = mps

    def link_ocr_files(self) -> int:
        """Prepare and link OCR-data"""

        self.ocr_files = odem_c.list_files(self.work_dir_main, odem_c.FILEGROUP_FULLTEXT)
        if not self.ocr_files:
            return 0
        proc = df.MetsProcessor(self.mets_file)
        _n_linked_ocr = integrate_ocr_file(proc.tree, self.ocr_files)
        proc.write()
        return _n_linked_ocr

    def create_text_bundle_data(self):
        """create additional dspace bundle for indexing ocr text
        read ocr-file sequential according to their number label
        and extract every row into additional text file"""

        txt_lines = extract_text_content(self.ocr_files)
        txt_content = '\n'.join(txt_lines)
        _out_path = os.path.join(self.work_dir_main, f'{self.mods_identifier}.pdf.txt')
        with open(_out_path, mode='w', encoding='UTF-8') as _writer:
            _writer.write(txt_content)
        self.the_logger.info("[%s] harvested %d lines from %d ocr files to %s",
                             self.process_identifier, len(txt_lines), len(self.ocr_files), _out_path)
        self._statistics_ocr['n_text_lines'] = len(txt_lines)

    def create_pdf(self):
        """Forward PDF-creation to Derivans"""

        _cfg_path_dir_bin = self.odem_configuration.get('derivans', 'derivans_dir_bin', fallback=None)
        path_bin = None
        if _cfg_path_dir_bin is not None:
            path_bin = os.path.join(odem_c.PROJECT_ROOT, _cfg_path_dir_bin)
        _cfg_path_dir_project = self.odem_configuration.get('derivans', 'derivans_dir_project', fallback=None)
        path_prj = None
        if _cfg_path_dir_project is not None:
            path_prj = os.path.join(odem_c.PROJECT_ROOT, _cfg_path_dir_project)
        path_cfg = os.path.join(
            odem_c.PROJECT_ROOT,
            self.odem_configuration.get('derivans', 'derivans_config')
        )
        derivans_image = self.odem_configuration.get('derivans', 'derivans_image', fallback=None)
        path_logging = self.odem_configuration.get('derivans', 'derivans_logdir', fallback=None)
        derivans: df.BaseDerivansManager = df.BaseDerivansManager.create(
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
            dresult: df.DerivansResult = derivans.start()
            self.the_logger.info("[%s] create derivates in %.1fs",
                                 self.process_identifier, dresult.duration)
        except subprocess.CalledProcessError as _sub_err:
            _err_msg = _sub_err.stdout.decode().split(os.linesep)[0].replace("'", "\"")
            _args = [_err_msg]
            _args.extend(_sub_err.args)
            raise odem_c.ODEMException(_args) from _sub_err

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

        postprocess_mets(self.mets_file, self.odem_configuration)

    def validate_metadata(self):
        """Forward (optional) validation concerning
        METS/MODS XML-schema and/or current DDB-schematron
        validation for 'digitalisierte medien'
        """
        check_ddb = False
        ignore_ddb = []
        if self.odem_configuration.has_option('mets', 'ddb_validation'):
            check_ddb = self.odem_configuration.getboolean('mets', 'ddb_validation', fallback=False)
        if self.odem_configuration.has_option('mets', 'ddb_validation_ignore'):
            raw_ignore_str = self.odem_configuration.get('mets', 'ddb_validation_ignore')
            ignore_ddb = [i.strip() for i in raw_ignore_str.split(',')]
        # dtype = 'Aa'
        # if 'pica' in self.record.info:
        #     dtype = self.record.info['pica']
        return validate(self.mets_file, validate_ddb=check_ddb, 
                        digi_type=self.digi_type, ddb_ignores=ignore_ddb)

    def export_data(self):
        """re-do metadata and transform into output format"""

        export_format: str = self.odem_configuration.get('export', 'export_format', fallback=odem_c.ExportFormat.SAF)
        export_mets: bool = self.odem_configuration.getboolean('export', 'export_mets', fallback=True)

        exp_dst = self.odem_configuration.get('export', 'local_export_dir')
        exp_tmp = self.odem_configuration.get('export', 'local_export_tmp')
        exp_col = self.odem_configuration.get('export', 'export_collection')
        exp_map = self.odem_configuration.getdict('export', 'export_mappings')
        # overwrite default mapping *.xml => 'mets.xml'
        # since we will have currently many more XML-files
        # created due OCR and do more specific mapping, though
        exp_map = {k: v for k, v in exp_map.items() if v != 'mets.xml'}
        if export_mets:
            exp_map[os.path.basename(self.mets_file)] = 'mets.xml'
        saf_name = self.mods_identifier
        if export_format == odem_c.ExportFormat.SAF:
            export_result = df.export_data_from(
                self.mets_file,
                exp_col,
                saf_final_name=saf_name,
                export_dst=exp_dst,
                export_map=exp_map,
                tmp_saf_dir=exp_tmp,
            )
        elif export_format == odem_c.ExportFormat.FLAT_ZIP:
            prefix = 'opendata-working-'
            source_path_dir = os.path.dirname(self.mets_file)
            tmp_dir = tempfile.gettempdir()
            if exp_tmp:
                tmp_dir = exp_tmp
            with tempfile.TemporaryDirectory(prefix=prefix, dir=tmp_dir) as tmp_dir:
                work_dir = os.path.join(tmp_dir, saf_name)
                export_mappings = df.map_contents(source_path_dir, work_dir, exp_map)
                for mapping in export_mappings:
                    mapping.copy()
                tmp_zip_path, size = ODEMProcessImpl.compress_flat(os.path.dirname(work_dir), saf_name)
                path_export_processing = dfx._move_to_tmp_file(tmp_zip_path, exp_dst)
                export_result = path_export_processing, size
        else:
            raise odem_c.ODEMException(f'Unsupported export format: {export_format}')
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

    @classmethod
    def compress_flat(cls, work_dir, archive_name):
        zip_file_path = os.path.join(os.path.dirname(work_dir), archive_name) + '.zip'
        previous_dir = os.getcwd()
        os.chdir(os.path.join(work_dir, archive_name))
        cmd = f'zip -q -r {zip_file_path} ./*'
        subprocess.run(cmd, shell=True, check=True)
        os.chmod(zip_file_path, 0o666)
        zip_size = int(os.path.getsize(zip_file_path) / 1024 / 1024)
        os.chdir(previous_dir)
        return zip_file_path, f"{zip_size}MiB"

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

        self._statistics_ocr['timedelta'] = f'{self.duration}'
        return self._statistics_ocr


# class ODEMWorkflowRunner:
#     """Wrap actual ODEM process execution"""

#     def __init__(self, identifier, n_executors, 
#                  internal_logger, odem_workflow) -> None:
#         self.process_identifier = identifier
#         self.n_executors = n_executors
#         self.logger:logging.Logger = internal_logger
#         self.odem_workflow: ODEMWorkflow = odem_workflow

#     def run(self):
#         input_data = self.odem_workflow.get_inputs()
#         the_outcomes = [(0, 0, 0, 0)]
#         if self.n_executors > 1:
#             the_outcomes = self.run_parallel(input_data)
#         else:
#             the_outcomes = self.run_sequential(input_data)
#         self.odem_workflow.foster_outputs()
#         return the_outcomes

#     def run_parallel(self, input_data):
#         """Run workflow parallel with given executors"""

#         n_inputs = len(input_data)
#         self.logger.info("[%s] %d inputs run_parallel by %d executors",
#                              self.process_identifier, n_inputs, self.n_executors)
#         try:
#             with concurrent.futures.ThreadPoolExecutor(
#                     max_workers=self.n_executors,
#                     thread_name_prefix='odem.ocrd'
#             ) as executor:
#                 return list(executor.map(self.odem_workflow.run, input_data))
#         except (OSError, AttributeError) as err:
#             self.logger.error(err)
#             raise odem_c.ODEMException(f"ODEM parallel: {err.args[0]}") from err

#     def run_sequential(self, input_data):
#         """run complete workflow plain sequential
#         For debugging or small machines
#         """

#         len_img = len(input_data)
#         estm_min = len_img * DEFAULT_RUNTIME_PAGE
#         self.logger.info("[%s] %d inputs run_sequential, estm. %dmin",
#                              self.process_identifier, len_img, estm_min)
#         try:
#             outcomes = [self.odem_workflow.run(the_input)
#                         for the_input in input_data]
#             return outcomes
#         except (OSError, AttributeError) as err:
#             self.logger.error(err)
#             raise odem_c.ODEMException(f"ODEM sequential: {err.args[0]}") from err


# class ODEMWorkflow:
#     """Base Interface"""

#     @staticmethod
#     def create(
#             workflow_type: OdemWorkflowProcessType | str,
#             odem: ODEMProcess,
#     ) -> ODEMWorkflow:
#         if (workflow_type == OdemWorkflowProcessType.ODEM_TESSERACT
#             or workflow_type == OdemWorkflowProcessType.ODEM_TESSERACT.value):
#             return ODEMTesseract(odem)
#         return OCRDPageParallel(odem)

#     def get_inputs(self) -> typing.List:
#         """Collect all input data files for processing"""

#     def run(self):
#         """Run actual implemented Workflow"""

#     def foster_outputs(self):
#         """Work to do after pipeline has been run successfully
#         like additional format transformations or sanitizings
#         """


# class OCRDPageParallel(ODEMWorkflow):
#     """Use page parallel workflow"""

#     def __init__(self, odem_process: ODEMProcess):
#         self.odem = odem_process
#         self.cfg = odem_process.odem_configuration
#         self.logger = odem_process.the_logger

#     def get_inputs(self):
#         return self.odem.images_4_ocr

#     def run(self, input_data):
#         """Create OCR Data"""

#         ocr_log_conf = os.path.join(
#             odem_c.PROJECT_ROOT, self.cfg.get(odem_c.CFG_SEC_OCR, 'ocrd_logging'))

#         # Preprare workspace with makefile
#         (image_path, ident) = input_data
#         os.chdir(self.odem.work_dir_main)
#         file_name = os.path.basename(image_path)
#         file_id = file_name.split('.')[0]
#         page_workdir = os.path.join(self.odem.work_dir_main, file_id)
#         if os.path.exists(page_workdir):
#             shutil.rmtree(page_workdir, ignore_errors=True)
#         os.mkdir(page_workdir)
#         shutil.copy(ocr_log_conf, page_workdir)
#         os.chdir(page_workdir)

#         # move and convert image data at once
#         processed_image_path = sanitize_image(image_path, page_workdir)

#         # init ocr-d workspace
#         ocrd_workspace_setup(page_workdir, processed_image_path)

#         # find model config for tesseract
#         model_config = self.odem.map_language_to_modelconfig(image_path)

#         stored = 0
#         mps = 0
#         filesize_mb = 0
#         # use original image rather than
#         # transformed one since PNG is
#         # usually 2-5 times larger than JPG
#         filestat = os.stat(image_path)
#         if filestat:
#             filesize_mb = filestat.st_size / 1048576
#         (mps, dpi) = get_imageinfo(image_path)

#         # how to identify data set?
#         if self.odem.record:
#             _ident = self.odem.process_identifier
#         else:
#             _ident = os.path.basename(self.odem.work_dir_main)
#         # OCR Generation
#         profiling = ('n.a.', 0)

#         container_name: str = f'{self.odem.process_identifier}_{os.path.basename(page_workdir)}'
#         container_memory_limit: str = self.cfg.get(odem_c.CFG_SEC_OCR, 'docker_container_memory_limit', fallback=None)
#         container_user = self.cfg.get(odem_c.CFG_SEC_OCR, 'docker_container_user', fallback=os.getuid())
#         container_timeout: int = self.cfg.getint(
#             odem_c.CFG_SEC_OCR,
#             'docker_container_timeout',
#             fallback=DEFAULT_DOCKER_CONTAINER_TIMEOUT
#         )
#         base_image = self.cfg.get(odem_c.CFG_SEC_OCR, 'ocrd_baseimage')
#         ocrd_process_list = self.cfg.getlist(odem_c.CFG_SEC_OCR, 'ocrd_process_list')
#         tesseract_model_rtl: typing.List[str] = self.cfg.getlist(odem_c.CFG_SEC_OCR, 'tesseract_model_rtl', fallback=odem_c.DEFAULT_RTL_MODELS)
#         ocrd_resources_volumes: typing.Dict[str, str] = self.cfg.getdict(odem_c.CFG_SEC_OCR, odem_c.CFG_SEC_OCR_OPT_RES_VOL, fallback={})

#         if self.odem.local_mode:
#             container_name = os.path.basename(page_workdir)
#         try:
#             profiling = run_ocr_page(
#                 page_workdir,
#                 base_image,
#                 container_memory_limit,
#                 container_timeout,
#                 container_name,
#                 container_user,
#                 ocrd_process_list,
#                 model_config,
#                 ocrd_resources_volumes,
#                 tesseract_model_rtl,
#             )
#             # will be unset in case of magic mocking for test
#             if profiling:
#                 self.logger.info("[%s] '%s' in %s (%.1fMP, %dDPI, %.1fMB)",
#                                      _ident, profiling[1], profiling[0], mps, dpi, filesize_mb)
#             self.logger.info("[%s] run ocr creation in '%s'",
#                                  _ident, page_workdir)
#             stored = self._store_fulltext(page_workdir, image_path)
#             if stored:
#                 self._preserve_log(page_workdir, ident)
#         except (subprocess.CalledProcessError, subprocess.TimeoutExpired) as exc:
#             self.logger.error("[%s] image '%s' failed due to subprocess timeout: %s",
#                                   _ident, base_image, exc)
#         except Exception as plain_exc:
#             self.logger.error("[%s] generic exc '%s' for image '%s'",
#                                   _ident, plain_exc, base_image)

#         os.chdir(self.odem.work_dir_main)
#         if self.cfg.getboolean(odem_c.CFG_SEC_OCR, 'keep_temp_orcd_data', fallback=False) is False:
#             shutil.rmtree(page_workdir, ignore_errors=True)
#         return stored, 1, mps, filesize_mb

#     def _preserve_log(self, work_subdir, image_ident):
#         """preserve ocrd.log for later analyzis as
#         sub directory identified by adopted local
#         identifier (local section of system OAI handle)"""

#         _root_log = self.cfg.get('global', 'local_log_dir')
#         _local_ident = self.odem.process_identifier.replace('/', '_')
#         _local_ocr_log = os.path.join(_root_log, _local_ident)
#         if not os.path.exists(_local_ocr_log):
#             os.makedirs(_local_ocr_log, exist_ok=True)

#         _org_log = os.path.join(work_subdir, 'ocrd.log')
#         if os.path.exists(_org_log):
#             _ts = time.strftime(ODEM_PAGE_TIME_FORMAT, time.localtime())
#             _log_label = f'ocrd_odem_{self.odem.process_identifier}_{image_ident}_{_ts}.log'
#             _rebranded = os.path.join(work_subdir, _log_label)
#             os.rename(_org_log, _rebranded)
#             shutil.copy(_rebranded, _local_ocr_log)
#         else:
#             self.logger.warning("[%s] No ocrd.log in %s",
#                                     self.odem.process_identifier, work_subdir)

#     def _store_fulltext(self, image_subdir, original_image_path) -> int:
#         """Move OCR Result from Workspace Subdir to export folder if exists"""

#         # inspect possible ocr result dirs from within
#         # the OCR-D subordinate workspaces for each image
#         old_id = os.path.basename(image_subdir)
#         ocr_result_dir = os.path.join(image_subdir, LOCAL_OCRD_RESULT_DIR)
#         if not os.path.isdir(ocr_result_dir):
#             self.logger.info("[%s] no ocr results for '%s'",
#                                  self.odem.process_identifier, ocr_result_dir)
#             return 0
#         ocrs = [os.path.join(ocr_result_dir, ocr)
#                 for ocr in os.listdir(ocr_result_dir)
#                 if str(ocr).endswith('.xml')]
#         self.logger.debug("[%s] %s ocr files",
#                               self.odem.process_identifier, ocrs)
#         if ocrs and len(ocrs) == 1:
#             # propably need to rename
#             # since file now is like 'PAGE_01.xml'
#             renamed = os.path.join(ocr_result_dir, old_id + '.xml')
#             os.rename(ocrs[0], renamed)
#             # regular case: OAI Workflow
#             if not self.odem.local_mode:
#                 # export to 'PAGE' dir
#                 wd_fulltext = os.path.join(self.odem.work_dir_main, LOCAL_OCRD_RESULT_DIR)
#                 if not os.path.exists(wd_fulltext):
#                     os.mkdir(wd_fulltext)

#             # special case: local runnings for straight evaluations
#             else:
#                 wd_fulltext = os.path.dirname(original_image_path)

#             # final storage
#             target_path = os.path.join(wd_fulltext, old_id + '.xml')
#             shutil.copy(renamed, target_path)
#         return 1

#     def foster_outputs(self):
#         """In this case:
#         * move files from dir PAGE to FULLTEXT
#         * convert OCR format PAGE => ALTO
#         * some additional tag stripping
#         """

#         n_candidates = len(self.odem.images_4_ocr)
#         ocrd_data_files = odem_c.list_files(self.odem.work_dir_main, LOCAL_OCRD_RESULT_DIR)
#         if len(ocrd_data_files) == 0 and n_candidates > 0:
#             raise odem_c.ODEMException(f"No OCR result for {n_candidates} candidates created!")
#         final_fulltext_dir = os.path.join(self.odem.work_dir_main, odem_c.FILEGROUP_FULLTEXT)
#         if not os.path.isdir(final_fulltext_dir):
#             os.makedirs(final_fulltext_dir, exist_ok=True)
#         self.ocr_files = convert_to_output_format(ocrd_data_files, final_fulltext_dir)
#         self.logger.info("[%s] converted '%d' files page-to-alto",
#                              self.odem.process_identifier, len(self.ocr_files))
#         strip_tags = self.cfg.getlist(odem_c.CFG_SEC_OCR, 'strip_tags')
#         for _ocr_file in self.ocr_files:
#             postprocess_ocr_file(_ocr_file, strip_tags)


# class ODEMTesseract(ODEMWorkflow):
#     """Tesseract Runner"""

#     def __init__(self, odem_process: ODEMProcess):
#         self.odem = odem_process
#         self.odem_configuration = odem_process.odem_configuration
#         self.logger = odem_process.the_logger
#         self.pipeline_configuration = None

#     def get_inputs(self):
#         images_4_ocr = self.odem.images_4_ocr
#         n_total = len(images_4_ocr)
#         pipeline_cfg = self.read_pipeline_config()
#         input_data = [(img, i, n_total, self.logger, pipeline_cfg)
#                       for i, img in enumerate(self.odem.images_4_ocr, start=1)]
#         return input_data

#     def run(self, input_data):

#         image_path = input_data[0][0]
#         pipeline_result = run_pipeline(input_data)
#         stored = pipeline_result is not None
#         mps = 0
#         filesize_mb = 0
#         filestat = os.stat(image_path)
#         if filestat:
#             filesize_mb = filestat.st_size / 1048576
#         (mps, _) = get_imageinfo(image_path)
#         return stored, 1, mps, filesize_mb
        
#     def read_pipeline_config(self, path_config=None) -> configparser.ConfigParser:
#         """Read pipeline configuration and replace
#         model_configs with known language data"""
        
#         if self.pipeline_configuration is None:
#             if path_config is None:
#                 if self.odem_configuration.has_option(odem_c.CFG_SEC_OCR, 'ocr_pipeline_config'):
#                     path_config = os.path.abspath(self.odem_configuration.get(odem_c.CFG_SEC_OCR, 'ocr_pipeline_config'))
#             if not os.path.isfile(path_config):
#                 raise odem_c.ODEMException(f"no ocr-pipeline conf {path_config} !")
#             pipe_cfg = configparser.ConfigParser()
#             pipe_cfg.read(path_config)
#             self.logger.info(f"use config '{path_config}'")
#             for sect in pipe_cfg.sections():
#                 if pipe_cfg.has_option(sect, 'model_configs'):
#                     known_langs = self.odem._statistics_ocr.get(odem_c.STATS_KEY_LANGS)
#                     model_files = self.odem.language_modelconfig(known_langs)
#                     models = model_files.replace('.traineddata','')
#                     pipe_cfg.set(sect, 'model_configs', models)
#                 if pipe_cfg.has_option(sect, STEP_MOVE_PATH_TARGET):
#                     pipe_cfg.set(sect, STEP_MOVE_PATH_TARGET, f'{self.odem.work_dir_main}/FULLTEXT')
#             self.pipeline_configuration = pipe_cfg
#         return self.pipeline_configuration

#     def foster_outputs(self):
#         self.ocr_files = odem_c.list_files(self.odem.work_dir_main, odem_c.FILEGROUP_FULLTEXT)
#         strip_tags = self.cfg.getlist(odem_c.CFG_SEC_OCR, 'strip_tags')
#         for _ocr_file in self.ocr_files:
#             postprocess_ocr_file(_ocr_file, strip_tags)
