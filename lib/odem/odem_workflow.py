"""ODEM Workflow API"""

from __future__ import annotations

import concurrent.futures
import configparser
import logging
import os
import shutil
import subprocess
import sys
import time
import typing

import lib.odem.odem_commons as odem_c
import lib.odem.odem_process as odem_p
import lib.odem.ocr.ocr_d as odem_ocrd
import lib.odem.ocr.ocr_pipeline as odem_tess
import lib.odem.processing.image as odem_img
import lib.odem.processing.ocr_files as odem_fmt

# estimated ocr-d runtime
# for a regular page (A4, 1MB)
DEFAULT_RUNTIME_PAGE = 1.0
# process duration format
ODEM_PAGE_TIME_FORMAT = '%Y-%m-%d_%H-%m-%S'
# how long to process single page?
DEFAULT_DOCKER_CONTAINER_TIMEOUT = 600

LOCAL_OCRD_RESULT_DIR = 'PAGE'


class ODEMWorkflowRunner:
    """Wrap actual ODEM process execution"""

    def __init__(self, identifier, n_executors,
                 internal_logger, odem_workflow) -> None:
        self.process_identifier = identifier
        self.n_executors = n_executors
        self.logger: logging.Logger = internal_logger
        self.odem_workflow: ODEMWorkflow = odem_workflow

    def run(self):
        """Actual run wrapper"""
        input_data = self.odem_workflow.get_inputs()
        self.logger.info("[%s] run %d images with %d executors (%s)",
                         self.process_identifier, len(input_data), self.n_executors,
                         self.odem_workflow.__class__.__name__)
        if self.n_executors > 1:
            raw_returned = self.run_parallel(input_data)
        else:
            raw_returned = self.run_sequential(input_data)
        n_processed = len(raw_returned)
        self.logger.info("[%s] processed %d candidates",
                         self.process_identifier, n_processed)
        filter_set = [r for r in raw_returned
                      if r.local_path != odem_c.UNSET]
        the_unsets = n_processed - len(filter_set)
        self.logger.info("[%s] from %d candidates filtered %d unset",
                         self.process_identifier, len(raw_returned),
                         the_unsets)
        self.odem_workflow.process_outputs(filter_set)
        self.logger.info("[%s] created %d ocr files for %d images",
                         self.process_identifier,
                         len(self.odem_workflow.ocr_results), len(input_data))
        return self.odem_workflow.ocr_results

    def run_parallel(self, input_data):
        """Run workflow parallel with given executors"""

        n_inputs = len(input_data)
        self.logger.info("[%s] %d inputs run_parallel with %d executors",
                         self.process_identifier, n_inputs, self.n_executors)
        try:
            outcomes = []
            with concurrent.futures.ThreadPoolExecutor(
                    max_workers=self.n_executors,
                    thread_name_prefix='odem.ocrd'
            ) as executor:
                outcomes = list(executor.map(self.odem_workflow.run, input_data))
            self.logger.info("[%s] created %d results with %d executors",
                             self.process_identifier, len(outcomes),
                             self.n_executors)
            return outcomes
        except (OSError, AttributeError) as err:
            self.logger.error("[%s] %s ", self.process_identifier, err)
            raise odem_c.ODEMException(f"ODEM parallel: {err.args[0]}") from err
        except:
            last_exc = str(sys.exc_info())
            self.logger.error("[%s] %s ", self.process_identifier, last_exc)
            raise odem_c.ODEMException(f"ODEM parallel: {last_exc}")

    def run_sequential(self, input_data):
        """run complete workflow plain sequential
        For debugging or small machines
        """

        len_img = len(input_data)
        estm_min = len_img * DEFAULT_RUNTIME_PAGE
        self.logger.info("[%s] %d inputs run_sequential, estm. %dmin",
                         self.process_identifier, len_img, estm_min)
        try:
            outcomes = [self.odem_workflow.run(the_input)
                        for the_input in input_data]
            return outcomes
        except (OSError, AttributeError) as err:
            self.logger.error(err)
            raise odem_c.ODEMException(f"ODEM sequential: {err.args[0]}") from err


class ODEMWorkflow:
    """Base Interface"""

    @staticmethod
    def create(
            workflow_type: odem_c.OdemWorkflowProcessType | str,
            odem: odem_p.ODEMProcessImpl,
    ) -> ODEMWorkflow:
        """Create actual instance"""
        if workflow_type == odem_c.OdemWorkflowProcessType.ODEM_TESSERACT:
            return ODEMTesseract(odem)
        return OCRDPageParallel(odem)

    def __init__(self, odem_process: odem_p.ODEMProcessImpl):
        self.odem_process = odem_process
        self.config = odem_process.configuration
        self.logger = odem_process.logger
        self.ocr_results: typing.List[odem_c.OCRResult] = []

    def get_inputs(self) -> typing.List:
        """Collect all input data files to run for ocr-ing"""

    def run(self, _: typing.List) -> odem_c.OCRResult:
        """Run actual implemented Workflow to generate
        single OCR Result"""

    def process_outputs(self, the_outcomes: typing.List[odem_c.OCRResult]):
        """Work to do after pipeline has been run successfully
        like additional format transformations or sanitizings
        """


class OCRDPageParallel(ODEMWorkflow):
    """Use page parallel workflow"""

    def get_inputs(self):
        return self.odem_process.ocr_candidates

    def run(self, input_data):

        ocr_log_conf = os.path.join(
            odem_c.PROJECT_ROOT, self.config.get(odem_c.CFG_SEC_OCR, 'ocrd_logging'))

        # Preprare workspace with makefile
        (image_path, ident) = input_data
        os.chdir(self.odem_process.work_dir_root)
        file_name = os.path.basename(image_path)
        file_id = file_name.split('.')[0]
        page_workdir = os.path.join(self.odem_process.work_dir_root, file_id)
        if os.path.exists(page_workdir):
            shutil.rmtree(page_workdir, ignore_errors=True)
        os.mkdir(page_workdir)
        shutil.copy(ocr_log_conf, page_workdir)
        os.chdir(page_workdir)

        # move and convert image data at once
        processed_image_path = odem_img.ensure_image_format(image_path, page_workdir)

        # init ocr-d workspace
        odem_ocrd.ocrd_workspace_setup(page_workdir, processed_image_path)

        # find model config for tesseract
        model_config = self.odem_process.map_language_to_modelconfig(image_path)

        stored = odem_c.UNSET
        mps = 0
        filesize_mb = 0
        # use original image rather than
        # transformed one since PNG is
        # usually 2-5 times larger than JPG
        filestat = os.stat(image_path)
        if filestat:
            filesize_mb = filestat.st_size / 1048576
        (mps, dpi) = odem_img.get_imageinfo(image_path)

        # how to identify data set?
        if self.odem_process.record:
            _ident = self.odem_process.process_identifier
        else:
            _ident = os.path.basename(self.odem_process.work_dir_root)
        # OCR Generation
        profiling = ('n.a.', 0)

        container_name: str = f'{self.odem_process.process_identifier}_{os.path.basename(page_workdir)}'
        container_memory_limit: str = self.config.get(odem_c.CFG_SEC_OCR,
                                                      'docker_container_memory_limit',
                                                      fallback=None)
        container_user = self.config.get(odem_c.CFG_SEC_OCR,
                                         'docker_container_user', fallback=os.getuid())
        container_timeout: int = self.config.getint(
            odem_c.CFG_SEC_OCR,
            'docker_container_timeout',
            fallback=DEFAULT_DOCKER_CONTAINER_TIMEOUT
        )
        base_image = self.config.get(odem_c.CFG_SEC_OCR, 'ocrd_baseimage')
        ocrd_process_list = self.config.getlist(odem_c.CFG_SEC_OCR, 'ocrd_process_list')
        tesseract_model_rtl: typing.List[str] = self.config.getlist(odem_c.CFG_SEC_OCR,
                                                                    'tesseract_model_rtl',
                                                                    fallback=odem_c.DEFAULT_RTL_MODELS)
        ocrd_resources_volumes: typing.Dict[str, str] = self.config.getdict(odem_c.CFG_SEC_OCR,
                                                                            odem_c.CFG_SEC_OCR_OPT_RES_VOL,
                                                                            fallback={})

        if self.odem_process.local_mode:
            container_name = os.path.basename(page_workdir)
        try:
            profiling = odem_ocrd.run_ocr_page(
                page_workdir,
                base_image,
                container_memory_limit,
                container_timeout,
                container_name,
                container_user,
                ocrd_process_list,
                model_config,
                ocrd_resources_volumes,
                tesseract_model_rtl,
            )
            # will be unset in case of magic mocking for test
            if profiling:
                self.logger.info("[%s] '%s' in %s (%.1fMP, %dDPI, %.1fMB)",
                                 _ident, profiling[1], profiling[0], mps, dpi, filesize_mb)
            self.logger.info("[%s] run ocr creation in '%s'",
                             _ident, page_workdir)
            stored = self._store_fulltext(page_workdir, image_path)
            if stored:
                self._preserve_log(page_workdir, ident)
        except (subprocess.CalledProcessError, subprocess.TimeoutExpired) as exc:
            self.logger.error("[%s] image '%s' failed due to subprocess timeout: %s",
                              _ident, base_image, exc)
        except Exception as plain_exc:
            self.logger.error("[%s] generic exc '%s' for image '%s'",
                              _ident, plain_exc, base_image)

        os.chdir(self.odem_process.work_dir_root)
        if self.config.getboolean(odem_c.CFG_SEC_OCR, 'keep_temp_orcd_data', fallback=False) is False:
            shutil.rmtree(page_workdir, ignore_errors=True)
        result = odem_c.OCRResult(stored)
        result.images_fsize = filesize_mb
        result.images_mps = mps
        return result

    def _preserve_log(self, work_subdir, image_ident):
        """preserve ocrd.log for later analyzis as
        sub directory identified by adopted local
        identifier (local section of system OAI handle)"""

        _root_log = self.config.get(odem_c.CFG_SEC_FLOW, 'local_log_dir')
        _local_ident = self.odem_process.process_identifier.replace('/', '_')
        _local_ocr_log = os.path.join(_root_log, _local_ident)
        if not os.path.exists(_local_ocr_log):
            os.makedirs(_local_ocr_log, exist_ok=True)

        _org_log = os.path.join(work_subdir, 'ocrd.log')
        if os.path.exists(_org_log):
            _ts = time.strftime(ODEM_PAGE_TIME_FORMAT, time.localtime())
            _log_label = f'ocrd_odem_{self.odem_process.process_identifier}_{image_ident}_{_ts}.log'
            _rebranded = os.path.join(work_subdir, _log_label)
            os.rename(_org_log, _rebranded)
            shutil.copy(_rebranded, _local_ocr_log)
        else:
            self.logger.warning("[%s] No ocrd.log in %s",
                                self.odem_process.process_identifier, work_subdir)

    def _store_fulltext(self, image_subdir, original_image_path) -> str:
        """Move OCR Result from Workspace Subdir to export folder if exists"""

        # inspect possible ocr result dirs from within
        # the OCR-D subordinate workspaces for each image
        old_id = os.path.basename(image_subdir)
        ocr_result_dir = os.path.join(image_subdir, LOCAL_OCRD_RESULT_DIR)
        if not os.path.isdir(ocr_result_dir):
            self.logger.info("[%s] no ocr results for '%s'",
                             self.odem_process.process_identifier, ocr_result_dir)
            return ""
        ocrs = [os.path.join(ocr_result_dir, ocr)
                for ocr in os.listdir(ocr_result_dir)
                if str(ocr).endswith('.xml')]
        self.logger.debug("[%s] %s ocr files",
                          self.odem_process.process_identifier, ocrs)
        if ocrs and len(ocrs) == 1:
            # propably need to rename
            # since file now is like 'PAGE_01.xml'
            renamed = os.path.join(ocr_result_dir, old_id + '.xml')
            os.rename(ocrs[0], renamed)
            # regular case: OAI Workflow
            if not self.odem_process.local_mode:
                # export to 'PAGE' dir
                wd_fulltext = os.path.join(self.odem_process.work_dir_root, LOCAL_OCRD_RESULT_DIR)
                if not os.path.exists(wd_fulltext):
                    os.mkdir(wd_fulltext)

            # special case: local runnings for straight evaluations
            else:
                wd_fulltext = os.path.dirname(original_image_path)

            # final storage
            target_path = os.path.join(wd_fulltext, old_id + '.xml')
            shutil.copy(renamed, target_path)
        return target_path

    def process_outputs(self, the_outcomes: typing.List[odem_c.OCRResult]):
        """In this case:
        * move files from dir PAGE to FULLTEXT
        * convert OCR format PAGE => ALTO
        * some additional tag stripping
        """

        n_candidates = len(self.odem_process.ocr_candidates)
        if len(the_outcomes) == 0 and n_candidates > 0:
            raise odem_c.ODEMException(f"No OCR result for {n_candidates} candidates created!")
        final_fulltext_dir = os.path.join(self.odem_process.work_dir_root,
                                          odem_c.FILEGROUP_FULLTEXT)
        if not os.path.isdir(final_fulltext_dir):
            os.makedirs(final_fulltext_dir, exist_ok=True)
        self.ocr_results = odem_fmt.convert_to_output_format(the_outcomes, final_fulltext_dir)
        self.logger.info("[%s] converted %d ocr results to alto",
                         self.odem_process.process_identifier, len(self.ocr_results))
        strip_tags = self.config.getlist(odem_c.CFG_SEC_OCR, 'strip_tags')
        for a_result in self.ocr_results:
            odem_fmt.postprocess_ocr_file(a_result.local_path, strip_tags)
        self.logger.info("[%s] postprocessed %d ocr files",
                         self.odem_process.process_identifier, len(self.ocr_results))


class ODEMTesseract(ODEMWorkflow):
    """Tesseract Runner"""

    def __init__(self, odem_process: odem_c.ODEMProcess):
        super().__init__(odem_process)
        self.pipeline_configuration = None

    def get_inputs(self):
        images_4_ocr = self.odem_process.ocr_candidates
        n_total = len(images_4_ocr)
        pipeline_cfg = self.read_pipeline_config()
        input_data = [(img, i, n_total, self.logger, pipeline_cfg)
                      for i, img in enumerate(self.odem_process.ocr_candidates, start=1)]
        return input_data

    def run(self, input_data):

        image_path = input_data[0][0]
        a_result: odem_c.OCRResult = odem_tess.run_pipeline(input_data)
        self.logger.debug("run_pipeline: '%s'", a_result)
        mps = 0
        filesize_mb = 0
        filestat = os.stat(image_path)
        if filestat:
            filesize_mb = filestat.st_size / 1048576
        (mps, _) = odem_img.get_imageinfo(image_path)
        a_result.images_fsize = filesize_mb
        a_result.images_mps = mps
        return a_result

    def read_pipeline_config(self, path_config=None) -> configparser.ConfigParser:
        """Read pipeline configuration and replace
        model_configs with known language data"""

        if self.pipeline_configuration is None:
            if path_config is None:
                if self.config.has_option(odem_c.CFG_SEC_OCR, "ocr_pipeline_config"):
                    path_config = os.path.abspath(self.config.get(odem_c.CFG_SEC_OCR,
                                                                  "ocr_pipeline_config"))
            if not os.path.isfile(path_config):
                raise odem_c.ODEMException(f"no ocr-pipeline conf {path_config} !")
            pipe_cfg = configparser.ConfigParser()
            pipe_cfg.read(path_config)
            self.logger.debug("use config '%s'", path_config)
            for sect in pipe_cfg.sections():
                if pipe_cfg.has_option(sect, "model_configs"):
                    lang_models = self.odem_process.process_statistics.get(odem_c.KEY_LANGUAGES)
                    if ".traineddata" not in lang_models:
                        lang_models = self.odem_process.resolve_language_modelconfig(lang_models)
                    lang_models = lang_models.replace('.traineddata', '')
                    pipe_cfg.set(sect, 'model_configs', lang_models)
                if pipe_cfg.has_option(sect, odem_tess.STEP_MOVE_PATH_TARGET):
                    move_target = f'{self.odem_process.work_dir_root}/FULLTEXT'
                    pipe_cfg.set(sect, odem_tess.STEP_MOVE_PATH_TARGET, move_target)
            self.pipeline_configuration = pipe_cfg
        return self.pipeline_configuration

    def process_outputs(self, the_outcomes: typing.List[odem_c.OCRResult]):
        """Additional processing to OCR results"""

        self.ocr_results = the_outcomes
        strip_tags = self.config.getlist(odem_c.CFG_SEC_OCR, 'strip_tags')
        for a_result in self.ocr_results:
            if a_result.local_path.exists():
                odem_fmt.postprocess_ocr_file(a_result.local_path, strip_tags)
            else:
                self.logger.warning("missing %s", a_result.local_path)
        if self.config.has_option(odem_c.CFG_SEC_OCR, "fulltext_subdir"):
            sub_dir = self.config.get(odem_c.CFG_SEC_OCR, "fulltext_subdir")
            final_dir = self.odem_process.work_dir_root / sub_dir
            if not final_dir.is_dir():
                final_dir.mkdir()
            for result in self.ocr_results:
                result.local_path = result.move(final_dir)
