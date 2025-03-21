"""OCR-Generation for records
Created as implementation project ODEM in OCR-D phase III 2021-2024

cf. https://gepris.dfg.de/gepris/projekt/460554747
"""

from __future__ import annotations

import datetime
import os
import shutil
import socket
import subprocess
import tempfile
import time
import typing

from pathlib import Path

import numpy as np
import digiflow as df
import digiflow.digiflow_export as dfx
import digiflow.digiflow_metadata as dfm
import digiflow.digiflow_io as dfo
import digiflow.record as df_r

import lib.odem.odem_commons as odem_c
import lib.odem.processing.image as odem_image

import lib.odem.processing.mets as odem_mets

# python process-wrapper limit
os.environ['OMP_THREAD_LIMIT'] = '1'


class ODEMModelMissingException(odem_c.ODEMException):
    """Mark ODEM process misses model configuration"""


class ODEMProcessImpl(odem_c.ODEMProcess):
    """Create OCR for OAI Records.

        Runs both wiht OAIRecord or local path as input.
        process_identifier may represent a local directory
        or the local part of an OAI-URN.

        Languages for ocr-ing are assumed to be enriched in
        OAI-Record-Metadata (MODS) or be part of local
        paths. They will be applied by a custom mapping
        for the underlying OCR-Engine Tesseract-OCR.
    """

    def __init__(self, configuration=None, work_dir=None,
                 logger=None, log_dir=None, record: df_r.Record = None):
        """Create new ODEM Process.
        Args:
            record (OAIRecord): OAI Record dataset
            work_dir (_type_): required local work path
                resolved to be an absolute Path
            executors (int, optional): Process pooling when running parallel.
                Defaults to 2.
            log_dir (_type_, optional): Path to store log file.
                Defaults to None.
        """

        super().__init__(configuration, work_dir=work_dir,
                         logger=logger, log_dir=log_dir, record=record)
        self.mods_identifier = None
        self.process_identifier = self.work_dir_root.name
        if record is not None and record.local_identifier is not None:
            self.process_identifier = record.local_identifier
        self.export_dir = None
        self.store: df.LocalStore = None
        self.ocr_files = []
        self._process_start = time.time()

    def load(self):
        request_identifier = self.record.identifier
        local_identifier = self.record.local_identifier
        if not self.configuration.has_option(odem_c.CFG_SEC_FLOW,
                                             odem_c.CFG_SEC_FLOW_OPT_URL):
            self.logger.info("[%s] no download basis provided", self.process_identifier)
            return
        oai_base_url = self.configuration.get(odem_c.CFG_SEC_FLOW,
                                              odem_c.CFG_SEC_FLOW_OPT_URL)
        req_dst_dir = os.path.join(
            os.path.dirname(self.work_dir_root), local_identifier)
        if not os.path.exists(req_dst_dir):
            os.makedirs(req_dst_dir, exist_ok=True)
        req_dst = self.mets_file_path
        self.logger.debug("[%s] download %s to %s",
                          self.process_identifier, request_identifier, req_dst)
        try:
            req_kwargs = {}
            if self.configuration.has_option(odem_c.CFG_SEC_FLOW,
                                             odem_c.CFG_SEC_FLOW_OPT_URL_KWARGS):
                requests_kwargs = self.configuration.get(odem_c.CFG_SEC_FLOW,
                                                         odem_c.CFG_SEC_FLOW_OPT_URL_KWARGS)
                req_kwargs = {dfo.OAI_KWARG_REQUESTS: requests_kwargs}
            load_fgroup = self.configuration.get(odem_c.CFG_SEC_METS,
                                                 odem_c.CFG_SEC_METS_FGROUP,
                                                 fallback=odem_c.DEFAULT_FGROUP)
            req_kwargs[dfo.OAI_KWARG_FGROUP_IMG] = load_fgroup
            loader = df.OAILoader(req_dst_dir, base_url=oai_base_url,
                                  post_oai=dfm.extract_mets, **req_kwargs)
            loader.store = self.store
            use_file_id = self.configuration.getboolean(odem_c.CFG_SEC_FLOW,
                                                        odem_c.CFG_SEC_FLOW_USE_FILEID,
                                                        fallback=False)
            loader.load(request_identifier, local_dst=req_dst, use_file_id=use_file_id)
        except df.ClientError as load_err:
            raise odem_c.ODEMException(load_err.args[0]) from load_err
        except RuntimeError as _err:
            raise odem_c.ODEMException(_err.args[0]) from _err

    def clear_mets_resources(self):
        """Remove OAI-Resources from store or even
        anything related to current process
        """

        if self.store is not None:
            sweeper = df.OAIFileSweeper(self.store.dir_store_root, '.xml')
            sweeper.sweep()
        if os.path.exists(self.work_dir_root):
            shutil.rmtree(self.work_dir_root)

    def inspect_metadata(self):
        insp = odem_mets.ODEMMetadataInspecteur(self.mets_file_path,
                                                self.record.identifier,
                                                cfg=self.configuration)
        try:
            insp.read()
            (mets, pica) = insp.types
            self.record.info["mets"] = mets
            self.record.info["pica"] = pica
            self.ocr_candidates = insp.image_pairs
        except RuntimeError as mde:
            raise odem_c.ODEMException(f"{mde.args[0]}") from mde
        self.mods_identifier = insp.mods_record_identifier
        for t, ident in insp.identifiers.items():
            self.process_statistics[t] = ident
        self.process_statistics['type'] = insp.types
        self.process_statistics[odem_c.STATS_KEY_LANGS] = insp.languages
        self.process_statistics['n_images_pages'] = insp.n_images_pages
        self.process_statistics['n_images_ocrable'] = insp.n_images_ocrable
        ocrable_ratio = insp.n_images_ocrable / insp.n_images_pages * 100
        if self.logger is not None:
            self.logger.info("[%s] %04d (%.2f%%) images used for OCR (total: %04d)",
                         self.process_identifier, insp.n_images_ocrable, ocrable_ratio,
                         insp.n_images_pages)
        self.process_statistics['host'] = socket.gethostname()

    def modify_mets_groups(self):
        """Clear METS/MODS of configured file groups"""

        blacklisted = self.configuration.getlist('mets', 'blacklist_file_groups')
        if len(blacklisted) == 0:
            self.logger.warning("[%s] no exsting METS filegroups removed",
                                self.process_identifier)
            return
        ident = self.process_identifier
        self.logger.info("[%s] remove %s", ident, blacklisted)
        proc = df.MetsProcessor(self.mets_file_path)
        proc.clear_filegroups(blacklisted)
        try:
            proc.write()
        except PermissionError:
            self.logger.error("[%s] permission denied %s", self.process_identifier,
                              self.mets_file_path)

    def resolve_language_modelconfig(self, languages=None) -> str:
        """resolve model configuration from
        * provided "languages" parameter
        * else use metadata language entries.

        Please note: Configured model mappings
        might contain compositions, therefore
        the additional inner loop
        """

        models = []
        model_mappings: dict = self.configuration.getdict(  # pylint: disable=no-member
            odem_c.CFG_SEC_OCR, 'model_mapping')
        if languages is None:
            languages = self.process_statistics.get(odem_c.STATS_KEY_LANGS)
        self.logger.info("[%s] map languages '%s'",
                         self.process_identifier, languages)
        for lang in languages:
            model_entry = model_mappings.get(lang)
            if not model_entry:
                raise ODEMModelMissingException(f"'{lang}' mapping not found (languages: {languages})!")
            for model in model_entry.split('+'):
                if self._is_model_available(model):
                    models.append(model)
                else:
                    raise ODEMModelMissingException(f"'{model}' model config not found !")
        model_cfg = models[0]
        if self.configuration.getboolean(odem_c.CFG_SEC_OCR, "model_combinable",
                                         fallback=True):
            model_cfg = '+'.join(models)
        self.process_statistics[odem_c.KEY_LANGUAGES] = model_cfg
        self.logger.info("[%s] mapped languages '%s' => '%s'",
                         self.process_identifier, languages, model_cfg)
        return model_cfg

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

        file_lang_suffixes = odem_c.DEFAULT_LANG
        # inspect language arg
        if self.configuration.has_option(odem_c.CFG_SEC_OCR, odem_c.KEY_LANGUAGES):
            file_lang_suffixes = self.configuration.get(odem_c.CFG_SEC_OCR,
                                                        odem_c.KEY_LANGUAGES).split('+')
            return self.resolve_language_modelconfig(file_lang_suffixes)
        # inspect final '_' segment of local file names
        if self.local_mode:
            try:
                image_name = Path(image_path).stem
                if '_' not in image_name:
                    raise odem_c.ODEMException(f"Miss language mark for '{image_name}'!")
                file_lang_suffixes = image_name.split('_')[-1].split('+')
            except odem_c.ODEMException as oxc:
                self.logger.warning("[%s] language mapping err '%s' for '%s', fallback to %s",
                                    self.process_identifier, oxc.args[0],
                                    image_path, odem_c.DEFAULT_LANG)
            return self.resolve_language_modelconfig(file_lang_suffixes)
        # inspect language information from MODS metadata
        return self.resolve_language_modelconfig()

    def _is_model_available(self, model) -> bool:
        """Determine whether model is available at execting
        host/machine"""

        resource_dir_mappings = self.configuration.getdict(odem_c.CFG_SEC_OCR,
                                                           odem_c.CFG_SEC_OCR_OPT_RES_VOL,
                                                           fallback={})
        for host_dir in resource_dir_mappings:
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

        image_dir = os.path.join(self.work_dir_root, 'MAX')
        if image_local_dir:
            if not isinstance(image_local_dir, Path):
                image_local_dir = Path(image_local_dir).resolve()
            if not image_local_dir.is_dir():
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

        self.logger.info("[%s] %d images total",
                         self.process_identifier, len(images))
        return images

    def set_local_images(self):
        """Construct pairs of local paths for 
        (optional previously filtered by object metadata)
        images and original page urn
        """
        images_of_interest = []
        images_dir = 'MAX'
        if self.configuration.has_option(odem_c.CFG_SEC_OCR, odem_c.CFG_SEC_OCR_OPT_IMG_SUBDIR):
            images_dir = self.configuration.get(odem_c.CFG_SEC_OCR,
                                                odem_c.CFG_SEC_OCR_OPT_IMG_SUBDIR)
        local_img_dir = os.path.join(self.work_dir_root, images_dir)
        self.logger.debug("[%s] inspect local image dir %s",
                          self.process_identifier, local_img_dir)
        for img, urn in self.ocr_candidates:
            the_file = os.path.join(local_img_dir, img)
            if not os.path.exists(the_file):
                raise odem_c.ODEMException(f"[{self.process_identifier}] missing {the_file}!")
            images_of_interest.append((the_file, urn))
        self.ocr_candidates = images_of_interest

    def postprocess(self, ocr_results: typing.List[odem_c.OCRResult]):
        """Encapsulate after-OCR workflow"""

        if ocr_results is None or len(ocr_results) == 0:
            raise odem_c.ODEMException(f"process run error: {self.record.identifier}")
        self.calculate_statistics_ocr(ocr_results)
        self.process_statistics[odem_c.STATS_KEY_N_EXECS] = self.configuration.get(
            odem_c.CFG_SEC_OCR,
            odem_c.CFG_SEC_OCR_OPT_EXECS)
        self.logger.info("[%s] %s", self.record.local_identifier, self.statistics)
        wf_enrich_ocr = self.configuration.getboolean(odem_c.CFG_SEC_METS,
                                                      odem_c.CFG_SEC_METS_OPT_ENRICH,
                                                      fallback=True)
        if wf_enrich_ocr:
            self.link_ocr_files()
        wf_create_derivates = self.configuration.getboolean('derivans', 'derivans_enabled',
                                                            fallback=False)
        if wf_create_derivates:
            self.create_derivates()
            self.postprocess_review_derivans_agents()
        if self.configuration.getboolean(odem_c.CFG_SEC_FLOW, odem_c.CFG_SEC_FLOW_OPT_TEXTLINE,
                                         fallback=False):
            self.create_text_bundle_data()
        # METS postprocessing has own configuration options
        self.postprocess_mets()
        if self.configuration.getboolean(odem_c.CFG_SEC_METS, 'postvalidate', fallback=True):
            self.validate_metadata()
        if self.configuration.getboolean(odem_c.CFG_SEC_EXP,
                                         odem_c.CFG_SEC_EXP_ENABLED, fallback=False):
            self.logger.info("[%s] start export", self.process_identifier)
            if self.configuration.has_option(odem_c.CFG_SEC_FLOW,
                                             odem_c.CFG_SEC_FLOW_OPT_DELETE_DIRS):
                del_dirs = self.configuration.getlist(odem_c.CFG_SEC_FLOW,
                                                      odem_c.CFG_SEC_FLOW_OPT_DELETE_DIRS)
                if len(del_dirs) > 0:
                    self.delete_local_directories(del_dirs)
            self.export_data()
        if self.configuration.getboolean(odem_c.CFG_SEC_FLOW,
                                         odem_c.CFG_SEC_FLOW_OPT_REM_RES,
                                         fallback=False):
            self.clear_mets_resources()

    def calculate_statistics_ocr(self, outcomes: typing.List[odem_c.OCRResult]):
        """Calculate stats from given ODEMOutcomes"""

        n_ocr_created = len(outcomes)
        self.logger.info("[%s] calculate statistics for %d results",
                         self.process_identifier,
                         n_ocr_created)
        total_mps = [round(o.images_mps, 1) for o in outcomes]
        mod_val_counts = np.unique(total_mps, return_counts=True)
        mps_np = list(zip(*mod_val_counts))
        mps = [(float(pair[0]), int(pair[1])) for pair in mps_np]  # since numpy 2.x
        total_mb = sum([o.images_fsize for o in outcomes], 0)
        self.process_statistics[odem_c.STATS_KEY_N_OCR] = n_ocr_created
        self.process_statistics[odem_c.STATS_KEY_MB] = round(total_mb, 2)
        self.process_statistics[odem_c.STATS_KEY_MPS] = mps
        n_ocr_cands = len(self.ocr_candidates)
        if n_ocr_created != n_ocr_cands:
            self.logger.warning("[%s] %d ocr candidates != %d ocr results",
                                self.process_identifier, n_ocr_cands,
                                n_ocr_created)
            img_candidate_names = [Path(pair[0]).stem
                                   for pair in self.ocr_candidates
                                   if isinstance(pair, tuple)]
            ocr_names = [Path(o.local_path).stem for o in outcomes]
            data_loss = set(img_candidate_names) ^ set(ocr_names)
            if len(data_loss) > 0:
                self.process_statistics[odem_c.STATS_KEY_OCR_LOSS] = list(data_loss)

    def link_ocr_files(self) -> int:
        """Prepare and link OCR-data"""

        list_from_dir = Path(self.work_dir_root) / odem_c.FILEGROUP_FULLTEXT
        self.ocr_files = odem_c.list_files(list_from_dir)
        if not self.ocr_files:
            return 0
        proc = df.MetsProcessor(self.mets_file_path)
        n_linked_ocr, n_dropped = odem_mets.integrate_ocr_file(proc.root, self.ocr_files)
        if n_dropped > 0:
            self.logger.warning("[%s] failed to link %d ocr files",
                                self.process_identifier, n_dropped)
        try:
            proc.write()
        except PermissionError:
            self.logger.error("[%s] permission error: can't link OCR files in %s",
                              self.process_identifier, self.mets_file_path)
            return 0
        return n_linked_ocr

    def create_text_bundle_data(self):
        """create additional dspace bundle for indexing ocr text
        read ocr-file sequential according to their number label
        and extract every row into additional text file"""

        txt_lines = odem_mets.extract_text_content(self.ocr_files)
        txt_content = '\n'.join(txt_lines)
        out_path = os.path.join(self.work_dir_root, f'{self.mods_identifier}.pdf.txt')
        with open(out_path, mode='w', encoding='UTF-8') as tl_writer:
            tl_writer.write(txt_content)
        self.logger.info("[%s] harvested %d lines from %d ocr files to %s",
                         self.process_identifier, len(txt_lines),
                         len(self.ocr_files), out_path)
        self.process_statistics['n_text_lines'] = len(txt_lines)

    def create_derivates(self):
        """Forward PDF-creation to Derivans"""

        cfg_path_dir_bin = self.configuration.get('derivans', 'derivans_dir_bin', fallback=None)
        path_bin = None
        if cfg_path_dir_bin is not None:
            path_bin = os.path.join(odem_c.PROJECT_ROOT, cfg_path_dir_bin)
        cfg_path_dir_project = self.configuration.get('derivans', 'derivans_dir_project',
                                                      fallback=None)
        path_prj = None
        if cfg_path_dir_project is not None:
            path_prj = os.path.join(odem_c.PROJECT_ROOT, cfg_path_dir_project)
        path_cfg = os.path.join(
            odem_c.PROJECT_ROOT,
            self.configuration.get('derivans', 'derivans_config')
        )
        derivans_image = self.configuration.get('derivans', 'derivans_image', fallback=None)
        path_logging = self.configuration.get('derivans', 'derivans_logdir', fallback=None)
        derivans: df.BaseDerivansManager = df.BaseDerivansManager.create(
            self.mets_file_path,
            container_image_name=derivans_image,
            path_binary=path_bin,
            path_configuration=path_cfg,
            path_mvn_project=path_prj,
            path_logging=path_logging,
        )
        if self.configuration.has_option(odem_c.CFG_SEC_DERIVANS, odem_c.CFG_SEC_DERIVANS_FGROUP):
            the_fgroup = self.configuration.get(odem_c.CFG_SEC_DERIVANS, odem_c.CFG_SEC_DERIVANS_FGROUP)
            derivans.images = the_fgroup
        derivans.init()
        # be cautious
        try:
            dresult: df.DerivansResult = derivans.start()
            self.logger.info("[%s] create derivates in %.1fs",
                             self.process_identifier, dresult.duration)
        except subprocess.CalledProcessError as _sub_err:
            err_msg = _sub_err.stdout.decode().split(os.linesep)[0].replace("'", "\"")
            err_args = [err_msg]
            err_args.extend(_sub_err.args)
            raise odem_c.ODEMException(err_args) from _sub_err

    def delete_local_directories(self, folders):
        """delete folders given by list"""

        work = self.work_dir_root
        self.logger.debug(
            "[%s] delete sub_dirs: %s", self.process_identifier, folders)
        for folder in folders:
            delete_me = os.path.join(work, folder)
            if os.path.exists(delete_me):
                shutil.rmtree(delete_me)

    def postprocess_mets(self):
        """wrap work related to processing METS/MODS"""

        try:
            odem_mets.postprocess_mets(self.mets_file_path, self.configuration)
        except PermissionError:
            self.logger.error("[%s] permission error: can't alter mets in %s",
                              self.process_identifier, self.mets_file_path)

    def postprocess_review_derivans_agents(self):
        """Wrap work related to changed derivans
        METS-agent entries
        """
        try:
            odem_mets.process_mets_derivans_agents(self.mets_file_path, self.configuration)
        except PermissionError:
            self.logger.error("[%s] permission error: can't alter derivans' mets:agents in %s",
                              self.process_identifier, self.mets_file_path)

    def validate_metadata(self):
        """Forward (optional) validation concerning
        METS/MODS XML-schema and/or current DDB-schematron
        validation for 'digitalisierte medien'
        """

        if not self.configuration.getboolean('mets', 'prevalidate', fallback=True):
            self.logger.warning("[%s] skipping pre-validation",
                                self.process_identifier)
            return
        if not self.configuration.getboolean('mets', 'postvalidate', fallback=False):
            self.logger.warning("[%s] skipping post-validation",
                                self.process_identifier)
            return
        ignore_ddb = []
        if self.configuration.has_option('mets', 'ddb_validation_ignore'):
            raw_ignore_str = self.configuration.get('mets', 'ddb_validation_ignore')
            ignore_ddb = [i.strip() for i in raw_ignore_str.split(',')]
        ddb_min_level = 'fatal'
        if self.configuration.has_option('mets', 'ddb_min_level'):
            ddb_min_level = self.configuration.get('mets', 'ddb_min_level')
        the_type = self.record.info["pica"]
        if the_type is None:
            the_type = "Ac"
            if self.logger is not None:
                self.logger.warning("[%s] no prime_mods pica type present, fallbabk to Ac",
                                    self.process_identifier)
        if self.logger is not None:
            self.logger.info("[%s] validate type %s ddb_ignore: %s", self.process_identifier,
                             the_type, ignore_ddb)
        return odem_mets.validate_mets(self.mets_file_path, digi_type=the_type,
                                       ddb_ignores=ignore_ddb,
                                       ddb_min_level=ddb_min_level)

    def export_data(self):
        """re-do metadata and transform into output format"""

        export_format: str = self.configuration.get(odem_c.CFG_SEC_EXP,
                                                    odem_c.CFG_SEC_EXP_OPT_FORMAT,
                                                    fallback=odem_c.ExportFormat.SAF)
        export_mets: bool = self.configuration.getboolean(odem_c.CFG_SEC_EXP,
                                                          odem_c.CFG_SEC_EXP_OPT_METS,
                                                          fallback=True)

        exp_dst = self.configuration.get(odem_c.CFG_SEC_EXP, odem_c.CFG_SEC_EXP_OPT_DST)
        exp_tmp = self.configuration.get(odem_c.CFG_SEC_EXP, odem_c.CFG_SEC_EXP_OPT_TMP)
        exp_col = self.configuration.get(odem_c.CFG_SEC_EXP,
                                         odem_c.CFG_SEC_EXP_OPT_COLLECTION)
        exp_map = self.configuration.getdict(odem_c.CFG_SEC_EXP,
                                             odem_c.CFG_SEC_EXP_OPT_MAPPINGS)
        exp_prefix = self.configuration.get(odem_c.CFG_SEC_EXP,
                                            odem_c.CFG_SEC_EXP_OPT_PREFIX,
                                            fallback=None)
        # overwrite default mapping *.xml => 'mets.xml'
        # since we will have currently many more XML-files
        # created due OCR and do more specific mapping, though
        exp_map = {k: v for k, v in exp_map.items() if v != 'mets.xml'}
        if export_mets:
            exp_map[os.path.basename(self.mets_file_path)] = 'mets.xml'
        saf_name = self.mods_identifier
        if exp_prefix is not None:
            saf_name = f"{exp_prefix}{saf_name}"
        if export_format == odem_c.ExportFormat.SAF:
            export_result = df.export_data_from(
                self.mets_file_path,
                exp_col,
                saf_final_name=saf_name,
                export_dst=exp_dst,
                export_map=exp_map,
                tmp_saf_dir=exp_tmp,
            )
        elif export_format == odem_c.ExportFormat.FLAT_ZIP:
            prefix = 'opendata-working-'
            source_path_dir = os.path.dirname(self.mets_file_path)
            tmp_dir = tempfile.gettempdir()
            if exp_tmp:
                tmp_dir = exp_tmp
            with tempfile.TemporaryDirectory(prefix=prefix, dir=tmp_dir) as tmp_dir:
                work_dir = os.path.join(tmp_dir, saf_name)
                export_mappings = df.map_contents(source_path_dir, work_dir, exp_map)
                for mapping in export_mappings:
                    mapping.copy()
                tmp_zip_path, size = ODEMProcessImpl.compress_flat(os.path.dirname(work_dir),
                                                                   saf_name)
                path_export_processing = dfx.move_to_tmp_file(tmp_zip_path, exp_dst)
                export_result = path_export_processing, size
        else:
            raise odem_c.ODEMException(f'Unsupported export format: {export_format}')
        self.logger.info("[%s] exported data: %s",
                         self.process_identifier, export_result)
        if export_result:
            pth, size = export_result
            self.logger.info("[%s] create %s (%s)",
                             self.process_identifier, pth, size)
            # final re-move at export destination
            if '.processing' in str(pth):
                final_path = pth.replace('.processing', '')
                self.logger.debug('[%s] rename %s to %s',
                                  self.process_identifier, pth, final_path)
                shutil.move(pth, final_path)
                return final_path, size
        return None

    @classmethod
    def compress_flat(cls, work_dir, archive_name):
        """Create flat ZIP file (instead of SAF with items)"""
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
    def mets_file_path(self) -> Path:
        """Get actual METS/MODS file from work_dir"""
        mets_file = f"{os.path.basename(self.work_dir_root)}.xml"
        return Path(self.work_dir_root) / mets_file

    @mets_file_path.setter
    def mets_file_path(self, mets_path):
        """Set enclosed MET/MODS data for testing purposes"""
        mets_dir = os.path.dirname(mets_path)
        self.work_dir_root = mets_dir

    @property
    def statistics(self):
        """Get some statistics as dictionary
        with execution duration updated each call by
        requesting it's string representation"""

        current_duration = datetime.timedelta(seconds=round(time.time() - self._process_start))
        self.process_statistics['timedelta'] = f'{current_duration}'
        return self.process_statistics
