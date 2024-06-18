# -*- coding: utf-8 -*-
"""OCR-Generation for OAI-Records"""

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
import digiflow.record as df_r

import lib.odem.odem_commons as odem_c
import lib.odem.processing.image as odem_image

from .processing.mets import (
    ODEMMetadataInspecteur,
    extract_text_content,
    integrate_ocr_file,
    postprocess_mets,
    validate,
)

# python process-wrapper limit
os.environ['OMP_THREAD_LIMIT'] = '1'
# default language fallback
# (only when processing local images)
DEFAULT_LANG = 'ger'


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

    def __init__(self, record: df_r.Record, work_dir,
                 log_dir=None, logger=None, configuration=None):
        """Create new ODEM Process.
        Args:
            record (OAIRecord): OAI Record dataset
            work_dir (_type_): required local work path
            executors (int, optional): Process pooling when running parallel.
                Defaults to 2.
            log_dir (_type_, optional): Path to store log file.
                Defaults to None.
        """

        super().__init__(configuration, work_dir_root=work_dir,
                         the_logger=logger, log_dir=log_dir, record=record)
        self.digi_type = None
        self.mods_identifier = None
        self.local_mode = record is None
        if self.local_mode:
            self.process_identifier = os.path.basename(work_dir)
        if record is not None and record.local_identifier is not None:
            self.process_identifier = record.local_identifier
        self.export_dir = None
        self.store: df.LocalStore = None
        self.ocr_files = []
        self._process_start = time.time()
        # self.mets_file = os.path.join(
        #     work_dir, os.path.basename(work_dir) + '.xml')

    def load(self):
        request_identifier = self.record.identifier
        local_identifier = self.record.local_identifier
        req_dst_dir = os.path.join(
            os.path.dirname(self.work_dir_root), local_identifier)
        if not os.path.exists(req_dst_dir):
            os.makedirs(req_dst_dir, exist_ok=True)
        # req_dst = os.path.join(req_dst_dir, local_identifier + '.xml')
        req_dst = self.mets_file_path
        self.the_logger.debug("[%s] download %s to %s",
                              self.process_identifier, request_identifier, req_dst)
        base_url = self.odem_configuration.get('global', 'base_url')
        try:
            loader = df.OAILoader(req_dst_dir, base_url=base_url, post_oai=dfm.extract_mets)
            loader.store = self.store
            loader.load(request_identifier, local_dst=req_dst)
        except df.ClientError as load_err:
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
        if os.path.exists(self.work_dir_root):
            shutil.rmtree(self.work_dir_root)

    def inspect_metadata(self):
        insp = ODEMMetadataInspecteur(self.mets_file_path,
                                      self.record.identifier,
                                      cfg=self.odem_configuration)
        try:
            the_report = insp.metadata_report()
            self.digi_type = the_report.type
            self.ocr_candidates = insp.image_pairs
        except RuntimeError as mde:
            raise odem_c.ODEMException(f"{mde.args[0]}") from mde
        self.mods_identifier = insp.mods_record_identifier
        for t, ident in insp.identifiers.items():
            self.process_statistics[t] = ident
        self.process_statistics['type'] = insp.type
        self.process_statistics[odem_c.STATS_KEY_LANGS] = insp.languages
        self.process_statistics['n_images_pages'] = insp.n_images_pages
        self.process_statistics['n_images_ocrable'] = insp.n_images_ocrable
        _ratio = insp.n_images_ocrable / insp.n_images_pages * 100
        self.the_logger.info("[%s] %04d (%.2f%%) images used for OCR (total: %04d)",
                             self.process_identifier, insp.n_images_ocrable, _ratio,
                             insp.n_images_pages)
        self.process_statistics['host'] = socket.gethostname()

    def clear_existing_entries(self):
        """Clear METS/MODS of configured file groups"""

        if self.odem_configuration:
            _blacklisted = self.odem_configuration.getlist('mets', 'blacklist_file_groups')
            _ident = self.process_identifier
            self.the_logger.info("[%s] remove %s", _ident, _blacklisted)
            _proc = df.MetsProcessor(self.mets_file_path)
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
            languages = self.process_statistics.get(odem_c.STATS_KEY_LANGS)
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
        self.process_statistics[odem_c.STATS_KEY_MODELS] = _model_conf
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

        image_dir = os.path.join(self.work_dir_root, 'MAX')
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
        _local_max_dir = os.path.join(self.work_dir_root, 'MAX')
        for _img, _urn in self.ocr_candidates:
            _the_file = os.path.join(_local_max_dir, _img)
            if not os.path.exists(_the_file):
                raise odem_c.ODEMException(f"[{self.process_identifier}] missing {_the_file}!")
            _images_of_interest.append((_the_file, _urn))
        self.ocr_candidates = _images_of_interest

    def calculate_statistics_ocr(self, outcomes: typing.List):
        """Calculate and aggregate runtime stats"""
        n_ocr = sum([e[0] for e in outcomes if e[0] == 1])
        _total_mps = [round(e[2], 1) for e in outcomes if e[0] == 1]
        _mod_val_counts = np.unique(_total_mps, return_counts=True)
        mps = list(zip(*_mod_val_counts))
        total_mb = sum([e[3] for e in outcomes if e[0] == 1])
        self.process_statistics[odem_c.STATS_KEY_N_OCR] = n_ocr
        self.process_statistics[odem_c.STATS_KEY_MB] = round(total_mb, 2)
        self.process_statistics[odem_c.STATS_KEY_MPS] = mps

    def link_ocr_files(self) -> int:
        """Prepare and link OCR-data"""

        list_from_dir = Path(self.work_dir_root) / odem_c.FILEGROUP_FULLTEXT
        self.ocr_files = odem_c.list_files(list_from_dir)
        if not self.ocr_files:
            return 0
        proc = df.MetsProcessor(self.mets_file_path)
        _n_linked_ocr = integrate_ocr_file(proc.tree, self.ocr_files)
        proc.write()
        return _n_linked_ocr

    def create_text_bundle_data(self):
        """create additional dspace bundle for indexing ocr text
        read ocr-file sequential according to their number label
        and extract every row into additional text file"""

        txt_lines = extract_text_content(self.ocr_files)
        txt_content = '\n'.join(txt_lines)
        _out_path = os.path.join(self.work_dir_root, f'{self.mods_identifier}.pdf.txt')
        with open(_out_path, mode='w', encoding='UTF-8') as _writer:
            _writer.write(txt_content)
        self.the_logger.info("[%s] harvested %d lines from %d ocr files to %s",
                             self.process_identifier, len(txt_lines), len(self.ocr_files), _out_path)
        self.process_statistics['n_text_lines'] = len(txt_lines)

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
            self.mets_file_path,
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

        work = self.work_dir_root
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

        postprocess_mets(self.mets_file_path, self.odem_configuration)

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
        return validate(self.mets_file_path, validate_ddb=check_ddb,
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
            exp_map[os.path.basename(self.mets_file_path)] = 'mets.xml'
        saf_name = self.mods_identifier
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
                tmp_zip_path, size = ODEMProcessImpl.compress_flat(os.path.dirname(work_dir), saf_name)
                path_export_processing = dfx.move_to_tmp_file(tmp_zip_path, exp_dst)
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
