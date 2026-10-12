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
from urllib.parse import urlsplit

import numpy as np
import PIL.Image
import requests
from requests.structures import CaseInsensitiveDict
import digiflow as df
import digiflow.digiflow_export as dfx
import digiflow.digiflow_io as dfo
import digiflow.record as df_r

import lib.odem.commons as oc
import lib.odem.processing.image as odem_image

import lib.odem.processing.mets as odem_mets

# python process-wrapper limit
os.environ['OMP_THREAD_LIMIT'] = '1'

__PDF_GLOB_PATTERN__ = "*.pdf"


class ODEMProcessImpl(oc.ODEMProcess):
    """Create OCR for OAI Records.

        Runs both wiht OAIRecord or local path as input.
        process_identifier may represent a local directory
        or the local part of an OAI-URN.

        Languages for ocr-ing are assumed to be enriched in
        OAI-Record-Metadata (MODS) or be part of local
        paths. They will be applied by a custom mapping
        for the underlying OCR-Engine Tesseract-OCR.
    """

    def __init__(self, record: df_r.Record, configuration=None, work_dir=None,
                  log_dir=None, logger=None):
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

        super().__init__(record, configuration, work_dir=work_dir,
                         log_dir=log_dir, logger=logger)
        self.artefact_identifier = None
        self.process_identifier = self.work_dir_root.name
        if record is not None and record.local_identifier is not None:
            self.process_identifier = record.local_identifier
        self.export_dir = None
        self.local_mets_mode = False
        self.store: typing.Optional[df.LocalStore] = None
        self.__mets_file_path: typing.Optional[Path] = None
        self.ocr_files = []
        self.page_languages: typing.Dict[str, typing.Tuple[str, ...]] = {}
        self.page_model_configs: typing.Dict[str, str] = {}
        self.image_sources: typing.Dict[typing.Tuple[str, str], str] = {}
        self._process_start = time.time()

    def load(self):
        request_identifier = self.record.identifier
        local_identifier = self.record.local_identifier
        if not self.configuration.has_option(oc.CFG_SEC_FLOW,
                                             oc.CFG_SEC_FLOW_OPT_URL):
            self.logger.info("[%s] no download URL provided", self.process_identifier)
            return
        oai_base_url = self.configuration.get(oc.CFG_SEC_FLOW,
                                              oc.CFG_SEC_FLOW_OPT_URL)
        req_dst_dir = os.path.join(
            os.path.dirname(self.work_dir_root), local_identifier)
        if not os.path.exists(req_dst_dir):
            os.makedirs(req_dst_dir, exist_ok=True)
        req_dst = self.mets_file_path
        self.logger.debug("[%s] download %s to %s",
                          self.process_identifier, request_identifier, req_dst)
        try:
            req_kwargs = {}
            if self.configuration.has_option(oc.CFG_SEC_FLOW,
                                             oc.CFG_SEC_FLOW_OPT_URL_KWARGS):
                requests_kwargs = self.configuration.get(oc.CFG_SEC_FLOW,
                                                         oc.CFG_SEC_FLOW_OPT_URL_KWARGS)
                req_kwargs = {dfo.OAI_KWARG_REQUESTS: requests_kwargs}
            load_fgroup = self.configuration.get(oc.CFG_SEC_METS,
                                                 oc.CFG_SEC_METS_FGROUP,
                                                 fallback=oc.DEFAULT_FGROUP)
            req_kwargs[dfo.OAI_KWARG_FGROUP_IMG] = load_fgroup
            loader = df.OAILoader(base_url=oai_base_url, **req_kwargs)
            headers = CaseInsensitiveDict(loader.request_kwargs.get('headers', {}))
            headers.update(oc.get_request_headers(self.configuration))
            if headers:
                loader.request_kwargs['headers'] = dict(headers)
            loader.store = self.store
            use_file_id = self.configuration.getboolean(oc.CFG_SEC_FLOW,
                                                        oc.CFG_SEC_FLOW_USE_FILEID,
                                                        fallback=False)
            loader.load(request_identifier, local_dst=req_dst, use_file_id=use_file_id)
        except df.ClientError as load_err:
            raise oc.ODEMException(load_err.args[0]) from load_err
        except df.LoadException as oai_err:
            raise oc.ODEMException(oai_err.args[0]) from oai_err
        except RuntimeError as _err:
            raise oc.ODEMException(_err.args[0]) from _err

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
        insp.read()
        (mets_type, pica_type) = insp.types
        if isinstance(self.record.info, dict):
            self.record.info["mets"] = mets_type
            self.record.info["pica"] = pica_type
        self.ocr_candidates = insp.image_pairs
        self.page_languages = insp.page_languages
        self.image_sources = insp.image_sources
        self.page_model_configs = {}
        metadata_ident = insp.set_metadata_identifier()
        if metadata_ident is None or insp.identifiers is None:
            raise oc.ODEMDataException(f"No record identifier: {self.mets_file_path}")
        self.artefact_identifier = metadata_ident
        for t, ident in insp.identifiers.items():
            self.process_statistics[t] = ident
        self.process_statistics['type'] = insp.types
        self.process_statistics[oc.STATS_KEY_LANGS] = insp.languages
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

        if languages is None:
            languages = self.process_statistics.get(oc.STATS_KEY_LANGS)
        model_cfg = self._resolve_models(languages)
        self.process_statistics[oc.KEY_LANGUAGES] = model_cfg
        return model_cfg

    def _resolve_models(self, languages) -> str:
        if isinstance(languages, str):
            languages = languages.split('+')
        if not languages:
            raise oc.ODEMModelMissingException("No language metadata or explicit language override")
        models = []
        model_mappings: dict = self.configuration.getdict(  # pylint: disable=no-member
            oc.CFG_SEC_OCR, 'model_mapping')
        self.logger.info("[%s] map languages '%s'",
                         self.process_identifier, languages)
        for lang in languages:
            model_entry = model_mappings.get(lang)
            if not model_entry:
                raise oc.ODEMModelMissingException(f"'{lang}' mapping not found (languages: {languages})!")
            for model in model_entry.split('+'):
                if self._is_model_available(model):
                    if model not in models:
                        models.append(model)
                else:
                    raise oc.ODEMModelMissingException(f"'{model}' model config not found !")
        model_cfg = models[0]
        if self.configuration.getboolean(oc.CFG_SEC_OCR, "model_combinable",
                                         fallback=True):
            model_cfg = '+'.join(models)
        elif len(models) > 1:
            self.logger.warning("[%s] models are not combinable; using %s instead of %s",
                                self.process_identifier, model_cfg, models)
        self.logger.info("[%s] mapped languages '%s' => '%s'",
                         self.process_identifier, languages, model_cfg)
        return model_cfg

    def prepare_page_modelconfigs(self):
        """Validate and freeze candidate models before starting parallel workers."""
        self.page_model_configs = {}
        self.process_statistics.pop('page_model_configs', None)
        self.process_statistics.pop(oc.KEY_LANGUAGES, None)
        self.process_statistics.pop('skipped_model_pages', None)
        if not self.ocr_candidates:
            raise oc.ODEMException("No OCR candidates for page model preparation")
        prepared = {}
        cache = {}
        skipped = {}
        skip_missing = self.local_mets_mode and self.configuration.has_option(
            oc.CFG_SEC_METS, 'corpus_identifier')
        override = self.configuration.get(oc.CFG_SEC_OCR, oc.KEY_LANGUAGES, fallback=None)
        for _, page_id in self.ocr_candidates:
            languages = (override.split('+') if override is not None
                         else self.page_languages.get(page_id)
                         or self.process_statistics.get(oc.STATS_KEY_LANGS))
            if not languages:
                raise oc.ODEMModelMissingException(f"Page {page_id}: no language metadata")
            key = tuple(languages)
            if key not in cache:
                try:
                    cache[key] = self._resolve_models(languages)
                except oc.ODEMModelMissingException as exc:
                    if skip_missing:
                        skipped[page_id] = str(exc)
                        self.logger.warning("[%s] skip page %s: %s",
                                            self.process_identifier, page_id, exc)
                        continue
                    raise oc.ODEMModelMissingException(f"Page {page_id}: {exc}") from exc
            prepared[page_id] = cache[key]
            self.logger.info("[%s] page %s languages %s => %s",
                             self.process_identifier, page_id, languages, prepared[page_id])
        if skipped:
            self.ocr_candidates = [(image, page_id) for image, page_id in self.ocr_candidates
                                   if page_id not in skipped]
            self.process_statistics['skipped_model_pages'] = skipped
            self.process_statistics['n_images_ocrable'] = len(self.ocr_candidates)
            if not self.ocr_candidates:
                raise odem_mets.ODEMNoImagesForOCRException(
                    f"{self.process_identifier} contains no images for OCR after model preflight")
        self.page_model_configs = prepared
        self.process_statistics['page_languages'] = dict(self.page_languages)
        self.process_statistics['page_model_configs'] = dict(prepared)
        unique_models = list(dict.fromkeys(prepared.values()))
        self.process_statistics[oc.KEY_LANGUAGES] = (
            unique_models[0] if len(unique_models) == 1 else unique_models)

    def map_language_to_modelconfig(self, image_path, page_id=None) -> str:
        """Use prepared page models, then explicit overrides and page metadata.

        Calls without page metadata retain the legacy image-only filename
        and document-language behavior.
        """

        if page_id is not None and self.page_model_configs:
            if page_id not in self.page_model_configs:
                raise oc.ODEMModelMissingException(f"Page {page_id}: no prepared model")
            return self.page_model_configs[page_id]
        if page_id is not None and page_id in self.page_languages:
            override = self.configuration.get(oc.CFG_SEC_OCR, oc.KEY_LANGUAGES, fallback=None)
            languages = (override.split('+') if override is not None
                         else self.page_languages[page_id]
                         or self.process_statistics.get(oc.STATS_KEY_LANGS))
            return self._resolve_models(languages)
        file_lang_suffixes = oc.DEFAULT_LANG
        # inspect language arg
        if self.configuration.has_option(oc.CFG_SEC_OCR, oc.KEY_LANGUAGES):
            file_lang_suffixes = self.configuration.get(oc.CFG_SEC_OCR,
                                                        oc.KEY_LANGUAGES).split('+')
            return self.resolve_language_modelconfig(file_lang_suffixes)
        # inspect final '_' segment of local file names
        if self.local_mode:
            try:
                image_name = Path(image_path).stem
                if '_' not in image_name:
                    raise oc.ODEMException(f"Miss language mark for '{image_name}'!")
                file_lang_suffixes = image_name.split('_')[-1].split('+')
            except oc.ODEMException as oxc:
                self.logger.warning("[%s] language mapping err '%s' for '%s', fallback to %s",
                                    self.process_identifier, oxc.args[0],
                                    image_path, oc.DEFAULT_LANG)
            return self.resolve_language_modelconfig(file_lang_suffixes)
        # inspect language information from MODS metadata
        return self.resolve_language_modelconfig()

    def _is_model_available(self, model) -> bool:
        """Determine whether model is available at execting
        host/machine"""

        resource_dir_mappings = self.configuration.getdict(oc.CFG_SEC_OCR,
                                                           oc.CFG_SEC_OCR_OPT_RES_VOL,
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
            raise oc.ODEMException(f"{self.record.identifier} contains no images!")

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
        if self.configuration.has_option(oc.CFG_SEC_OCR, oc.CFG_SEC_OCR_OPT_IMG_SUBDIR):
            images_dir = self.configuration.get(oc.CFG_SEC_OCR,
                                                oc.CFG_SEC_OCR_OPT_IMG_SUBDIR)
        local_img_dir = os.path.join(self.work_dir_root, images_dir)
        self.logger.debug("[%s] inspect local image dir %s",
                          self.process_identifier, local_img_dir)
        for img, urn in self.ocr_candidates:
            the_file = os.path.join(local_img_dir, img)
            if self.image_sources:
                image_name = Path(img).name
                already_bound = Path(img).is_absolute() \
                    and Path(img).parent.resolve() == Path(local_img_dir).resolve()
                if (image_name != img and not already_bound) or image_name in ('.', '..'):
                    raise oc.ODEMException(f"Page {urn}: unsafe local image name {img}")
                if not os.path.isfile(the_file):
                    source = self.image_sources.get((image_name, urn))
                    if source is None:
                        raise oc.ODEMException(f"Page {urn}: no image source for {img}")
                    self._download_corpus_image(source, Path(the_file), urn)
            elif not os.path.isfile(the_file):
                raise oc.ODEMException(f"[{self.process_identifier}] missing {the_file}!")
            images_of_interest.append((the_file, urn))
        self.ocr_candidates = images_of_interest

    def _download_corpus_image(self, source: str, destination: Path, page_id: str):
        """Publish a missing corpus image only after a complete, valid download."""
        url = urlsplit(source)
        if url.scheme not in ('http', 'https') or not url.netloc:
            raise oc.ODEMException(
                f"Page {page_id}: missing {destination}; image source is not an HTTP(S) URL: {source}")
        self.logger.info("[%s] page %s download %s to %s",
                         self.process_identifier, page_id, source, destination)
        temporary_path = None
        try:
            if not destination.parent.exists():
                destination.parent.mkdir(parents=True, mode=0o775)
                destination.parent.chmod(0o775)
            with requests.get(source, stream=True, timeout=dfo.REQUESTS_DEFAULT_TIMEOUT,
                              headers=oc.get_request_headers(self.configuration)) as response:
                response.raise_for_status()
                content_type = response.headers.get('Content-Type', '').split(';')[0].strip().lower()
                if not content_type.startswith('image/'):
                    raise oc.ODEMException(
                        f"Page {page_id}: {source} returned non-image Content-Type {content_type!r}")
                with tempfile.NamedTemporaryFile(
                        dir=destination.parent, prefix='.odem-image-', suffix='.part',
                        delete=False) as image_file:
                    temporary_path = Path(image_file.name)
                    for chunk in response.iter_content(chunk_size=1024 * 1024):
                        image_file.write(chunk)
            with PIL.Image.open(temporary_path) as image:
                image.verify()
            temporary_path.chmod(0o664)
            os.link(temporary_path, destination)
        except (requests.RequestException, OSError, ValueError, SyntaxError,
                PIL.Image.DecompressionBombError) as exc:
            raise oc.ODEMException(
                f"Page {page_id}: failed to download {source} to {destination}: {exc}") from exc
        finally:
            if temporary_path is not None:
                temporary_path.unlink(missing_ok=True)

    def postprocess(self, ocr_results: typing.List[oc.OCRResult]):
        """Encapsulate after-OCR workflow"""

        if ocr_results is None or len(ocr_results) == 0:
            the_alert = f"zero results from {len(self.ocr_candidates)} candidates"
            raise oc.ODEMException(the_alert)
        self.calculate_statistics_ocr(ocr_results)
        self.process_statistics[oc.STATS_KEY_N_EXECS] = self.configuration.get(
            oc.CFG_SEC_OCR,
            oc.CFG_SEC_OCR_OPT_EXECS)
        self.logger.info("[%s] %s", self.record.local_identifier, self.statistics)
        wf_enrich_ocr = self.configuration.getboolean(oc.CFG_SEC_METS,
                                                      oc.CFG_SEC_METS_OPT_ENRICH,
                                                      fallback=True)
        if wf_enrich_ocr:
            self.link_ocr_files()
        wf_create_derivates = self.configuration.getboolean(oc.CFG_SEC_DERIVANS,
                                                            oc.CFG_SEC_DERIVANS_ENABLED,
                                                            fallback=False)
        if wf_create_derivates:
            self.create_derivates()
            self.postprocess_review_derivans_agents()
        if self.configuration.getboolean(oc.CFG_SEC_FLOW, oc.CFG_SEC_FLOW_OPT_TEXTLINE,
                                         fallback=False):
            self.create_text_bundle_data()
        # METS postprocessing has own configuration options
        self.postprocess_mets()
        if self.configuration.getboolean(oc.CFG_SEC_METS, 'postvalidate', fallback=True):
            self.validate_metadata()
        if self.configuration.getboolean(oc.CFG_SEC_EXP,
                                         oc.CFG_SEC_EXP_ENABLED, fallback=False):
            self.logger.info("[%s] start export", self.process_identifier)
            if self.configuration.has_option(oc.CFG_SEC_FLOW,
                                             oc.CFG_SEC_FLOW_OPT_DELETE_DIRS):
                del_dirs = self.configuration.getlist(oc.CFG_SEC_FLOW,
                                                      oc.CFG_SEC_FLOW_OPT_DELETE_DIRS)
                if len(del_dirs) > 0:
                    self.delete_local_directories(del_dirs)
            self.export_data()
        if self.configuration.getboolean(oc.CFG_SEC_FLOW,
                                         oc.CFG_SEC_FLOW_OPT_REM_RES,
                                         fallback=False):
            self.clear_mets_resources()

    def calculate_statistics_ocr(self, outcomes: typing.List[oc.OCRResult]):
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
        self.process_statistics[oc.STATS_KEY_N_OCR] = n_ocr_created
        self.process_statistics[oc.STATS_KEY_MB] = round(total_mb, 2)
        self.process_statistics[oc.STATS_KEY_MPS] = mps
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
                self.process_statistics[oc.STATS_KEY_OCR_LOSS] = list(data_loss)

    def link_ocr_files(self) -> int:
        """Prepare and link OCR-data"""

        list_from_dir = Path(self.work_dir_root) / oc.FILEGROUP_FULLTEXT
        self.ocr_files = oc.list_files(list_from_dir)
        if not self.ocr_files:
            return 0
        proc = df.MetsProcessor(self.mets_file_path)
        image_pages = None
        if self.image_sources:
            image_pages = {Path(image).stem: (page_id, Path(image).name)
                           for image, page_id in self.ocr_candidates}
        n_linked_ocr, n_dropped = odem_mets.integrate_ocr_file(
            proc.root, self.ocr_files, image_pages=image_pages)
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
        bundle_label = None
        if self.artefact_identifier is not None:
            bundle_label = self.artefact_identifier
        else:
            pdf_path_gen = Path(self.work_dir_root).glob(__PDF_GLOB_PATTERN__)
            try:
                pdf_path = next(pdf_path_gen)
                bundle_label = pdf_path.name
            except StopIteration:
                self.logger.warning("[%s] no text bundle: dont know the name",
                                    self.process_identifier)
        if bundle_label is not None:
            if not str(bundle_label).endswith(".pdf"):
                bundle_label += ".pdf"
            out_path = os.path.join(self.work_dir_root, f"{bundle_label}.txt")
            with open(out_path, mode='w', encoding='UTF-8') as tl_writer:
                tl_writer.write(txt_content)
            self.logger.info("[%s] harvested %d lines from %d ocr files to %s",
                             self.process_identifier, len(txt_lines),
                             len(self.ocr_files), out_path)
            self.process_statistics['n_text_lines'] = len(txt_lines)


    def create_derivates(self):
        """Forward PDF-creation to Derivans
        assuming that configuration if relative is set in PROJECT_ROOT"""

        path_cfg = Path(self.configuration.get(oc.CFG_SEC_DERIVANS, oc.CFG_SEC_DERIVANS_CONFIG))
        if not path_cfg.is_absolute():
            path_cfg = oc.PROJECT_ROOT / path_cfg
        derivans_image = self.configuration.get(oc.CFG_SEC_DERIVANS, oc.CFG_SEC_DERIVANS_IMAGE,
                                                fallback=None)
        path_logging_cfg = self.configuration.get(oc.CFG_SEC_DERIVANS, oc.CFG_SEC_DERIVANS_LOGDIR,
                                              fallback=None)
        path_logging = None
        if path_logging_cfg is not None:
            path_logging = Path(path_logging_cfg)
            if not path_logging.is_absolute():
                path_logging = oc.PROJECT_ROOT / path_logging
        assert self.mets_file_path is not None
        assert derivans_image is not None
        assert path_logging is not None
        derivans: df.BaseDerivansManager = df.BaseDerivansManager.create(
            self.mets_file_path,
            path_configuration=path_cfg,
            path_logging=path_logging,
            container_image_name=derivans_image
        )
        if self.configuration.has_option(oc.CFG_SEC_DERIVANS, oc.CFG_SEC_DERIVANS_FGROUP):
            the_fgroup = self.configuration.get(oc.CFG_SEC_DERIVANS, oc.CFG_SEC_DERIVANS_FGROUP)
            derivans.additional_args += f'--file-group {the_fgroup}'
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
            raise oc.ODEMException(err_args) from _sub_err
        if self.configuration.has_option(oc.CFG_SEC_DERIVANS,
                                            oc.CFG_SEC_DERIVANS_FGROUP_CHECK_PDF):
            check_required = self.configuration.getboolean(oc.CFG_SEC_DERIVANS,
                                            oc.CFG_SEC_DERIVANS_FGROUP_CHECK_PDF)
            if check_required:
                the_root = self.work_dir_root
                if not isinstance(the_root, Path):
                    the_root = Path(the_root)
                if not any(the_root.rglob(__PDF_GLOB_PATTERN__)):
                    err_msg = f"PDF missing in {the_root}!"
                    raise oc.ODEMDerivateException(err_msg)
                pdf_estm_path = list(the_root.rglob(__PDF_GLOB_PATTERN__))
                pdf_size = pdf_estm_path[0].stat().st_size
                if  pdf_size < 1000:
                    err_msg = f"PDF too small: {pdf_size}B for {pdf_estm_path[0]}"
                    raise oc.ODEMDerivateException(err_msg)

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

        if not self.configuration.getboolean(oc.CFG_SEC_METS,
                                             'prevalidate',
                                             fallback=True):
            self.logger.warning("[%s] skipping pre-validation",
                                self.process_identifier)
            return
        if not self.configuration.getboolean(oc.CFG_SEC_METS,
                                             'postvalidate',
                                             fallback=False):
            self.logger.warning("[%s] skipping post-validation",
                                self.process_identifier)
            return
        ignore_ddb = []
        if self.configuration.has_option(oc.CFG_SEC_METS,
                                         'ddb_validation_ignore'):
            raw_ignore_str = self.configuration.get(oc.CFG_SEC_METS,
                                                    'ddb_validation_ignore')
            ignore_ddb = [i.strip() for i in raw_ignore_str.split(',')]
        ddb_min_level = 'fatal'
        if self.configuration.has_option(oc.CFG_SEC_METS, 'ddb_min_level'):
            ddb_min_level = self.configuration.get(oc.CFG_SEC_METS,
                                                   'ddb_min_level')
        the_type = "Ac"
        if isinstance(self.record.info, dict) and "pica" in self.record.info:
            the_type = self.record.info["pica"]
        if self.logger is not None:
            self.logger.info("[%s] validate type %s ddb_ignore: %s", self.process_identifier,
                             the_type, ignore_ddb)
        return odem_mets.validate_mets(self.mets_file_path, digi_type=the_type,
                                       ddb_ignores=ignore_ddb,
                                       ddb_min_level=ddb_min_level)

    def export_data(self):
        """re-do metadata and transform into output format"""

        export_format: str = self.configuration.get(oc.CFG_SEC_EXP,
                                                    oc.CFG_SEC_EXP_OPT_FORMAT,
                                                    fallback=oc.ExportFormat.SAF)
        export_mets: bool = self.configuration.getboolean(oc.CFG_SEC_EXP,
                                                          oc.CFG_SEC_EXP_OPT_METS,
                                                          fallback=True)

        exp_dst = self.configuration.get(oc.CFG_SEC_EXP, oc.CFG_SEC_EXP_OPT_DST)
        exp_tmp = self.configuration.get(oc.CFG_SEC_EXP, oc.CFG_SEC_EXP_OPT_TMP)
        exp_map = self.configuration.getdict(oc.CFG_SEC_EXP,
                                             oc.CFG_SEC_EXP_OPT_MAPPINGS)
        exp_prefix = self.configuration.get(oc.CFG_SEC_EXP,
                                            oc.CFG_SEC_EXP_OPT_PREFIX,
                                            fallback=None)
        exp_name = self.configuration.get(oc.CFG_SEC_EXP,
                                          oc.CFG_SEC_EXP_OPT_NAME,
                                          fallback=None)
        # overwrite default mapping *.xml => 'mets.xml'
        # since we will have currently many more XML-files
        # created due OCR and do more specific mapping, though
        exp_map = {k: v for k, v in exp_map.items() if v != 'mets.xml'}
        if export_mets:
            exp_map[os.path.basename(self.mets_file_path)] = 'mets.xml'
        saf_name = self.artefact_identifier
        if exp_name is not None:
            saf_name = exp_name
        if exp_prefix is not None:
            saf_name = f"{exp_prefix}{saf_name}"
        exp_col = None
        if export_format == oc.ExportFormat.SAF:
            # for DSpace-SAF target collection required
            exp_col = self.configuration.get(oc.CFG_SEC_EXP,
                                             oc.CFG_SEC_EXP_OPT_COLLECTION)
            export_result = df.export_data_from(
                self.mets_file_path,
                saf_final_name=saf_name,
                export_dst=exp_dst,
                collection=exp_col,
                export_map=exp_map,
                tmp_saf_dir=exp_tmp,
            )
        elif export_format == oc.ExportFormat.FLAT_ZIP:
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
                path_export_processing = dfx.move_file_to(tmp_zip_path, exp_dst)
                export_result = path_export_processing, size
        else:
            raise oc.ODEMException(f'Unsupported export format: {export_format}')
        self.logger.info("[%s] exported data: %s",
                         self.process_identifier, export_result)
        if export_result:
            pth, size = export_result
            self.logger.info("[%s] create %s (%s)",
                             self.process_identifier, pth, size)
            # final re-move at export destination
            if '.processing' in str(pth):
                old_path = pth
                pth = pth.replace('.processing', '')
                self.logger.debug('[%s] rename %s to %s',
                                  self.process_identifier, old_path, pth)
                shutil.move(old_path, pth)
            self.statistics["export_name"] = os.path.basename(pth)
            self.statistics["export_size"] = size
            if exp_col is not None:
                self.statistics["export_coll"] = exp_col
            return pth, size
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

        if self.__mets_file_path is None:
            mets_file = f"{os.path.basename(self.work_dir_root)}.xml"
            self.__mets_file_path = Path(self.work_dir_root) / mets_file
        return self.__mets_file_path

    @mets_file_path.setter
    def mets_file_path(self, mets_path):
        """Set enclosed MET/MODS data for testing purposes or local mounts"""
        self.__mets_file_path = Path(mets_path)
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
