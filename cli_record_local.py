# -*- coding: utf-8 -*-
"""MAIN CLI OAI LOCAL ODEM"""

import ast
import argparse
import os
import shutil
import sys

import digiflow as df
import digiflow.record as df_r

import lib.odem as odem
import lib.odem.monitoring.resource as odem_rm

from lib.odem.odem_commons import (
    RECORD_IDENTIFIER,
    RECORD_INFO,
)
from lib.odem import (
    MARK_OCR_BUSY,
    MARK_OCR_DONE,
    MARK_OCR_OPEN,
    MARK_OCR_FAIL,
    ODEMProcessImpl,
    ODEMException,
    get_configparser,
    get_logger,
)

DEFAULT_EXECUTORS = 2


def _trnfrm(row):
    oai_id = row[RECORD_IDENTIFIER]
    try:
        _info = ast.literal_eval(row[RECORD_INFO])
    except SyntaxError:
        _info = row[RECORD_INFO]
    _record = df_r.Record(oai_id,)
    _record.info = _info
    return _record


########
# MAIN #
########
if __name__ == "__main__":
    PARSER = argparse.ArgumentParser(
        description="generate ocr-data for OAI-Record")
    PARSER.add_argument(
        "data",
        help="path to file with OAI-Record information")
    PARSER.add_argument(
        "-c",
        "--config",
        required=False,
        default="resources/odem.ini",
        help="path to configuration file")
    PARSER.add_argument(
        "-e",
        "--executors",
        required=False,
        help="Number of OCR-D Executors in parallel mode")
    PARSER.add_argument(
        "-s",
        "--sequential-mode",
        required=False,
        default=False,
        action="store_true",
        help="Disable parallel mode, just run sequential")
    PARSER.add_argument(
        "-k",
        "--keep-resources",
        required=False,
        default=False,
        action='store_true',
        help="keep stored images after processing")
    PARSER.add_argument(
        "-l",
        "--lock-mode",
        required=False,
        default=False,
        action='store_true',
        help="lock each run to avoid parallel starts")
    ARGS = PARSER.parse_args()

    # check some pre-conditions
    # inspect configuration settings
    CONF_FILE = os.path.abspath(ARGS.config)
    if not os.path.exists(CONF_FILE):
        print(f"[ERROR] no config at '{CONF_FILE}'! Halt execution!")
        sys.exit(1)

    # pick common args
    SEQUENTIAL = ARGS.sequential_mode
    MUST_KEEP_RESOURCES = ARGS.keep_resources
    MUST_LOCK = ARGS.lock_mode
    EXECUTOR_ARGS = ARGS.executors

    CFG = get_configparser()
    configurations_read = CFG.read(CONF_FILE)
    if not configurations_read:
        print(f"unable to read config from '{CONF_FILE}! exit!")
        sys.exit(1)

    # set work_dirs and logger
    LOCAL_WORK_ROOT = CFG.get('global', 'local_work_root')
    LOCAL_DELETE_BEVOR_EXPORT = []
    if CFG.has_option('export', 'delete_before_export'):
        LOCAL_DELETE_BEVOR_EXPORT = CFG.getlist('export', 'delete_before_export')
    LOCAL_LOG_DIR = CFG.get('global', 'local_log_dir')
    if not os.path.exists(LOCAL_LOG_DIR) or not os.access(
            LOCAL_LOG_DIR, os.W_OK):
        raise RuntimeError(f"cant store log files at invalid {LOCAL_LOG_DIR}")
    LOG_FILE_NAME = None
    if CFG.has_option('global', 'logfile_name'):
        LOG_FILE_NAME = CFG.get('global', 'logfile_name')
    LOGGER = get_logger(LOCAL_LOG_DIR, LOG_FILE_NAME)

    # inspect what kind of input to process
    # oai record file *OR* local data directory must be set
    OAI_RECORD_FILE = os.path.abspath(ARGS.data)

    # if valid n_executors via cli, use it's value
    if EXECUTOR_ARGS and int(EXECUTOR_ARGS) > 0:
        CFG.set(odem.CFG_SEC_OCR, 'n_executors', str(EXECUTOR_ARGS))
    EXECUTORS = CFG.getint(odem.CFG_SEC_OCR, 'n_executors', fallback=DEFAULT_EXECUTORS)
    if SEQUENTIAL:
        EXECUTORS = 1
    LOGGER.debug("local work_root: '%s', executors:%s, keep_res:%s, lock:%s",
                 LOCAL_WORK_ROOT, EXECUTORS, MUST_KEEP_RESOURCES, MUST_LOCK)

    # request next open oai record data
    DATA_FIELDS = CFG.getlist('global', 'data_fields')
    LOGGER.info("data fields: '%s'", DATA_FIELDS)
    LOGGER.info("use records from '%s'", OAI_RECORD_FILE)
    handler = df_r.RecordHandler(
        OAI_RECORD_FILE, data_fields=DATA_FIELDS, transform_func=_trnfrm)
    record: df_r.Record = handler.next_record(state=MARK_OCR_OPEN)
    if not record:
        LOGGER.info("no open records in '%s', work done", OAI_RECORD_FILE)
        sys.exit(1)

    def wrap_save_record_state(status: str, urn, **kwargs):
        handler.save_record_state(urn, status, **kwargs)

    try:
        handler.save_record_state(record.identifier, MARK_OCR_BUSY)
        local_ident = record.local_identifier
        req_dst_dir = os.path.join(LOCAL_WORK_ROOT, local_ident)
        if os.path.exists(req_dst_dir):
            shutil.rmtree(req_dst_dir)
        proc_type = CFG.get(odem.CFG_SEC_OCR, 'workflow_type', fallback=odem.DEFAULT_WORKLFOW)
        odem_process: ODEMProcessImpl = ODEMProcessImpl(CFG, req_dst_dir, LOGGER, log_dir=None, record=record)
        odem_process.logger = LOGGER
        odem_process.logger.info("[%s] odem from %s, %d executors", local_ident,
                                 OAI_RECORD_FILE, EXECUTORS)
        odem_process.configuration = CFG
        local_store_root = CFG.get('global', 'local_store_root', fallback=None)
        if local_store_root is not None:
            store_root_dir = os.path.join(local_store_root, local_ident)
            odem_process.store = df.LocalStore(store_root_dir, req_dst_dir)
        process_resource_monitor: odem_rm.ProcessResourceMonitor = odem_rm.ProcessResourceMonitor(
            odem_rm.from_configuration(CFG),
            LOGGER.error,
            wrap_save_record_state,
            None,
            odem_process.process_identifier,
            record.identifier
        )
        process_resource_monitor.check_vmem()
        process_resource_monitor.monit_disk_space(odem_process.load)
        odem_process.inspect_metadata()
        if CFG.getboolean('mets', 'prevalidate', fallback=True):
            odem_process.validate_metadata()
        odem_process.clear_existing_entries()
        odem_process.language_modelconfig()
        odem_process.set_local_images()
        odem_pipeline = odem.ODEMWorkflow.create(proc_type, odem_process)
        odem_runner = odem.ODEMWorkflowRunner(local_ident, EXECUTORS, LOGGER, odem_pipeline)
        if CFG.getboolean(odem.CFG_SEC_MONITOR, 'live', fallback=False):
            LOGGER.info("[%s] live-monitoring of ocr workflow resources",
                        local_ident)
            ocr_results = process_resource_monitor.monit_vmem(odem_runner.run)
        else:
            LOGGER.info("[%s] execute ocr workflow with poolsize %d",
                        local_ident, EXECUTORS)
            ocr_results = odem_runner.run()
        if ocr_results is None or len(ocr_results) == 0:
            raise ODEMException(f"process run error: {record.identifier}")
        odem_process.calculate_statistics_ocr(ocr_results)
        odem_process.process_statistics[odem.STATS_KEY_N_EXECS] = EXECUTORS
        odem_process.logger.info("[%s] %s", local_ident, odem_process.statistics)
        wf_enrich_ocr = CFG.getboolean(odem.CFG_SEC_METS, odem.CFG_SEC_METS_OPT_ENRICH,
                                       fallback=True)
        if wf_enrich_ocr:
            odem_process.link_ocr_files()
        wf_create_pdf = CFG.getboolean('derivans', 'derivans_enabled', fallback=True)
        if wf_create_pdf:
            odem_process.create_derivates()
            odem_process.create_text_bundle_data()
        odem_process.postprocess_mets()
        if CFG.getboolean('mets', 'postvalidate', fallback=True):
            odem_process.validate_metadata()
        if not MUST_KEEP_RESOURCES:
            odem_process.delete_local_directories(LOCAL_DELETE_BEVOR_EXPORT)
        odem_process.export_data()
        _kwargs = odem_process.statistics
        if odem_process.record.info != 'n.a.':
            try:
                if isinstance(odem_process.record.info, str):
                    _info = dict(ast.literal_eval(odem_process.record.info))
                odem_process.record.info.update(_kwargs)
                _info = f"{odem_process.record.info}"
            except:
                odem_process.logger.warning("Can't parse '%s', store info str",
                                            odem_process.record.info)
                _info = f"{_kwargs}"
        else:
            _info = f"{_kwargs}"
        handler.save_record_state(record.identifier, MARK_OCR_DONE, INFO=_info)
        _mode = 'sequential' if SEQUENTIAL else f'n_execs:{EXECUTORS}'
        odem_process.logger.info("[%s] duration: %s/%s (%s)", odem_process.process_identifier,
                                 odem_process.statistics['timedelta'], _mode,
                                 odem_process.statistics)
        # finale
        LOGGER.info("[%s] odem done in '%s' (%d executors)",
                    odem_process.process_identifier,
                    odem_process.statistics['timedelta'], EXECUTORS)
    except (odem.ODEMNoTypeForOCRException, 
            odem.ODEMNoImagesForOCRException,
            odem.ODEMModelMissingException) as odem_missmatch:
        exc_label = odem_missmatch.__class__.__name__
        LOGGER.warning("[%s] odem skips '%s'",
                       odem_process.process_identifier, odem_missmatch.args)
        exc_dict = {exc_label: odem_missmatch.args[0]}
        handler.save_record_state(record.identifier, 
                                  status=odem.MARK_OCR_SKIP,
                                  **exc_dict)
    except ODEMException as _odem_exc:
        _err_args = {'ODEMException': _odem_exc.args[0]}
        LOGGER.error("[%s] odem fails with: '%s'", odem_process.process_identifier, _err_args)
        handler.save_record_state(record.identifier, MARK_OCR_FAIL, INFO=f'{_err_args}')
    except RuntimeError as exc:
        LOGGER.error("odem fails for '%s' after %s with: '%s'",
                     record, odem_process.statistics['timedelta'], str(exc))
        handler.save_record_state(record.identifier, MARK_OCR_FAIL,
                                  INFO=f'{str(exc) : exc.args[0]}')
        sys.exit(1)
