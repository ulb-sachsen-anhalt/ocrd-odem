# -*- coding: utf-8 -*-
"""MAIN CLI OAI LOCAL ODEM"""

import argparse
import os
import shutil
import sys
from ast import (
    literal_eval,
)
from digiflow import (
    OAIRecordHandler,
    OAIRecord,
    LocalStore
)

from lib.ocrd3_odem.odem_commons import (
    RECORD_IDENTIFIER,
    RECORD_INFO,
)
from lib.ocrd3_odem import (
    MARK_OCR_BUSY,
    MARK_OCR_DONE,
    MARK_OCR_OPEN,
    MARK_OCR_FAIL,
    ODEMProcess,
    OCRDPageParallel,
    ODEMException,
    get_configparser,
    get_logger,
)
from lib.resources_monitoring import ProcessResourceMonitor, ProcessResourceMonitorConfig

DEFAULT_EXECUTORS = 2


def trnfrm(row):
    oai_id = row[RECORD_IDENTIFIER]
    try:
        _info = literal_eval(row[RECORD_INFO])
    except:
        _info = row[RECORD_INFO]
    _record = OAIRecord(oai_id,)
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

    CREATE_PDF: bool = CFG.getboolean('derivans', 'derivans_enabled', fallback=True)
    ENRICH_METS_FULLTEXT: bool = CFG.getboolean('export', 'enrich_mets_fulltext', fallback=True)

    # set work_dirs and logger
    LOCAL_WORK_ROOT = CFG.get('global', 'local_work_root')
    LOCAL_DELETE_BEVOR_EXPORT = []
    if CFG.has_option('global', 'delete_bevor_export'):
        LOCAL_DELETE_BEVOR_EXPORT = CFG.getlist('global', 'delete_bevor_export')
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
        CFG.set('ocr', 'n_executors', str(EXECUTOR_ARGS))
    EXECUTORS = CFG.getint('ocr', 'n_executors', fallback=DEFAULT_EXECUTORS)
    if SEQUENTIAL:
        EXECUTORS = 1
    LOGGER.debug("local work_root: '%s', executors:%s, keep_res:%s, lock:%s",
                 LOCAL_WORK_ROOT, EXECUTORS, MUST_KEEP_RESOURCES, MUST_LOCK)

    # request next open oai record data
    DATA_FIELDS = CFG.getlist('global', 'data_fields')
    LOGGER.info("data fields: '%s'", DATA_FIELDS)
    LOGGER.info("use records from '%s'", OAI_RECORD_FILE)
    handler = OAIRecordHandler(
        OAI_RECORD_FILE, data_fields=DATA_FIELDS, transform_func=trnfrm)
    record = handler.next_record(state=MARK_OCR_OPEN)
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

        PROCESS: ODEMProcess = OCRDPageParallel(record, req_dst_dir, EXECUTORS)
        PROCESS.the_logger = LOGGER
        PROCESS.the_logger.info("[%s] odem from %s, %d executors", local_ident, OAI_RECORD_FILE, EXECUTORS)
        PROCESS.cfg = CFG
        LOCAL_STORE_ROOT = CFG.get('global', 'local_store_root', fallback=None)
        if LOCAL_STORE_ROOT is not None:
            STORE_DIR = os.path.join(LOCAL_STORE_ROOT, local_ident)
            STORE = LocalStore(STORE_DIR, req_dst_dir)
            PROCESS.store = STORE
        process_resource_monitor: ProcessResourceMonitor = ProcessResourceMonitor(
            ProcessResourceMonitorConfig(
                enable_resource_monitoring=CFG.getboolean('resource-monitoring', 'enable', fallback=False),
                polling_interval=CFG.getfloat('resource-monitoring', 'polling_interval', fallback=1),
                path_disk_usage=CFG.get('resource-monitoring', 'path_disk_usage', fallback='/home/ocr'),
                factor_free_disk_space_needed=CFG.getfloat(
                    'resource-monitoring',
                    'factor_free_disk_space_needed',
                    fallback=3.0
                ),
                max_vmem_percentage=CFG.getfloat('resource-monitoring', 'max_vmem_percentage', fallback=None),
                max_vmem_bytes=CFG.getint('resource-monitoring', 'max_vmem_bytes', fallback=None),
            ),
            LOGGER.error,
            wrap_save_record_state,
            None,
            PROCESS.process_identifier,
            record.identifier
        )
        process_resource_monitor.check_vmem()
        process_resource_monitor.monit_disk_space(PROCESS.load)
        if CFG.getboolean('mets','prevalidate', fallback=True):
            PROCESS.validate_mets()
        PROCESS.inspect_metadata()
        PROCESS.clear_existing_entries()
        PROCESS.language_modelconfig()
        PROCESS.set_local_images()
        OUTCOMES = process_resource_monitor.monit_vmem(PROCESS.run)
        PROCESS.calculate_statistics(OUTCOMES)
        PROCESS.the_logger.info("[%s] %s", local_ident, PROCESS.statistics)
        PROCESS.link_ocr()
        if CREATE_PDF:
            PROCESS.create_pdf()
        PROCESS.postprocess_ocr()
        if CREATE_PDF:
            PROCESS.create_text_bundle_data()
        PROCESS.postprocess_mets()
        if CFG.getboolean('mets','postvalidate', fallback=True):
            PROCESS.validate_mets()
        PROCESS.export_data()
        if not MUST_KEEP_RESOURCES:
            PROCESS.delete_before_export(LOCAL_DELETE_BEVOR_EXPORT)
        _kwargs = PROCESS.statistics
        if PROCESS.record.info != 'n.a.':
            try:
                if isinstance(PROCESS.record.info, str):
                    _info = dict(literal_eval(PROCESS.record.info))
                PROCESS.record.info.update(_kwargs)
                _info = f"{PROCESS.record.info}"
            except:
                PROCESS.the_logger.error("Can't parse '%s', store info literally",
                                         PROCESS.record.info)
                _info = f"{_kwargs}"
        else:
            _info = f"{_kwargs}"
        handler.save_record_state(record.identifier, MARK_OCR_DONE, INFO=_info)
        _mode = 'sequential' if SEQUENTIAL else f'n_execs:{EXECUTORS}'
        PROCESS.the_logger.info("[%s] duration: %s/%s (%s)", PROCESS.process_identifier,
                                PROCESS.duration, _mode, PROCESS.statistics)
        # finale
        LOGGER.info("[%s] odem done in '%s' (%d executors)",
                    PROCESS.process_identifier, PROCESS.duration, EXECUTORS)
    except ODEMException as _odem_exc:
        _err_args = {'ODEMException': _odem_exc.args[0]}
        LOGGER.error("[%s] odem fails with: '%s'", PROCESS.process_identifier, _err_args)
        handler.save_record_state(record.identifier, MARK_OCR_FAIL, INFO=f'{_err_args}')
    except RuntimeError as exc:
        LOGGER.error("odem fails for '%s' after %s with: '%s'",
                     record, PROCESS.duration, str(exc))
        handler.save_record_state(record.identifier, MARK_OCR_FAIL, INFO=f'{str(exc) : exc.args[0]}')
        sys.exit(1)
