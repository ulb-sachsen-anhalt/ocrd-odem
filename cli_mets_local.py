# -*- coding: utf-8 -*-
"""MAIN CLI plain OCR with optional export"""

import argparse
import os
import sys

from pathlib import Path

import digiflow as df

import lib.odem as odem
import lib.odem.monitoring.resource as odem_rm


DEFAULT_EXECUTORS = 2


########
# MAIN #
########
if __name__ == "__main__":
    PARSER = argparse.ArgumentParser(
        description="generate ocr-data for Record")
    PARSER.add_argument(
        "mets_file",
        help="path to digital object's METS/MODS file")
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

    CFG = odem.get_configparser()
    configurations_read = CFG.read(CONF_FILE)
    if not configurations_read:
        print(f"unable to read config from '{CONF_FILE}! exit!")
        sys.exit(1)

    CREATE_PDF = CFG.getboolean('derivans', 'derivans_enabled', fallback=True)


    # set work_dirs and logger
    DELETE_BEVOR_EXPORT = []
    if CFG.has_option('export', 'delete_before_export'):
        DELETE_BEVOR_EXPORT = CFG.getlist('export', 'delete_before_export')
    LOCAL_LOG_DIR = CFG.get('global', 'local_log_dir')
    if not os.path.exists(LOCAL_LOG_DIR) or not os.access(
            LOCAL_LOG_DIR, os.W_OK):
        raise RuntimeError(f"cant store log files at invalid {LOCAL_LOG_DIR}")
    LOG_FILE_NAME = None
    if CFG.has_option('global', 'logfile_name'):
        LOG_FILE_NAME = CFG.get('global', 'logfile_name')
    LOGGER = odem.get_logger(LOCAL_LOG_DIR, LOG_FILE_NAME)

    mets_file: Path = Path(ARGS.mets_file).absolute()
    if not mets_file.is_file():
        print(f"unable to read file '{mets_file}! exit!")
        sys.exit(1)
    LOGGER.info("use '%s'", mets_file)
    mets_file_dir = mets_file.parent

    # if valid n_executors via cli, use it's value
    if EXECUTOR_ARGS and int(EXECUTOR_ARGS) > 0:
        CFG.set(odem.CFG_SEC_OCR, 'n_executors', str(EXECUTOR_ARGS))
    EXECUTORS = CFG.getint(odem.CFG_SEC_OCR, 'n_executors', fallback=DEFAULT_EXECUTORS)
    if SEQUENTIAL:
        EXECUTORS = 1
    LOGGER.debug("local work_root: '%s', executors:%s, keep_res:%s, lock:%s",
                 mets_file_dir, EXECUTORS, MUST_KEEP_RESOURCES, MUST_LOCK)

    try:
        local_ident = mets_file.stem
        proc_type: str = CFG.get(odem.CFG_SEC_OCR, 'workflow_type', fallback=None)
        if proc_type is None:
            LOGGER.warning("no 'workflow_type' config option in section ocr defined. defaults to 'OCRD_PAGE_PARALLEL'")
        record = df.OAIRecord(local_ident)
        odem_process: odem.ODEMProcessImpl = odem.ODEMProcessImpl(record, mets_file_dir)
        odem_process.logger = LOGGER
        odem_process.logger.info("[%s] odem from %s, %d executors", local_ident, mets_file, EXECUTORS)
        odem_process.configuration = CFG
        process_resource_monitor: odem_rm.ProcessResourceMonitor = odem_rm.ProcessResourceMonitor(
            odem_rm.from_configuration(CFG),
            LOGGER.error,
            None,
            odem_process.process_identifier,
            record.identifier
        )
        process_resource_monitor.check_vmem()
        # process_resource_monitor.monit_disk_space(odem_process.load)
        odem_process.inspect_metadata()
        if CFG.getboolean('mets', 'prevalidate', fallback=True):
            odem_process.validate_metadata()
        odem_process.clear_existing_entries()
        odem_process.language_modelconfig()
        odem_process.set_local_images()
        odem_pipeline = odem.ODEMWorkflow.create(proc_type, odem_process)
        odem_runner = odem.ODEMWorkflowRunner(local_ident, EXECUTORS, LOGGER, odem_pipeline)
        ocr_results = process_resource_monitor.monit_vmem(odem_runner.run)
        if ocr_results is None or len(ocr_results) == 0:
            raise odem.ODEMException(f"OCR Process Runner error for {record.identifier}")
        odem_process.calculate_statistics_ocr(ocr_results)
        odem_process.process_statistics[odem.STATS_KEY_N_EXECS] = EXECUTORS
        odem_process.logger.info("[%s] %s", local_ident, odem_process.statistics)
        odem_process.link_ocr_files()
        odem_process.postprocess_ocr()
        wf_enrich_ocr = CFG.getboolean(odem.CFG_SEC_METS, odem.CFG_SEC_METS_OPT_ENRICH, fallback=True)
        if wf_enrich_ocr:
            odem_process.link_ocr_files()
        if CREATE_PDF:
            odem_process.create_pdf()
        if CREATE_PDF:
            odem_process.create_text_bundle_data()
        odem_process.postprocess_mets()
        if CFG.getboolean('mets', 'postvalidate', fallback=True):
            odem_process.validate_metadata()
        if odem_process.configuration.has_option('export', 'local_export_dir'):
            odem_process.logger.info("[%s] start to export data",
                                         odem_process.process_identifier)
            if not MUST_KEEP_RESOURCES and len(DELETE_BEVOR_EXPORT) > 0:
                odem_process.delete_before_export(DELETE_BEVOR_EXPORT)
            odem_process.export_data()
        _mode = 'sequential' if SEQUENTIAL else f'n_execs:{EXECUTORS}'
        odem_process.logger.info("[%s] duration: %s/%s (%s)", odem_process.process_identifier,
                                     odem_process.statistics['timedelta'], _mode, odem_process.statistics)
        LOGGER.info("[%s] odem done in '%s' (%d executors)",
                    odem_process.process_identifier, odem_process.statistics['timedelta'], EXECUTORS)
    except odem.ODEMNoTypeForOCRException as type_unknown:
        LOGGER.warning("[%s] odem skips '%s'",
                       odem_process.process_identifier, type_unknown.args[0])
    except odem.ODEMNoImagesForOCRException as not_ocrable:
        LOGGER.warning("[%s] odem no ocrables '%s'",
                       odem_process.process_identifier, not_ocrable.args)
    except odem.ODEMException as _odem_exc:
        _err_args = {'ODEMException': _odem_exc.args[0]}
        LOGGER.error("[%s] odem fails with: '%s'", odem_process.process_identifier, _err_args)
    except RuntimeError as exc:
        LOGGER.error("odem fails for '%s' after %s with: '%s'",
                     record, odem_process.statistics['timedelta'], str(exc))
        sys.exit(1)
