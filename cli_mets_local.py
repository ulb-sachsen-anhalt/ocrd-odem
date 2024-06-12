# -*- coding: utf-8 -*-
"""MAIN CLI plain OCR with optional export"""

import argparse
import os
import sys

from pathlib import Path

import digiflow as df

import lib.ocrd3_odem as o3o

from lib.resources_monitoring import ProcessResourceMonitor, ProcessResourceMonitorConfig

DEFAULT_EXECUTORS = 2


########
# MAIN #
########
if __name__ == "__main__":
    PARSER = argparse.ArgumentParser(
        description="generate ocr-data for OAI-Record")
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

    CFG = o3o.get_configparser()
    configurations_read = CFG.read(CONF_FILE)
    if not configurations_read:
        print(f"unable to read config from '{CONF_FILE}! exit!")
        sys.exit(1)

    CREATE_PDF: bool = CFG.getboolean('derivans', 'derivans_enabled', fallback=True)
    ENRICH_METS_FULLTEXT: bool = CFG.getboolean('export', 'enrich_mets_fulltext', fallback=True)

    # set work_dirs and logger
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
    LOGGER = o3o.get_logger(LOCAL_LOG_DIR, LOG_FILE_NAME)

    mets_file: Path = Path(ARGS.mets_file).absolute()
    if not mets_file.is_file():
        print(f"unable to read file '{mets_file}! exit!")
        sys.exit(1)
    LOGGER.info("use '%s'", mets_file)
    mets_file_dir = mets_file.parent

    # if valid n_executors via cli, use it's value
    if EXECUTOR_ARGS and int(EXECUTOR_ARGS) > 0:
        CFG.set('ocr', 'n_executors', str(EXECUTOR_ARGS))
    EXECUTORS = CFG.getint('ocr', 'n_executors', fallback=DEFAULT_EXECUTORS)
    if SEQUENTIAL:
        EXECUTORS = 1
    LOGGER.debug("local work_root: '%s', executors:%s, keep_res:%s, lock:%s",
                 mets_file_dir, EXECUTORS, MUST_KEEP_RESOURCES, MUST_LOCK)

    try:
        local_ident = mets_file.stem
        proc_type: str = CFG.get('ocr', 'workflow_type', fallback=None)
        if proc_type is None:
            LOGGER.warning("no 'workflow_type' config option in section 'ocr' defined. defaults to 'OCRD_PAGE_PARALLEL'")
        record = df.OAIRecord(local_ident)
        odem_process: o3o.ODEMProcess = o3o.ODEMProcess(record, mets_file_dir)
        odem_process.the_logger = LOGGER
        odem_process.the_logger.info("[%s] odem from %s, %d executors", local_ident, mets_file, EXECUTORS)
        odem_process.odem_configuration = CFG
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
            None,
            odem_process.process_identifier,
            record.identifier
        )
        process_resource_monitor.check_vmem()
        # process_resource_monitor.monit_disk_space(odem_process.load)
        odem_process.inspect_metadata()
        if CFG.getboolean('mets','prevalidate', fallback=True):
            odem_process.validate_metadata()
        odem_process.clear_existing_entries()
        odem_process.language_modelconfig()
        odem_process.set_local_images()

        # NEW NEW NEW
        odem_pipeline = o3o.ODEMOCRPipeline.create(proc_type, odem_process)
        odem_runner = o3o.ODEMPipelineRunner(local_ident, EXECUTORS, LOGGER, odem_pipeline)
        OUTCOMES = process_resource_monitor.monit_vmem(odem_runner.run)
        if OUTCOMES is None or len(OUTCOMES) == 0:
            raise o3o.ODEMException(f"process run error: {record.identifier}")
        
        odem_process.calculate_statistics_ocr(OUTCOMES)
        odem_process.the_logger.info("[%s] %s", local_ident, odem_process.statistics)
        odem_process.link_ocr()
        if CREATE_PDF:
            odem_process.create_pdf()
        odem_process.postprocess_ocr()
        if CREATE_PDF:
            odem_process.create_text_bundle_data()
        odem_process.postprocess_mets()
        if CFG.getboolean('mets','postvalidate', fallback=True):
            odem_process.validate_metadata()
        if odem_process.odem_configuration.has_option('export', 'local_export_dir'):
            odem_process.the_logger.info("[%s] start to export data", 
                                         odem_process.process_identifier)
            if not MUST_KEEP_RESOURCES:
                odem_process.delete_before_export(LOCAL_DELETE_BEVOR_EXPORT)
            odem_process.export_data()
        _mode = 'sequential' if SEQUENTIAL else f'n_execs:{EXECUTORS}'
        odem_process.the_logger.info("[%s] duration: %s/%s (%s)", odem_process.process_identifier,
                                odem_process.duration, _mode, odem_process.statistics)
        # finale
        LOGGER.info("[%s] odem done in '%s' (%d executors)",
                    odem_process.process_identifier, odem_process.duration, EXECUTORS)
    except o3o.ODEMNoTypeForOCRException as type_unknown:
        # we don't ocr this one
        LOGGER.warning("[%s] odem skips '%s'", 
                       odem_process.process_identifier, type_unknown.args[0])
    except o3o.ODEMNoImagesForOCRException as not_ocrable:
        LOGGER.warning("[%s] odem no ocrables '%s'", 
                       odem_process.process_identifier,  not_ocrable.args)
    except o3o.ODEMException as _odem_exc:
        _err_args = {'ODEMException': _odem_exc.args[0]}
        LOGGER.error("[%s] odem fails with: '%s'", odem_process.process_identifier, _err_args)
    except RuntimeError as exc:
        LOGGER.error("odem fails for '%s' after %s with: '%s'",
                     record, odem_process.duration, str(exc))
        sys.exit(1)
