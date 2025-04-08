# -*- coding: utf-8 -*-
"""MAIN CLI METS-based Workflow alike
legacy OCR-Pipeline from ULB newspaper
digitalization (2019-2022)

Please note, that this module with newspapers
currently only works well with Tesseract based
workflows. Regular OCR-D-Workflows are going
to die.
"""

import argparse
import os
import sys

from pathlib import Path

import digiflow.record as df_r

from lib import odem
import lib.odem.odem_commons as odem_c
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
    ARGS = PARSER.parse_args()

    # check some pre-conditions
    # inspect configuration settings
    CONF_FILE = os.path.abspath(ARGS.config)
    if not os.path.exists(CONF_FILE):
        print(f"[ERROR] no config at '{CONF_FILE}'! Halt execution!")
        sys.exit(1)

    EXECUTOR_ARGS = ARGS.executors
    CFG = odem.get_configparser()
    configurations_read = CFG.read(CONF_FILE)
    if not configurations_read:
        print(f"unable to read config from '{CONF_FILE}! exit!")
        sys.exit(1)

    # set work_dirs and logger
    LOCAL_LOG_DIR = CFG.get(odem_c.CFG_SEC_FLOW, 'local_log_dir')
    if not os.path.exists(LOCAL_LOG_DIR) or not os.access(
            LOCAL_LOG_DIR, os.W_OK):
        raise RuntimeError(f"cant store log files at invalid {LOCAL_LOG_DIR}")
    LOG_FILE_NAME = None
    if CFG.has_option(odem_c.CFG_SEC_FLOW, 'logfile_name'):
        LOG_FILE_NAME = CFG.get(odem_c.CFG_SEC_FLOW, 'logfile_name')
    LOGGER = odem.get_worker_logger(LOCAL_LOG_DIR, LOG_FILE_NAME)

    METS_FILE: Path = Path(ARGS.mets_file).resolve()
    if not METS_FILE.is_file():
        print(f"unable to read file '{METS_FILE}! exit!")
        sys.exit(1)
    LOGGER.info("use '%s'", METS_FILE)
    mets_file_dir = METS_FILE.parent

    # if valid n_executors via cli, use it's value
    if EXECUTOR_ARGS and int(EXECUTOR_ARGS) > 0:
        CFG.set(odem.CFG_SEC_OCR, odem_c.CFG_SEC_OCR_OPT_EXECS, str(EXECUTOR_ARGS))
    EXECUTORS = CFG.getint(odem.CFG_SEC_OCR, odem_c.CFG_SEC_OCR_OPT_EXECS,
                           fallback=DEFAULT_EXECUTORS)
    LOGGER.debug("local work_root: '%s', executors:%s", mets_file_dir, EXECUTORS)

    try:
        local_ident = METS_FILE.stem
        proc_type: str = CFG.get(odem.CFG_SEC_OCR, 'workflow_type', fallback=None)
        if proc_type is None:
            LOGGER.warning("no 'workflow_type' in section ocr defined. defaults to 'OCRD_PAGE_PARALLEL'")
        record = df_r.Record(urn=local_ident)
        odem_process: odem.ODEMProcessImpl = odem.ODEMProcessImpl(CFG, mets_file_dir,
                                                                  LOGGER, LOCAL_LOG_DIR, record)
        odem_process.logger = LOGGER
        odem_process.logger.info("[%s] odem from %s, %d executors", local_ident, METS_FILE, EXECUTORS)
        odem_process.configuration = CFG
        process_resource_monitor: odem_rm.ProcessResourceMonitor = odem_rm.ProcessResourceMonitor(
            odem_rm.from_configuration(CFG),
            LOGGER.error,
            None,
            odem_process.process_identifier,
            record.identifier
        )
        process_resource_monitor.check_vmem()
        process_resource_monitor.monit_disk_space(odem_process.load)
        odem_process.inspect_metadata()
        odem_process.validate_metadata()
        odem_process.modify_mets_groups()
        odem_process.resolve_language_modelconfig()
        odem_process.set_local_images()
        odem_pipeline = odem.ODEMWorkflow.create(proc_type, odem_process)
        odem_runner = odem.ODEMWorkflowRunner(local_ident, EXECUTORS, LOGGER, odem_pipeline)
        ocr_results = process_resource_monitor.monit_vmem(odem_runner.run)
        odem_process.postprocess(ocr_results)
        time_delta = odem_process.statistics['timedelta']
        odem_process.logger.info("[%s] duration: %s/%s (%s)", odem_process.process_identifier,
                                 time_delta, EXECUTORS, odem_process.statistics)
        LOGGER.info("[%s] odem done in '%s' (%d executors)",
                    odem_process.process_identifier, odem_process.statistics['timedelta'], EXECUTORS)
    except (odem.ODEMNoTypeForOCRException,
            odem.ODEMNoImagesForOCRException,
            odem.ODEMModelMissingException) as odem_missmatch:
        exc_label = odem_missmatch.__class__.__name__
        LOGGER.warning("[%s] odem skips '%s'",
                       odem_process.process_identifier, odem_missmatch.args)
    except odem.ODEMException as _odem_exc:
        _err_args = {'ODEMException': _odem_exc.args[0]}
        LOGGER.error("[%s] odem fails with: '%s'", odem_process.process_identifier, _err_args)
    except RuntimeError as exc:
        LOGGER.error("odem fails for '%s' after %s with: '%s'",
                     record, odem_process.statistics['timedelta'], str(exc))
        sys.exit(1)
