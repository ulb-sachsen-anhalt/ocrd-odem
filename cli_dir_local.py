# -*- coding: utf-8 -*-
"""MAIN CLI ODEM"""

import argparse
import configparser
import os
import pathlib
import shutil
import sys

import ocrd_utils

from lib import odem
import lib.odem.odem_commons as odem_c


########
# MAIN #
########
if __name__ == "__main__":
    PARSER = argparse.ArgumentParser(
        description="generate ocr-data from local directory")
    PARSER.add_argument(
        "path",
        help="root path to image directory to be used")
    PARSER.add_argument(
        "-c",
        "--config",
        required=False,
        help="path to configuration file")
    PARSER.add_argument(
        f"-{odem.ARG_S_EXECS}",
        f"--{odem.ARG_L_EXECS}",
        required=False,
        default=odem.DEFAULT_EXECUTORS,
        type=int,
        help="Number of parallel Executors. Setting to '1' implies sequential execution.")
    PARSER.add_argument(
        f"-{odem.ARG_S_LANGUAGES}",
        f"--{odem.ARG_L_LANGUAGES}",
        required=False,
        help="ISO 639-3 language code (default:unset)")
    PARSER.add_argument(
        f"-{odem.ARG_S_MODEL_MAP}",
        f"--{odem.ARG_L_MODEL_MAP}",
        help="""Map ISO 639-3 language code to local ocr model configuration label.\n
        For example "deu": "frk.traineddata".
        May include multiple, comma-separated mappings (default:unset)""")
    ARGS = vars(PARSER.parse_args())

    # check some pre-conditions
    # inspect configuration settings
    CONF_FILE = os.path.abspath(ARGS["config"])
    if not os.path.exists(CONF_FILE):
        print(f"[ERROR] no config at '{CONF_FILE}'! Halt execution!")
        sys.exit(1)

    CFG: configparser.ConfigParser = odem.get_configparser()
    configurations_read = CFG.read(CONF_FILE)
    if not configurations_read:
        print(f"unable to read config from '{CONF_FILE}! exit!")
        sys.exit(1)

    # set work_dirs and logger
    LOCAL_WORK_ROOT = CFG.get(odem_c.CFG_SEC_FLOW, 'local_work_root')
    ocrd_utils.initLogging()
    log_dir = CFG.get(odem_c.CFG_SEC_FLOW, 'local_log_dir')
    if not os.path.exists(log_dir) or not os.access(log_dir, os.W_OK):
        raise RuntimeError(f"cant store log files at invalid {log_dir}")
    LOGGER = odem.get_logger(log_dir)

    # inspect what kind of input to process
    # oai record file *OR* local data directory must be set
    ROOT_PATH = ARGS["path"]
    MERGED = odem.merge_args(CFG, ARGS)
    LOGGER.info("merged '%s' args with config entries", MERGED)
    EXECUTORS = CFG.getint(odem.CFG_SEC_OCR, odem.CFG_SEC_OCR_OPT_EXECS)
    LOGGER.info("process data in '%s' with %s executors",
                LOCAL_WORK_ROOT, EXECUTORS)

    REQ_IDENT = odem.UNSET
    try:
        REQ_IDENT = os.path.basename(ROOT_PATH)
        req_dst_dir = os.path.join(LOCAL_WORK_ROOT, REQ_IDENT)
        if os.path.exists(req_dst_dir):
            shutil.rmtree(req_dst_dir)
        os.makedirs(req_dst_dir, exist_ok=True)
        proc_type = CFG.get(odem.CFG_SEC_OCR, 'workflow_type', fallback=odem.DEFAULT_WORKLFOW)
        odem_process: odem.ODEMProcessImpl = odem.ODEMProcessImpl(CFG, ROOT_PATH, logger=LOGGER,
                                                                 log_dir=log_dir)
        local_images = odem_process.get_local_image_paths(image_local_dir=ROOT_PATH)
        odem_process.process_statistics[odem.STATS_KEY_N_PAGES] = len(local_images)
        odem_process.process_statistics[odem.STATS_KEY_N_OCRABLE] = 0
        odem_process.process_statistics[odem.STATS_KEY_N_EXECS] = EXECUTORS
        if odem.ARG_L_LANGUAGES in ARGS:
            languages = ARGS[odem.ARG_L_LANGUAGES].split(",")
            odem_process.resolve_language_modelconfig(languages)
        candidate_tuples = list(zip(local_images, [pathlib.Path(i).stem for i in local_images]))
        odem_process.ocr_candidates = candidate_tuples
        the_workflow: odem.ODEMWorkflow = odem.ODEMWorkflow.create(proc_type, odem_process)
        odem_runner = odem.ODEMWorkflowRunner(REQ_IDENT, EXECUTORS, LOGGER, the_workflow)
        odem_runner.run()
        odem_process.logger.info("[%s] duration: %s (%s)", REQ_IDENT,
                                odem_process.statistics['timedelta'], odem_process.statistics)
    except Exception as exc:
        LOGGER.error("odem fails for '%s' after %s with: '%s'",
                     REQ_IDENT, odem_process.statistics['timedelta'], str(exc))
        sys.exit(1)
