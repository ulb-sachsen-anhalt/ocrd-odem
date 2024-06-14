# -*- coding: utf-8 -*-
"""MAIN CLI ODEM"""

import argparse
import configparser
import os
import pathlib
import shutil
import sys

import ocrd_utils

import lib.odem as odem


########
# MAIN #
########
if __name__ == "__main__":
    PARSER = argparse.ArgumentParser(
        description="generate ocr-data for OAI-Record")
    PARSER.add_argument(
        "path",
        help="root path to images that will be used to create OCR")
    PARSER.add_argument(
        "-c",
        "--config",
        required=False,
        default="resources/odem.ini",
        help="path to configuration file")
    PARSER.add_argument(
        f"-{odem.ARG_S_EXECS}",
        f"--{odem.ARG_L_EXECS}",
        required=False,
        default=odem.DEFAULT_EXECUTORS,
        type=int,
        help="Number of parallel OCR-D Executors")
    PARSER.add_argument(
        f"-{odem.ARG_S_SEQUENTIAL_MODE}",
        f"--{odem.ARG_L_SEQUENTIAL_MODE}",
        required=False,
        default=False,
        action="store_true",
        help="Disable parallel workflow and run each image sequential")
    PARSER.add_argument(
        f"-{odem.ARG_S_LANGUAGES}",
        f"--{odem.ARG_L_LANGUAGES}",
        required=False,
        help="ISO 639-3 language code (default:unset)")
    PARSER.add_argument(
        f"-{odem.ARG_S_MODEL_MAP}",
        f"--{odem.ARG_L_MODEL_MAP}",
        help="List of comma-separated pairs <ISO 639-3 language code>: <ocr-model> (default:unset)")
    ARGS = PARSER.parse_args()

    # check some pre-conditions
    # inspect configuration settings
    CONF_FILE = os.path.abspath(ARGS.config)
    if not os.path.exists(CONF_FILE):
        print(f"[ERROR] no config at '{CONF_FILE}'! Halt execution!")
        sys.exit(1)

    CFG: configparser.ConfigParser = odem.get_configparser()
    configurations_read = CFG.read(CONF_FILE)
    if not configurations_read:
        print(f"unable to read config from '{CONF_FILE}! exit!")
        sys.exit(1)

    # set work_dirs and logger
    LOCAL_WORK_ROOT = CFG.get('global', 'local_work_root')
    ocrd_utils.initLogging()
    log_dir = CFG.get('global', 'local_log_dir')
    if not os.path.exists(log_dir) or not os.access(log_dir, os.W_OK):
        raise RuntimeError(f"cant store log files at invalid {log_dir}")
    LOGGER = odem.get_logger(log_dir)

    # inspect what kind of input to process
    # oai record file *OR* local data directory must be set
    ROOT_PATH = ARGS.path
    MERGED = odem.merge_args(CFG, ARGS)
    LOGGER.info("merged '%s' config entries with args", MERGED)
    EXECUTORS = CFG.getint(odem.CFG_SEC_OCR, odem.KEY_EXECS)
    RUN_SEQUENTIAL = ARGS.sequential_mode
    LOGGER.info("process data in '%s' with %s executors in mode %s",
                LOCAL_WORK_ROOT, EXECUTORS, RUN_SEQUENTIAL)

    req_idn = 'n.a.'
    try:
        req_idn = os.path.basename(ROOT_PATH)
        req_dst_dir = os.path.join(LOCAL_WORK_ROOT, req_idn)
        if os.path.exists(req_dst_dir):
            shutil.rmtree(req_dst_dir)
        os.makedirs(req_dst_dir, exist_ok=True)

        proc_type = CFG.get(odem.CFG_SEC_OCR, 'workflow_type', fallback=None)
        if proc_type is None:
            LOGGER.warning("no 'workflow_type' config option in section ocr defined. defaults to 'OCRD_PAGE_PARALLEL'")
        PROCESS: odem.ODEMProcessImpl = odem.ODEMProcessImpl.create(proc_type, None, req_dst_dir, EXECUTORS)
        PROCESS.local_mode = True
        PROCESS.odem_configuration = CFG
        PROCESS.the_logger = LOGGER
        local_images = PROCESS.get_local_image_paths(image_local_dir=ROOT_PATH)
        PROCESS._statistics_ocr[odem.STATS_KEY_N_PAGES] = len(local_images)
        PROCESS._statistics_ocr[odem.STATS_KEY_N_OCRABLE] = 0
        PROCESS._statistics_ocr[odem.STATS_KEY_N_EXECS] = EXECUTORS
        PROCESS.images_4_ocr = local_images
        # Type and Value change!!!
        # ODEMProcess.single_ocr() needs Tuple[str,str], in non-local
        # this is assigned to "PROCESS.images_4_ocr" in ODEMProcess.filter_images()
        # thats why we have to manually fit that requirement
        PROCESS.images_4_ocr = list(zip(PROCESS.images_4_ocr, [pathlib.Path(i).stem for i in PROCESS.images_4_ocr]))
        PROCESS.run()
        PROCESS.the_logger.info("[%s] duration: %s (%s)", req_idn,
                                PROCESS.duration, PROCESS.statistics)
    except Exception as exc:
        LOGGER.error("odem fails for '%s' after %s with: '%s'",
                     req_idn, PROCESS.duration, str(exc))
        sys.exit(0)
