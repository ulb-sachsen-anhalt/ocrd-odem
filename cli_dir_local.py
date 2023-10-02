# -*- coding: utf-8 -*-
"""MAIN CLI ODEM"""

import argparse
import os
import pathlib
import shutil
import sys

from configparser import (
    ConfigParser,
)

from ocrd_utils import (
    initLogging
)

from lib.ocrd3_odem import (
    ARG_S_EXECS,
    ARG_S_LANGUAGES,
    ARG_S_MODEL_MAP,
    ARG_S_SEQUENTIAL_MODE,
    ARG_L_EXECS,
    ARG_L_LANGUAGES,
    ARG_L_MODEL_MAP,
    ARG_L_SEQUENTIAL_MODE,
    CFG_SEC_OCR,
    DEFAULT_EXECUTORS,
    KEY_EXECS,
    ODEMProcess,
    get_configparser,
    get_logger,
    merge_args,
)


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
        f"-{ARG_S_EXECS}",
        f"--{ARG_L_EXECS}",
        required=False,
        default=DEFAULT_EXECUTORS,
        type=int,
        help="Number of parallel OCR-D Executors")
    PARSER.add_argument(
        f"-{ARG_S_SEQUENTIAL_MODE}",
        f"--{ARG_L_SEQUENTIAL_MODE}",
        required=False,
        default=False,
        action="store_true",
        help="Disable parallel workflow and run each image sequential")
    PARSER.add_argument(
        f"-{ARG_S_LANGUAGES}",
        f"--{ARG_L_LANGUAGES}",
        required=False,
        help="ISO 639-3 language code (default:unset)")
    PARSER.add_argument(
        f"-{ARG_S_MODEL_MAP}",
        f"--{ARG_L_MODEL_MAP}",
        help="List of comma-separated pairs <ISO 639-3 language code>: <ocr-model> (default:unset)")
    ARGS = PARSER.parse_args()

    # check some pre-conditions
    # inspect configuration settings
    CONF_FILE = os.path.abspath(ARGS.config)
    if not os.path.exists(CONF_FILE):
        print(f"[ERROR] no config at '{CONF_FILE}'! Halt execution!")
        sys.exit(1)

    CFG: ConfigParser = get_configparser()
    configurations_read = CFG.read(CONF_FILE)
    if not configurations_read:
        print(f"unable to read config from '{CONF_FILE}! exit!")
        sys.exit(1)

    # set work_dirs and logger
    LOCAL_WORK_ROOT = CFG.get('global', 'local_work_root')
    initLogging()
    log_dir = CFG.get('global', 'local_log_dir')
    if not os.path.exists(log_dir) or not os.access(
            log_dir, os.W_OK):
        raise RuntimeError(f"cant store log files at invalid {log_dir}")
    LOGGER = get_logger(log_dir)

    # inspect what kind of input to process
    # oai record file *OR* local data directory must be set
    ROOT_PATH = ARGS.path
    MERGED = merge_args(CFG, ARGS)
    LOGGER.info("merged '%s' config entries with args", MERGED)
    EXECUTORS = CFG.getint(CFG_SEC_OCR, KEY_EXECS)
    RUN_MODE = ARGS.sequential_mode
    LOGGER.info("process data in '%s' with %s executors in mode %s",
                LOCAL_WORK_ROOT, EXECUTORS, RUN_MODE)

    req_idn = 'n.a.'
    try:
        req_idn = os.path.basename(ROOT_PATH)
        req_dst_dir = os.path.join(LOCAL_WORK_ROOT, req_idn)
        if os.path.exists(req_dst_dir):
            shutil.rmtree(req_dst_dir)
        os.makedirs(req_dst_dir, exist_ok=True)
        PROCESS = ODEMProcess(None, req_dst_dir, EXECUTORS)
        PROCESS.local_mode = True
        PROCESS.cfg = CFG
        PROCESS.the_logger = LOGGER
        local_images = PROCESS.get_local_image_paths(image_local_dir=ROOT_PATH)
        PROCESS._statistics['n_images_total'] = len(local_images)
        PROCESS._statistics['n_images_ocrable'] = 0
        PROCESS.images_4_ocr = local_images
        # Type and Value change!!!
        # ODEMProcess.single_ocr() needs Tuple[str,str], in non-local
        # this is assigned to "PROCESS.images_4_ocr" in ODEMProcess.filter_images()
        # thats why we have to manually fit that requirement
        PROCESS.images_4_ocr = list(zip(PROCESS.images_4_ocr, [pathlib.Path(i).stem for i in PROCESS.images_4_ocr]))
        if RUN_MODE:
            PROCESS.run_sequential()
        else:
            PROCESS.run_parallel()
        PROCESS.the_logger.info("[%s] duration: %s (%s)", req_idn,
                                PROCESS.duration, PROCESS.statistics)
    except Exception as exc:
        LOGGER.error("odem fails for '%s' after %s with: '%s'",
                     req_idn, PROCESS.duration, str(exc))
        sys.exit(0)
