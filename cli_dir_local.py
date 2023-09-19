# -*- coding: utf-8 -*-
"""MAIN CLI ODEM"""

import argparse
import os
import pathlib
import shutil
import sys

from ocrd_utils import (
    initLogging
)

from lib.ocrd3_odem import (
    get_configparser,
    get_logger,
    ODEMProcess,
)

DEFAULT_EXECUTORS = 2

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
        "-e",
        "--executors",
        required=False,
        help="Number of OCR-D Executors in parallel mode")
    PARSER.add_argument(
        "-m",
        "--mode-sequential",
        required=False,
        default=False,
        action="store_true",
        help="Disable parallel mode, just run sequential")
    ARGS = PARSER.parse_args()

    # check some pre-conditions
    # inspect configuration settings
    CONF_FILE = os.path.abspath(ARGS.config)
    if not os.path.exists(CONF_FILE):
        print(f"[ERROR] no config at '{CONF_FILE}'! Halt execution!")
        sys.exit(1)
    EXECUTOR_ARGS = ARGS.executors
    RUN_MODE = ARGS.mode_sequential

    CFG = get_configparser()
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

    # if valid part_by via cli, use it's value
    if EXECUTOR_ARGS and int(EXECUTOR_ARGS) > 0:
        CFG.set('ocr', 'n_executors', str(EXECUTOR_ARGS))
    EXECUTORS = CFG.getint('ocr', 'n_executors', fallback=DEFAULT_EXECUTORS)
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
