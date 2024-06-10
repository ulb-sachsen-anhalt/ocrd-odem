# -*- coding: utf-8 -*-
"""MAIN CLI ODEM OAI Client"""
# pylint: disable=invalid-name
import argparse
import os
import shutil
import sys
import time

from logging import (
    Logger
)
from typing import (
    Optional,
)
import requests

from digiflow import (
    OAIRecord,
    LocalStore,
    smtp_note,
)

import lib.ocrd3_odem as o3o

from lib.resources_monitoring import ProcessResourceMonitorConfig
from lib.resources_monitoring.ProcessResourceMonitor import ProcessResourceMonitor
from lib.resources_monitoring.exceptions import (
    NotEnoughDiskSpaceException,
    VirtualMemoryExceededException,
)
from lib.ocrd3_odem.odem_commons import (
    RECORD_IDENTIFIER,
    RECORD_INFO,
    RECORD_STATE,
    RECORD_TIME,
)
from lib.ocrd3_odem import (
    MARK_OCR_DONE,
    MARK_OCR_OPEN,
    MARK_OCR_FAIL,
    OdemWorkflowProcessType,
    ODEMProcess,
    OCRDPageParallel,
    ODEMException,
    get_configparser,
    get_logger,
)

from cli_oai_server import (
    MARK_DATA_EXHAUSTED_PREFIX
)

# number of OCR-D executors
# when running parallel
DEFAULT_EXECUTORS = 2

# internal lock file
# when running lock mode
LOCK_FILE_NAME = '.workflow_running'
LOCK_FILE_PATH = os.path.join(os.path.dirname(__file__), LOCK_FILE_NAME)

# date format pattern
STATETIME_FORMAT = '%Y-%m-%d_%H:%M:%S'

LOGGER = None
CFG = None


class OAIRecordExhaustedException(Exception):
    """Mark that given file contains no open records"""


def trnfrm(row):
    """callback function"""
    oai_id = row['IDENTIFIER']
    oai_record = OAIRecord(oai_id)
    return oai_record


def _notify(subject, message):
    if CFG.has_section('mail') and CFG.has_option('mail', 'connection'):
        try:
            conn = CFG.get('mail', 'connection')
            sender = CFG.get('mail', 'sender')
            recipiens = CFG.get('mail', 'recipients')
            smtp_note(conn, subject, message, sender, recipiens)
        except Exception as _exc:
            LOGGER.error(_exc)
    else:
        LOGGER.warning("No [mail] section in config, no mail sent!")


class OAIServiceClient:
    """Implementation of OAI Service client with
    capabilities to get next OAI Record data
    and communicate results (done|fail)
    """

    def __init__(self, oai_record_list_label, host, port):
        self.oai_record_list_label = oai_record_list_label
        self.port = port
        self.host = host
        self.record_data = {}
        self.oai_server_url = \
            f'http://{self.host}:{self.port}/{oai_record_list_label}'
        self.logger: Optional[Logger] = None

    def _request_record(self):
        """Request next open OAI record from service
           return OAIRecord as json encoded content"""
        try:
            response = requests.get(f'{self.oai_server_url}/next', timeout=300)
        except requests.exceptions.RequestException as err:
            if self.logger is not None:
                self.logger.error("OAI server connection fails: %s", err)
            _notify(f'[OCR-D-ODEM] Failure for {self.oai_server_url}', err)
            sys.exit(1)
        status = response.status_code
        result = response.content
        if status == 404:
            # probably nothing more to do?
            if MARK_DATA_EXHAUSTED_PREFIX in str(result):
                if self.logger is not None:
                    self.logger.info(result)
                raise OAIRecordExhaustedException(result.decode(encoding='utf-8'))
            # otherwise exit anyway
            sys.exit(1)

        if status != 200:
            if self.logger is not None:
                self.logger.error(
                    "OAI server connection status: %s -> %s", status, result)
            sys.exit(1)
        return response.json()

    def get_record(self) -> OAIRecord:
        """Return requested data
        as temporary OAI Record but
        store internally as plain dictionary"""

        self.record_data = self._request_record()
        _oai_record = OAIRecord(self.record_data[RECORD_IDENTIFIER])
        return _oai_record

    def update(self, status, urn, **kwargs):
        """Store status update && send message to OAI Service"""
        if self.logger is not None:
            self.logger.debug("update record  status: %s urn: %s", status, urn)
        right_now = time.strftime(STATETIME_FORMAT)
        self.record_data[RECORD_IDENTIFIER] = urn
        self.record_data[RECORD_STATE] = status
        self.record_data[RECORD_TIME] = right_now
        # if we have to report somethin' new, then append it
        if kwargs is not None and len(kwargs) > 0:
            self.record_data[RECORD_INFO] = f'{kwargs}'
        if self.logger is not None:
            self.logger.debug("update record %s url %s", self.record_data, self.oai_server_url)
        return requests.post(f'{self.oai_server_url}/update', json=self.record_data, timeout=60)


CLIENT: Optional[OAIServiceClient] = None


def oai_arg_parser(value):
    """helper function for parsing args"""
    if '.' in value:
        print(
            'Please provide datafile argument as *pure* name '
            'with no extension')
        value = value.rsplit('.', 1)[0]
        sys.exit(1)
    if '/' in value:
        print(
            'Please provide datafile argument as *pure* name '
            'with no path')
        value = value.rsplit('/', 1)[1]
        sys.exit(1)
    return value


def _clear_sub_dirs(root_dir: str):
    for sub_dir in os.listdir(root_dir):
        LOGGER.debug("remove dir %s in %s", sub_dir, root_dir)
        shutil.rmtree(os.path.join(root_dir, sub_dir))


########
# MAIN #
########
if __name__ == "__main__":
    PARSER = argparse.ArgumentParser(
        description="generate ocr-data for OAI-Record")
    PARSER.add_argument(
        "data_file",
        type=oai_arg_parser,
        help="Name of file with OAI-Record information")
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

    # evaluate commandline arguments
    ARGS = PARSER.parse_args()
    OAI_RECORD_FILE_NAME = ARGS.data_file
    MUST_KEEP_RESOURCES = ARGS.keep_resources
    MUST_LOCK = ARGS.lock_mode

    # check some pre-conditions
    # inspect configuration settings
    CONF_FILE = os.path.abspath(ARGS.config)
    if not os.path.exists(CONF_FILE):
        print(f"[ERROR] no config at '{CONF_FILE}'! Halt execution!")
        sys.exit(1)
    CFG = get_configparser()
    configurations_read = CFG.read(CONF_FILE)
    if not configurations_read:
        print(f"[ERROR] unable to read config from '{CONF_FILE}! exit!")
        sys.exit(1)

    CREATE_PDF: bool = CFG.getboolean('derivans', 'derivans_enabled', fallback=True)
    ENRICH_METS_FULLTEXT: bool = CFG.getboolean('export', 'enrich_mets_fulltext', fallback=True)

    # set work_dirs and logger
    LOCAL_WORK_ROOT = CFG.get('global', 'local_work_root')
    LOCAL_DELETE_BEFORE_EXPORT = []
    if CFG.has_option('export', 'delete_before_export'):
        LOCAL_DELETE_BEFORE_EXPORT = CFG.getlist('export', 'delete_before_export')
    LOG_FILE_NAME = None
    if CFG.has_option('global', 'logfile_name'):
        LOG_FILE_NAME = CFG.get('global', 'logfile_name')
    LOCAL_LOG_DIR = CFG.get('global', 'local_log_dir')
    if not os.path.exists(LOCAL_LOG_DIR) or not os.access(
            LOCAL_LOG_DIR, os.W_OK):
        raise RuntimeError(f"cant store log files at invalid {LOCAL_LOG_DIR}")
    LOGGER = get_logger(LOCAL_LOG_DIR, LOG_FILE_NAME)

    # respect possible lock
    if MUST_LOCK:
        LOGGER.debug("workflow lock mode enforced")
        if os.path.isfile(LOCK_FILE_PATH):
            LOGGER.info("workflow already running and locked, skip processing")
            sys.exit(0)
        else:
            LOGGER.info("set workflow lock %s right now", LOCK_FILE_PATH)
            with open(LOCK_FILE_PATH, mode="+w", encoding="UTF-8") as _lock_file:
                _msg = (f"start odem workflow with record file '{OAI_RECORD_FILE_NAME}' "
                        f"and configuration '{CONF_FILE}' at {time.strftime(STATETIME_FORMAT)}")
                _lock_file.write(_msg)
    else:
        LOGGER.warning("no workflow lock mode set, handle with great responsibility")

    # determine execution mode and how many
    # parallel OCR-D instances shall be used
    EXECUTOR_ARGS = ARGS.executors
    if EXECUTOR_ARGS and int(EXECUTOR_ARGS) > 0:
        CFG.set('ocr', 'n_executors', str(EXECUTOR_ARGS))
    EXECUTORS = CFG.getint('ocr', 'n_executors', fallback=DEFAULT_EXECUTORS)
    LOGGER.debug("local work_root: '%s', executors:%s, keep_res:%s, lock:%s",
                 LOCAL_WORK_ROOT, EXECUTORS, MUST_KEEP_RESOURCES, MUST_LOCK)

    DATA_FIELDS = CFG.getlist('global', 'data_fields')
    HOST = CFG.get('oai-server', 'oai_server_url')
    PORT = CFG.getint('oai-server', 'oai_server_port')
    LOGGER.info("OAIServiceClient instance listens %s:%s for '%s' (format:%s)",
                HOST, PORT, OAI_RECORD_FILE_NAME, DATA_FIELDS)
    CLIENT = OAIServiceClient(OAI_RECORD_FILE_NAME, HOST, PORT)
    CLIENT.logger = LOGGER

    # try to get next data record
    try:
        record = CLIENT.get_record()
        if not record:
            # if no open data records, lock worker and exit
            LOGGER.info("no open records in '%s', work done", OAI_RECORD_FILE_NAME)
            sys.exit(1)
    except OAIRecordExhaustedException as _rec_ex:
        _err_args = _rec_ex.args[0]
        LOGGER.warning("no data for '%s' from '%s':'%s': %s",
                       OAI_RECORD_FILE_NAME, HOST, PORT, _err_args)
        _notify('[OCR-D-ODEM] Date done', _err_args)
        # don't remove lock file, human interaction required
        sys.exit(1)

    STATE = MARK_OCR_OPEN

    rec_ident = record.identifier
    local_ident = record.local_identifier
    req_dst_dir = os.path.join(LOCAL_WORK_ROOT, local_ident)

    proc_type: str = CFG.get('ocr', 'workflow_type', fallback=None)
    if proc_type is None:
        LOGGER.warning("no 'workflow_type' config option in section 'ocr' defined. defaults to 'OCRD_PAGE_PARALLEL'")
    PROCESS: ODEMProcess = ODEMProcess.create(proc_type, record, req_dst_dir, EXECUTORS)

    PROCESS.the_logger = LOGGER
    PROCESS.the_logger.debug(
        "request %s from %s, %s part slots)",
        local_ident,
        CLIENT.host, EXECUTORS
    )
    PROCESS.cfg = CFG

    try:
        if os.path.exists(req_dst_dir):
            shutil.rmtree(req_dst_dir)

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
            CLIENT.update,
            _notify,
            PROCESS.process_identifier,
            rec_ident
        )

        process_resource_monitor.check_vmem()
        process_resource_monitor.monit_disk_space(PROCESS.load)
        PROCESS.inspect_metadata()
        if CFG.getboolean('mets', 'prevalidate', fallback=True):
            PROCESS.validate_metadata()
        PROCESS.clear_existing_entries()
        PROCESS.language_modelconfig()
        PROCESS.set_local_images()

        outcomes = process_resource_monitor.monit_vmem(PROCESS.run)
        PROCESS.calculate_statistics_ocr(outcomes)
        _stats_ocr = PROCESS.statistics
        PROCESS.the_logger.info("[%s] %s", local_ident, _stats_ocr)
        if ENRICH_METS_FULLTEXT:
            PROCESS.link_ocr()
        if CREATE_PDF:
            PROCESS.create_pdf()
        PROCESS.postprocess_ocr()
        if CREATE_PDF:
            PROCESS.create_text_bundle_data()
        PROCESS.postprocess_mets()
        if CFG.getboolean('mets', 'postvalidate', fallback=True):
            PROCESS.validate_metadata()
        if not MUST_KEEP_RESOURCES:
            PROCESS.delete_before_export(LOCAL_DELETE_BEFORE_EXPORT)
        PROCESS.export_data()
        # report outcome
        _response = CLIENT.update(MARK_OCR_DONE, rec_ident, **_stats_ocr)
        status_code = _response.status_code
        if status_code == 200:
            LOGGER.info("[%s] state %s set", PROCESS.process_identifier, status_code)
        else:
            LOGGER.error("[%s] update request failed: %s", PROCESS.process_identifier, status_code)
        # finale
        shutil.rmtree(req_dst_dir)
        LOGGER.info("[%s] odem done in '%s' (%d executors)",
                    PROCESS.process_identifier, PROCESS.duration, EXECUTORS)
    except o3o.ODEMNoTypeForOCRException as type_unknown:
        # we don't ocr this one
        LOGGER.warning("[%s] odem skips '%s'", 
                       PROCESS.process_identifier,  type_unknown.args)
        CLIENT.update(status=o3o.MARK_OCR_SKIP, urn=rec_ident)
    except ODEMException as _odem_exc:
        # raised if record
        # * contains no PPN (gbv)
        # * contains no language mapping for mods:language
        # * misses model config for language
        # * contains no images
        # * contains no OCR results but should have at least one page
        _err_args = {'ODEMException': _odem_exc.args[0]}
        LOGGER.error("[%s] odem fails with ODEMException:"
                     "'%s'", PROCESS.process_identifier, _err_args)
        CLIENT.update(status=MARK_OCR_FAIL, urn=rec_ident, info=_err_args)
        _notify(f'[OCR-D-ODEM] Failure for {rec_ident}', f'{_err_args}')
    except NotEnoughDiskSpaceException as _space_exc:
        _err_args = {'NotEnoughDiskSpaceException': _space_exc.args[0]}
        LOGGER.error("[%s] odem fails with NotEnoughDiskSpaceException:"
                     "'%s'", PROCESS.process_identifier, _err_args)
        CLIENT.update(status=MARK_OCR_FAIL, urn=rec_ident, info=_err_args)
        _notify(f'[OCR-D-ODEM] Failure for {rec_ident}', f'{_err_args}')
        LOGGER.warning("[%s] remove working sub_dirs beneath '%s'",
                       PROCESS.process_identifier, LOCAL_WORK_ROOT)
        _clear_sub_dirs(LOCAL_WORK_ROOT)
    except VirtualMemoryExceededException as _vmem_exc:
        _err_args = {'VirtualMemoryExceededException': _vmem_exc.args[0]}
        LOGGER.error("[%s] odem fails with NotEnoughDiskSpaceException:"
                     "'%s'", PROCESS.process_identifier, _err_args)
        CLIENT.update(status=MARK_OCR_FAIL, urn=rec_ident, info=_err_args)
        _notify(f'[OCR-D-ODEM] Failure for {rec_ident}', f'{_err_args}')
        LOGGER.warning("[%s] remove working sub_dirs beneath '%s'",
                       PROCESS.process_identifier, LOCAL_WORK_ROOT)
    except Exception as exc:
        # pick whole error context, since some exception's args are
        # rather mysterious, i.e. "13" for PermissionError
        _err_args = {str(exc): str(exc.args[0])}
        _name = type(exc).__name__
        LOGGER.error("[%s] odem fails with %s:"
                     "'%s'", PROCESS.process_identifier, _name, _err_args)
        CLIENT.update(status=MARK_OCR_FAIL, urn=rec_ident, info=_err_args)
        _notify(f'[OCR-D-ODEM] Failure for {rec_ident}', f'{_err_args}')
        # don't remove lock file, human interaction required
        sys.exit(1)

        # if exception thrown previously which doesn't
        # resulted in hard workflow exit(1) then
        # remove the workflow lock file finally
        # to try next data record after the flesh
    if MUST_LOCK and os.path.isfile(LOCK_FILE_PATH):
        os.remove(LOCK_FILE_PATH)
        LOGGER.info("[%s] finally removed %s, ready for next onslaught",
                    PROCESS.process_identifier, LOCK_FILE_PATH)
