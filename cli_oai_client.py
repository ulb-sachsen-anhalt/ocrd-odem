# -*- coding: utf-8 -*-
"""MAIN CLI ODEM OAI Client"""
# pylint: disable=invalid-name

import argparse
import ast
import logging
import os
import shutil
import sys
import time
import typing

import requests
import digiflow as df

import lib.odem as odem
import lib.odem.monitoring as odem_rm

# internal lock file
# when running lock mode
LOCK_FILE_NAME = '.workflow_running'
LOCK_FILE_PATH = os.path.join(os.path.dirname(__file__), LOCK_FILE_NAME)

# date format pattern
STATETIME_FORMAT = '%Y-%m-%d_%H:%M:%S'

LOGGER = None
CFG = None


def trnfrm(row):
    """callback function"""
    oai_id = row['IDENTIFIER']
    oai_record = df.OAIRecord(oai_id)
    return oai_record


def _notify(subject, message):
    if CFG.has_section('mail') and CFG.has_option('mail', 'connection'):
        try:
            conn = CFG.get('mail', 'connection')
            sender = CFG.get('mail', 'sender')
            recipiens = CFG.get('mail', 'recipients')
            df.smtp_note(conn, subject, message, sender, recipiens)
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
        self.logger: typing.Optional[logging.Logger] = None

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
            if odem.MARK_DATA_EXHAUSTED_PREFIX in str(result):
                if self.logger is not None:
                    self.logger.info(result)
                raise odem.OAIRecordExhaustedException(result.decode(encoding='utf-8'))
            # otherwise exit anyway
            sys.exit(1)

        if status != 200:
            if self.logger is not None:
                self.logger.error(
                    "OAI server connection status: %s -> %s", status, result)
            sys.exit(1)
        return response.json()

    def get_record(self) -> df.OAIRecord:
        """Return requested data
        as temporary OAI Record but
        store internally as plain dictionary"""

        self.record_data = self._request_record()
        _oai_record = df.OAIRecord(self.record_data[odem.RECORD_IDENTIFIER])
        return _oai_record

    def update(self, status, urn, **kwargs):
        """Store status update && send message to OAI Service"""
        if self.logger is not None:
            self.logger.debug("update record  status: %s urn: %s", status, urn)
        right_now = time.strftime(STATETIME_FORMAT)
        self.record_data[odem.RECORD_IDENTIFIER] = urn
        self.record_data[odem.RECORD_STATE] = status
        self.record_data[odem.RECORD_TIME] = right_now
        # if we have to report somethin' new, then append it
        if kwargs is not None and len(kwargs) > 0:
            try:
                prev_info = ast.literal_eval(self.record_data[odem.RECORD_INFO])
                prev_info.update(kwargs)
                self.record_data[odem.RECORD_INFO] = f'{prev_info}'
            except:
                self.logger.error("failed to update info data for %s",
                                  self.record_data[odem.RECORD_IDENTIFIER])
        if self.logger is not None:
            self.logger.debug("update record %s url %s", self.record_data, self.oai_server_url)
        return requests.post(f'{self.oai_server_url}/update', json=self.record_data, timeout=60)


CLIENT: typing.Optional[OAIServiceClient] = None


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
    CFG = odem.get_configparser()
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
    LOGGER = odem.get_logger(LOCAL_LOG_DIR, LOG_FILE_NAME)

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
    EXECUTORS = CFG.getint('ocr', 'n_executors', fallback=odem.DEFAULT_EXECUTORS)
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
    except odem.OAIRecordExhaustedException as _rec_ex:
        err_dict = _rec_ex.args[0]
        LOGGER.warning("no data for '%s' from '%s':'%s': %s",
                       OAI_RECORD_FILE_NAME, HOST, PORT, err_dict)
        _notify('[OCR-D-ODEM] Date done', err_dict)
        # don't remove lock file, human interaction required
        sys.exit(1)

    rec_ident = record.identifier
    local_ident = record.local_identifier
    req_dst_dir = os.path.join(LOCAL_WORK_ROOT, local_ident)
    odem_process: odem.ODEMProcess = odem.ODEMProcess(record, req_dst_dir)
    odem_process.the_logger = LOGGER
    odem_process.the_logger.debug(
        "request %s from %s, %s part slots)",
        local_ident,
        CLIENT.host, EXECUTORS
    )
    odem_process.odem_configuration = CFG

    try:
        if os.path.exists(req_dst_dir):
            shutil.rmtree(req_dst_dir)

        LOCAL_STORE_ROOT = CFG.get('global', 'local_store_root', fallback=None)
        if LOCAL_STORE_ROOT is not None:
            STORE_DIR = os.path.join(LOCAL_STORE_ROOT, local_ident)
            STORE = df.LocalStore(STORE_DIR, req_dst_dir)
            odem_process.store = STORE

        process_resource_monitor: odem_rm.ProcessResourceMonitor = odem_rm.ProcessResourceMonitor(
            odem_rm.from_configuration(CFG),
            LOGGER.error,
            CLIENT.update,
            _notify,
            odem_process.process_identifier,
            rec_ident
        )

        process_resource_monitor.check_vmem()
        process_resource_monitor.monit_disk_space(odem_process.load)
        odem_process.inspect_metadata()
        if CFG.getboolean('mets', 'prevalidate', fallback=True):
            odem_process.validate_metadata()
        odem_process.clear_existing_entries()
        odem_process.language_modelconfig()
        odem_process.set_local_images()

        # NEW NEW NEW
        proc_type: str = CFG.get('ocr', 'workflow_type', fallback=None)
        odem_pipeline = odem.ODEMOCRPipeline.create(proc_type, odem_process)
        odem_runner = odem.ODEMPipelineRunner(local_ident, EXECUTORS, LOGGER, odem_pipeline)
        ocr_results = process_resource_monitor.monit_vmem(odem_runner.run)
        if ocr_results is None or len(ocr_results) == 0:
            raise odem.ODEMException(f"process run error: {record.identifier}")
        ocr_results[odem.STATS_KEY_N_EXECS] = EXECUTORS
        odem_process.calculate_statistics_ocr(ocr_results)
        _stats_ocr = odem_process.statistics
        odem_process.the_logger.info("[%s] %s", local_ident, _stats_ocr)
        if ENRICH_METS_FULLTEXT:
            odem_process.link_ocr_files()
        if CREATE_PDF:
            odem_process.create_pdf()
        odem_process.postprocess_ocr()
        if CREATE_PDF:
            odem_process.create_text_bundle_data()
        odem_process.postprocess_mets()
        if CFG.getboolean('mets', 'postvalidate', fallback=True):
            odem_process.validate_metadata()
        if not MUST_KEEP_RESOURCES:
            odem_process.delete_before_export(LOCAL_DELETE_BEFORE_EXPORT)
        odem_process.export_data()
        # report outcome
        _response = CLIENT.update(odem.MARK_OCR_DONE, rec_ident, **_stats_ocr)
        status_code = _response.status_code
        if status_code == 200:
            LOGGER.info("[%s] state %s set", odem_process.process_identifier, status_code)
        else:
            LOGGER.error("[%s] update request failed: %s", odem_process.process_identifier, status_code)
        # finale
        odem_process.clear_resources(remove_all=True)
        LOGGER.info("[%s] odem done in '%s' (%d executors)",
                    odem_process.process_identifier, odem_process.duration, EXECUTORS)
    except odem.ODEMNoTypeForOCRException as type_unknown:
        LOGGER.warning("[%s] odem skips '%s'", 
                       odem_process.process_identifier,  type_unknown.args)
        err_dict = {'NoTypeForOCR': type_unknown.args[0]}
        CLIENT.update(status=odem.MARK_OCR_SKIP, urn=rec_ident, **err_dict)
        odem_process.clear_resources(remove_all=True)
    except odem.ODEMNoImagesForOCRException as not_ocrable:
        LOGGER.warning("[%s] odem no ocrables '%s'", 
                       odem_process.process_identifier,  not_ocrable.args)
        err_dict = {'NoImagesForOCR': not_ocrable.args[0]}
        CLIENT.update(status=odem.MARK_OCR_SKIP, urn=rec_ident, **err_dict)
        odem_process.clear_resources(remove_all=True)
    except odem.ODEMException as _odem_exc:
        # raised if record
        # * contains no PPN (gbv)
        # * contains no language mapping for mods:language
        # * misses model config for language
        # * contains no images
        # * contains no OCR results but should have at least one page
        err_dict = {'ODEMException': _odem_exc.args[0]}
        LOGGER.error("[%s] odem fails with ODEMException:"
                     "'%s'", odem_process.process_identifier, err_dict)
        CLIENT.update(status=odem.MARK_OCR_FAIL, urn=rec_ident, **err_dict)
        _notify(f'[OCR-D-ODEM] Failure for {rec_ident}', f'{err_dict}')
        odem_process.clear_resources()
    except odem_rm.NotEnoughDiskSpaceException as _space_exc:
        err_dict = {'NotEnoughDiskSpaceException': _space_exc.args[0]}
        LOGGER.error("[%s] odem fails with NotEnoughDiskSpaceException:"
                     "'%s'", odem_process.process_identifier, err_dict)
        CLIENT.update(status=odem.MARK_OCR_FAIL, urn=rec_ident, info=err_dict)
        _notify(f'[OCR-D-ODEM] Failure for {rec_ident}', f'{err_dict}')
        LOGGER.warning("[%s] remove working sub_dirs beneath '%s'",
                       odem_process.process_identifier, LOCAL_WORK_ROOT)
        odem_process.clear_resources(remove_all=True)
    except odem_rm.VirtualMemoryExceededException as _vmem_exc:
        err_dict = {'VirtualMemoryExceededException': _vmem_exc.args[0]}
        LOGGER.error("[%s] odem fails with NotEnoughDiskSpaceException:"
                     "'%s'", odem_process.process_identifier, err_dict)
        CLIENT.update(status=odem.MARK_OCR_FAIL, urn=rec_ident, info=err_dict)
        _notify(f'[OCR-D-ODEM] Failure for {rec_ident}', f'{err_dict}')
        LOGGER.warning("[%s] remove working sub_dirs beneath '%s'",
                       odem_process.process_identifier, LOCAL_WORK_ROOT)
        odem_process.clear_resources(remove_all=True)
    except Exception as exc:
        # pick whole error context, since some exception's args are
        # rather mysterious, i.e. "13" for PermissionError
        err_dict = {str(exc): str(exc.args[0])}
        _name = type(exc).__name__
        LOGGER.error("[%s] odem fails with %s:"
                     "'%s'", odem_process.process_identifier, _name, err_dict)
# when running parallel
        CLIENT.update(status=odem.MARK_OCR_FAIL, urn=rec_ident, info=err_dict)
        _notify(f'[OCR-D-ODEM] Failure for {rec_ident}', f'{err_dict}')
        odem_process.clear_resources()
        # don't remove lock file, human interaction required
        sys.exit(1)

        # if exception thrown previously which doesn't
        # resulted in hard workflow exit(1) then
        # remove the workflow lock file finally
        # to try next data record after the flesh
    if MUST_LOCK and os.path.isfile(LOCK_FILE_PATH):
        os.remove(LOCK_FILE_PATH)
        LOGGER.info("[%s] finally removed %s, ready for next onslaught",
                    odem_process.process_identifier, LOCK_FILE_PATH)
