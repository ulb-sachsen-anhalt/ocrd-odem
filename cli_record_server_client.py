"""MAIN CLI ODEM OAI Client"""
# pylint: disable=invalid-name
# pylint: disable=broad-exception-caught

import argparse
import configparser
import logging
import os
import shutil
import sys
import time

import digiflow as df
import digiflow.record as df_r

from lib import odem
import lib.odem.commons as odem_c
import lib.odem.monitoring.datatypes as odem_md
import lib.odem.monitoring.resource as odem_rm

# internal lock file
# when running lock mode
LOCK_FILE_NAME = '.workflow_running'
LOCK_FILE_PATH = os.path.join(os.path.dirname(__file__), LOCK_FILE_NAME)

# date format pattern
STATETIME_FORMAT = '%Y-%m-%d_%H:%M:%S'

LOGGER: logging.Logger = None
CFG: configparser.ConfigParser = None


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


def _oai_arg_parser(value):
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
        description="generate ocr-data for records")
    PARSER.add_argument(
        "data_file",
        type=_oai_arg_parser,
        help="name of record data file managed by server")
    PARSER.add_argument(
        "-c",
        "--config",
        help="absolute path to configuration file")
    PARSER.add_argument(
        "-e",
        "--executors",
        required=False,
        help="number of parallel executors, overwrites configuration")

    # evaluate commandline arguments
    ARGS = PARSER.parse_args()
    OAI_RECORD_FILE_NAME = ARGS.data_file

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

    # set work_dirs and logger
    LOCAL_WORK_ROOT = CFG.get(odem_c.CFG_SEC_FLOW, 'local_work_root')
    LOG_FILE_NAME = None
    if CFG.has_option(odem_c.CFG_SEC_FLOW, 'logfile_name'):
        LOG_FILE_NAME = CFG.get(odem_c.CFG_SEC_FLOW, 'logfile_name')
    LOCAL_LOG_DIR = CFG.get(odem_c.CFG_SEC_FLOW, 'local_log_dir')
    if not os.path.exists(LOCAL_LOG_DIR) or not os.access(
            LOCAL_LOG_DIR, os.W_OK):
        raise RuntimeError(f"cant store log files at invalid {LOCAL_LOG_DIR}")
    LOGGER = odem.get_worker_logger(LOCAL_LOG_DIR, LOG_FILE_NAME)

    # respect possible lock
    if os.path.isfile(LOCK_FILE_PATH):
        LOGGER.info("workflow already running and locked, skip processing")
        sys.exit(0)
    else:
        LOGGER.info("set workflow lock %s right now", LOCK_FILE_PATH)
        with open(LOCK_FILE_PATH, mode="+w", encoding="UTF-8") as a_lock_file:
            the_msg = (f"start odem workflow with record file '{OAI_RECORD_FILE_NAME}' "
                       f"and configuration '{CONF_FILE}' at {time.strftime(STATETIME_FORMAT)}")
            a_lock_file.write(the_msg)

    # if valid n_executors via cli, use it's value
    EXECUTOR_ARGS = ARGS.executors
    if EXECUTOR_ARGS and int(EXECUTOR_ARGS) > 0:
        CFG.set(odem.CFG_SEC_OCR, odem_c.CFG_SEC_OCR_OPT_EXECS, str(EXECUTOR_ARGS))
    EXECUTORS = CFG.getint(odem.CFG_SEC_OCR, odem_c.CFG_SEC_OCR_OPT_EXECS)
    LOGGER.debug("local work_root: '%s', executors:%s", LOCAL_WORK_ROOT, EXECUTORS)
    # pylint: disable=no-member
    DATA_FIELDS = CFG.getlist(odem_c.CFG_SEC_FLOW, 'data_fields')
    HOST = CFG.get('record-server', 'record_server_url')
    PORT = CFG.getint('record-server', 'record_server_port')
    ODEM_OPEN = CFG.get('record-server', 'record_state_open', fallback=odem.MARK_OCR_OPEN)
    ODEM_BUSY = CFG.get('record-server', 'record_state_busy', fallback=odem.MARK_OCR_BUSY)
    ODEM_SKIP = CFG.get('record-server', 'record_state_skip', fallback=odem.MARK_OCR_SKIP)
    ODEM_FAIL = CFG.get('record-server', 'record_state_fails', fallback=odem.MARK_OCR_FAIL)
    ODEM_DONE = CFG.get('record-server', 'record_state_done', fallback=odem.MARK_OCR_DONE)
    LOGGER.info("client requests %s:%s/%s for records (state: %s, fmt:%s)",
                HOST, PORT, OAI_RECORD_FILE_NAME, ODEM_OPEN, DATA_FIELDS)
    CLIENT = df_r.Client(OAI_RECORD_FILE_NAME, HOST, PORT, logger=LOGGER)

    # try to get next data record
    try:
        record = CLIENT.get_record(get_record_state=ODEM_OPEN,
                                   set_record_state=ODEM_BUSY)
        if not record:
            # if no open data records, lock worker and exit
            LOGGER.info("no open records in '%s', work done", OAI_RECORD_FILE_NAME)
            sys.exit(1)
    except (odem.OAIRecordExhaustedException, df_r.RecordsServiceException) as req_exc:
        exc_dict = req_exc.args[0]
        LOGGER.warning("no data for '%s' from '%s':'%s': %s",
                       OAI_RECORD_FILE_NAME, HOST, PORT, exc_dict)
        _notify('[OCR-D-ODEM] Date done', exc_dict)
        # human interaction required
        sys.exit(1)

    rec_ident = record.identifier
    local_ident = record.local_identifier
    req_dst_dir = os.path.join(LOCAL_WORK_ROOT, local_ident)
    odem_process: odem.ODEMProcessImpl = odem.ODEMProcessImpl(CFG, req_dst_dir,
                                                              LOGGER, None,
                                                              record=record)
    try:
        if os.path.exists(req_dst_dir):
            shutil.rmtree(req_dst_dir)

        local_store_root = CFG.get(odem_c.CFG_SEC_FLOW, 'local_store_root', fallback=None)
        if local_store_root is not None:
            store_root_dir = os.path.join(local_store_root, local_ident)
            odem_process.store = df.LocalStore(store_root_dir, req_dst_dir)

        pr_monitor: odem_rm.ProcessResourceMonitor = odem_rm.ProcessResourceMonitor(
            odem_rm.from_configuration(CFG),
            LOGGER.error,
            CLIENT.update,
            _notify,
            odem_process.process_identifier,
            rec_ident
        )

        pr_monitor.check_vmem()
        pr_monitor.monit_disk_space(odem_process.load)
        odem_process.inspect_metadata()
        odem_process.validate_metadata()
        odem_process.modify_mets_groups()
        odem_process.resolve_language_modelconfig()
        odem_process.set_local_images()
        proc_type = CFG.get(odem.CFG_SEC_OCR, 'workflow_type', fallback=None)
        ocr_workflow = odem.OCRWorkflow.create(proc_type, odem_process)
        the_runner = odem.OCRWorkflowRunner(local_ident, EXECUTORS, LOGGER, ocr_workflow)
        if CFG.getboolean(odem.CFG_SEC_MONITOR, 'live', fallback=False):
            LOGGER.info("[%s] live-monitoring of ocr workflow resources",
                        local_ident)
            ocr_results = pr_monitor.monit_vmem(the_runner.run)
        else:
            LOGGER.info("[%s] execute ocr workflow with poolsize %d",
                        local_ident, EXECUTORS)
            ocr_results = the_runner.run()
        odem_process.postprocess(ocr_results)
        # communicate outcome
        the_stats = odem_process.statistics
        the_resp = CLIENT.update(ODEM_DONE, rec_ident, **the_stats)
        status_code = the_resp.status_code
        if status_code == 200:
            LOGGER.info("[%s] state %s set", odem_process.process_identifier, status_code)
        else:
            LOGGER.error("[%s] update request failed: %s", odem_process.process_identifier,
                         status_code)
        # finale
        LOGGER.info("[%s] odem done in '%s' (%d executors)",
                    odem_process.process_identifier,
                    odem_process.statistics['timedelta'], EXECUTORS)
    except (odem.ODEMNoTypeForOCRException,
            odem.ODEMNoImagesForOCRException,
            odem.ODEMModelMissingException) as odem_missmatch:
        exc_label = odem_missmatch.__class__.__name__
        LOGGER.warning("[%s] odem skips '%s'",
                       odem_process.process_identifier, odem_missmatch.args)
        exc_dict = {exc_label: odem_missmatch.args[0]}
        exc_suffix = ""
        if exc_label == "ODEMNoTypeForOCRException":
            exc_suffix = "_type"
        elif exc_label == "ODEMNoImagesForOCRException":
            exc_suffix = "_input"
        elif exc_label == "ODEMModelMissingException":
            exc_suffix = "_model"
        the_state = f"{ODEM_SKIP}{exc_suffix}"
        CLIENT.update(status=the_state, oai_urn=rec_ident, **exc_dict)
        odem_process.clear_mets_resources()
    except (odem.ODEMMetadataMetsException, odem.ODEMDerivateException,
            odem.ODEMException) as data_exc:
        # raised if record
        # * contains not required PPN identifier ("gbv", "vd17-ppn")
        # * contains no language mapping for mods:language
        # * misses model config for language
        # * contains no images
        # * contains no OCR results but should have at least one page
        exc_dict = {'ODEMException': data_exc.args[0]}
        LOGGER.error("[%s] odem fails with ODEMException:"
                     "'%s'", odem_process.process_identifier, exc_dict)
        CLIENT.update(status=ODEM_FAIL, oai_urn=rec_ident, **exc_dict)
        _notify(f'[OCR-D-ODEM] Failure for {rec_ident}', f'{exc_dict}')
        odem_process.clear_mets_resources()
    except odem_md.NotEnoughDiskSpaceException as _space_exc:
        exc_dict = {'NotEnoughDiskSpaceException': _space_exc.args[0]}
        LOGGER.error("[%s] odem fails with NotEnoughDiskSpaceException:"
                     "'%s'", odem_process.process_identifier, exc_dict)
        CLIENT.update(status=ODEM_FAIL, oai_urn=rec_ident, info=exc_dict)
        _notify(f'[OCR-D-ODEM] Failure for {rec_ident}', f'{exc_dict}')
        LOGGER.warning("[%s] remove working sub_dirs beneath '%s'",
                       odem_process.process_identifier, LOCAL_WORK_ROOT)
        odem_process.clear_mets_resources()
    except odem_md.VirtualMemoryExceededException as _vmem_exc:
        exc_dict = {'VirtualMemoryExceededException': _vmem_exc.args[0]}
        LOGGER.error("[%s] odem fails with NotEnoughDiskSpaceException:"
                     "'%s'", odem_process.process_identifier, exc_dict)
        CLIENT.update(status=ODEM_FAIL, oai_urn=rec_ident, info=exc_dict)
        _notify(f'[OCR-D-ODEM] Failure for {rec_ident}', f'{exc_dict}')
        LOGGER.warning("[%s] remove working sub_dirs beneath '%s'",
                       odem_process.process_identifier, LOCAL_WORK_ROOT)
        odem_process.clear_mets_resources()

    except Exception as exc:
        # pick whole error context, since some exception's args are
        # rather mysterious, i.e. "13" for PermissionError
        exc_dict = {str(exc): str(exc.args[0])}
        _name = type(exc).__name__
        LOGGER.error("[%s] odem fails with %s:"
                     "'%s'", odem_process.process_identifier, _name, exc_dict)
        # when running parallel
        CLIENT.update(status=ODEM_FAIL, oai_urn=rec_ident, info=exc_dict)
        _notify(f'[OCR-D-ODEM] Failure for {rec_ident}', f'{exc_dict}')
        odem_process.clear_mets_resources()
        # don't remove lock file, human interaction required
        sys.exit(1)

    # if exception thrown previously which doesn't
    # resulted in hard workflow exit(1) then
    # remove the workflow lock file finally
    # to try next data record after the flesh
    if os.path.isfile(LOCK_FILE_PATH):
        os.remove(LOCK_FILE_PATH)
        LOGGER.info("[%s] finally removed %s, ready for next onslaught",
                    odem_process.process_identifier, LOCK_FILE_PATH)
