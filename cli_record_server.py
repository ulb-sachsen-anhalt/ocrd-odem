"""Record Service
   Providing and manage access to local resource list files
"""

import configparser
import logging.config
import os
import sys

from pathlib import Path

import digiflow.record as df_r

import lib.odem.odem_commons as oc


PROJECT_ROOT = Path(__file__).resolve().parent
DEFAULT_ODEM_LOGCONF_FILE = "odem_logging.ini"
DEFAULT_LOGCONF_DIR = PROJECT_ROOT / "resources"
DEFAULT_ODEM_LOGCONF = DEFAULT_LOGCONF_DIR / DEFAULT_ODEM_LOGCONF_FILE
DEFAULT_LOG_FILE = "odem_service.log"
DEFAULT_ODEM_LOG_NAME = "odem.service"

########
# Script
########
# since unknown what may happen with server
# pylint: disable=broad-exception-caught
if __name__ == "__main__":
    try:
        config_file = sys.argv[1]
    except IndexError:
        print('please provide configuration path')
        sys.exit(1)

    # initialze configuration
    THE_CONF: configparser.ConfigParser = configparser.ConfigParser()
    THE_CONF.read(config_file)

    # check loggin pre-conditions
    LOG_DIR = Path(THE_CONF.get(oc.CFG_SEC_FLOW, oc.CFG_SEC_FLOW_LOG_DIR))
    if not os.access(LOG_DIR, os.F_OK and os.W_OK):
        print(f"cant store log files at directory {LOG_DIR}")
        sys.exit(1)

    # initialize server side logging
    LOG_FILE = THE_CONF.get(oc.CFG_SEC_FLOW, oc.CFG_SEC_FLOW_LOGFILE,
                            fallback=DEFAULT_LOG_FILE)
    if not Path(LOG_FILE).is_absolute():
        LOG_FILE = LOG_DIR / LOG_FILE
    CFG_DICT = {"logname": str(LOG_FILE)}
    CONF_FILE_LOGGING = THE_CONF.get(oc.CFG_SEC_FLOW, oc.CFG_SEC_FLOW_LOGCONF,
                                     fallback=DEFAULT_ODEM_LOGCONF_FILE)
    if not Path(CONF_FILE_LOGGING).is_absolute():
        CONF_FILE_LOGGING = DEFAULT_LOGCONF_DIR / CONF_FILE_LOGGING
    logging.config.fileConfig(str(CONF_FILE_LOGGING), defaults=CFG_DICT)
    LOG_NAME = THE_CONF.get(oc.CFG_SEC_FLOW, oc.CFG_SEC_FLOW_LOGNAME,
                            fallback=DEFAULT_ODEM_LOG_NAME)
    LOGGER = logging.getLogger(LOG_NAME)
    LOGGER.info("configured logging using %s", CONF_FILE_LOGGING)
    LOGGER.info("logs stored at %s", LOG_FILE)

    # evaluate configured server data
    SRV_HOST = THE_CONF.get('record-server', 'record_server_url')
    SRV_PORT = THE_CONF.getint('record-server', 'record_server_port')
    SRV_RESOURCE_DIR = THE_CONF.get(
        'record-server', 'record_server_resource_dir')
    RAW_IPS = THE_CONF.get('record-server', "accepted_ips", fallback="")
    CLIENT_IPS = [c.strip() for c in RAW_IPS.split(",") if len(c.strip()) > 0]

    # foster the record dir path, propably shortened
    if '~' in SRV_RESOURCE_DIR:
        SRV_RESOURCE_DIR = Path(SRV_RESOURCE_DIR).expanduser()
    SRV_RESOURCE_DIR = Path(SRV_RESOURCE_DIR).absolute().resolve()

    # forward to request handler
    server_info: df_r.HandlerInformation = df_r.HandlerInformation(
        SRV_RESOURCE_DIR, LOGGER)
    server_info.client_ips = CLIENT_IPS
    try:
        df_r.run_server(SRV_HOST, SRV_PORT, start_data=server_info)
    except Exception as exc:
        LOGGER.error("Record server encoutered %s", exc.args)
        sys.exit(1)
