"""Record Service
   Providing and manage access to local resource list files
"""

import configparser
import logging.config
import os
import sys
import time

from pathlib import Path

import digiflow.record as df_r

import lib.odem.odem_commons as odem_c


_PROJECT_ROOT = Path(__file__).resolve().parent
_ODEM_LOG_CONFIG_FILE = _PROJECT_ROOT / 'resources' / 'odem_logging.ini'
_ODEM_LOG_NAME = 'odem'


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
    SCRIPT_CONFIGURATION = configparser.ConfigParser()
    SCRIPT_CONFIGURATION.read(config_file)

    # check loggin pre-conditions
    LOG_DIR = Path(SCRIPT_CONFIGURATION.get(odem_c.CFG_SEC_FLOW, 'local_log_dir'))
    if not os.access(LOG_DIR, os.F_OK and os.W_OK):
        print(f"cant store log files at directory {LOG_DIR}")
        sys.exit(1)
    if not _ODEM_LOG_CONFIG_FILE.exists():
        print(f"config file not found {_ODEM_LOG_CONFIG_FILE.resolve()}")
        sys.exit(1)

    # initialize server side logging
    _today = time.strftime('%Y-%m-%d', time.localtime())
    _logfile_name = Path(LOG_DIR, f"odem_oai_service_{_today}.log")
    _conf_logname = {'logname': str(_logfile_name)}
    logging.config.fileConfig(str(_ODEM_LOG_CONFIG_FILE), defaults=_conf_logname)
    LOGGER = logging.getLogger(_ODEM_LOG_NAME)
    LOGGER.info("logging initialized - store log entry in %s", _logfile_name)

    # evaluate configured server data
    SRV_HOST = SCRIPT_CONFIGURATION.get('record-server', 'record_server_url')
    SRV_PORT = SCRIPT_CONFIGURATION.getint('record-server', 'record_server_port')
    SRV_RESOURCE_DIR = SCRIPT_CONFIGURATION.get('record-server', 'record_server_resource_dir')

    # foster the record dir path, propably shortened
    if '~' in SRV_RESOURCE_DIR:
        SRV_RESOURCE_DIR = Path(SRV_RESOURCE_DIR).expanduser()
    SRV_RESOURCE_DIR = Path(SRV_RESOURCE_DIR).absolute().resolve()

    # forward to request handler
    server_info: df_r.HandlerInformation = df_r.HandlerInformation(SRV_RESOURCE_DIR, LOGGER)
    try:
        df_r.run_server(SRV_HOST, SRV_PORT, start_data=server_info)
    except Exception as exc:
        LOGGER.error("Record server encoutered %s", exc.args)
        sys.exit(1)
