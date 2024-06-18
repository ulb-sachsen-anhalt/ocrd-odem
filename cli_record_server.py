# -*- coding: utf-8 -*-
# pylint: disable=invalid-name
"""Service Http Server
   Providing and managing OAI records for more OCR clients
"""

import configparser
import contextlib
import functools
import http.server
import json
import logging.config
import os
import sys
import time
import threading

from pathlib import Path

import digiflow.record as df_r

from lib import odem

from lib.odem.odem_commons import (
    MARK_OCR_BUSY,
    RECORD_IDENTIFIER,
    RECORD_INFO,
    RECORD_STATE,
    to_dict,
)

_PROJECT_ROOT = Path(__file__).resolve().parent
ODEM_LOG_CONFIG_FILE = _PROJECT_ROOT / 'resources' / 'odem_logging.ini'
LOCKFILE = 'SERVER_RUNNING'
ODEM_LOG_NAME = 'odem'
NEXT_COMMAND = 'next'
UPDATE_COMMAND = 'update'
_MIME_TXT = 'text/plain'
LOGGER = None


# def _to_full_record(row):
#     """Serialize CSV row into OAIRecord
#     with all attributes being evaluated"""

#     oai_id = row[RECORD_IDENTIFIER]
#     record = df_r.Record(oai_id)
#     # legacy field for backward compatibility
#     if RECORD_SPEC in row:
#         record.set = row[RECORD_SPEC]
#     # legacy field for backward compatibility
#     if RECORD_RELEASED in row:
#         record.date_stamp = row[RECORD_RELEASED]
#     record.info = row[RECORD_INFO]
#     return record


class RecordRequestHandler(http.server.SimpleHTTPRequestHandler):
    """Http Request handler for POST and GET requests"""

    def __init__(self, data_path:Path, *args, **kwargs):
        """Overrides __init__ from regular SimpleHTTPRequestHandler
        data_path: folder where we expect record files"""

        self.record_list_directory: Path = data_path
        super().__init__(*args, **kwargs)

    def do_GET(self):
        """handle GET request"""
        client_name = self.address_string()
        if self.path == '/favicon.ico':
            return
        LOGGER.debug("handle GET request for '%s' from client address:%s", self.path, client_name)
        try:
            _, file_name, command = self.path.split('/')
        except ValueError:
            self.wfile.write(
                b'please provide record file name and command '
                b' e.g.: /oai_record_vd18/next')
            LOGGER.warning("missing data: '%s'", self.path)
            return
        if command == NEXT_COMMAND:
            state, data = self.get_next_record(file_name, client_name)
            LOGGER.debug("deliver next record: '%s'", data)
            if isinstance(data, str):
                self._set_headers(state, _MIME_TXT)
                self.wfile.write(data.encode('utf-8'))
            else:
                self._set_headers(state)
                self.wfile.write(json.dumps(data, default=to_dict).encode('utf-8'))

    def do_POST(self):
        """handle POST request"""
        data = 'no data available'
        client_name = self.address_string()
        LOGGER.info('url path %s from %s', self.path, client_name)
        try:
            _, file_name, command = self.path.split('/')
        except ValueError:
            self.wfile.write(
                b'please provide record file name and command '
                b' e.g.: /records-vd18/next')
            LOGGER.error('request next record with umbiguous data')
        content_length = int(self.headers['Content-Length'])
        post_data = self.rfile.read(content_length)
        data_dict = json.loads(post_data)
        LOGGER.debug("POST request, Path: %s", self.path)
        LOGGER.info('do_POST: %s', data_dict)
        if command == UPDATE_COMMAND:
            ident = data_dict.get(RECORD_IDENTIFIER)
            if ident:
                state, data = self.update_record(file_name, data_dict)
                if isinstance(data, str):
                    self._set_headers(state, _MIME_TXT)
                    self.wfile.write(data.encode('utf-8'))
                else:
                    self._set_headers(state)
                    self.wfile.write(json.dumps(data, default=to_dict).encode('utf-8'))
            else:
                self._set_headers(400, _MIME_TXT)
                self.wfile.write(f"no entry for {ident} in {file_name}!".encode('utf-8'))

    def _set_headers(self, state=200, mime_type='application/json') -> None:
        self.send_response(state)
        self.send_header('Content-type', mime_type)
        self.end_headers()

    def get_data_file(self, data_file_name: str):
        """data_file_name comes with no extension!
           so we must search for a valid match-
           returns propably None-values if
           nothing found.
        """
        for _file in self.record_list_directory.iterdir():
            if data_file_name == Path(_file).stem:
                data_file = self.record_list_directory / _file.name
                return data_file
        return None

    def get_next_record(self, file_name, client_name) -> tuple:
        """Deliver next record data if both
        * in watched directory exists record list matching file_name
        * inside this record list are open records available
        """

        data_file_path = self.get_data_file(file_name)
        # no match results in 404 - resources not available after all
        if data_file_path is None:
            LOGGER.warning("no '%s' found in '%s'", data_file_path, self.record_list_directory)
            return (404, f"no file '{file_name}' in {self.record_list_directory}")

        handler = df_r.RecordHandler(data_file_path, transform_func=df_r.row_to_record)
        next_record: df_r.Record = handler.next_record()
        # if no record in resource available, alert no resource after all, too
        if not next_record:
            _msg = f'{odem.MARK_DATA_EXHAUSTED.format(data_file_path)}'
            return (404, _msg)

        # store information which client got the package delivered
        _info = {'client': client_name}
        if next_record.info != 'n.a.':
            _info = f"{next_record.info},{_info}"
        handler.save_record_state(
            next_record.identifier, MARK_OCR_BUSY, **{RECORD_INFO: f'{_info}'})
        return (200, next_record)

    def update_record(self, data_file, data) -> tuple:
        """write data dict send by client
        throws RuntimeError if record to update not found
        """

        data_file_path = self.get_data_file(data_file)
        if data_file_path is None:
            LOGGER.error('do_POST: %s not found', data_file_path)
            return (404, f"data file not found: {data_file_path}")
        try:
            handler = df_r.RecordHandler(data_file_path)
            _ident = data[RECORD_IDENTIFIER]
            _record: df_r.Record = handler.get(_ident)
            _prev_info = _record.info
            _info = f'{data[RECORD_INFO]}'
            if _prev_info != 'n.a.':
                _info = f'{_prev_info},{_info}'
            handler.save_record_state(_ident,
                                      state=data[RECORD_STATE], **{RECORD_INFO: _info})
            _msg = f"update done for {_ident} in '{data_file_path}"
            LOGGER.info(_msg)
            return (200, _msg)
        except RuntimeError as _rer:
            _msg = f"update fail for {_ident} in '{data_file_path}' ({_rer.args[0]})"
            LOGGER.error(_msg)
            return (500, _msg)


class RecordServer():
    """helper class to start server"""

    @contextlib.contextmanager
    def http_server(self, host: str, port: int, directory: str):
        """init server"""
        server = http.server.HTTPServer(
            (host, port),
            functools.partial(RecordRequestHandler, directory)
        )
        server_thread = threading.Thread(target=server.serve_forever, name="http_server")
        server_thread.start()

        try:
            yield
        finally:
            lock = Path(LOCKFILE)
            if lock.exists():
                # remove lock
                LOGGER.info("no lock file found - stop server")
                lock.unlink()
            server.shutdown()
            server_thread.join()

    def serve(self, host, port, pth):
        """Start infinity loop to serve resources
        within data_path until the magic marker
        LOCKFILE gets removed (or CTRL+C pressed)
        """

        LOGGER.info("server starts listen at: %s:%s", host, port)
        LOGGER.info("serve record files from: %s", pth)
        LOGGER.info("call for next record with: %s:%s/<record-file>/next", host, port)
        LOGGER.info("post a record update with: %s:%s/<record-file>/update", host, port)
        with self.http_server(host, port, pth):
            with open(LOCKFILE, 'w', encoding='UTF-8') as lock:
                lock.write(f'delete me to stop server @{host}:{port}')
            path_ = Path(LOCKFILE)
            while Path(path_).exists():
                try:
                    # wait some seconds until recheck lock file
                    time.sleep(10)
                except KeyboardInterrupt:
                    LOGGER.info('server stopped gracefull by keybord interrupt')
                    break
            else:
                # no lock file? --> quit server
                LOGGER.info('Lockfile deleted, server stopped!')
                sys.exit()


########
# Script
########
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
    _log_dir = Path(SCRIPT_CONFIGURATION.get('global', 'local_log_dir'))
    if not os.access(_log_dir, os.F_OK and os.W_OK):
        print(f"cant store log files at directory {_log_dir}")
        sys.exit(1)
    if not ODEM_LOG_CONFIG_FILE.exists():
        print(f"config file not found {ODEM_LOG_CONFIG_FILE.resolve()}")
        sys.exit(1)

    # initialize server side logging
    _today = time.strftime('%Y-%m-%d', time.localtime())
    _logfile_name = Path(_log_dir, f"odem_oai_service_{_today}.log")
    _conf_logname = {'logname': str(_logfile_name)}
    logging.config.fileConfig(str(ODEM_LOG_CONFIG_FILE), defaults=_conf_logname)
    LOGGER = logging.getLogger(ODEM_LOG_NAME)
    LOGGER.info("logging initialized - store log entry in %s", _logfile_name)

    # evaluate configured server data
    _port = SCRIPT_CONFIGURATION.getint('record-server', 'record_server_port')
    _host = SCRIPT_CONFIGURATION.get('record-server', 'record_server_url')
    _oai_res_dir = SCRIPT_CONFIGURATION.get('record-server', 'record_server_resource_dir')

    # foster the record dir path, propably shortened
    if '~' in _oai_res_dir:
        _oai_res_dir = Path(_oai_res_dir).expanduser()
    _oai_res_dir = Path(_oai_res_dir).absolute().resolve()

    # forward to request handler
    RecordServer().serve(_host, _port, _oai_res_dir)
