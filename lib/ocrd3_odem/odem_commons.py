"""ODEM Core"""

import configparser
import logging
import os
import socket
import time

from configparser import (
    ConfigParser,
)
from enum import Enum
from pathlib import (
    Path
)
from typing import (
    List
)

from ocrd_utils import (
    initLogging
)

#
# ODEM States
#
MARK_OCR_OPEN = 'n.a.'
MARK_OCR_BUSY = 'ocr_busy'
MARK_OCR_FAIL = 'ocr_fail'
MARK_OCR_DONE = 'ocr_done'
MARK_OCR_SKIP = 'ocr_skip'

# how many parallel procs
DEFAULT_EXECUTORS = 2


class ExportFormat(str, Enum):
    SAF = 'SAF'
    FLAT_ZIP = 'FLAT_ZIP'


#
# ODEM configuration keys
CFG_SEC_OCR = 'ocr'
KEY_EXECS = 'n_executors'
KEY_LANGUAGES = 'language_model'
KEY_MODEL_MAP = 'model_mapping'
KEY_SEQUENTIAL_MODE = 'sequential_mode'
#
# ODEM arguments
# = shortform, longform
#   where keyform is also used as
#   pythonic configuration key
ARG_S_SEQUENTIAL_MODE = 's'
ARG_L_SEQUENTIAL_MODE = 'sequential-mode'
ARG_S_LANGUAGES = 'l'
ARG_L_LANGUAGES = 'language-model'
ARG_S_MODEL_MAP = 'm'
ARG_L_MODEL_MAP = 'model-mapping'
ARG_S_EXECS = 'e'
ARG_L_EXECS = 'executors'

#
# ODEM metadata
#
# file groups
FILEGROUP_OCR = 'FULLTEXT'
FILEGROUP_IMG = 'MAX'
# statistic keys
STATS_KEY_LANGS = 'langs'
# default language for fallback
# when processing local images
DEFAULT_LANG = 'ger'
# recognition level for tesserocr
# must switch otherwise glyphs are reverted
# for each word
DEFAULT_RTL_MODELS = ["ara.traineddata", "fas.traineddata", "heb.traineddata"]

PROJECT_ROOT = Path(__file__).resolve().parents[2]
DEFAULT_LOG_CONFIG = os.path.join(PROJECT_ROOT, 'resources', 'odem_logging.ini')


class ODEMException(Exception):
    """Mark custom ODEM Workflow Exceptions"""


def get_configparser():
    """init plain configparser"""

    def _parse_dict(row):
        """
        Custom config converter to create a dictionary represented as string
        lambda s: {e[0]:e[1] for p in s.split(',') for e in zip(*p.strip().split(':'))}
        """
        a_dict = {}
        for pairs in row.split(','):
            pair = pairs.split(':')
            a_dict[pair[0].strip()] = pair[1].strip()
        return a_dict

    return configparser.ConfigParser(
        interpolation=configparser.ExtendedInterpolation(),
        converters={
            'list': lambda s: [e.strip() for e in s.split(',')],
            'dict': _parse_dict
        })


def get_logger(log_dir, log_infix=None, path_log_config=None) -> logging.Logger:
    """Create logger with log_infix to divide several
    instances running on same host and
    using configuration from path_log_config
    in log_dir.
    Log output from "page-to-alto" set to disable WARNING:
        "PAGE-XML has Border but no PrintSpace - Margins will be empty"
    
    please note:
    call of OCR-D initLogging() required, otherwise something like this pops up:

    CRITICAL root - getLogger was called before initLogging. Source of the call:
    CRITICAL root -   File "<odem-path>/venv/lib/python3.8/site-packages/ocrd/resolver.py",
                      line 231, in workspace_from_nothing
    CRITICAL root -     log = getLogger('ocrd.resolver.workspace_from_nothing')
    ...
    """

    initLogging()
    logging.getLogger('page-to-alto').setLevel('CRITICAL')
    _today = time.strftime('%Y-%m-%d', time.localtime())
    _host = socket.gethostname()
    _label = log_infix if log_infix is not None else ''
    _logfile_name = os.path.join(
        log_dir, f"odem_{_host}{_label}_{_today}.log")
    conf_logname = {'logname': _logfile_name}
    _conf_path = DEFAULT_LOG_CONFIG
    if path_log_config is not None and os.path.isfile(path_log_config):
        _conf_path = path_log_config
    logging.config.fileConfig(_conf_path, defaults=conf_logname)
    return logging.getLogger('odem')


def merge_args(the_configuration: ConfigParser, the_args) -> List:
    """Merge additionally provided arguements into
    existing configurations, overwrite these and
    communication the replaced options
    """

    _repls = []
    if not isinstance(the_args, dict):
        the_args = vars(the_args)
    if KEY_EXECS in the_args and int(the_args[KEY_EXECS]) > 0:
        _upd01 = (CFG_SEC_OCR, KEY_EXECS, str(the_args[KEY_EXECS]))
        the_configuration.set(*_upd01)
        _repls.append(_upd01)
    if KEY_LANGUAGES in the_args and the_args[KEY_LANGUAGES] is not None:
        _upd02 = (CFG_SEC_OCR, KEY_LANGUAGES, the_args[KEY_LANGUAGES])
        the_configuration.set(*_upd02)
        _repls.append(_upd02)
    if KEY_MODEL_MAP in the_args and the_args[KEY_MODEL_MAP] is not None:
        _upd03 = (CFG_SEC_OCR, KEY_MODEL_MAP, the_args[KEY_MODEL_MAP])
        the_configuration.set(*_upd03)
        _repls.append(_upd03)
    return _repls
