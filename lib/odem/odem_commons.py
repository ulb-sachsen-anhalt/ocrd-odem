"""ODEM Core"""

import configparser
import logging
import os
import socket
import time
import typing

from enum import Enum
from pathlib import Path

import ocrd_utils
import digiflow as df

#
# ODEM States
#
MARK_OCR_OPEN = 'n.a.'
MARK_OCR_BUSY = 'ocr_busy'
MARK_OCR_FAIL = 'ocr_fail'
MARK_OCR_DONE = 'ocr_done'
MARK_OCR_SKIP = 'ocr_skip'

MARK_DATA_EXHAUSTED_PREFIX = 'no open records'
MARK_DATA_EXHAUSTED = MARK_DATA_EXHAUSTED_PREFIX + ' in {}, please inspect resource'

# how many parallel procs
DEFAULT_EXECUTORS = 2


class ExportFormat(str, Enum):
    SAF = 'SAF'
    FLAT_ZIP = 'FLAT_ZIP'


#
# ODEM configuration keys
CFG_SEC_OCR = 'ocr'
CFG_SEC_OCR_OPT_RES_VOL = "ocrd_resources_volumes"
CFG_SEC_OCR_OPT_MODEL_COMBINABLE = "model_combinable"
CFG_SEC_METS = 'mets'
CFG_SEC_METS_OPT_AGENTS = 'agents'
CFG_SEC_METS_OPT_ENRICH = 'enrich_fulltext'
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
# record attributes
#
RECORD_IDENTIFIER = 'IDENTIFIER'
RECORD_SPEC = 'SETSPEC'
RECORD_RELEASED = 'CREATED'
RECORD_INFO = 'INFO'
RECORD_STATE = 'STATE'
RECORD_TIME = 'STATE_TIME'

#
# ODEM metadata
#
# file groups
FILEGROUP_FULLTEXT = 'FULLTEXT'
FILEGROUP_IMG = 'MAX'
# statistic keys
STATS_KEY_LANGS = 'languages'
STATS_KEY_MODELS = 'models'
STATS_KEY_TYPE = 'pica'
STATS_KEY_N_PAGES = 'n_images_pages'
STATS_KEY_N_OCRABLE = 'n_images_ocrable'
STATS_KEY_N_LINES = 'n_text_lines'
STATS_KEY_N_EXECS = 'n_execs'
STATS_KEY_N_OCR = 'n_ocr'
STATS_KEY_MB = 'mb'
STATS_KEY_MPS = 'mps'

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


class OAIRecordExhaustedException(Exception):
    """Mark that given file contains no open records"""


class OdemWorkflowProcessType(str, Enum):
    OCRD_PAGE_PARALLEL = "OCRD_PAGE_PARALLEL"
    ODEM_TESSERACT = "ODEM_TESSERACT"


class OdemProcess:
    """Basic Interface for ODEM"""

    def load(self):
        """Load Data via OAI-PMH-API very LAZY
        i.e. if not metadata file exists already in
        configured workspace directory"""

    def inspect_metadata(self):
        """Inspected record data and try to make sense (or go nuts if invalid)
        Invalid means:
        * no print work type (i.e. C-stage, newspaper year)
        * no language
        * missing links between physical and logical structs
          (otherwise viewer navigation and PDF outline
           will be corrupt at this segment)
        * no page images for OCR
        """
    
    def export_data(self):
        """re-do metadata and transform into output format"""


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

    ocrd_utils.initLogging()
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


def merge_args(the_configuration: configparser.ConfigParser, the_args) -> typing.List:
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


def to_dict(record: df.OAIRecord) -> typing.Dict:
    """Serialize OAIRecord into dictionary
    as input for JSON format"""

    return {
        RECORD_IDENTIFIER: record.identifier,
        RECORD_RELEASED: record.date_stamp,
        RECORD_INFO: record.info,
        RECORD_STATE: record.state,
        RECORD_TIME: record.state_datetime,
    }

def from_dict(data) -> df.OAIRecord:
    """deserialize into OAIRecord"""

    _record = df.OAIRecord(data[RECORD_IDENTIFIER])
    _record.info = data[RECORD_INFO]
    return _record


def list_files(dir_root, sub_dir, format='.xml') -> typing.List:
    actual_dir = os.path.join(dir_root, sub_dir)
    return [
        os.path.join(actual_dir, dir_file)
        for dir_file in os.listdir(actual_dir)
        if Path(dir_file).suffix == format
    ]
