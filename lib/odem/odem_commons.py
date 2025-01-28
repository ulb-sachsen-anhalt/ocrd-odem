"""ODEM Core"""

import configparser
import dataclasses
import logging
import logging.config
import os
import socket
import time
import typing

from enum import Enum
from pathlib import Path

import ocrd_utils
import digiflow.record as df_r

#
# ODEM States
#
UNSET = 'n.a.'
MARK_OCR_OPEN = UNSET
MARK_OCR_BUSY = 'ocr_busy'
MARK_OCR_FAIL = 'ocr_fail'
MARK_OCR_DONE = 'ocr_done'
MARK_OCR_SKIP = 'ocr_skip'

MARK_DATA_EXHAUSTED_PREFIX = 'no open records'
MARK_DATA_EXHAUSTED = MARK_DATA_EXHAUSTED_PREFIX + ' in {}, please inspect resource'

# how many parallel procs
DEFAULT_EXECUTORS = 2


class ExportFormat(str, Enum):
    """Set of excepted eport formats"""
    SAF = 'SAF'
    FLAT_ZIP = 'FLAT_ZIP'


#
# ODEM configuration keys
CFG_SEC_FLOW = 'workflow'
CFG_SEC_FLOW_OPT_URL = "base_url"
CFG_SEC_FLOW_OPT_URL_KWARGS = "base_url_requests_kwargs"
CFG_SEC_FLOW_OPT_REM_RES = 'remove_resources'
CFG_SEC_FLOW_OPT_TEXTLINE = "create_textline_asset"
CFG_SEC_FLOW_OPT_DELETE_DIRS = "delete_before_export"
CFG_SEC_FLOW_USE_FILEID = "use_file_id"
CFG_SEC_MONITOR = 'monitoring'
CFG_SEC_OCR = 'ocr'
CFG_SEC_OCR_OPT_EXECS = 'n_executors'
CFG_SEC_OCR_OPT_RES_VOL = "ocrd_resources_volumes"
CFG_SEC_OCR_OPT_MODEL_COMBINABLE = "model_combinable"
CFG_SEC_OCR_OPT_IMG_SUBDIR = 'image_subpath'
CFG_SEC_METS = 'mets'
CFG_SEC_METS_OPT_AGENTS = 'agents'
CFG_SEC_METS_OPT_ENRICH = 'enrich_fulltext'
CFG_SEC_METS_OPT_CLEAN = 'post_clean'
CFG_SEC_METS_OPT_ID_XPR = "record_identifier_xpr"
CFG_SEC_METS_FGROUP = "use_fgroup"
CFG_SEC_DERIVANS = "derivans"
CFG_SEC_DERIVANS_FGROUP = "image_fgroup"
CFG_SEC_EXP = 'export'
CFG_SEC_EXP_ENABLED = "export_enabled"
CFG_SEC_EXP_OPT_DEL_SDIRS = 'delete_subdirs_before_export'
CFG_SEC_EXP_OPT_PREFIX = "export_prefix"
CFG_SEC_EXP_OPT_FORMAT = "export_format"
CFG_SEC_EXP_OPT_DST = "export_dst"
CFG_SEC_EXP_OPT_TMP = "export_tmp"
CFG_SEC_EXP_OPT_METS = "export_mets"
CFG_SEC_EXP_OPT_COLLECTION = "export_collection"
CFG_SEC_EXP_OPT_MAPPINGS = "export_mappings"
KEY_LANGUAGES = 'language_model'
KEY_MODEL_MAP = 'model_mapping'
KEY_SEQUENTIAL_MODE = 'sequential_mode'

#
# ODEM arguments
# = shortform, longform
#   where keyform is also used as
#   pythonic configuration key
ARG_S_SEQUENTIAL_MODE = 's'
ARG_L_SEQUENTIAL_MODE = 'sequential_mode'
ARG_S_LANGUAGES = 'l'
ARG_L_LANGUAGES = 'language_model'
ARG_S_MODEL_MAP = 'm'
ARG_L_MODEL_MAP = 'model_mapping'
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
STATS_KEY_OCR_LOSS = 'ocr_loss'
STATS_KEY_MB = 'mb'
STATS_KEY_MPS = 'mps'

# default language for fallback
# when processing local images
DEFAULT_LANG = 'ger'
DEFAULT_FGROUP = FILEGROUP_IMG
# recognition level for tesserocr
# must switch otherwise glyphs are reverted
# for each word
DEFAULT_RTL_MODELS = ["ara.traineddata", "fas.traineddata", "heb.traineddata"]

PROJECT_ROOT = Path(__file__).resolve().parents[2]
DEFAULT_LOG_CONFIG = os.path.join(PROJECT_ROOT, 'resources', 'odem_logging.ini')


class ODEMException(Exception):
    """Basic ODEM Exception"""


class ODEMDataException(ODEMException):
    """Mark problems related to data, not to
    general workflow or external factors"""


class OAIRecordExhaustedException(Exception):
    """Mark given resource contains no open records"""


class OdemWorkflowProcessType(str, Enum):
    """Accepted values for process types"""

    OCRD_PAGE_PARALLEL = "OCRD_PAGE_PARALLEL"
    ODEM_TESSERACT = "ODEM_TESSERACT"


DEFAULT_WORKLFOW = OdemWorkflowProcessType.OCRD_PAGE_PARALLEL


@dataclasses.dataclass
class OCRResult:
    """Describe the outcome of Running a
    OCR-like workflow as a container
    with all desired information for
    later statistical processing"""

    local_path: Path
    images_fsize: int
    images_mps: float


class ODEMProcess:
    """Basic Interface for ODEM"""

    def __init__(self,
                 configuration: configparser.ConfigParser,
                 work_dir: Path,
                 logger: logging.Logger,
                 log_dir=None,
                 record: df_r.Record = None):
        self.configuration = configuration
        self.work_dir_root = work_dir
        self.record = record
        self.process_identifier = None
        self.process_statistics = {}
        self.ocr_candidates = []
        self.logger = logger
        if logger is not None:
            self.logger = logger
        if log_dir is not None and os.path.exists(log_dir):
            self._init_logger(log_dir)

    def _init_logger(self, log_dir):
        today = time.strftime('%Y-%m-%d', time.localtime())
        if not log_dir:
            log_parent = os.path.dirname(os.path.dirname(self.work_dir_root))
            if not os.access(log_parent, os.W_OK):
                raise RuntimeError(f"cant store log files at invalid {log_dir}")
            log_dir = os.path.join(log_parent, 'log')
            os.makedirs(log_dir, exist_ok=True)
        logfile_name = os.path.join(
            log_dir, f"odem_{today}.log")
        conf_logname = {'logname': logfile_name}
        conf_file_location = os.path.join(PROJECT_ROOT, 'resources', 'odem_logging.ini')
        logging.config.fileConfig(conf_file_location, defaults=conf_logname)
        self.logger = logging.getLogger('odem')

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

    repls_tracked = []
    if not isinstance(the_args, dict):
        the_args = vars(the_args)
    if CFG_SEC_OCR_OPT_EXECS in the_args and int(the_args[CFG_SEC_OCR_OPT_EXECS]) > 0:
        upd01 = (CFG_SEC_OCR, CFG_SEC_OCR_OPT_EXECS, str(the_args[CFG_SEC_OCR_OPT_EXECS]))
        the_configuration.set(*upd01)
        repls_tracked.append(upd01)
    if KEY_LANGUAGES in the_args and the_args[KEY_LANGUAGES] is not None:
        upd02 = (CFG_SEC_OCR, KEY_LANGUAGES, the_args[KEY_LANGUAGES])
        the_configuration.set(*upd02)
        repls_tracked.append(upd02)
    if KEY_MODEL_MAP in the_args and the_args[KEY_MODEL_MAP] is not None:
        upd03 = (CFG_SEC_OCR, KEY_MODEL_MAP, the_args[KEY_MODEL_MAP])
        the_configuration.set(*upd03)
        repls_tracked.append(upd03)
    if ARG_L_EXECS in the_args:
        upd_04 = (CFG_SEC_OCR, CFG_SEC_OCR_OPT_EXECS, str(the_args[ARG_L_EXECS]))
        the_configuration.set(*upd_04)
        repls_tracked.append(upd_04)
    return repls_tracked


def list_files(the_directory, file_ext='.xml') -> typing.List:
    """List all files in the_directory with given suffix"""
    return [
        os.path.join(the_directory, dir_file)
        for dir_file in os.listdir(the_directory)
        if Path(dir_file).suffix == file_ext
    ]
