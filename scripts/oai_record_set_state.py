import argparse
import os.path
import sys
from enum import Enum
from typing import List

from digiflow import (
    OAIRecordHandler,
    OAIRecordCriteriaText,
    OAIRecordCriteriaState,
    OAIRecordCriteriaIdentifier,
    OAIRecordCriteriaDatetime,
    F_STATE_INFO,
    RECORD_STATE_UNSET,
)


class CriteriaKey(Enum):
    TEXT = 'text',
    STATE = 'state',
    IDENTIFIER = 'identifier',
    DATETIME = 'datetime',


CRITERIA_MAP = {
    CriteriaKey.TEXT.value[0]: OAIRecordCriteriaText,
    CriteriaKey.STATE.value[0]: OAIRecordCriteriaState,
    CriteriaKey.IDENTIFIER.value[0]: OAIRecordCriteriaIdentifier,
    CriteriaKey.DATETIME.value[0]: OAIRecordCriteriaDatetime,
}

argParser: argparse.ArgumentParser = argparse.ArgumentParser(
    description="(Re-)set OAI-Recors, which are matched by specific criteria"
)

argParser.add_argument(
    "oai_list",
    help="path to file with OAI-Record information"
)

argParser.add_argument(
    "-D",
    "--dry-run",
    action='store_true',
    required=False,
    help="dont persist changes (optional; default: 'False')",
)

argParser.add_argument(
    "-V",
    "--verbose",
    action='store_true',
    required=False,
    help="verbosity (optional; default: 'False')",
)

argParser.add_argument(
    "-S",
    "--new-state",
    default=RECORD_STATE_UNSET,
    help=f"new State (optional; default: '{RECORD_STATE_UNSET}')",
)

argParser.add_argument(
    "-t",
    f"--{CriteriaKey.TEXT.value[0]}",
    nargs='+',
    default=[],
    help='text criteria(s) for filter records, which are affected'
)

argParser.add_argument(
    "-s",
    f"--{CriteriaKey.STATE.value[0]}",
    nargs='+',
    default=[],
    help='state criteria(s) for filter records, which are affected'
)

argParser.add_argument(
    "-i",
    f"--{CriteriaKey.IDENTIFIER.value[0]}",
    nargs='+',
    default=[],
    help='idntifier criteria(s) for filter records, which are affected'
)

argParser.add_argument(
    "-d",
    f"--{CriteriaKey.DATETIME.value[0]}",
    nargs='+',
    default=[],
    help='datetime criteria(s) for filter records, which are affected (not implmented yet)'
)
args = argParser.parse_args()
# print(args)

oai_list_file_path: str = args.oai_list
oai_list_file_abspath: str = os.path.abspath(oai_list_file_path)

if not os.path.isfile(oai_list_file_abspath):
    errMsg: str = f'Error: AOI-Record List "{oai_list_file_abspath}" does not exist'
    # raise FileNotFoundError(errMsg)
    print(errMsg)
    sys.exit(-1)

handler: OAIRecordHandler = OAIRecordHandler(oai_list_file_abspath)

texts: List[str] = args.text
criterias = []
if len(texts):
    for text in texts:
        criterias.append(OAIRecordCriteriaText(text, field=F_STATE_INFO))
states: List[str] = args.state
if len(states):
    for state in states:
        criterias.append(OAIRecordCriteriaState(state))
identifiers: List[str] = args.identifier
if len(identifiers):
    for identifier in identifiers:
        criterias.append(OAIRecordCriteriaIdentifier(identifier))
datetimes: List[str] = args.datetime
if len(datetimes):
    print('datetime criteria not implemented yet')

dryRun = args.dry_run
verbose = args.verbose
new_state = args.new_state
result = handler.states(
    criterias,
    set_state=new_state,
    dry_run=dryRun,
    verbose=verbose
)
print(result)
