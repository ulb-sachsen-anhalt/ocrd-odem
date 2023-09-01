
from unittest import (
    mock
)

import pytest

from cli_oai_client import (
    OAIServiceClient,
    OAIRecordExhaustedException,
)

from cli_oai_server import (
    MARK_DATA_EXHAUSTED,
    MARK_DATA_EXHAUSTED_PREFIX,
)


@pytest.mark.parametrize("file_path,result",
                         [
                             ('/data/oai/test.csv', 'no open records in /data/oai/test.csv, please inspect resource'),
                             ('', 'no open records in , please inspect resource'),
                             (None, 'no open records in None, please inspect resource')
                         ])
def test_mark_exhausted_matching(file_path, result):
    """Check formatting behavior"""

    # assert
    assert MARK_DATA_EXHAUSTED.format(file_path) == result


@mock.patch('digiflow.requests.get')
def test_exit_on_data_exhausted(mock_request):
    """Ensure dedicated state is communicated 
    to OAIClient when no more records present
    """

    # arrange
    _the_list_label = 'oai-record-test'
    _rsp = f'{MARK_DATA_EXHAUSTED_PREFIX}{MARK_DATA_EXHAUSTED.format(_the_list_label)}'
    client = OAIServiceClient(_the_list_label, '1.2.3.4', '9999')
    mock_resp = mock.Mock()
    mock_resp.status_code = 404
    mock_resp.headers = {'Content-Type': 'text/xml'}
    mock_resp.content = _rsp
    mock_request.return_value = mock_resp

    # act
    with pytest.raises(OAIRecordExhaustedException) as exhausted:
        client.get_record()

    # assert
    assert 'no open records in oai-record-test, please inspect resource' == exhausted.value.args[0]
