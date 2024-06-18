"""Specification for OAI Client module"""

import unittest
import unittest.mock

import pytest

import lib.odem as odem

import cli_record_server_client as rsc

# from cli_record_server_client import ServiceClient, oai_arg_parser


@pytest.mark.parametrize("file_path,result",
                         [
                             ('/data/oai/test.csv', 'no open records in /data/oai/test.csv, please inspect resource'),
                             ('', 'no open records in , please inspect resource'),
                             (None, 'no open records in None, please inspect resource')
                         ])
def test_mark_exhausted_matching(file_path, result):
    """Check formatting behavior"""

    # assert
    assert odem.MARK_DATA_EXHAUSTED.format(file_path) == result


@unittest.mock.patch('digiflow.requests.get')
def test_exit_on_data_exhausted(mock_request):
    """Ensure dedicated state is communicated 
    to OAIClient when no more records present

    Please note: *real* responses return
    byte-object rather!
    """

    # arrange
    _the_list_label = 'oai-record-test'
    _rsp = f'{odem.MARK_DATA_EXHAUSTED.format(_the_list_label)}'.encode()
    client = rsc.OAIServiceClient(_the_list_label, '1.2.3.4', '9999')
    mock_resp = unittest.mock.Mock()
    mock_resp.status_code = 404
    mock_resp.headers = {'Content-Type': 'text/xml'}
    mock_resp.content = _rsp
    mock_request.return_value = mock_resp

    # act
    with pytest.raises(odem.OAIRecordExhaustedException) as exhausted:
        client.get_record()

    # assert
    assert 'no open records in oai-record-test, please inspect resource' == exhausted.value.args[0]


@pytest.mark.parametrize("value",
                         [
                             ('oai-records-sample.csv'),
                             ('/data/oai-records-sample.csv'),
                             ('oai-records-sample/next')
                         ])
def test_oai_arg_parser(value):
    """Some formats of how the record list
    information *must not* be provided
    """

    # actsert
    with pytest.raises(SystemExit) as _exit:
        rsc.oai_arg_parser(value)
    assert 1 == _exit.value.args[0]
