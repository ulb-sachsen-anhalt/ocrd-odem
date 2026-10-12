"""regression tests for monitored workflow execution."""

import os
import runpy
import threading

from pathlib import Path
from unittest.mock import Mock

import pytest

from lib import odem
from lib.odem import commons as oc
from lib.odem.monitoring.datatypes import ProcessResourceMonitorConfig, TheProcess
from lib.odem.monitoring.resource import ProcessResourceMonitor, ResourceMonitor


@pytest.fixture(name='monitored_workflow')
def _monitored_workflow(monkeypatch):
    """yield a monitor, tracked children, and a mocked stop hook with a five-second watchdog.

    Real processes and IPC remain active, but resource sampling is mocked to
    isolate transport and lifecycle behavior. Record a parent-side timeout
    reason before terminating a stuck child so tests cannot silently pass.
    """
    processes = []
    stopped = Mock(return_value=True)

    def create_process(*args):
        """retain each real workflow child for lifecycle assertions and cancellation."""
        process = TheProcess(*args)
        processes.append(process)
        return process

    monkeypatch.setattr('lib.odem.monitoring.resource.odem_mdt.TheProcess', create_process)
    monkeypatch.setattr(ResourceMonitor, 'start', Mock(return_value=True))
    monkeypatch.setattr(ResourceMonitor, 'stop', stopped)
    monitor = ProcessResourceMonitor(ProcessResourceMonitorConfig(
        enable_resource_monitoring=True,
        polling_interval=1,
        path_disk_usage='/',
        factor_free_disk_space_needed=3,
    ))
    timed_out = threading.Event()

    def stop_stuck_process():
        """mark timeout and terminate a live child with a parent-visible failure reason."""
        timed_out.set()
        if processes and processes[-1].is_alive():
            process = processes[-1]
            process.kill_with_exception(RuntimeError('monitor result transfer timed out'))

    watchdog = threading.Timer(5, stop_stuck_process)
    watchdog.start()
    try:
        yield monitor, processes, stopped
    finally:
        watchdog.cancel()
        watchdog.join()
        assert not timed_out.is_set(), 'monitored workflow timed out'


@pytest.mark.parametrize('expected', [None, [], b'x' * (1024 * 1024)],
                         ids=['none', 'empty', 'large'])
def test_monit_vmem_result(monitored_workflow, expected):
    """preserve valid outcomes and finish the child, including a large serialized result.

    None and an empty list are legitimate results, not missing outcomes. The
    1 MiB payload exposes the former join-before-draining queue deadlock.
    """
    monitor, processes, stopped = monitored_workflow
    result = monitor.monit_vmem(lambda: expected)
    assert result == expected
    assert not processes[-1].is_alive()
    assert stopped.called


def test_monit_vmem_child_exception(monitored_workflow):
    """propagate a serializable workflow exception instead of losing child-local state.

    The parent must receive the ValueError and stop the child and monitor rather
    than treating the absent result as successful completion.
    """
    monitor, processes, stopped = monitored_workflow

    def fail():
        """raise a representative picklable workflow failure in the child."""
        raise ValueError('workflow failed')

    with pytest.raises(ValueError, match='workflow failed'):
        monitor.monit_vmem(fail)
    assert not processes[-1].is_alive()
    assert stopped.called


def test_monit_vmem_abrupt_exit(monitored_workflow):
    """report an abrupt child exit without an outcome instead of returning silent success.

    os._exit() bypasses outcome delivery; the supervisor must detect the closed
    pipe, raise RuntimeError, and retain the child's nonzero exit status.
    """
    monitor, processes, stopped = monitored_workflow
    with pytest.raises(RuntimeError, match='without returning a result'):
        monitor.monit_vmem(lambda: os._exit(7))
    assert processes[-1].exitcode == 7
    assert stopped.called


def test_monit_vmem_unpicklable_result(monitored_workflow):
    """surface result-serialization failure through IPC and invoke monitor shutdown.

    Returning a local lambda makes result transport fail; its representative
    pickling error must reach the parent rather than leave it waiting forever.
    """
    monitor, _, stopped = monitored_workflow
    with pytest.raises(AttributeError, match='pickle'):
        monitor.monit_vmem(lambda: lambda: None)
    assert stopped.called


def test_monit_vmem_monitor_termination(monitored_workflow, monkeypatch):
    """preserve a supervisor's failure reason when termination prevents result delivery.

    Simulate a limit breach after process startup and verify that its recorded
    reason takes precedence over EOF. This does not exercise actual RAM sampling.
    """
    monitor, processes, stopped = monitored_workflow

    def stop_workflow(_self):
        """simulate monitor-triggered cancellation only after the child has a PID."""
        process = processes[-1]
        assert process.pid is not None
        process.kill_with_exception(RuntimeError('memory limit exceeded'))

    monkeypatch.setattr(ResourceMonitor, 'start', stop_workflow)
    with pytest.raises(RuntimeError, match='memory limit exceeded'):
        monitor.monit_vmem(lambda: threading.Event().wait(10))
    assert not processes[-1].is_alive()
    assert stopped.called


def test_monit_vmem_disabled(monkeypatch):
    """execute in the parent without IPC when resource monitoring is disabled.

    A parent-visible list mutation distinguishes direct execution from a child
    process whose ordinary object mutations would not be shared.
    """
    monitor = ProcessResourceMonitor(ProcessResourceMonitorConfig(
        enable_resource_monitoring=False,
        polling_interval=1,
        path_disk_usage='/',
        factor_free_disk_space_needed=3,
    ))
    pipe = Mock(side_effect=AssertionError('disabled monitoring must not create IPC'))
    monkeypatch.setattr('lib.odem.monitoring.resource.multiprocessing.Pipe', pipe)
    results = []

    def run():
        """mutate parent-owned state so the test can distinguish direct execution."""
        results.append('ran in parent')
        return results

    assert monitor.monit_vmem(run) == ['ran in parent']
    assert results == ['ran in parent']
    assert not pipe.called


@pytest.mark.parametrize('live', [None, False, True], ids=['default', 'disabled', 'enabled'])
def test_mets_cli_live_monitoring(monkeypatch, tmp_path, live):
    """require explicit live opt-in while retaining preflight and forwarding OCR results.

    Exercise the METS entry point with live omitted, false, and true. OCR and
    resource sampling are mocked; the assertions verify execution selection and
    postprocessing handoff, not real METS parsing or host-memory enforcement.
    """
    config = odem.get_configparser()
    config.add_section(oc.CFG_SEC_FLOW)
    config.set(oc.CFG_SEC_FLOW, 'local_log_dir', str(tmp_path))
    config.add_section(odem.CFG_SEC_OCR)
    config.set(odem.CFG_SEC_OCR, oc.CFG_SEC_OCR_OPT_EXECS, '2')
    config.add_section(odem.CFG_SEC_MONITOR)
    config.set(odem.CFG_SEC_MONITOR, 'enable', 'true')
    if live is not None:
        config.set(odem.CFG_SEC_MONITOR, 'live', str(live))
    config_file = tmp_path / 'config.ini'
    config_file.write_text('', encoding='utf-8')
    mets_file = tmp_path / 'sample.xml'
    mets_file.write_text('', encoding='utf-8')

    logger = Mock()
    process = Mock(statistics={'timedelta': 0})
    workflow = Mock()
    results = ['ocr result']
    runner = Mock()
    runner.run.return_value = results
    monitor = Mock()
    monitor.monit_vmem.side_effect = lambda run: run()
    monkeypatch.setattr(odem, 'get_configparser', lambda: config)
    monkeypatch.setattr(odem, 'get_worker_logger', Mock(return_value=logger))
    monkeypatch.setattr(odem, 'ODEMProcessImpl', Mock(return_value=process))
    monkeypatch.setattr(odem.OCRWorkflow, 'create', Mock(return_value=workflow))
    monkeypatch.setattr(odem, 'OCRWorkflowRunner', Mock(return_value=runner))
    monkeypatch.setattr('lib.odem.monitoring.resource.ProcessResourceMonitor',
                        Mock(return_value=monitor))
    monkeypatch.setattr('digiflow.record.Record', Mock(return_value=Mock(identifier='sample')))
    monkeypatch.setattr('sys.argv', ['cli_mets_local.py', str(mets_file), '-c', str(config_file)])

    runpy.run_path(str(Path(__file__).resolve().parents[1] / 'cli_mets_local.py'),
                   run_name='__main__')

    monitor.check_vmem.assert_called_once_with()
    runner.run.assert_called_once_with()
    process.postprocess.assert_called_once_with(results)
    if live:
        monitor.monit_vmem.assert_called_once_with(runner.run)
    else:
        monitor.monit_vmem.assert_not_called()