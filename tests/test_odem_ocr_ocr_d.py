"""Specification for OCR-D related functionalities"""

from pathlib import Path
import subprocess

import digiflow as df
import lxml.etree as ET

import pytest

import lib.odem as odem
import lib.odem.ocr.ocr_d as o3o_ocrd

from .conftest import create_test_tif

# pylint:disable=c-extension-no-member

@pytest.mark.parametrize("model_configuration,recognotion_level",
                         [
                             ('ger.traineddata', 'word'),
                             ('ara.traineddata', 'glyph'),
                             ('ara.traineddata+ger.traineddata', 'glyph'),
                             ('ger.traineddata+lat.traineddata', 'word'),
                             ('fas.traineddata', 'glyph'),
                             ('ger.traineddata+lat.traineddata', 'word'),
                             ('eng.traineddata+fas.traineddata', 'glyph')
                         ])
def test_odem_recognition_level(model_configuration, recognotion_level):
    """Check determined recognition level passed
    forth to tesserocr for common model configurations"""

    the_lvl = o3o_ocrd.get_recognition_level(model_configuration, rtl_models=odem.DEFAULT_RTL_MODELS)
    assert the_lvl == recognotion_level


@pytest.mark.parametrize("model_conf,rec_level",
                         [
                             ('ger', 'word'),
                             ('gt4ara', 'glyph'),
                             ('ulb-fas', 'glyph'),
                         ])
def test_odem_recognition_level_custom(model_conf, rec_level):
    """Ensure custom rtl configuration respected"""

    _custom_rtl = ['gt4ara', 'ulb-fas']
    assert o3o_ocrd.get_recognition_level(model_conf, _custom_rtl) == rec_level


@pytest.mark.parametrize("runtime_args, expected", [
    ('', []),
    ('--rm --memory=2GiB --memory-swap=2GiB --cpus=2 --gpus all',
     ['--rm', '--memory=2GiB', '--memory-swap=2GiB', '--cpus=2', '--gpus', 'all']),
    ('--gpus \'"device=0,1"\' --label "title=OCR job"',
     ['--gpus', '"device=0,1"', '--label', 'title=OCR job']),
])
def test_ocr_docker_runtime_arguments(tmp_path, monkeypatch, runtime_args, expected):
    commands = []

    def capture_run(command, **kwargs):
        commands.append((command, kwargs))

    monkeypatch.setattr(o3o_ocrd.subprocess, "run", capture_run)
    workspace = tmp_path / "OCR workspace"
    o3o_ocrd.run_ocr_page(
        workspace, "ocrd/test", runtime_args, 60, "urn+page", 1000,
        ['tesserocr-recognize -P model {model_config} -P textequiv_level {tesseract_level}'],
        'ger.traineddata', {str(tmp_path / 'model directory'): '/models'}, [])
    command, options = commands[0]
    assert command[:2 + len(expected)] == ['docker', 'run', *expected]
    assert command[2 + len(expected):2 + len(expected) + 4] == [
        '-u', '1000', '--name', 'urn-page']
    assert f'{workspace}:/data' in command
    assert f'{tmp_path / "model directory"}:/models' in command
    assert command[-4:] == ['ocrd/test', 'ocrd', 'process',
                           'tesserocr-recognize -P model ger -P textequiv_level word']
    assert options == {'shell': False, 'check': True, 'timeout': 60, 'cwd': str(workspace)}


@pytest.mark.parametrize("cleanup_fails", [False, True])
def test_ocr_docker_timeout_removes_container(tmp_path, monkeypatch, cleanup_fails):
    commands = []
    original_timeout = subprocess.TimeoutExpired('docker run', 60)

    def timed_out_run(command, **kwargs):
        commands.append((command, kwargs))
        if len(commands) == 1:
            raise original_timeout
        if cleanup_fails:
            raise OSError('Docker unavailable')

    monkeypatch.setattr(o3o_ocrd.subprocess, "run", timed_out_run)
    with pytest.raises(subprocess.TimeoutExpired) as error:
        o3o_ocrd.run_ocr_page(tmp_path, 'ocrd/test', '--rm', 60, 'urn+page', 1000,
                            ['recognize'], 'ger.traineddata', {}, [])
    assert error.value is original_timeout
    assert commands[1] == (['docker', 'rm', '--force', 'urn-page'],
                           {'shell': False, 'check': False, 'timeout': 30})


def test_create_workspace_mets(tmp_path):
    """Check what kind of workspace created
    and stored as METS/MODS for single added page

    cf. https://github.com/OCR-D/assets/tree/master/data/kant_aufklaerung_1784
    =>
    LOCTYPE="OTHER" OTHERLOCTYPE="FILE"

    Fix: move created png image to workspace, no copy
    """

    path_workspace = tmp_path / "00000001"
    path_image: Path = tmp_path / "00000001.tif"
    create_test_tif(path_image)
    o3o_ocrd.setup_workspace(path_workspace, path_image)
    path_ws_mets: Path = path_workspace / "mets.xml"

    # assert
    assert path_image.is_file()
    assert path_ws_mets.is_file()

    ws_mets_root = ET.parse(path_ws_mets).getroot()
    flocats = ws_mets_root.findall(".//mets:file/mets:FLocat", df.XMLNS)
    assert len(flocats) == 1
    assert flocats[0].attrib == {'{http://www.w3.org/1999/xlink}href': 'MAX/00000001.png',
                                 'LOCTYPE': 'OTHER',
                                 'OTHERLOCTYPE': 'FILE'}

    assert (path_workspace / "MAX" / "00000001.png").exists()
    assert not (tmp_path / "00000001.png").exists()
