"""Functional coverage of METS page metadata through OCR backend inputs."""

import concurrent.futures
import io
import logging
import threading
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path

import digiflow as df
import digiflow.record as df_r
import lxml.etree as ET
import PIL.Image
import pytest
import requests

from lib import odem
from lib.odem.ocr import ocr_d, ocr_pipeline, ocr_workflow
from lib.odem.processing import mets
from .conftest import TEST_RES, create_test_tif, fixture_configuration


@pytest.fixture
def corpus_root():
    return ET.fromstring(b"""
    <mets:mets xmlns:mets="http://www.loc.gov/METS/"
               xmlns:mods="http://www.loc.gov/mods/v3"
               xmlns:xlink="http://www.w3.org/1999/xlink">
      <mets:dmdSec ID="DE"><mets:mdWrap MDTYPE="MODS"><mets:xmlData><mods:mods>
        <mods:language><mods:languageTerm type="code" authority="iso639-2b">ger</mods:languageTerm></mods:language>
      </mods:mods></mets:xmlData></mets:mdWrap></mets:dmdSec>
      <mets:dmdSec ID="LA"><mets:mdWrap MDTYPE="MODS"><mets:xmlData><mods:mods>
        <mods:language><mods:languageTerm type="code" authority="iso639-2b">lat</mods:languageTerm></mods:language>
      </mods:mods></mets:xmlData></mets:mdWrap></mets:dmdSec>
      <mets:dmdSec ID="EMPTY"><mets:mdWrap MDTYPE="MODS"><mets:xmlData><mods:mods>
        <mods:language><mods:languageTerm type="text">German</mods:languageTerm></mods:language>
      </mods:mods></mets:xmlData></mets:mdWrap></mets:dmdSec>
      <mets:fileSec><mets:fileGrp USE="MAX">
        <mets:file ID="IMG1"><mets:FLocat xlink:href="MAX/one_lat.tif"/></mets:file>
        <mets:file ID="IMG2"><mets:FLocat xlink:href="MAX/two_ger.tif"/></mets:file>
      </mets:fileGrp></mets:fileSec>
      <mets:structMap TYPE="PHYSICAL"><mets:div ID="physroot" TYPE="physSequence">
        <mets:div ID="P1" TYPE="page" ORDERLABEL="1" CONTENTIDS="urn:test:p1/fragment/page=0001"><mets:fptr FILEID="IMG1"/></mets:div>
        <mets:div ID="P2" TYPE="page" ORDERLABEL="2" CONTENTIDS="urn:test:p2/fragment/page=0002"><mets:fptr FILEID="IMG2"/></mets:div>
      </mets:div></mets:structMap>
      <mets:structMap TYPE="LOGICAL"><mets:div ID="corpus" TYPE="document">
        <mets:div ID="BOOK1" TYPE="monograph" DMDID="DE">
          <mets:div ID="CHAPTER1" TYPE="chapter"/>
        </mets:div>
        <mets:div ID="BOOK2" TYPE="volume" DMDID="LA"/>
      </mets:div></mets:structMap>
      <mets:structLink>
        <mets:smLink xlink:from="CHAPTER1" xlink:to="P1"/>
        <mets:smLink xlink:from="BOOK2" xlink:to="P2"/>
      </mets:structLink>
    </mets:mets>
    """)


def node(root, identifier):
    return root.xpath('//*[@ID=$identifier]', identifier=identifier)[0]


@pytest.mark.parametrize(
    "physical_dmd, chapter_dmd, expected",
    [
        (None, None, ("ger",)),
        (None, "LA", ("lat",)),
        ("LA", "DE", ("lat",)),
        ("EMPTY", "LA", ("lat",)),
        (None, "DE LA DE", ("ger", "lat")),
        (None, "EMPTY", ("ger",)),
    ],
)
def test_nearest_page_language_metadata(corpus_root, physical_dmd, chapter_dmd, expected):
    if physical_dmd:
        node(corpus_root, "P1").set("DMDID", physical_dmd)
    if chapter_dmd:
        node(corpus_root, "CHAPTER1").set("DMDID", chapter_dmd)
    assert mets.page_languages_from_metadata(corpus_root, ["P1", "P2"]) == {
        "P1": expected, "P2": ("lat",),
    }


def test_multiple_logical_branches_combine_in_order(corpus_root):
    links = corpus_root.find("mets:structLink", df.XMLNS)
    ET.SubElement(links, ET.QName(df.XMLNS["mets"], "smLink"), {
        ET.QName(df.XMLNS["xlink"], "from").text: "BOOK2",
        ET.QName(df.XMLNS["xlink"], "to").text: "P1",
    })
    assert mets.page_languages_from_metadata(corpus_root, ["P1"]) == {
        "P1": ("ger", "lat"),
    }


def test_link_to_ancestor_does_not_override_more_specific_language(corpus_root):
    node(corpus_root, "CHAPTER1").set("DMDID", "LA")
    links = corpus_root.find("mets:structLink", df.XMLNS)
    ET.SubElement(links, ET.QName(df.XMLNS["mets"], "smLink"), {
        ET.QName(df.XMLNS["xlink"], "from").text: "BOOK1",
        ET.QName(df.XMLNS["xlink"], "to").text: "P1",
    })
    assert mets.page_languages_from_metadata(corpus_root, ["P1"])["P1"] == ("lat",)


@pytest.mark.parametrize("identifier", ["P1", "CHAPTER1", "BOOK1"])
def test_broken_dmd_reference_is_not_a_language_fallback(corpus_root, identifier):
    node(corpus_root, identifier).set("DMDID", "MISSING")
    with pytest.raises(mets.ODEMMetadataMetsException, match="Page P1.*missing DMDID MISSING"):
        mets.page_languages_from_metadata(corpus_root, ["P1"])


def test_broken_logical_reference_is_reported(corpus_root):
    corpus_root.find("mets:structLink/mets:smLink", df.XMLNS).set(
        ET.QName(df.XMLNS["xlink"], "from"), "MISSING")
    with pytest.raises(mets.ODEMMetadataMetsException, match="P1.*missing logical section MISSING"):
        mets.page_languages_from_metadata(corpus_root, ["P1"])


def test_duplicate_dmd_identifier_is_reported(corpus_root):
    node(corpus_root, "EMPTY").set("ID", "DE")
    with pytest.raises(mets.ODEMMetadataMetsException, match="Duplicate DMDID DE"):
        mets.page_languages_from_metadata(corpus_root, ["P1"])


@pytest.fixture
def corpus_process(tmp_path, corpus_root):
    config = fixture_configuration()
    config.set("mets", "corpus_identifier", "urn:test:corpus")
    config.set("mets", "use_fgroup", "MAX")
    config.remove_option("export", "export_artefact_name")
    config.set("ocr", "image_subpath", "MAX")
    config.set("ocr", "model_mapping", "ger: ger.traineddata, lat: lat.traineddata")
    model_dir = tmp_path / "models"
    model_dir.mkdir()
    for lang in ("ger", "lat"):
        (model_dir / f"{lang}.traineddata").write_bytes(b"test model")
    config.set("ocr", "ocrd_resources_volumes", f"{model_dir}:/models")
    config.remove_option("ocr", "language_model")
    path = tmp_path / "mets.xml"
    ET.ElementTree(corpus_root).write(str(path))
    images = tmp_path / "MAX"
    images.mkdir()
    images.chmod(0o775)
    for name in ("one_lat.tif", "two_ger.tif",
                 "urn+test+p1-fp-0001.tif", "urn+test+p2-fp-0002.tif"):
        create_test_tif(images / name)
    process = odem.ODEMProcessImpl(
        df_r.Record("urn:test:corpus"), config, work_dir=tmp_path,
        logger=logging.getLogger("test.page_languages"))
    process.mets_file_path = path
    return process


def inspect_and_prepare(process):
    process.inspect_metadata()
    process.prepare_page_modelconfigs()
    process.set_local_images()
    return process


def test_corpus_inspection_preserves_language_through_local_path_binding(corpus_process):
    process = inspect_and_prepare(corpus_process)
    assert process.artefact_identifier == "urn+test+corpus"
    assert process.process_statistics["type"] == ("document", None)
    assert process.process_statistics["languages"] == []
    assert process.page_languages == {"P1": ("ger",), "P2": ("lat",)}
    assert process.page_model_configs == {
        "P1": "ger.traineddata", "P2": "lat.traineddata",
    }
    for image, page in process.ocr_candidates:
        assert Path(image).is_file()
        assert process.map_language_to_modelconfig(image, page) == process.page_model_configs[page]
    assert process.process_statistics["language_model"] == [
        "ger.traineddata", "lat.traineddata",
    ]


def test_contentids_naming_keeps_colliding_source_names_page_specific(corpus_process, corpus_root):
    process = corpus_process
    process.configuration.set("workflow", "use_file_id", "True")
    node(corpus_root, "IMG2")[0].set(ET.QName(df.XMLNS["xlink"], "href"), "other/one_lat.tif")
    ET.ElementTree(corpus_root).write(str(process.mets_file_path))
    process.inspect_metadata()
    process.prepare_page_modelconfigs()
    assert process.ocr_candidates == [
        ("urn+test+p1-fp-0001.tif", "P1"), ("urn+test+p2-fp-0002.tif", "P2")]
    assert process.page_model_configs == {"P1": "ger.traineddata", "P2": "lat.traineddata"}


def test_corpus_requires_explicit_intake(corpus_process):
    corpus_process.configuration.remove_option("mets", "corpus_identifier")
    with pytest.raises(mets.ODEMMetadataMetsException):
        corpus_process.inspect_metadata()


def test_single_record_retains_document_identity_and_individual_languages(
        corpus_process, corpus_root):
    process = corpus_process
    process.configuration.remove_option("mets", "corpus_identifier")
    process.configuration.remove_option("mets", "record_identifier_xpr")
    node(corpus_root, "corpus").set("TYPE", "monograph")
    node(corpus_root, "corpus").set("DMDID", "DE")
    mods = node(corpus_root, "DE").find(".//mods:mods", df.XMLNS)
    identifier = ET.SubElement(mods, ET.QName(df.XMLNS["mods"], "identifier"), type="urn")
    identifier.text = "urn:test:book"
    links = corpus_root.find("mets:structLink", df.XMLNS)
    ET.SubElement(links, ET.QName(df.XMLNS["mets"], "smLink"), {
        ET.QName(df.XMLNS["xlink"], "from").text: "BOOK1",
        ET.QName(df.XMLNS["xlink"], "to").text: "P1",
    })
    ET.ElementTree(corpus_root).write(str(process.mets_file_path))
    inspect_and_prepare(process)
    assert process.artefact_identifier == "urn:test:book"
    assert process.process_statistics["languages"] == ["ger"]
    assert process.page_model_configs == {"P1": "ger.traineddata", "P2": "lat.traineddata"}


def test_corpus_rejects_nonprint_page(corpus_process, corpus_root):
    node(corpus_root, "BOOK2").set("TYPE", "map")
    ET.ElementTree(corpus_root).write(str(corpus_process.mets_file_path))
    with pytest.raises(mets.ODEMNoTypeForOCRException, match="P2.*no print structure"):
        corpus_process.inspect_metadata()


def test_blacklisted_page_is_excluded_from_language_model_preflight(corpus_process, corpus_root):
    node(corpus_root, "BOOK2").set("TYPE", "cover_front")
    corpus_root.remove(node(corpus_root, "LA"))
    ET.ElementTree(corpus_root).write(str(corpus_process.mets_file_path))
    process = inspect_and_prepare(corpus_process)
    assert process.page_model_configs == {"P1": "ger.traineddata"}
    assert len(process.ocr_candidates) == 1


def test_explicit_override_wins_without_changing_metadata(corpus_process):
    process = corpus_process
    process.configuration.set("ocr", "language_model", "lat")
    inspect_and_prepare(process)
    assert process.page_languages["P1"] == ("ger",)
    assert process.page_model_configs == {"P1": "lat.traineddata", "P2": "lat.traineddata"}
    assert process.process_statistics["language_model"] == "lat.traineddata"


@pytest.mark.parametrize("missing", ["language", "mapping", "resource"])
def test_page_model_preflight_fails_explicitly(corpus_process, missing):
    process = corpus_process
    process.inspect_metadata()
    if missing == "language":
        process.page_languages["P2"] = ()
    elif missing == "mapping":
        process.configuration.set("ocr", "model_mapping", "ger: ger.traineddata")
    else:
        (Path(process.work_dir_root) / "models" / "lat.traineddata").unlink()
    with pytest.raises(odem.ODEMModelMissingException):
        process.prepare_page_modelconfigs()
    assert process.page_model_configs == {}


@pytest.mark.parametrize("missing", ["mapping", "resource"])
def test_local_corpus_skips_pages_with_missing_models(corpus_process, missing, caplog):
    process = corpus_process
    process.local_mets_mode = True
    process.inspect_metadata()
    if missing == "mapping":
        process.configuration.set("ocr", "model_mapping", "ger: ger.traineddata")
    else:
        (Path(process.work_dir_root) / "models" / "lat.traineddata").unlink()
    process.prepare_page_modelconfigs()
    process.set_local_images()
    assert [page for _, page in process.ocr_candidates] == ["P1"]
    assert process.page_model_configs == {"P1": "ger.traineddata"}
    assert process.process_statistics["language_model"] == "ger.traineddata"
    assert process.process_statistics["n_images_ocrable"] == 1
    assert process.process_statistics["n_images_pages"] == 2
    assert set(process.process_statistics["skipped_model_pages"]) == {"P2"}
    assert "skip page P2" in caplog.text
    assert process.page_languages["P2"] == ("lat",)


def test_local_regular_record_still_fails_on_missing_model(corpus_process):
    process = corpus_process
    process.local_mets_mode = True
    process.inspect_metadata()
    process.configuration.remove_option("mets", "corpus_identifier")
    process.page_languages["P2"] = ("unknown",)
    with pytest.raises(odem.ODEMModelMissingException, match="Page P2.*unknown"):
        process.prepare_page_modelconfigs()
    assert len(process.ocr_candidates) == 2
    assert process.page_model_configs == {}


def test_local_corpus_with_no_supported_models_has_no_ocr_images(corpus_process):
    process = corpus_process
    process.local_mets_mode = True
    process.inspect_metadata()
    process.configuration.set("ocr", "model_mapping", "unknown: unknown.traineddata")
    with pytest.raises(odem.ODEMNoImagesForOCRException, match="after model preflight"):
        process.prepare_page_modelconfigs()
    assert process.ocr_candidates == []
    assert process.page_model_configs == {}
    assert process.process_statistics["n_images_ocrable"] == 0
    assert set(process.process_statistics["skipped_model_pages"]) == {"P1", "P2"}
    assert "language_model" not in process.process_statistics


def test_local_corpus_runner_only_processes_supported_pages(corpus_process, monkeypatch):
    process = corpus_process
    process.local_mets_mode = True
    process.inspect_metadata()
    process.page_languages["P2"] = ("unknown",)
    workflow = ocr_workflow.OCRDPageParallel(process)

    def supported_inputs():
        assert [page for _, page in process.ocr_candidates] == ["P1"]
        assert process.page_model_configs == {"P1": "ger.traineddata"}
        return []

    monkeypatch.setattr(workflow, "get_inputs", supported_inputs)
    monkeypatch.setattr(workflow, "process_outputs", lambda results: None)
    runner = ocr_workflow.OCRWorkflowRunner("corpus", 1, process.logger, workflow)
    assert runner.run() == []


def test_failed_repreparation_does_not_leave_stale_models(corpus_process):
    process = inspect_and_prepare(corpus_process)
    process.page_languages["P2"] = ("unknown",)
    with pytest.raises(odem.ODEMModelMissingException, match="Page P2.*unknown"):
        process.prepare_page_modelconfigs()
    assert process.page_model_configs == {}
    assert "page_model_configs" not in process.process_statistics
    assert "language_model" not in process.process_statistics


def test_runner_rejects_invalid_page_models_before_building_backend_inputs(
        corpus_process, monkeypatch):
    process = corpus_process
    process.inspect_metadata()
    process.page_languages["P2"] = ("unknown",)
    workflow = ocr_workflow.OCRDPageParallel(process)

    def unexpected_inputs():
        pytest.fail("Backend inputs must not be built before successful model preflight")

    monkeypatch.setattr(workflow, "get_inputs", unexpected_inputs)
    runner = ocr_workflow.OCRWorkflowRunner("corpus", 2, process.logger, workflow)
    with pytest.raises(odem.ODEMModelMissingException, match="Page P2.*unknown"):
        runner.run()


@pytest.mark.parametrize("combinable, expected", [
    (True, "ger.traineddata+lat.traineddata"), (False, "ger.traineddata"),
])
def test_model_composition_is_ordered_and_deduplicated(corpus_process, combinable, expected):
    process = corpus_process
    process.inspect_metadata()
    process.page_languages["P1"] = ("ger", "lat", "ger")
    process.configuration.set("ocr", "model_mapping",
                              "ger: ger.traineddata+lat.traineddata, lat: lat.traineddata")
    process.configuration.set("ocr", "model_combinable", str(combinable))
    process.prepare_page_modelconfigs()
    assert process.page_model_configs["P1"] == expected


def test_missing_page_language_uses_document_fallback(corpus_process):
    process = corpus_process
    process.inspect_metadata()
    process.page_languages["P2"] = ()
    process.process_statistics["languages"] = ["ger"]
    process.prepare_page_modelconfigs()
    assert process.page_model_configs["P2"] == "ger.traineddata"


def test_image_only_tesseract_inputs_still_use_filename_languages(corpus_process, tmp_path):
    process = corpus_process
    process.local_mode = True
    process.ocr_candidates = [
        (str(tmp_path / "MAX" / "one_lat.tif"), "one"),
        (str(tmp_path / "MAX" / "two_ger.tif"), "two"),
    ]
    pipeline = tmp_path / "steps.ini"
    pipeline.write_text("[step_01]\ntype = StepTesseract\nmodel_configs = stale\n")
    process.configuration.set("ocr", "ocr_pipeline_config", str(pipeline))
    workflow = ocr_workflow.ODEMTesseract(process)
    inputs = workflow.get_inputs()
    assert inputs[0][4]["step_01"]["model_configs"] == "lat"
    assert inputs[1][4]["step_01"]["model_configs"] == "ger"
    assert workflow.pipeline_configuration["step_01"]["model_configs"] == "stale"


def test_tesseract_pipeline_inherits_workflow_request_headers(corpus_process, tmp_path):
    pipeline = tmp_path / "steps.ini"
    pipeline.write_text('[step_01]\ntype = StepEstimateOCR\n'
                        'request_header = {"User-Agent": "stale"}\n')
    process = corpus_process
    process.configuration.set("ocr", "ocr_pipeline_config", str(pipeline))
    process.configuration.set("workflow", "request_header", '{"User-Agent": "my-agent"}')
    workflow = ocr_workflow.ODEMTesseract(process)
    config = workflow.read_pipeline_config(resolve_models=False)
    step = ocr_pipeline.StepEstimateOCR(dict(config["step_01"]))
    assert step.request_headers == {"User-Agent": "my-agent"}


@pytest.mark.parametrize("parameter", ["model_configs", "-l", "both", "none"])
def test_tesseract_pages_get_isolated_configs_and_runtime_commands(
        corpus_process, tmp_path, monkeypatch, parameter):
    process = inspect_and_prepare(corpus_process)
    pipeline = tmp_path / "steps.ini"
    params = {"model_configs": "model_configs = stale",
              "-l": "-l = stale",
              "both": "model_configs = stale\n-l = stale",
              "none": ""}[parameter]
    pipeline.write_text(f"[step_01]\ntype = StepTesseract\n{params}\noutput_configs = alto\n")
    process.configuration.set("ocr", "ocr_pipeline_config", str(pipeline))
    workflow = ocr_workflow.ODEMTesseract(process)
    commands = {}

    def execute(step):
        commands[step.path_in.name] = step.cmd
        step.path_next.write_text("<alto/>")

    monkeypatch.setattr(ocr_pipeline.StepTesseract, "execute", execute)
    inputs = workflow.get_inputs()
    assert inputs[0][4] is not inputs[1][4]
    assert workflow.pipeline_configuration is not inputs[0][4]
    with concurrent.futures.ThreadPoolExecutor(max_workers=2) as executor:
        results = list(executor.map(workflow.run, inputs))
    assert all(Path(result.local_path).is_file() for result in results)
    assert " -l ger " in commands["urn+test+p1-fp-0001.tif"]
    assert " -l lat " in commands["urn+test+p2-fp-0002.tif"]
    assert "stale" not in "".join(commands.values())
    assert workflow.get_inputs()[0][4]["step_01"]["model_configs"] == "ger"
    assert process.process_statistics["language_model"] == [
        "ger.traineddata", "lat.traineddata",
    ]


@pytest.mark.parametrize("docker_args", [None, "", "--rm --memory=2GiB --cpus=2 --gpus all"])
def test_ocrd_pages_pass_individual_model_to_backend(corpus_process, monkeypatch, docker_args):
    process = inspect_and_prepare(corpus_process)
    if docker_args is None:
        process.configuration.remove_option("ocr", "docker_args")
    else:
        process.configuration.set("ocr", "docker_args", docker_args)
    process.configuration.set("ocr", "docker_container_memory_limit", "99GiB")
    workflow = ocr_workflow.OCRDPageParallel(process)
    received = []
    received_runtime_args = []

    def run_page(*args):
        received.append(args[7])
        received_runtime_args.append(args[2])
        return ("test", 0)

    monkeypatch.setattr(ocr_d, "setup_workspace", lambda *args: None)
    monkeypatch.setattr(ocr_d, "run_ocr_page", run_page)
    monkeypatch.setattr(workflow, "_store_fulltext", lambda _, image: str(Path(image).with_suffix(".xml")))
    monkeypatch.setattr(workflow, "_preserve_log", lambda *args: None)
    monkeypatch.setattr(ocr_workflow.os, "chdir", lambda _: None)
    for candidate in workflow.get_inputs():
        workflow.run(candidate)
    assert received == ["ger.traineddata", "lat.traineddata"]
    expected_args = '--rm' if docker_args is None else docker_args
    assert received_runtime_args == [expected_args, expected_args]


def test_ocrd_command_contains_per_page_model_and_rtl_level(tmp_path, monkeypatch):
    commands = []
    monkeypatch.setattr(ocr_d.subprocess, "run", lambda command, **kwargs: commands.append(command))
    for model in ("ger.traineddata", "lat.traineddata", "heb.traineddata"):
        ocr_d.run_ocr_page(
            str(tmp_path), "ocrd/test", "--rm --memory=1GiB --memory-swap=1GiB", 60, "page", 1000,
            ["tesserocr-recognize -P model {model_config} -P textequiv_level {tesseract_level}"],
            model, {}, ["heb.traineddata"])
    assert commands[0][-1] == "tesserocr-recognize -P model ger -P textequiv_level word"
    assert commands[1][-1] == "tesserocr-recognize -P model lat -P textequiv_level word"
    assert commands[2][-1] == "tesserocr-recognize -P model heb -P textequiv_level glyph"


@pytest.fixture
def image_server():
    body = io.BytesIO()
    PIL.Image.new("RGB", (20, 30), "white").save(body, format="TIFF")
    routes = {
        "/one.tif": (200, "image/tiff", body.getvalue()),
        "/two.tif": (200, "image/tiff", body.getvalue()),
        "/bad.tif": (200, "image/tiff", b"not an image"),
        "/empty.tif": (200, "image/tiff", b""),
        "/html.tif": (200, "text/html", b"<html>login</html>"),
    }
    requested = []

    class Handler(BaseHTTPRequestHandler):
        def do_GET(self):
            requested.append(self.path)
            status, content_type, data = routes.get(self.path, (404, "text/plain", b"missing"))
            self.send_response(status)
            self.send_header("Content-Type", content_type)
            self.send_header("Content-Length", str(len(data)))
            self.end_headers()
            self.wfile.write(data)

        def log_message(self, format, *args):
            pass

    server = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
    thread = threading.Thread(target=server.serve_forever)
    thread.start()
    try:
        yield f"http://127.0.0.1:{server.server_port}", requested, routes
    finally:
        server.shutdown()
        server.server_close()
        thread.join()


def configure_remote_images(process, root, base_url):
    for file_id, endpoint in (("IMG1", "/one.tif"), ("IMG2", "/two.tif")):
        node(root, file_id)[0].set(ET.QName(df.XMLNS["xlink"], "href"), base_url + endpoint)
    ET.ElementTree(root).write(str(process.mets_file_path))


def test_corpus_downloads_only_missing_images_and_keeps_local_files(
        corpus_process, corpus_root, image_server):
    base_url, requested, _ = image_server
    configure_remote_images(corpus_process, corpus_root, base_url)
    images = Path(corpus_process.work_dir_root) / "MAX"
    existing = images / "urn+test+p1-fp-0001.tif"
    original = existing.read_bytes()
    missing = images / "urn+test+p2-fp-0002.tif"
    missing.unlink()
    inspect_and_prepare(corpus_process)
    assert requested == ["/two.tif"]
    assert existing.read_bytes() == original
    with PIL.Image.open(missing) as image:
        assert image.size == (20, 30)
    assert missing.stat().st_mode & 0o777 == 0o664
    assert not list(images.glob(".odem-image-*"))
    assert corpus_process.page_model_configs["P2"] == "lat.traineddata"
    original_candidates = list(corpus_process.ocr_candidates)
    corpus_process.set_local_images()
    assert corpus_process.ocr_candidates == original_candidates
    assert requested == ["/two.tif"]


def test_corpus_download_uses_workflow_request_headers(
        corpus_process, corpus_root, image_server, monkeypatch):
    base_url, requested, _ = image_server
    configure_remote_images(corpus_process, corpus_root, base_url)
    corpus_process.configuration.set(
        "workflow", "request_header", '{"User-Agent": "my-agent", "X-ODEM": "corpus"}')
    images = Path(corpus_process.work_dir_root) / "MAX"
    (images / "urn+test+p2-fp-0002.tif").unlink()
    original_get = requests.get
    sent_headers = []

    def tracked_get(*args, **kwargs):
        sent_headers.append(kwargs.get("headers"))
        return original_get(*args, **kwargs)

    monkeypatch.setattr(requests, "get", tracked_get)
    inspect_and_prepare(corpus_process)
    assert requested == ["/two.tif"]
    assert sent_headers == [{"User-Agent": "my-agent", "X-ODEM": "corpus"}]


@pytest.mark.parametrize("image_subpath", [None, "downloaded"])
def test_missing_image_directory_is_created_and_used(
        corpus_process, corpus_root, image_server, image_subpath):
    base_url, requested, _ = image_server
    configure_remote_images(corpus_process, corpus_root, base_url)
    process = corpus_process
    if image_subpath is None:
        process.configuration.remove_option("ocr", "image_subpath")
        directory = Path(process.work_dir_root) / "MAX"
        for image in directory.iterdir():
            image.unlink()
        directory.rmdir()
    else:
        process.configuration.set("ocr", "image_subpath", image_subpath)
        directory = Path(process.work_dir_root) / image_subpath
    inspect_and_prepare(process)
    assert requested == ["/one.tif", "/two.tif"]
    assert directory.is_dir()
    assert directory.stat().st_mode & 0o777 == 0o775
    assert all(Path(image).parent == directory for image, _ in process.ocr_candidates)
    assert all(Path(image).is_file() for image, _ in process.ocr_candidates)


def test_blacklisted_corpus_images_are_not_downloaded(corpus_process, corpus_root, image_server):
    base_url, requested, _ = image_server
    configure_remote_images(corpus_process, corpus_root, base_url)
    node(corpus_root, "BOOK2").set("TYPE", "cover_front")
    ET.ElementTree(corpus_root).write(str(corpus_process.mets_file_path))
    images = Path(corpus_process.work_dir_root) / "MAX"
    (images / "urn+test+p1-fp-0001.tif").unlink()
    (images / "urn+test+p2-fp-0002.tif").unlink()
    inspect_and_prepare(corpus_process)
    assert requested == ["/one.tif"]
    assert not (images / "urn+test+p2-fp-0002.tif").exists()


@pytest.mark.parametrize("endpoint", ["/missing.tif", "/html.tif", "/empty.tif", "/bad.tif"])
def test_failed_download_does_not_publish_image_and_can_be_retried(
        corpus_process, corpus_root, image_server, endpoint):
    base_url, requested, routes = image_server
    configure_remote_images(corpus_process, corpus_root, base_url)
    node(corpus_root, "IMG2")[0].set(ET.QName(df.XMLNS["xlink"], "href"), base_url + endpoint)
    ET.ElementTree(corpus_root).write(str(corpus_process.mets_file_path))
    images = Path(corpus_process.work_dir_root) / "MAX"
    missing = images / "urn+test+p2-fp-0002.tif"
    missing.unlink()
    process = corpus_process
    process.inspect_metadata()
    with pytest.raises(odem.ODEMException, match="Page P2"):
        process.set_local_images()
    assert not missing.exists()
    assert not list(images.glob(".odem-image-*"))
    routes[endpoint] = routes["/two.tif"]
    process.set_local_images()
    assert missing.is_file()
    assert requested == [endpoint, endpoint]


def test_interrupted_download_removes_partial_file(corpus_process, corpus_root, monkeypatch):
    configure_remote_images(corpus_process, corpus_root, "http://localhost")
    images = Path(corpus_process.work_dir_root) / "MAX"
    missing = images / "urn+test+p2-fp-0002.tif"
    missing.unlink()

    class InterruptedResponse:
        headers = {"Content-Type": "image/tiff"}

        def __enter__(self):
            return self

        def __exit__(self, *args):
            return False

        def raise_for_status(self):
            pass

        def iter_content(self, chunk_size):
            yield b"partial image"
            raise requests.ConnectionError("connection lost")

    monkeypatch.setattr(requests, "get", lambda *args, **kwargs: InterruptedResponse())
    corpus_process.inspect_metadata()
    with pytest.raises(odem.ODEMException, match="Page P2.*connection lost"):
        corpus_process.set_local_images()
    assert not missing.exists()
    assert not list(images.glob(".odem-image-*"))


def test_missing_relative_source_fails_instead_of_attempting_network(corpus_process):
    images = Path(corpus_process.work_dir_root) / "MAX"
    (images / "urn+test+p2-fp-0002.tif").unlink()
    corpus_process.inspect_metadata()
    with pytest.raises(odem.ODEMException, match="Page P2.*not an HTTP"):
        corpus_process.set_local_images()


@pytest.mark.parametrize("content_id", ["", "../../outside", "urn:test:p1 urn:test:other"])
def test_corpus_requires_safe_single_contentids(corpus_process, corpus_root, content_id):
    node(corpus_root, "P1").set("CONTENTIDS", content_id)
    ET.ElementTree(corpus_root).write(str(corpus_process.mets_file_path))
    with pytest.raises(mets.ODEMMetadataMetsException, match="P1.*CONTENTIDS"):
        corpus_process.inspect_metadata()


def test_duplicate_contentids_fail_before_download(corpus_process, corpus_root):
    node(corpus_root, "P2").set("CONTENTIDS", node(corpus_root, "P1").get("CONTENTIDS"))
    ET.ElementTree(corpus_root).write(str(corpus_process.mets_file_path))
    with pytest.raises(mets.ODEMMetadataMetsException, match="CONTENTIDS.*not unique"):
        corpus_process.inspect_metadata()


@pytest.mark.parametrize("source", [
    "https://example.test/00000512.jpg",
    "https://example.test/00000512.jpg?token=test",
    "https://example.test/retrieve",
])
def test_real_world_contentids_image_name(corpus_process, corpus_root, source):
    node(corpus_root, "P1").set(
        "CONTENTIDS", "urn:nbn:de:gbv:3:1-119201541511222-535647891-13/fragment/page=0512")
    node(corpus_root, "IMG1")[0].set(ET.QName(df.XMLNS["xlink"], "href"), source)
    ET.ElementTree(corpus_root).write(str(corpus_process.mets_file_path))
    corpus_process.inspect_metadata()
    expected = "urn+nbn+de+gbv+3+1-119201541511222-535647891-13-fp-0512.jpg"
    assert corpus_process.ocr_candidates[0] == (expected, "P1")
    assert corpus_process.image_sources[(expected, "P1")] == source


def test_corpus_ocr_results_link_to_physical_ids_after_image_renaming(
        corpus_process, corpus_root, tmp_path):
    process = inspect_and_prepare(corpus_process)
    fulltext = tmp_path / "FULLTEXT"
    fulltext.mkdir()
    original = (TEST_RES / "0041.xml").read_bytes()
    for image, _ in process.ocr_candidates:
        (fulltext / Path(image).with_suffix(".xml").name).write_bytes(original)
    assert process.link_ocr_files() == 2
    root = ET.parse(str(process.mets_file_path)).getroot()
    assert node(root, "P1").find('mets:fptr[@FILEID="FULLTEXT_P1"]', df.XMLNS) is not None
    assert node(root, "P2").find('mets:fptr[@FILEID="FULLTEXT_P2"]', df.XMLNS) is not None
    for image, page_id in process.ocr_candidates:
        file_id = "FULLTEXT_" + page_id
        file_entry = node(root, file_id)
        assert file_entry.find("mets:FLocat", df.XMLNS).get(mets.Q_XLINK_HREF).endswith(
            Path(image).with_suffix(".xml").name)
        alto = ET.parse(str(fulltext / Path(image).with_suffix(".xml").name))
        assert alto.xpath('//*[local-name()="sourceImageInformation"]/'
                          '*[local-name()="fileName"]/text()') == [Path(image).name]


def test_existing_basename_does_not_hide_missing_page_identified_image(
        corpus_process, corpus_root, image_server):
    base_url, requested, _ = image_server
    configure_remote_images(corpus_process, corpus_root, base_url)
    images = Path(corpus_process.work_dir_root) / "MAX"
    (images / "urn+test+p2-fp-0002.tif").unlink()
    (images / "two.tif").write_bytes(b"wrong page with same source basename")
    inspect_and_prepare(corpus_process)
    assert requested == ["/two.tif"]


def test_noncorpus_missing_image_retains_existing_error(corpus_process, monkeypatch):
    process = corpus_process
    process.ocr_candidates = [("absent.tif", "P1")]
    monkeypatch.setattr(requests, "get", lambda *args, **kwargs: pytest.fail("Unexpected download"))
    with pytest.raises(odem.ODEMException, match="missing .*absent.tif"):
        process.set_local_images()
