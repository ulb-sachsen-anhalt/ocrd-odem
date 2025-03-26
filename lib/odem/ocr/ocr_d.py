"""Implementation of OCR-D related OCR generation functionalities"""

import os
import shutil
import subprocess
import typing

from pathlib import Path

import digiflow as df
import lxml.etree as ET

import lib.odem.odem_commons as oc
import lib.odem.processing.image as oi

# pylint: disable=c-extension-no-member

def setup_workspace(path_workspace, image_src):
    """Wrap ocrd workspace init and add single file"""

    # init clean workspace
    page_dir = Path(path_workspace).absolute()
    if page_dir.exists():
        shutil.rmtree(page_dir)
    png_image = oi.ensure_format_png(image_src)
    image_dir = page_dir / oc.FILEGROUP_IMG
    image_dir.mkdir(parents=True)
    dst_image = image_dir / png_image.name
    shutil.copyfile(png_image, dst_image)
    mets_path = shutil.copyfile(oc.PROJECT_RES / "mets_empty.xml", page_dir / "mets.xml")
    mets_proc = df.MetsProcessor(mets_path)
    mets_proc.enrich_agent(agent_name="OCR-D", agent_note="page parallel")
    max_group = mets_proc.root.find(".//mets:fileGrp[@USE='MAX']", namespaces=df.XMLNS)
    file_attr = {"ID": "MAX_01", "MIMETYPE" : f"image/{png_image.suffix[1:]}"}
    mets_file = ET.SubElement(max_group, "{http://www.loc.gov/METS/}file", file_attr)
    locat_attr = {"{http://www.w3.org/1999/xlink}href": f"MAX/{png_image.name}",
                  "LOCTYPE": "OTHER", "OTHERLOCTYPE": "FILE"}
    ET.SubElement(mets_file, "{http://www.loc.gov/METS/}FLocat", locat_attr)
    page_root = mets_proc.root.find(".//mets:div[@TYPE='physSequence']",
                                    namespaces=df.XMLNS)
    page_01 = ET.SubElement(page_root, "{http://www.loc.gov/METS/}div",
                            {"ID": "PHYS_01", "TYPE": "page"})
    ET.SubElement(page_01, "{http://www.loc.gov/METS/}fptr", {"FILEID": "MAX_01"})
    mets_proc.write()
    return page_dir


def get_recognition_level(model_config: str, rtl_models: typing.List) -> str:
    """Determine tesseract recognition level
    with respect to language order by model
    configuration"""

    if any((m for m in model_config.split('+') if m in rtl_models)):
        return 'glyph'
    return 'word'


@df.run_profiled
def run_ocr_page(*args):
    """wrap ocr container process
    *Please note*
    Trailing dot (".") is cruical, since it means "this directory"
    and is mandatory since 2022
    """

    ocr_dir = args[0]
    container_image = args[1]
    container_memory_limit: str = args[2]
    container_timeout: int = args[3]
    container_name = args[4]
    container_user_id = args[5]
    ocrd_process_list: typing.List = args[6]
    model_config = args[7]
    ocrd_resources_volumes = args[8]
    tesseract_model_rtl = args[9]

    # determine if language requires word-level for RTLs
    tess_level = get_recognition_level(model_config, tesseract_model_rtl)

    model_config = model_config.replace('.traineddata', '')

    os.chdir(ocr_dir)
    # replace not allowed chars
    container_name = container_name.replace('+', '-')

    ocrd_process_args = {
        'tesseract_level': tess_level,
        'model_config': model_config
    }
    ocrd_process_list: typing.List = [f'"{p.format(**ocrd_process_args)}"' for p in ocrd_process_list]
    ocrd_process_str: str = " ".join(ocrd_process_list)
    cmd: str = f"docker run --rm -u {container_user_id}"
    cmd += f" --name {container_name}"
    if container_memory_limit is not None:
        cmd += f" --memory {container_memory_limit}"
        cmd += f" --memory-swap {container_memory_limit}"  # same value disables swap
    cmd += f" -w /data -v {ocr_dir}:/data"
    for host_dir, cntr_dir in ocrd_resources_volumes.items():
        cmd += f" -v {host_dir}:{cntr_dir}"
    cmd += f" {container_image}"
    cmd += f" ocrd process {ocrd_process_str}"
    subprocess.run(cmd, shell=True, check=True, timeout=container_timeout)
