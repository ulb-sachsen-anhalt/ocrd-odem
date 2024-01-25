"""Implementation of OCR-D related OCR generation functionalities"""

import os
import subprocess
from typing import List

from ocrd.resolver import (
    Resolver
)

from digiflow import (
    run_profiled,
)

from .odem_commons import (
    FILEGROUP_IMG,
)


def ocrd_workspace_setup(path_workspace, image_path):
    """Wrap ocrd workspace init and add single file"""

    # init clean workspace
    the_dir = os.path.abspath(path_workspace)
    resolver = Resolver()
    workspace = resolver.workspace_from_nothing(
        directory=the_dir
    )
    workspace.save_mets()

    # add the one image which resides
    # already inside 'MAX' directory
    image_name = os.path.basename(image_path)
    resolver.download_to_directory(
        the_dir,
        image_path,
        subdir=FILEGROUP_IMG)
    kwargs = {
        'fileGrp': FILEGROUP_IMG,
        'ID': 'MAX_01',
        'mimetype': 'image/png',
        'pageId': 'PHYS_01',
        'url': f"{FILEGROUP_IMG}/{image_name}"}
    workspace.mets.add_file(**kwargs)
    workspace.save_mets()
    return image_path


def get_recognition_level(model_config: str, rtl_models: List[str]) -> str:
    """Determine tesseract recognition level
    with respect to language order by model
    configuration"""

    if any((m for m in model_config.split('+') if m in rtl_models)):
        return 'glyph'
    return 'word'


@run_profiled
def run_ocr_page(*args):
    """wrap ocr container process
    *Please note*
    Trailing dot (".") is cruical, since it means "this directory"
    and is mandatory since 2022
    """

    ocr_dir = args[0]
    base_image = args[1]

    container_memory_limit: str = args[2]
    container_timeout: int = args[3]
    container_name = args[4]
    container_user_id = args[5]

    ocrd_process_list: List[str] = args[6]

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
    ocrd_process_list: List[str] = [f'"{p.format(**ocrd_process_args)}"' for p in ocrd_process_list]
    ocrd_process_str: str = " ".join(ocrd_process_list)

    cmd: str = f"docker run --rm -u {container_user_id}"
    cmd += f" --name {container_name}"
    if container_memory_limit is not None:
        cmd += f" --memory {container_memory_limit}"
        cmd += f" --memory-swap {container_memory_limit}"  # same value disables swap
    cmd += f" -w /data -v {ocr_dir}:/data"
    for host_dir, cntr_dir in ocrd_resources_volumes.items():
        cmd += f" -v {host_dir}:{cntr_dir}"
    cmd += f" {base_image}"
    cmd += f" ocrd process {ocrd_process_str}"
    subprocess.run(cmd, shell=True, check=True, timeout=container_timeout)