"""Implementation of OCR-D related OCR generation functionalities"""

import os
import subprocess

from ocrd.resolver import (
    Resolver
)

from digiflow import (
    run_profiled,
)

from .odem_commons import (
    FILEGROUP_IMG,
    RTL_LANGUAGES,
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


def get_recognition_level(model_config: str) -> str:
    """Determine tesseract recognition level
    with respect to language order by model
    configuration"""

    if any((m for m in model_config.split('+') if m in RTL_LANGUAGES)):
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
    model = args[1]
    base_image = args[2]
    makefile = args[3]
    tess_host = args[4]
    tess_cntn = args[5]
    # determine if language requires word-level for RTLs
    tess_level = get_recognition_level(model)
    docker_container_memory_limit: str = args[6]
    docker_container_timeout: int = args[7]
    container_name = args[8]
    os.chdir(ocr_dir)
    user_id = os.getuid()
    # replace not allowed chars
    container_name = container_name.replace('+', '-')
    cmd: str = f"docker run --rm -u {user_id}"
    cmd += f" --name {container_name}"
    if docker_container_memory_limit is not None:
        cmd += f" --memory {docker_container_memory_limit}"
        cmd += f" --memory-swap {docker_container_memory_limit}" # same value disables swap
    cmd += f" -w /data -v {ocr_dir}:/data"
    cmd += f" -v {tess_host}:{tess_cntn} {base_image}"
    cmd += f" ocrd-make TESSERACT_CONFIG={model} TESSERACT_LEVEL={tess_level} -f {makefile} . "
    subprocess.run(cmd, shell=True, check=True, timeout=docker_container_timeout)
