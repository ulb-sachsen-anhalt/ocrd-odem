import argparse
import configparser
import os
import pathlib
import shutil

from PIL import Image
from ocrd.resolver import Resolver
from ocrd_utils import initLogging

from lib.ocrd3_odem import get_configparser

PROJECT_ROOT = pathlib.Path(__file__).resolve().parents[1]
EXT_JPG = '.jpg'
EXT_PNG = '.png'
DEFAULT_DPI = (300, 300)

initLogging()

arg_parser: argparse.ArgumentParser = argparse.ArgumentParser()
arg_parser.add_argument(
    "image",
    help="path to image file")
arg_parser.add_argument(
    "-c",
    "--config",
    required=False,
    default="resources/odem.ini",
    help="path to configuration file"
)
args = arg_parser.parse_args()
conf_file = os.path.abspath(args.config)
cfg_parser: configparser.ConfigParser = get_configparser()
cfg_parser.read(conf_file)

ocr_log_conf = os.path.join(PROJECT_ROOT, cfg_parser.get('ocr', 'ocrd_logging'))
ocr_makefile = os.path.join(PROJECT_ROOT, cfg_parser.get('ocr', 'ocrd_makefile'))
LOCAL_WORK_ROOT = cfg_parser.get('global', 'local_work_root')

# prepare page work dir
image_file_path = os.path.abspath(args.image)
image_file_basename = os.path.basename(image_file_path)
file_id = image_file_basename.split('.')[0]
page_workdir = os.path.join(LOCAL_WORK_ROOT, file_id)
if os.path.exists(page_workdir):
    shutil.rmtree(page_workdir, ignore_errors=True)
os.mkdir(page_workdir)
shutil.copy(ocr_log_conf, page_workdir)
shutil.copy(ocr_makefile, page_workdir)
os.chdir(page_workdir)

# preproc image
if not str(image_file_path).endswith(EXT_JPG):
    image_file_path = f"{image_file_path}{EXT_JPG}"
input_image = Image.open(image_file_path)
file_name = os.path.basename(image_file_path)
image_max_dir = os.path.join(page_workdir, 'MAX')
if not os.path.isdir(image_max_dir):
    os.mkdir(image_max_dir)
output_path = os.path.join(image_max_dir, file_name).replace(EXT_JPG, EXT_PNG)
res_dpi = DEFAULT_DPI
input_image.save(output_path, format='png', dpi=res_dpi)

# setup ocrd WS
the_dir = os.path.abspath(page_workdir)
resolver = Resolver()
workspace = resolver.workspace_from_nothing(
    directory=the_dir
)
image_name = os.path.basename(output_path)
resolver.download_to_directory(
    the_dir,
    output_path,
    subdir='MAX')
kwargs = {
    'fileGrp': 'MAX',
    'ID': 'MAX_01',
    'mimetype': 'image/png',
    'pageId': 'PHYS_01',
    'url': f"MAX/{image_name}"}
workspace.mets.add_file(**kwargs)
workspace.save_mets()

tessdir_host = cfg_parser.get('ocr', 'tessdir_host')
tessdir_cntr = cfg_parser.get('ocr', 'tessdir_cntr')
base_image = cfg_parser.get('ocr', 'ocrd_baseimage')

os.chdir(page_workdir)

cmd_inside_cntr = "ocrd-make TESSERACT_CONFIG=gt4hist_5000k TESSERACT_LEVEL=word -f ulb-odem.mk ."

print(("#" * 30), " VERSION 1 ", ("#" * 30))
print(f"cd {page_workdir}")
cmd: str = f'docker run --rm -u 1000 -w /data '
cmd += f'-v {page_workdir}:/data '
cmd += f'-v {tessdir_host}:{tessdir_cntr} '
cmd += f'{base_image} '
cmd += cmd_inside_cntr
print(cmd)

print(("#" * 30), " VERSION 2 ", ("#" * 30))
print(f"cd {page_workdir}")
cmd: str = f'docker run -it -u 1000 -w /data '
cmd += f'-v {page_workdir}:/data '
cmd += f'-v {tessdir_host}:{tessdir_cntr} '
cmd += f'{base_image} '
cmd += f"bash"
print(cmd)

print('-' * 80)
print(cmd_inside_cntr)
