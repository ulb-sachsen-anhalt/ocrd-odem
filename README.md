# ULB ODEM

![Python application](https://github.com/ulb-sachsen-anhalt/ocrd-odem/actions/workflows/python-app.yml/badge.svg)

Implementation Project of the University and State Library Sachsen-Anhalt (ULB Sachsen-Anhalt) for [OCR-D-Phase III](https://ocr-d.de/de/phase3) founded by [DFG](https://gepris.dfg.de/gepris/projekt/460554747) 2021-2024 to generate fulltext for existing digitalisates of ["Drucke des 18. Jahrhunderts (VD18)"](https://opendata.uni-halle.de/handle/1981185920/31824). More information can be found in ["API Magazin Bd. 6 Nr. 1 (2025)"](https://journals.sub.uni-hamburg.de/hup3/apimagazin/issue/view/15) ["Volltext für digitale Sammlungen separat erzeugen"](https://doi.org/10.15460/apimagazin.2025.6.1.221).


Digitized prints are accessed as records via [OAI-PMH](https://www.openarchives.org/pmh/) from a record list which. Corresponding images are load to a local worker machine, then each page is processed individually with a complete OCR-D-Workflow. Afterwards, the results are transformed into ALTO-OCR and an archive file containing a new complete PDF for the print with textlayer is generated. The resulting archive file complies to the [SAF fileformat](https://wiki.lyrasis.org/display/DSDOC6x/Importing+and+Exporting+Items+via+Simple+Archive+Format) of [DSpace-Systems](https://github.com/DSpace/DSpace) like [Share_it](https://opendata.uni-halle.de/).

## Features

* Process prints / directories on page level, so due errors only singles page is lost
* Use metadata (`mods:language`) to match OCR model configuration
* Use metadata (if present) to filter pages to process concerning logical and physical information
* Monitor computing resources (RAM / disk space)
* Runs in different execution modes 
  * local using shared directories (NFS)
  * isolated clients on working machines
  
## Runtime Requirements

* Linux Server (Ubuntu 24.04 LTS, min. 12 GB RAM / 8 CPUs, 100GB disc space, `zip` , `git`)
* Docker images, prefer to pull/build before usage:
  * ocr-d: `ocr-d/all:2023-02-07` (size: 13.9GB)
  * opt. derivans for PDF: `ghcr.io/ulb-sachsen-anhalt/digital-derivans:2.0.0` (size: 476MB)
* Python 3.10+
* high quality model configurations for Tesseract-OCR can be loaded from ["UB Mannheim"](https://digi.bib.uni-mannheim.de/tesseract/traineddata/)
  and must be placed to the proper directory (see configuration `[ocr][ocrd_resources_volumes]`)

## Installation

```bash
# clone
git clone <repo-url> <local-dir>

# setup python venv
python3 -m venv venv
pip install -U pip
pip install -r requirements.txt

# run tests
python -m pip install pytest-cov
python -m pytest --cov=lib tests/ -v
```

## Configuration

Since the overall workflow takes place in an isolated, local workspace, it's important to adjust
the configuration properly to this local context.

Configuration Options are grouped into 6 main sections:

* `[workflow]` : basic configuration of local work/log directories.  
   At least `[local_work_root]` and `[local_log_dir]` must be set accordingly.
* `[resource-monitoring]` : limits for local space and virtual memory usage
* `[mets]` : Blacklists for pages/logical sections, validation of metadata
* `[ocr]` : Container images and language model configuration mappings  
   Most critical options are `[model_mapping]` for mapping of `mods:language` to a OCR configuration, `[ocrd_resources_volumes]` for mapping local resources into each OCR-container and for OCR-D `[ocrd_process_list]` to define the ocr-d-processing steps
* `[derivans]` : Derivans container image and configuration (optional)
* `[export]` : Export asset and it's contents (optional)  
   If export data requiered, set options `[export_tmp]` and `[export_dst]` to valid directories 

See for example `resources/odem.local.example.ini`.

## Execution

### Local METS/MODS Mode

Assumes local accessible directory containing metadata (METS/MODS-XML file) in `/home/ocr/data/digital-object-01` and local ODEM clone at `/home/ocr/odem` which contains adopted configurations under `resources/odem.record.local.ini`.

```bash
cd /home/ocr/odem
python3.10 -m venv venv
. venv/bin/activate
pip install -U pip 
pip install -r requirements.txt
python cli_mets_local.py /home/ocr/data/digital-object-01/1234.xml -c resources/odem.record.local.ini
```

### Trigger Server/Client Workflow by Crontab

Assumes record list (CSV-file) managed by `cli_record_server.py` module, which start simple HTTP-Service to serve data. No authentications means are included but an IP-whitelist.

ODEM client instances are executed periodically by cron jobs.
Assuming a local installation at `/home/ocr/odem` and configurations located at `<PROJECT>/resources/`:

Setup and start server process:

```bash
cd /home/ocr/odem
python3.10 -m venv venv
. venv/bin/activate
pip install -U pip 
pip install -r requirements.txt
python cli_record_server.py resources/odem.ocrd.tesseract.ini
```

Crontab entry for executing actual worker:

```bash
PYTHON_BIN=/home/ocr/odem/venv/bin/python3
PROJECT=/home/ocr/odem
RECORD_LIST=oai-records-opendata-vd18-odem

*/5  08-23  * * *  ${PYTHON_BIN} ${PROJECT}/cli_record_server_client.py ${RECORD_LIST} -c ${PROJECT}/resources/odem.ocr-worker01.ini -l
```

## License

This project's source code is licensed under terms of the [MIT license](https://opensource.org/licenses/MIT).
