# ULB ODEM

![Python application](https://github.com/ulb-sachsen-anhalt/ocrd-odem/actions/workflows/python-app.yml/badge.svg)

Project of the University and State Library Sachsen-Anhalt (ULB Sachsen-Anhalt) for [OCR-D-Phase III](https://ocr-d.de/de/phase3) founded by [DFG](https://gepris.dfg.de/gepris/projekt/460554747) 2021-2024 to implement an OCR-D-based Workflow for fulltext generation for existing digitalisates of ["Drucke des 18. Jahrhunderts (VD18)"](https://opendata.uni-halle.de/handle/1981185920/31824).

Digitized prints are accessed as records via [OAI-PMH](https://www.openarchives.org/pmh/) from a record list which, at the time of project start, included about 40.000 prints (monographs and multivolumes) with total about 6Mio pages. Corresponding images are load to a local worker machine, then each page is processed individually with a complete OCR-D-Workflow. Afterwards, the results are transformed into ALTO-OCR and an archive file containing a new complete PDF for the print with textlayer is generated. The resulting archive file complies to the [SAF fileformat](https://wiki.lyrasis.org/display/DSDOC6x/Importing+and+Exporting+Items+via+Simple+Archive+Format) of [DSpace-Systems](https://github.com/DSpace/DSpace) like [Share_it](https://opendata.uni-halle.de/).

## Features

* Monitors computing resources (RAM / disk space)
* Runs both in virtual environment using local mount points or in isolated server machines
* Processing print on page-level: In case of errors/problems, only single page is lost
* Utilize print metadata (MODS) to select matching OCR model configuration
* Utilize print metadata (METS) to filter pages for ocr-ing by blacklisting pages by logical structs or physical struct
  
## Runtime Requirements

* Minimum: Ubuntu Linux Server 20.04 LTS with 12 GB RAM / 8 CPUs
  (Recommended: 24 GB RAM / 12 CPUs)
* Docker CE 19.03.13
* Python 3.10
* `git`, `zip`

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

Options can found in the following sections:

* `[resource-monitoring]` : monitor limits for disk and virtual memory usage
* `[mets]` : blacklists for pages/logical sections
* `[ocr]` : OCR-D-Container image, language model configuration mappings
* `[derivans]` : Derivans container image and configuration

See for example `resources/odem.ocrd.tesseract.ini`.

### Trigger Workflow via Crontab

Usually there is a record list (simple CSV-file) in the backend managed by `cli_record_server.py` module, which needs to be started. Please note, that no authentication restrictions are include. Ensure yourself it runs only in closed network environments.

ODEM client instances can be executed peridically, triggered by server cron jobs entries.
Assuming there is local installation in `/home/ocr/odem` and a custom configurations located at `<PROJECT>/resources/`, it may look like this:

Start server process:

```bash
cd /home/ode/odem
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
