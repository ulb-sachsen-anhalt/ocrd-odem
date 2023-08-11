# ULB ODEM

Implementationproject of the University and Statelibrary Sachsem-Anhalt for [OCR-D-Phase III](https://ocr-d.de/de/phase3) which implements an OCR-D-based Workflow for fulltext generation via [Tesseract-OCR]() of an already existing retro digitalisates in ["Drucke des 18. Jahrhunderts (VD18)"](https://opendata.uni-halle.de/handle/1981185920/31824), see.

Digitized prints are accessed as records via [OAI-PMH]() from a record list with, at the time of project start, consisted about 40.000 prints, both monographs and multivolumes. Correspondig images are load to a local worker machine, then each page is processed individually with a complete OCR-D-Workflow. Afterwards, the results are transformed into ALTO-OCR and an archive file containing a new complete PDF for the print with textlayer is generated. The resulting archive file complies to the [SAF fileformat]() of [DSpace-Systems]() like [Share_it](https://opendata.uni-halle.de/).

## Features

* Monitor required computing ressources (RAM / disc space)
* Runs both in virtual environment using local mount points or in isolated server machines
* Processing print on page-level: In case of errors/problems, only single page is lost
* Utilize print metadata (MODS) to select matching OCR modell configuration
* Utilize print metadata (METS) to filter pages for ocr-ing by blacklisting pages by logical structs or physical struct
  
## Runtime Requirements

* Minimum: Ubuntu Linux Server 20.04 LTS with 12 GB RAM / 8 CPUs
  (Recommended: 24 GB RAM / 12 CPUs)
* Docker CE 19.03.13
* Python 3.8+
* `git`, `zip`

## Local Installation

```bash
# clone
git clone <repo-url> <local-dir>

# setup python venv
python3 -m venv venv
pip install -U pip
pip install -r requirements.txt

# run tests
python -m pip install -r ./tests/test_requirements.txt
pytest -v
```

## Configuration

See `ressources/ode.ini` for complete template.

* `[resource-monitoring]` : include limits for disc and virtual memory usage
* `[mets]` : includes blacklists for pages/logical sections
* `[ocr]` : includes used OCR-D-Container image and language model configuration mappings

### Trigger Workflow via Crontab

```bash
#
# OCR-D ODEM
#
PYTHON_BIN=/home/ocr/ulb-ocr-odem/venv/bin/python3
PROJECT=/home/ocr/ulb-ocr-odem
OAI_RECORDS=oai-records-opendata-vd18-odem

*/5  08-23  * * *  ${PYTHON_BIN} ${PROJECT}/cli_oai_client.py ${OAI_RECORDS} -c ${PROJECT}/resources/odem.ocr-workerXX.ini -l
```

## Licence

Under terms of the [MIT license](https://opensource.org/licenses/MIT)
