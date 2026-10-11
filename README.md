# ULB ODEM

![Python application](https://github.com/ulb-sachsen-anhalt/ocrd-odem/actions/workflows/python-app.yml/badge.svg)

Implementation Project of the University and State Library Sachsen-Anhalt (ULB Sachsen-Anhalt) for [OCR-D-Phase III](https://ocr-d.de/de/phase3) founded by [DFG](https://gepris.dfg.de/gepris/projekt/460554747) 2021-2024 to generate fulltext for existing digital images of ["Drucke des 18. Jahrhunderts (VD18)"](https://opendata.uni-halle.de/handle/1981185920/31824). More information can be found in ["API Magazin Bd. 6 Nr. 1 (2025)"](https://journals.sub.uni-hamburg.de/hup3/apimagazin/issue/view/15) ["Volltext für digitale Sammlungen separat erzeugen"](https://doi.org/10.15460/apimagazin.2025.6.1.221).


Digitized prints are accessed as records via [OAI-PMH](https://www.openarchives.org/pmh/) from a record list. Corresponding images are loaded to a local worker machine, then each page is processed individually passing through a complete OCR-D-Workflow. Subsequently, the results are transformed into ALTO-OCR and an archive file containing a new complete PDF for the print with textlayer is generated. The resulting archive file complies with the [SAF fileformat](https://wiki.lyrasis.org/display/DSDOC9x/Importing+and+Exporting+Items+via+Simple+Archive+Format) of [DSpace-Systems](https://github.com/DSpace/DSpace) like [Share_it](https://opendata.uni-halle.de/).

## Features

* Process prints / directories on page level, so only a single page is lost if errors occur
* Use metadata (`mods:language`) to match OCR model configuration
* Use metadata (if present) to filter pages to process concerning logical and physical information
* Monitor computing resources (RAM / disk space)
* Runs in different execution modes 
  * local using shared directories (NFS)
  * isolated clients on working machines
  
## Runtime Requirements

* Linux Server (Ubuntu 24.04 LTS, min. 12 GB RAM / 8 CPUs, 100GB disc space, `zip` , `git`)
* Docker images, prefer to pull/build before usage:
  * ocr-d: `ocrd/all:2023-02-07` (size: 13.9GB)
  * opt. derivans for PDF: `ghcr.io/ulb-sachsen-anhalt/digital-derivans:2.2.4` (size: 495MB)
* Python 3.10 (currently *not* running with 3.12+)
  * as python 3.10 is currently no longer supported, it needs to be installed externally, e.g. 
  ```sudo add-apt-repository ppa:deadsnakes/ppa -y && sudo apt update && sudo apt install python3.10-full -y```
* high quality model configurations for Tesseract-OCR can be loaded from ["UB Mannheim"](https://digi.bib.uni-mannheim.de/tesseract/traineddata/)
  and must be placed to the proper directory (see configuration `[ocr][ocrd_resources_volumes]`)
  * this repository itself contains no model configurations

## Installation

```bash
# clone
git clone <repo-url> <local-dir>

# setup python venv
python3.10 -m venv venv
. venv/bin/activate
# (venv) should be activated now
pip install -U pip
pip install -r requirements.txt

# run tests
python -m pip install pytest-cov
python -m pytest --cov=lib tests/ -v
```

## Configuration

Since the overall workflow takes place in an isolated, local workspace, it's important to adjust
the configuration properly to this local context.

Configuration options are grouped into 6 main sections:

* `[workflow]` : basic configuration of local work/log directories.
   At least `[local_work_root]` and `[local_log_dir]` must be set accordingly.
* `[resource-monitoring]` : limits for local space and virtual memory usage
* `[mets]` : Blacklists for pages/logical sections, validation of metadata
* `[ocr]` : Container images and language model configuration mappings
   Most critical options are `[model_mapping]` for mapping of `mods:language` to a OCR configuration, `[ocrd_resources_volumes]` for mapping local resources into each OCR-container and for OCR-D `[ocrd_process_list]` to define the ocr-d-processing steps
* `[derivans]` : Derivans container image and configuration (optional)
* `[export]` : Export asset and it's contents (optional)
   If export data required, set options `[export_tmp]` and `[export_dst]` to valid directories 

See for example `resources/odem.local.example.ini`.

## Execution

### Local METS/MODS Mode

Assumes locally accessible directory containing metadata (METS/MODS-XML file) in `<data_dir>/mets.xml` and local ODEM clone at `<local_dir>` which contains adopted configurations under `resources/odem.record.local.ini`. 

```bash
cd <local_dir>

python cli_mets_local.py <data_dir>/mets.xml -c resources/odem.record.local.ini
```

The command additionally supports the `-e <int>` argument, which allows the script to run `<int>` instances synchronously. If not provided, the standard value is 1, leading to sequential execution.

#### Per-page languages and corpora

METS workflows resolve languages individually for each selected physical page:
image `FILEID` -> physical page -> `structLink` -> logical section `DMDID`
-> MODS `languageTerm[@type="code"]`. Physical-page language metadata takes
precedence; otherwise each linked logical branch inherits from its nearest
ancestor with language metadata. Multiple languages/branches are combined in
metadata order without duplicates; redundant links to ancestors do not dilute
more specific metadata. Unrelated MODS sections are not included.
Broken references raise metadata errors rather than selecting a default language.

An explicit `[ocr] language_model` override takes precedence over page metadata.
Pages without language metadata use the document language; if neither exists,
model preparation fails before OCR starts. Filename language suffixes remain
available for image-only directory workflows. Both OCR-D and direct Tesseract
receive page-specific models, validated against `model_mapping` and
`ocrd_resources_volumes` before workers start. With `model_combinable = False`,
only the first mapped model is used and a warning is logged.

For a corpus whose logical root is `TYPE="document"` or `TYPE="collection"`
without a primary `DMDID`, explicitly enable corpus intake:

```ini
[mets]
corpus_identifier = urn:example:my-corpus
```

This identifier names the whole corpus; no constituent book is treated as its
primary MODS record. Every selected corpus page must be linked to a print
structure (directly or through a logical ancestor). Configure the image file
group (`[mets] use_fgroup`, e.g. `GT-IMAGE`) and local image directory
(`[ocr] image_subpath`, default `MAX`) as usual. Missing selected corpus images
are downloaded automatically from their HTTP(S) `FLocat` URLs during local-image
binding. Existing local files are preserved, and blacklisted pages are not
downloaded. Failed HTTP requests, non-image responses, and corrupt images stop
processing explicitly; partial downloads are removed so a subsequent run can retry.

Corpus images are named from the physical page's `CONTENTIDS`, replacing `:`
with `+` and `/fragment/page=` with `-fp-`, and appending the source image
extension (default `.jpg`). For example, `urn:nbn:de:example/fragment/page=0229`
becomes `urn+nbn+de+example-fp-0229.jpg`. This naming takes precedence over
`use_file_id` in corpus mode. Missing, unsafe, or duplicate names raise metadata
errors. Existing basename-only files are not reused as page-identified images.
Remove document-wide identifier/export XPath
settings that do not uniquely identify the corpus. Corpus intake does not disable schema
or DDB validation; downstream PDF/export tooling must also support the corpus
structure, or those optional stages should be disabled.

Statistics retain the original page languages and selected models under
`page_languages` and `page_model_configs`. The aggregate `language_model` is a
string for a homogeneous run and a list of distinct configurations otherwise.

### Local Record Mode

Assumes locally accessible directory containing a CSV-file `<data_dir>/input.csv` and local ODEM clone at `<local_dir>` which contains adopted configurations under `resources/odem.record.local.ini`.

```bash
cd <local_dir>

python cli_record_local.py <data_dir>/input.csv -c resources/odem.record.local.ini
```

The command additionally supports the `-e <int>` argument, which allows the script to run `<int>` instances synchronously. If not provided, the standard value is 1, leading to sequential execution.

### Local Directory Mode

Assumes locally accessible directory containing only individual pages as .jpg files in `<data_dir>/inputdir` and local ODEM clone at `<local_dir>` which contains adopted configurations under `resources/odem.record.local.ini`.

```bash
cd <local_dir>

python cli_dir_local.py <data_dir>/inputdir -c resources/odem.record.local.ini
```

This command supports multiple arguments:
* the `-e <int>` (or `--executors <int>`) argument, which allows the script to run `<int>` instances synchronously. If not provided, the standard value is 1, leading to sequential execution.
* the `-l <string>` (or `--language_model <string>`) argument, which tells the script which languages to look for. `<string>` is an ISO 639-3 language code. If not provided, the process attempts to determine the language itself. If multiple languages are supported, the argument can be given an arbitrary amount of times with different values.
* the `-m <string>` (or `--model_mapping <string>`) argument, which assigns languages to models. `<string>` has the shape `<language_code>:<model>`. If multiple languages are supported, the argument can be given an arbitrary amount of times with different values.

Example with arguments:

```bash
cd <local_dir>

python cli_dir_local.py <data_dir>/inputdir -c resources/odem.record.local.ini -e 8 -l lat -m lat:lat.traineddata
```

### Trigger Server/Client Workflow by Crontab

Assumes record list (CSV-file) managed by `cli_record_server.py` module, which start simple HTTP-Service to serve data. No authentications means are included but an IP-whitelist.

ODEM client instances are executed periodically by cron jobs.
Assuming a local installation at `<local_dir>` and configurations located at `<local_dir>/resources/`:

Setup and start server process:

```bash
cd <local_dir>

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
