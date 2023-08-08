# ODEM OCR-D-Phase III Implementationsprojekt ULB: ODEM

[![pipeline status](https://git.itz.uni-halle.de/ulb/ulb-ocr-odem/badges/master/pipeline.svg)](https://git.itz.uni-halle.de/ulb/ulb-ocr-odem/badges/master/pipeline.svg)
[![coverage report](https://git.itz.uni-halle.de/ulb/ulb-ocr-odem/badges/master/coverage.svg)](https://git.itz.uni-halle.de/ulb/ulb-ocr-odem/commits/master)

Implementationsprojekt ULB Sachsen-Anhalt für OCR-D Phase III zur verteilten Erzeugung und Anreicherung von OCR-Daten.
Die Daten der Digitalisate werden über OAI-PMH bzw. HTTP geladen, lokal auf OCR-Clients bzw. Workern verarbeitet und
anschließend mit Volltext und neuen PDF-Daten (mit Textlayer) als SAF-Pakete für Share_it gespeichert.

## Workflow

* jede Printausgabe mit Images wird als OAI-Record über die OAI-PMH-Schnittstelle des Präsentationssystems geladen ()
* Metadaten des Records werden ausgewertet umd Images für das OCR auszuwählen und Sprachinformationen für OCR festzulegen
* je ausgewählter Seite wird ein kompletter OCR-D-Workflow durchgeführt (produktiv: 8-12 Seiten parallel je nach System)
  Bricht ein Container ab, fehlt für diese Seite OCR
* alle erzeugten OCR-Daten in die Metadaten des Records integriert und es wird eine neue PDF erstellt
* abschließend wird ein neues Datenpaket für das Share_it-System generiert (SAF), was die vorhanden Daten im Präsentationssystem ersetzt bzw. ergänzt

## Installation

### Requirements

* Docker CE 19.03.13 / Version `ocrd-all` - Containerimage laut Konfiguration
* Python 3.8+
* OpenJDK 11+
* maven 3.6+
* `zip` Befehl
* Git
* Access-Token für [interne library digiflow](https://git.itz.uni-halle.de/ulb/ulb-digiflow)

Systemcheck via `./check-host.sh`

## Development Setup

* git clone rekursiv (Submodule!)
* `./setup-host.sh <access-token-name> <access-token-value>`

### Test Dependencies

   python -m pip install -r ./tests/test_requirements.txt
   pytest -v

## Konfiguration (ODEM Projekt INI-Configuration)

Je nach Worker muss eine Konfiguration unter `./resources/odem.<SHORT_HOSTNAME>.ini` erstellt/angepasst werden.
Das aktuelle `odem.ini` Template ist zu finden unter `./resources/odem.ini`

## Projektmaschine Setup (OCR Worker / OAI Client)

### 1. Grundeinrichtung

OCR-User (Funktionsaccount), Gruppen, Verzeichnisse und Software einrichten mit `sudo ./scripts/setup-worker.sh <password-funktionsaccount>`
Passwort: `"Laufwerk T:"/IT-DD/ULB-IT-Database.kdbx` unter `Funktionsaccount OCR System`

### 2. CI/CD

Die Aktualisierung der Pipeline findet durch einen lokal vorhandenen Gitlab Runner statt.
Dieser muss auf jedem System neu eingerichtet und anschließend mit der gitlab-Konfiguration verbunden werden.

* auf dem Host/Worker `sudo gitlab-runner register` ausführen
* Registrierungsinfos: auf [Projektseite](https://git.itz.uni-halle.de/ulb/ulb-ocr-odem) unter `Settings > CI/CD > Runners > Specific runners`
* Executor: `shell`
* Benennung == `tag` in gitlab-Projekt-Konfiguration
* ca. 30s nach Abschluss muss in der Webansicht in GitLab der neu registrierte Runner gelistet werden
* füge Jobs für den neuen Runner in `.gitlab-ci.yml` hinzu

### 4. Modelldaten hinzufügen

Wenn auf dem System das OCR-Austauschlaufwerk gemountet ist, stehen die Modelle direkt zur Verfügung.
Wenn nicht, werden sie via `scp` nach `/home/ocr/odem-tessdata` kopiert:

Bsp.:

   ```bash
    scp -r /data/ocr/tesseract4/tessdata/* aqspw@ocr-worker07.bibliothek.uni-halle.de:/home/ocr/odem-tessdata
   ```

### 5. ODEM Konfiguration erstellen und anpassen

siehe [Konfiguration](#konfiguration-odem-projekt-ini-configuration)

### 6. Cronjob einrichten/aktivieren (Crontab OAI Client)

**ACHTUNG!!!** \
Zum editieren des Cronjiobs via `crontab -e` muss zuvor zum Nutzer `ocr` via `sudo su - ocr` gewechselt worden sein

Auf den Workern lauft der Aufruf von `cli_oai_client.py` Cronjob getriggert.

**ACHTUNG!!!** \
`*.ini`-Namen anpassen

```bash
#
# OCR-D ODEM
#
PYTHON_BIN=/home/ocr/ulb-ocr-odem/venv/bin/python3
PROJECT=/home/ocr/ulb-ocr-odem
OAI_RECORDS=oai-records-opendata-vd18-odem

*/5  08-23  * * *  ${PYTHON_BIN} ${PROJECT}/cli_oai_client.py ${OAI_RECORDS} -c ${PROJECT}/resources/odem.ocr-workerXX.ini -l
```

## Zusatz Workflows - Scripte

### Setzte STATE (zurück) von OAI-Records in der CSV

Beispielabruf - dry-run mit Verbosität.  
Setze State des/der Eintrag/äge mit folgen Kriterien auf `ocr_busy`:
IDENTIFIER entspricht `1981185920/48087` und STATE entspricht `ocr_fail` und INFO enthält '.jpg'

```bash
source venv/bin/activate
python ./scripts/oai_record_set_state.py ./temp/reset_me.csv -S ocr_busy -DV -s ocr_fail -t .jpg -i 1981185920/48087
```

### Statistik Verdächtige Non-OCR-ed #9880

Siehe Ticket #9880. Statistik über Verhältnis OCR-able zu OCR-ed Seiten, der bisher abgeschlossenen Vorgänge

```bash
source venv/bin/activate
python ./scripts/feat9880_diff_detection.py <path-to-oai-records-csv>
# i.e. python ./scripts/feat9880_diff_detection.py ./resources/oai-records-opendata-vd18-odem.csv
```

### OCR Statistics

Statistiken über den Fortschritt des OCR-ing aller ~40k Vorgänge

```bash
source venv/bin/activate
python ./scripts/ulb_statistics.py <path-to-oai-records-csv>
# i.e. python ./scripts/ulb_statistics.py ./resources/oai-records-opendata-vd18-odem.csv
```

### Analyse

Testdaten VD18 (Monographien + F-Stufen): 40844 OAI-Records

Beispielabruf METS/MODS:

`https://digitale.bibliothek.uni-halle.de/vd18/oai/?verb=GetRecord&metadataPrefix=mets&identifier=<VL ID>`

### oai2img Skript

Verwendung:

```bash
cd scripts

python3.6 oai2img.py resources/oai-urn-vd18-pages.vlids
```

Skript erzeugt zunächst eine Tupelliste mit `len(elem)==3` mit OAI-URN, physID und Image-URL.
Das Ergebnis wird in `result.tsv` geschrieben.

Aktuell wird von jedem Datensatz mindestens 1, sonst `len(pages) // 100` Bildadressen ausgegeben, was zu mehr als 42k
Dateien führen würde.

### create_lists.py

Das Skript iteriert über die Liste `resources/oai-urn-vd18-pages.vlids`, holt OAI-IDs, lädt die entsprechende MODS-Datei
und erzeugt drei Dateien:

1. `bibinfo.tsv`: Liste der OAI-Datensätze mit: OAI-ID, Erscheinungsort, Erscheinungsjahr, Sprache(n), Zahl physischer
   Seiten
2. `result.tsv`: Liste aller physischer Seiten aus den logischen `section`-Abschnitten: OAI-ID, physID der phys. Seite,
   URL der maximalen Bilddatei
3. `logs.txt`: Log-File mit Fehlermeldungen (in erster Version dummerweise ohne ID, es ist also für eine Nachbearbeitung
   der fehlerhaften Abrufe ein weiteres kleines Programm zu schreiben (-> `2ndrun.py`)
