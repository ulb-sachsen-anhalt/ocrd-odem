###
# From here on, custom configuration begins.
MODEL_CONFIG = Fraktur
# possible ocr levels: line, word, glyph
TESSERACT_LEVEL = word

info:
	@echo "OCR-D Workflow ODEM (ULB Sachsen-Anhalt) using tesseract model config $(TESSERACT_CONFIG)"

START = MAX

BIN = OCR-D-BINPAGE
$(BIN): $(START)
$(BIN): TOOL = ocrd-olena-binarize
$(BIN): LOGLEVEL = DEBUG
$(BIN): PARAMS = "impl": "sauvola-ms-split", "dpi": 300

CROP = OCR-D-SEG-PAGE-ANYOCR
$(CROP): $(BIN)
$(CROP): TOOL = ocrd-anybaseocr-crop
$(CROP): PARAMS = "dpi": 300

DEN = OCR-D-DENOISE-OCROPY
$(DEN): $(CROP)
$(DEN): TOOL = ocrd-cis-ocropy-denoise
$(DEN): PARAMS = "level-of-operation": "page", "noise_maxsize": 3.0, "dpi": 300

#DESK = OCR-D-DESKEW-OCROPY
#$(DESK): $(DEN)
#$(DESK): TOOL = ocrd-cis-ocropy-deskew
#$(DESK): PARAMS = "level-of-operation": "page", "maxskew": 5

BLOCK = OCR-D-SEG-BLOCK-TESSERACT
$(BLOCK): $(DEN)
$(BLOCK): TOOL = ocrd-tesserocr-segment-region
$(BLOCK): PARAMS = "padding": 5, "find_tables": false, "dpi": 300

PLAUSIBLE = OCR-D-SEGMENT-REPAIR
$(PLAUSIBLE): $(BLOCK)
$(PLAUSIBLE): TOOL = ocrd-segment-repair
$(PLAUSIBLE): PARAMS = "plausibilize": true, "plausibilize_merge_min_overlap": 0.7

CLIP = OCR-D-CLIP
$(CLIP): $(PLAUSIBLE)
$(CLIP): TOOL = ocrd-cis-ocropy-clip

LINE = OCR-D-SEGMENT-OCROPY
$(LINE): $(CLIP)
$(LINE): TOOL = ocrd-cis-ocropy-segment
$(LINE): PARAMS = "spread": 2.4, "dpi": 300

DEW = OCR-D-DEWARP
$(DEW): $(LINE)
$(DEW): TOOL = ocrd-cis-ocropy-dewarp

OCR = PAGE
$(OCR): $(DEW)
$(OCR): PARAMS = "model" : "$(MODEL_CONFIG)"
$(OCR): TOOL = ocrd-kraken-recognize

sanitize: $(OCR)
	ocrd workspace prune-files

.PHONY: sanitize
.DEFAULT_GOAL = sanitize

# Down here, custom configuration ends.
###

include Makefile
