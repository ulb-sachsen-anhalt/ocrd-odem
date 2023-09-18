"""ODEM Core"""

import configparser


# ODEM States
MARK_OCR_OPEN = 'n.a.'
MARK_OCR_BUSY = 'ocr_busy'
MARK_OCR_FAIL = 'ocr_fail'
MARK_OCR_DONE = 'ocr_done'
MARK_OCR_SKIP = 'ocr_skip'
# important file groups
FILEGROUP_OCR = 'FULLTEXT'
FILEGROUP_IMG = 'MAX'
# statistic keys
STATS_KEY_LANGS = 'langs'
# default language for fallback
# when processing local images
DEFAULT_LANG = 'ger'
# recognition level for tesserocr
# must switch otherwise glyphs are reverted
# for each word
RTL_LANGUAGES = ['ara', 'fas', 'heb']


def get_config():
    """init plain configparser"""

    def _parse_dict(row):
        """
        Custom config converter to create a dictionary represented as string
        lambda s: {e[0]:e[1] for p in s.split(',') for e in zip(*p.strip().split(':'))}
        """
        a_dict = {}
        for pairs in row.split(','):
            pair = pairs.split(':')
            a_dict[pair[0].strip()] = pair[1].strip()
        return a_dict

    return configparser.ConfigParser(
        interpolation=configparser.ExtendedInterpolation(),
        converters={
            'list': lambda s: [e.strip() for e in s.split(',')],
            'dict': _parse_dict
        })


class ODEMException(Exception):
    """Mark custom ODEM Workflow Exceptions"""
