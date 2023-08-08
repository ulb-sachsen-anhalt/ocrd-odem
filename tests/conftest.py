# -*- coding: utf-8 -*-
"""Shared test functionalities"""

import os

import pathlib

from lib.ocrd3_odem import (
    get_config
)

# store path
PROJECT_ROOT_DIR = pathlib.Path(__file__).resolve().parents[1]
TEST_RES = pathlib.Path(__file__).parents[0] / 'resources'


def fixture_configuration():
    """
    pwc: productive working configuration 
         => dc (say: 'default configuration')
    """

    config = get_config()
    config.read(os.path.join(PROJECT_ROOT_DIR, 'resources', 'odem.ini'))
    config.set('global', 'data_fields', 'IDENTIFIER, SETSPEC, CREATED, INFO, STATE, STATE_TIME')
    config.set('mets', 'blacklist_file_groups', 'DEFAULT, THUMB, THUMBS, MIN, FULLTEXT, DOWNLOAD')
    config.set('mets', 'blacklist_logical_containers', 'cover_front,cover_back')
    config.set('mets', 'blacklist_physical_container_labels', 'Auftragszettel,Colorchecker,Leerseite,Rückdeckel,Deckblatt,Vorderdeckel')
    config.set('ocr', 'strip_tags', 'alto:Shape,alto:Processing,alto:Illustration,alto:GraphicalElement')
    config.set('ocr', 'ocrd_baseimage', 'ocrd/all:2022-08-15')
    return config
