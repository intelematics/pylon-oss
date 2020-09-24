import uuid
import datetime
import json

from ..interfaces.serializing import JsonSerializable
from ..utils import logging

class IngestionStep(JsonSerializable):

    def __init__(self):
        self.ingestionId       = ''
        self.parentIngestionId = None
        self.artifactName      = ''
        self.artifactVersion   = ''
        self.dimUTCTimestamp   = 0
        self.dimUTCDateId      = 0
        self.dimUTCHour        = 0
        self.metadata          = {}

    def populate(self, config: dict, parentIngestionId: str=None):

        self.ingestionId = str(uuid.uuid1())
        self.parentIngestionId = parentIngestionId

        if 'IMAGE_NAME' in config:
            self.artifactName = config['IMAGE_NAME']
        else:
            logging.warning(
                'IMAGE_NAME not defined in configuration, '
                f'IngestionStep {self.ingestionId} is missing key information'
            )
        if 'VERSION' in config:
            self.artifactVersion = config['VERSION']
        else:
            logging.warning(
                'VERSION not defined in configuration, '
                f'IngestionStep {self.ingestionId} is missing key information'
            )

        self.dimUTCTimestamp = int(datetime.datetime.utcnow().timestamp())
        self.dimUTCDateId = int(datetime.datetime.utcnow().strftime('%Y%m%d'))
        self.dimUTCHour = datetime.datetime.utcnow().hour

        if 'INGESTION_ATTRS' in config:
            self.updateMetadata(config['INGESTION_ATTRS'])

    def updateMetadata(self, newMetadata: dict):
        self.metadata.update(newMetadata)
