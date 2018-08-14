import luigi
import logging
import json
import os
import datetime
import workflow_common.common as wc
from os.path import join

log = logging.getLogger('luigi-interface')

class GetPreviousProductsList(luigi.ExternalTask):
    pathRoots = luigi.DictParameter()

    def output(self):
        outputFolder = self.pathRoots["processingDir"]
        return wc.getLocalStateTarget(outputFolder, 'ProductsList.json')