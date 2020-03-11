import luigi
import logging
import os
import json
from os.path import join
from luigi import LocalTarget

log = logging.getLogger('luigi-interface')

class PrepareArdProcessing(luigi.ExternalTask):
    stateMount = luigi.Parameter()

    def output(self):
        outputFolder = self.stateMount
        return LocalTarget(os.path.join(outputFolder, "PrepareArdProcessing.json"))