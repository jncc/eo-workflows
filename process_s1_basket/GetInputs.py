import luigi
import logging
import os
import glob
import workflow_common.common as wc
import json
from os.path import join

log = logging.getLogger('luigi-interface')

class GetInputs(luigi.Task):
    paths = luigi.DictParameter()

    def run(self):
        basketDir = self.paths["basketDir"]

        inputFiles = []
        for inputFile in glob.glob(os.path.join(basketDir, "S1*")):
            inputFiles.append(inputFile)

        outputFile = {
            "basket": basketDir,
            "inputFiles": inputFiles
        }

        with self.output().open("w") as outFile:
            outFile.write(wc.getFormattedJson(outputFile))

    def output(self):
        outputFolder = self.paths["stateDir"]
        return wc.getLocalStateTarget(outputFolder, "GetInputs.json")