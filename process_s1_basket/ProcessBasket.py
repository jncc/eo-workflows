import luigi
import logging
import os
import glob
import workflow_common.common as wc
import json
from workflow_common.RunJob import RunJob
from os.path import join

log = logging.getLogger('luigi-interface')

class ProcessBasket(luigi.Task):
    paths = luigi.DictParameter()
    spatialConfig = luigi.DictParameter()
    testProcessing = luigi.BoolParameter(default = False)

    def run(self):
        basketDir = self.paths["basketDir"]

        tasks = []
        for inputFile in glob.glob(os.path.join(basketDir, "S1*")):
            task = RunJob(
                inputPath = inputFile,
                paths = self.paths,
                spatialConfig = self.spatialConfig,
                removeSourceFile = True,
                testProcessing = self.testProcessing
            )

            tasks.append(task)
        yield tasks

        outputFile = {
            "basket": basketDir,
            "submittedProducts": []
        }

        for task in tasks:
            with task.output().open('r') as taskOutput:
                submittedProduct = json.load(taskOutput)
                outputFile["submittedProducts"].append(submittedProduct)

        with self.output().open("w") as outFile:
            outFile.write(wc.getFormattedJson(outputFile))

    def output(self):
        outputFolder = self.paths["stateDir"]
        return wc.getLocalStateTarget(outputFolder, "ProcessBasket.json")