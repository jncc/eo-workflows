import luigi
import logging
import os
import glob
import workflow_common.common as wc
import json
from process_s1_basket.GetInputs import GetInputs
from process_s1_basket.SetupWorkDir import SetupWorkDir
from luigi.util import requires
from os.path import join

log = logging.getLogger('luigi-interface')

@requires(GetInputs)
class SetupWorkDirs(luigi.Task):
    paths = luigi.DictParameter()
    spatialConfig = luigi.DictParameter()

    def run(self):
        getInputs = {}
        with self.input().open('r') as getInputsInfo:
            getInputs = json.load(getInputsInfo)

        tasks = []
        for inputFile in getInputs["inputFiles"]:
            task = SetupWorkDir(
                inputPath = inputFile,
                paths = self.paths,
                spatialConfig = self.spatialConfig,
                removeSourceFile = True
            )

            tasks.append(task)
        yield tasks

        outputFile = {
            "productSetups": []
        }

        for task in tasks:
            with task.output().open('r') as taskOutput:
                productSetup = json.load(taskOutput)
                outputFile["productSetups"].append(productSetup)

        with self.output().open("w") as outFile:
            outFile.write(wc.getFormattedJson(outputFile))

    def output(self):
        outputFolder = self.paths["stateDir"]
        return wc.getLocalStateTarget(outputFolder, "SetupWorkDirs.json")