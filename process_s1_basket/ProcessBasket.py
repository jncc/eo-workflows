import luigi
import logging
import os
import glob
import workflow_common.common as wc
import json
from workflow_common.RunJob import RunJob
from process_s1_basket.SetupWorkDirs import SetupWorkDirs
from process_s1_basket.GetInputs import GetInputs
from luigi.util import requires
from os.path import join

log = logging.getLogger('luigi-interface')

@requires(SetupWorkDirs)
class ProcessBasket(luigi.Task):
    paths = luigi.DictParameter()
    spatialConfig = luigi.DictParameter()
    testProcessing = luigi.BoolParameter(default = False)

    def run(self):
        setupWorkDirs = {}
        with self.input().open('r') as setupWorkDirsInfo:
            setupWorkDirs = json.load(setupWorkDirsInfo)

        basketDir = self.paths["basketDir"]

        tasks = []
        for productSetup in setupWorkDirs["productSetups"]:
            task = RunJob(
                paths = self.paths,
                inputPath = productSetup["inputPath"],
                workspaceRoot = productSetup["workspaceRoot"],
                runScriptPath = productSetup["runScriptPath"],
                queueName = "short-serial",
                maxMemory = "18000",
                maxTime = "12:00",
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