import luigi
import logging
import os
import workflow_common.common as wc
import json
from string import Template
from workflow_common.SubmitJob import SubmitJob
from process_s1_basket.SetupWorkDirs import SetupWorkDirs
from luigi.util import requires
from os.path import join

log = logging.getLogger('luigi-interface')

@requires(SetupWorkDirs)
class ProcessS1Basket(luigi.Task):
    paths = luigi.DictParameter()
    spatialConfig = luigi.DictParameter()
    testProcessing = luigi.BoolParameter(default = False)

    def run(self):
        setupWorkDirs = {}
        with self.input().open('r') as setupWorkDirsInfo:
            setupWorkDirs = json.load(setupWorkDirsInfo)

        basketDir = self.paths["basketDir"]

        with open(os.path.join(self.paths["templatesDir"], 's1_job_template.bsub'), 'r') as t:
            bsubTemplate = Template(t.read())

        tasks = []
        for productSetup in setupWorkDirs["productSetups"]:
            productName = wc.getProductNameFromPath(productSetup["inputPath"])

            bsubParams = {
                "jobWorkingDir" : productSetup["workspaceRoot"],
                "runScriptPath" : productSetup["runScriptPath"]
            }

            bsub = bsubTemplate.substitute(bsubParams)
            bsubScriptPath = os.path.join(productSetup["workspaceRoot"], "process_s1_ard.bsub")

            with open(bsubScriptPath, 'w') as bsubScriptFile:
                bsubScriptFile.write(bsub)

            task = SubmitJob(
                paths = self.paths,
                productName = productName,
                bsubScriptPath = bsubScriptPath,
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
        return wc.getLocalStateTarget(outputFolder, "ProcessS1Basket.json")