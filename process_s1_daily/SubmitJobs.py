import luigi
import logging
import json
import os
import workflow_common.common as wc
import shutil
import copy
from luigi.util import requires
from process_s1_daily.GetProductsToProcessList import GetProductsToProcessList
from workflow_common.RunJob import RunJob
from os.path import join

log = logging.getLogger('luigi-interface')

@requires(GetProductsToProcessList)
class SubmitJobs(luigi.Task):
    pathRoots = luigi.DictParameter()
    maxScenes = luigi.IntParameter()
    runDate = luigi.DateParameter()
    outputFilePattern = luigi.Parameter()
    testProcessing = luigi.BoolParameter(default = False)

    def run(self):
        with self.input().open('r') as stateFile:
            contents = json.load(stateFile)

        output = {
            "queryWindow": contents["queryWindow"],
            "maxScenes": self.maxScenes,
            "submittedProducts": [],
            "currentlyProcessingProducts": contents["currentlyProcessingProducts"]
        }

        productsToSubmit = contents["productsToProcess"]

        tasks = []
        for product in productsToSubmit:
            jobPathRoots = {
                "processingDir": os.path.join(os.path.join(self.pathRoots["processingRootDir"], str(self.runDate)), "processing"),
                "statesDir": os.path.join(os.path.join(self.pathRoots["processingRootDir"], str(self.runDate)), "states"),
                "demDir": self.pathRoots["demDir"],
                "outputDir": self.pathRoots["outputDir"],
                "singularityDir": self.pathRoots["singularityDir"],
                "singularityImgDir": self.pathRoots["singularityImgDir"]
            }

            task = RunJob(
                inputFile = product["filepath"],
                outputFilePattern = self.outputFilePattern,
                pathRoots = jobPathRoots,
                removeSourceFile = False,
                testProcessing = self.testProcessing
            )

            tasks.append(task)

            taskOutput = {
                "productId": product["productId"],
                "filepath": product["filepath"],
                "initialRunDate": product["initialRunDate"],
                "jobId": None
            }
            
            output["submittedProducts"].append(taskOutput)

        yield tasks

        for task in tasks:
            with task.output().open('r') as taskOutput:
                runJobOutput = json.load(taskOutput)
                
                for product in output["submittedProducts"]:
                    if runJobOutput["productId"] == product["productId"]:
                        product["jobId"] = runJobOutput["jobId"]
                        product["submitTime"] = runJobOutput["submitTime"]
                        break

        with self.output().open("w") as out:
            out.write(wc.getFormattedJson(output))

    def output(self):
        return wc.getLocalDatedStateTarget(self.pathRoots["processingRootDir"], self.runDate, "SubmittedJobs.json")