import luigi
import logging
import json
import os
import workflow_common.common as wc
import shutil
import copy
from luigi.util import inherits
from process_s1_daily.SetupDirectories import SetupDirectories
from process_s1_daily.GetProductsToProcessList import GetProductsToProcessList
from workflow_common.RunJob import RunJob
from os.path import join

log = logging.getLogger('luigi-interface')

@inherits(SetupDirectories)
@inherits(GetProductsToProcessList)
class SubmitJobs(luigi.Task):
    pathRoots = luigi.DictParameter()
    maxScenes = luigi.IntParameter()
    runDate = luigi.DateParameter()
    demFilename = luigi.Parameter()
    testProcessing = luigi.BoolParameter(default = False)

    def requires(self):
        t = []
        t.append(self.clone(SetupDirectories))
        t.append(self.clone(GetProductsToProcessList))
        return t

    def run(self):
        productsToProcessListInfo = {}
        with self.input()[1].open("r") as productsToProcessList:
            productsToProcessListInfo = json.load(productsToProcessList)

        productsToSubmit = productsToProcessListInfo["products"]
        output = {
            "queryWindow": productsToProcessListInfo["queryWindow"],
            "maxScenes": self.maxScenes,
            "products": []
        }

        tasks = []
        for product in productsToSubmit:
            jobPathRoots = {
                "processingDir": os.path.join(os.path.join(self.pathRoots["processingRootDir"], str(self.runDate)), "products"),
                "stateDir": os.path.join(os.path.join(self.pathRoots["processingRootDir"], str(self.runDate)), "state"),
                "staticDir": self.pathRoots["staticDir"],
                "outputDir": self.pathRoots["outputDir"],
                "singularityDir": self.pathRoots["singularityDir"],
                "singularityImgDir": self.pathRoots["singularityImgDir"]
            }

            task = RunJob(
                inputFile = product["filepath"],
                demFilename = self.demFilename,
                pathRoots = jobPathRoots,
                removeSourceFile = False,
                testProcessing = self.testProcessing
            )
            tasks.append(task)

            submittedProductOutput = {
                "productId": product["productId"],
                "filepath": product["filepath"],
                "initialRunDate": product["initialRunDate"],
                "jobId": None
            }
            output["products"].append(submittedProductOutput)
        yield tasks

        output["products"] = self.addTaskOutputsToOutput(tasks, output["products"])

        with self.output().open("w") as out:
            out.write(wc.getFormattedJson(output))

    def addTaskOutputsToOutput(self, tasks, submittedProductsOutput):
        for task in tasks:
            taskOutputInfo = {}
            with task.output().open("r") as taskOutput:
                taskOutputInfo = json.load(taskOutput)
                
            for product in submittedProductsOutput:
                if taskOutputInfo["productId"] == product["productId"]:
                    product["jobId"] = taskOutputInfo["jobId"]
                    product["submitTime"] = taskOutputInfo["submitTime"]
                    break

        return submittedProductsOutput


    def output(self):
        return wc.getLocalDatedStateTarget(self.pathRoots["processingRootDir"], self.runDate, "SubmittedJobs.json")