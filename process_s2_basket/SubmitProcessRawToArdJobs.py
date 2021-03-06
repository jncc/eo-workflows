import luigi
import logging
import os
import workflow_common.common as wc
import json
from string import Template
from workflow_common.SubmitJob import SubmitJob
from process_s2_basket.GetInputSwaths import GetInputSwaths
from process_s2_basket.SetupWorkDirs import SetupWorkDirs
from process_s2_basket.SubmitPrepareArdProcessingJobs import SubmitPrepareArdProcessingJobs
from luigi.util import requires
from os.path import join

log = logging.getLogger('luigi-interface')

@requires(GetInputSwaths, SetupWorkDirs, SubmitPrepareArdProcessingJobs)
class SubmitProcessRawToArdJobs(luigi.Task):
    paths = luigi.DictParameter()
    testProcessing = luigi.BoolParameter(default = False)

    def run(self):
        getInputSwaths = {}
        with self.input()[0].open('r') as getInputSwathsInfo:
            getInputSwaths = json.load(getInputSwathsInfo)

        setupWorkDirs = {}
        with self.input()[1].open('r') as setupWorkDirsInfo:
            setupWorkDirs = json.load(setupWorkDirsInfo)

        prepareArdProcessingJobs = {}
        with self.input()[2].open('r') as submitPrepareArdProcessingJobsInfo:
            prepareArdProcessingJobs = json.load(submitPrepareArdProcessingJobsInfo)

        basketDir = self.paths["basketDir"]

        with open(os.path.join(self.paths["templatesDir"], 's2_mpi_ProcessRawToArd_job_template.sbatch'), 'r') as t:
            sbatchTemplate = Template(t.read())

        tasks = []
        for swathSetup in setupWorkDirs["swathSetups"]:
            productName = wc.getProductNameFromPath(swathSetup["swathDir"])

            for swath in getInputSwaths["swaths"]:
                if swath["swathDir"] == swathSetup["swathDir"]:
                    noOfGranules = len(swath["productPaths"])
                    break

            for submittedSwath in prepareArdProcessingJobs["submittedSwaths"]:
                if submittedSwath["productId"] == productName:
                    upstreamJobId = submittedSwath["jobId"]

            testProcessing = "--testProcessing" if self.testProcessing else ""

            sbatchParams = {
                "upstreamJobId": upstreamJobId,
                "nodes": noOfGranules + 1,
                "jobWorkingDir" : swathSetup["workspaceRoot"],
                "workingMount" : swathSetup["workingFileRoot"],
                "stateMount" : swathSetup["stateFileRoot"],
                "inputMount" : swathSetup["swathDir"],
                "staticMount" : self.paths["staticDir"],
                "singularityDir": self.paths["singularityDir"],
                "arcsiContainer": self.paths["arcsiMpiBaseImg"],
                "testProcessing": testProcessing
            }

            sbatch = sbatchTemplate.substitute(sbatchParams)
            sbatchScriptPath = os.path.join(swathSetup["workspaceRoot"], "submit_ProcessRawToArd_job_for_{}.sbatch".format(productName))

            with open(sbatchScriptPath, 'w') as sbatchScriptFile:
                sbatchScriptFile.write(sbatch)

            tasks.append(SubmitJob(
                paths = self.paths,
                productName = productName,
                sbatchScriptPath = sbatchScriptPath,
                testProcessing = self.testProcessing
            ))
        yield tasks

        outputFile = {
            "basket": basketDir,
            "submittedSwaths": []
        }

        for task in tasks:
            with task.output().open('r') as taskOutput:
                submittedSwath = json.load(taskOutput)
                outputFile["submittedSwaths"].append(submittedSwath)

        with self.output().open("w") as outFile:
            outFile.write(wc.getFormattedJson(outputFile))

    def output(self):
        outputFolder = self.paths["stateDir"]
        return wc.getLocalStateTarget(outputFolder, "SubmitProcessRawToArdJobs.json")