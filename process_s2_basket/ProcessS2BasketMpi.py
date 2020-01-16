import luigi
import logging
import os
import workflow_common.common as wc
import json
import pathlib
from string import Template
from workflow_common.SubmitJob import SubmitJob
from process_s2_basket.SetupWorkDirs import SetupWorkDirs
from process_s2_basket.SubmitProcessRawToArdJobs import SubmitProcessRawToArdJobs
from luigi.util import requires
from os.path import join

log = logging.getLogger('luigi-interface')

@requires(SetupWorkDirs, SubmitProcessRawToArdJobs)
class ProcessS2BasketMpi(luigi.Task):
    paths = luigi.DictParameter()
    demFilename = luigi.Parameter()
    outWktFilename = luigi.Parameter()
    projAbbv = luigi.Parameter()
    arcsiReprojection = luigi.BoolParameter()
    metadataConfigFile = luigi.Parameter()
    metadataTemplate = luigi.Parameter()
    maxCogProcesses = luigi.IntParameter()
    testProcessing = luigi.BoolParameter(default = False)

    def run(self):
        setupWorkDirs = {}
        with self.input()[0].open('r') as setupWorkDirsInfo:
            setupWorkDirs = json.load(setupWorkDirsInfo)

        prepareArdProcessingJobs = {}
        with self.input()[1].open('r') as submitProcessRawToArdJobsInfo:
            prepareArdProcessingJobs = json.load(submitProcessRawToArdJobsInfo)

        basketDir = self.paths["basketDir"]

        with open(os.path.join(pathlib.Path(__file__).parent, 'templates/s2_mpi_FinaliseOutputs_job_template.bsub'), 'r') as t:
            bsubTemplate = Template(t.read())

        tasks = []
        for swathSetup in setupWorkDirs["swathSetups"]:
            productName = wc.getProductNameFromPath(swathSetup["swathDir"])

            for submittedSwath in prepareArdProcessingJobs["submittedSwaths"]:
                if submittedSwath["productId"] == productName:
                    upstreamJobId = submittedSwath["jobId"]

            arcsiReprojection = "--outWkt={} --projAbbv={}".format(self.outWktFilename, self.projAbbv) if self.arcsiReprojection else ""

            bsubParams = {
                "upstreamJobId": upstreamJobId,
                "jobWorkingDir" : swathSetup["workspaceRoot"],
                "workingMount" : swathSetup["workingFileRoot"],
                "stateMount" : swathSetup["stateFileRoot"],
                "inputMount" : swathSetup["swathDir"],
                "staticMount" : self.paths["staticDir"],
                "outputMount" : self.paths["outputDir"],
                "s2ArdContainer": self.paths["singularityImgPath"],
                "arcsiReprojection": arcsiReprojection,
                "dem": self.demFilename,
                "metadataConfigFile": self.metadataConfigFile,
                "metadataTemplate": self.metadataTemplate,
                "maxCogProcesses": self.maxCogProcesses
            }

            bsub = bsubTemplate.substitute(bsubParams)
            bsubScriptPath = os.path.join(swathSetup["workspaceRoot"], "submit_FinaliseOutputs_job_for_{}.bsub".format(productName))

            with open(bsubScriptPath, 'w') as bsubScriptFile:
                bsubScriptFile.write(bsub)

            tasks.append(SubmitJob(
                paths = self.paths,
                productName = productName,
                bsubScriptPath = bsubScriptPath,
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
        return wc.getLocalStateTarget(outputFolder, "ProcessS2BasketMpi.json")