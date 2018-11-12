import luigi
import logging
import json
import os
import workflow_common.common as wc
import copy
from functional import seq
from os.path import join
from process_s1_daily.SetupDirectories import SetupDirectories
from process_s1_daily.GetCurrentlyProcessingProductsList import GetCurrentlyProcessingProductsList
from process_s1_daily.GetProductsToProcessList import GetProductsToProcessList
from process_s1_daily.SubmitJobs import SubmitJobs
from luigi.util import inherits

log = logging.getLogger('luigi-interface')

@inherits(SetupDirectories)
@inherits(GetCurrentlyProcessingProductsList)
@inherits(GetProductsToProcessList)
@inherits(SubmitJobs)
class GenerateProductsList(luigi.Task):
    pathRoots = luigi.DictParameter()
    runDate = luigi.DateParameter()
    maxScenes = luigi.IntParameter()

    def requires(self):
        t = []
        t.append(self.clone(SetupDirectories))
        t.append(self.clone(GetCurrentlyProcessingProductsList))
        t.append(self.clone(GetProductsToProcessList))
        t.append(self.clone(SubmitJobs))
        return t

    def run(self):
        currentlyProcessingProductsListInfo = {}
        with self.input()[1].open('r') as currentlyProcessingProductsList:
            currentlyProcessingProductsListInfo = json.load(currentlyProcessingProductsList)

        submitJobsInfo = {}
        with self.input()[2].open('r') as submitJobs:
            submitJobsInfo = json.load(submitJobs)

        productsList = submitJobsInfo["products"]
        productsList.extend(currentlyProcessingProductsListInfo["products"])

        submittedJobIds = []
        submittedProductsCount = len(submitJobsInfo["products"])
        totalProductCount = len(productsList)

        for submittedProduct in submitJobsInfo["products"]:
            submittedJobIds.append(submittedProduct["jobId"])

        output = {
            "queryWindow": submitJobsInfo["queryWindow"],
            "maxScenes": str(self.maxScenes),
            "totalProductCount": totalProductCount,
            "submittedProductCount": submittedProductsCount,
            "submittedJobIds": submittedJobIds,
            "products": productsList
        }

        with self.output().open('w') as out:
            out.write(wc.getFormattedJson(output))

    def output(self):
        return wc.getLocalDatedStateTarget(self.pathRoots["processingRootDir"], self.runDate, "ProductsList.json")