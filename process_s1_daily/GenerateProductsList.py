import luigi
import logging
import json
import os
import workflow_common.common as wc
import copy
from functional import seq
from os.path import join
from process_s1_daily.SubmitJobs import SubmitJobs
from process_s1_daily.GetCurrentlyProcessingJobsList import GetCurrentlyProcessingJobsList
from luigi.util import requires

log = logging.getLogger('luigi-interface')

@requires(SubmitJobs)
class GenerateProductsList(luigi.Task):
    pathRoots = luigi.DictParameter()
    runDate = luigi.DateParameter()
    maxScenes = luigi.IntParameter()

    def run(self):
        with self.input().open('r') as inputFile:
            submittedProductsInput = json.load(inputFile)

            productsList = copy.deepcopy(submittedProductsInput["submittedProducts"])
            productsList.extend(submittedProductsInput["currentlyProcessingProducts"])

            submittedJobIds = []
            submittedProductsCount = len(submittedProductsInput["submittedProducts"])
            totalProductCount = len(productsList)

            for submittedProduct in submittedProductsInput["submittedProducts"]:
                submittedJobIds.append(submittedProduct["jobId"])

            output = {
                "queryWindow": submittedProductsInput["queryWindow"],
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