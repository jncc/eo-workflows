import luigi
import logging
import json
import os
import workflow_common.common as wc
from functional import seq
from os.path import join
from luigi.util import inherits
from process_s1_daily.GetActiveJobsList import GetActiveJobsList
from process_s1_daily.GetNewProductsList import GetNewProductsList
from process_s1_daily.GetPreviousProductsList import GetPreviousProductsList

log = logging.getLogger('luigi-interface')

@inherits(GetActiveJobsList)
@inherits(GetNewProductsList)
@inherits(GetPreviousProductsList)
class GetCurrentlyProcessingProductsList(luigi.Task):
    pathRoots = luigi.DictParameter()
    runDate = luigi.DateParameter()
    testProcessing = luigi.BoolParameter(default = False)
    
    def requires(self):
        t = []
        t.append(self.clone(GetActiveJobsList))
        t.append(self.clone(GetNewProductsList))
        t.append(self.clone(GetPreviousProductsList))
        return t

    def run(self):
        activeJobsListInfo = {}
        with self.input()[0].open("r") as getActiveJobsList:
            activeJobsListInfo = json.load(getActiveJobsList)

        newProductsListInfo = {}
        with self.input()[1].open("r") as getNewProductsList:
            newProductsListInfo = json.load(getNewProductsList)
            
        previousProductsListInfo = {}
        with self.input()[2].open("r") as getPreviousProductsList:
            previousProductsListInfo = json.load(getPreviousProductsList)

        productsList = previousProductsListInfo["products"]

        # Get new products that aren't already in the list
        newProducts = (seq(newProductsListInfo["products"])
            .filter_not(lambda x: seq(productsList)
                .where(lambda y: y["productId"] == x["productId"])
                .any())
            ).to_list()

        # Add new products
        productsList.extend(newProducts)

        # Get products from list which match the active job IDs
        currentlyProcessingProductsList = (seq(productsList)
            .filter(lambda x: seq(activeJobsListInfo["jobIds"])
                                .where(lambda y: y == x["jobId"])
                                .any())
            ).to_list()

        output = {
            "products": currentlyProcessingProductsList
        }

        with self.output().open("w") as out:
            out.write(wc.getFormattedJson(output))

    def output(self):
        return wc.getLocalDatedStateTarget(self.pathRoots["processingRootDir"], self.runDate, "CurrentlyProcessingProductsList.json")