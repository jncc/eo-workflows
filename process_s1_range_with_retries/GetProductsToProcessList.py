import luigi
import logging
import json
import os
import datetime
import workflow_common.common as wc
from process_s1_range_with_retries.GetNewProductsList import GetNewProductsList
from process_s1_range_with_retries.GetPreviousProductsList import GetPreviousProductsList
from process_s1_range_with_retries.GetPreviousProcessedProductsList import GetPreviousProcessedProductsList
from process_s1_range_with_retries.GetCurrentlyProcessingJobsList import GetCurrentlyProcessingJobsList
from luigi.util import inherits
from os.path import join
from functional import seq

log = logging.getLogger('luigi-interface')

@inherits(GetNewProductsList)
@inherits(GetPreviousProductsList)
@inherits(GetPreviousProcessedProductsList)
@inherits(GetCurrentlyProcessingJobsList)
class GetProductsToProcessList(luigi.Task):
    pathRoots = luigi.DictParameter()

    def requires(self):
        t = []
        t.append(self.clone(GetNewProductsList))
        t.append(self.clone(GetPreviousProductsList))
        t.append(self.clone(GetPreviousProcessedProductsList))
        t.append(self.clone(GetCurrentlyProcessingJobsList))

        return t

    def run(self):
        with self.input()[0].open('r') as newProductsFile, \
            self.input()[1].open('r') as previousProductsFile, \
            self.input()[2].open('r') as previousProcessedProductsFile, \
            self.input()[3].open('r') as currentlyProcessingJobsFile:

            newProducts = json.load(newProductsFile)
            previousProducts = json.load(previousProductsFile)
            previousProcessedProducts = json.load(previousProcessedProductsFile)
            currentlyProcessingJobs = json.load(currentlyProcessingJobsFile)

            productsToProcess = {
                "products": []
            }

            # Add new products
            productsToProcess["products"].extend(newProducts["products"])

            # Remove products that have been previously listed
            productsToProcess["products"] = (seq(productsToProcess["products"])
                .filter_not(lambda x: seq(previousProducts["products"])
                    .where(lambda y: y["productId"] == x["productId"])
                    .any())
                ).to_list()

            # Add previously listed products
            productsToProcess["products"].extend(previousProducts["products"])

            # Remove processed products
            productsToProcess["products"] = (seq(productsToProcess["products"])
                .filter_not(lambda x: seq(previousProcessedProducts["products"])
                                    .where(lambda y: y["productId"] == x["productId"])
                                    .any())
                ).to_list()

            # Save products list where processing has not started or is incomplete
            with open(os.path.join(self.pathRoots["processingDir"], "ProductsList.json"), 'w') as out:
                out.write(json.dumps(productsToProcess))

            # Remove currently processing products
            productsToProcess["products"] = (seq(productsToProcess["products"])
                .filter_not(lambda x: seq(currentlyProcessingJobs["jobIds"])
                                    .where(lambda y: y == x["jobId"])
                                    .any())
                ).to_list()

        # Save list of products where processing has not started or has failed
        with self.output().open('w') as out:
            out.write(json.dumps(productsToProcess))

    def output(self):
        outputFolder = self.pathRoots["processingDir"]
        return wc.getLocalStateTarget(outputFolder, "ProductsToProcessList.json")