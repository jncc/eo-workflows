import luigi
import logging
import json
import os
import datetime
import workflow_common.common as wc
from process_s1_daily.SetupDirectories import SetupDirectories
from process_s1_daily.GetNewProductsList import GetNewProductsList
from process_s1_daily.GetPreviousProductsList import GetPreviousProductsList
from process_s1_daily.GetPreviousProcessedProductsList import GetPreviousProcessedProductsList
from process_s1_daily.GetCurrentlyProcessingProductsList import GetCurrentlyProcessingProductsList
from luigi.util import inherits
from os.path import join
from functional import seq

log = logging.getLogger('luigi-interface')

@inherits(SetupDirectories)
@inherits(GetNewProductsList)
@inherits(GetPreviousProductsList)
@inherits(GetPreviousProcessedProductsList)
@inherits(GetCurrentlyProcessingProductsList)
class GetProductsToProcessList(luigi.Task):
    pathRoots = luigi.DictParameter()
    runDate = luigi.DateParameter()

    def requires(self):
        t = []
        t.append(self.clone(GetNewProductsList))
        t.append(self.clone(GetPreviousProductsList))
        t.append(self.clone(GetPreviousProcessedProductsList))
        t.append(self.clone(GetCurrentlyProcessingProductsList))
        return t

    def run(self):
        newProductsListInfo = []
        with self.input()[0].open("r") as newProductsList:
            newProductsListInfo = json.load(newProductsList)

        previousProductsListInfo = []
        with self.input()[1].open("r") as previousProductsList:
            previousProductsListInfo = json.load(previousProductsList)

        previousProcessedProductsListInfo = []
        with self.input()[2].open("r") as previousProcessedProductsList:
            previousProcessedProductsListInfo = json.load(previousProcessedProductsList)

        currentlyProcessingProductsListInfo = []
        with self.input()[3].open("r") as currentlyProcessingProductsList:
            currentlyProcessingProductsListInfo = json.load(currentlyProcessingProductsList)

        output = {
            "queryWindow": newProductsListInfo["queryWindow"],
            "products": []
        }

        productsList = previousProductsListInfo["products"]

        # Get new products that aren't already in the list
        newProducts = (seq(newProductsListInfo["products"])
            .filter_not(lambda x: seq(productsList)
                .where(lambda y: y["productId"] == x["productId"])
                .any())
            ).to_list()

        # Add new products
        productsList.extend(newProducts)

        # Remove processed products and currently processing products
        productsList = (seq(productsList)
            .filter_not(lambda x: seq(previousProcessedProductsListInfo["products"])
                .where(lambda y: y["productId"] == x["productId"])
                .any())
            .filter_not(lambda x: seq(currentlyProcessingProductsListInfo["products"])
                .where(lambda y: y["productId"] == x["productId"])
                .any())
            ).to_list()            

        output["products"] = productsList

        # Save list of products where (re)processing is needed
        with self.output().open('w') as out:
            out.write(wc.getFormattedJson(output))

    def output(self):
        return wc.getLocalDatedStateTarget(self.pathRoots["processingRootDir"], self.runDate, "ProductsToProcessList.json")