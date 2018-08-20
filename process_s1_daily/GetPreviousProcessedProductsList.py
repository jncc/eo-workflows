import luigi
import logging
import json
import os
import datetime
import workflow_common.common as wc
from os.path import join
from process_s1_daily.GetPreviousProductsList import GetPreviousProductsList
from process_s1_daily.GetNewProductsList import GetNewProductsList
from luigi.util import inherits
from functional import seq

log = logging.getLogger('luigi-interface')

@inherits(GetNewProductsList)
@inherits(GetPreviousProductsList)
class GetPreviousProcessedProductsList(luigi.Task):
    pathRoots = luigi.DictParameter()
    runDate = luigi.DateParameter()

    def requires(self):
        t = []
        t.append(self.clone(GetNewProductsList))
        t.append(self.clone(GetPreviousProductsList))

        return t

    def run(self):
        output = {
            "products": []
        }

        with self.input()[0].open('r') as newProductsFile, \
            self.input()[1].open('r') as previousProductsFile:

            newProducts = json.load(newProductsFile)
            previousProducts = json.load(previousProductsFile)

            productsToCheck = []
            productsToCheck.extend(newProducts["products"])
            productsToCheck.extend(previousProducts["products"])

            productsToCheck = (seq(productsToCheck)
                .distinct_by(lambda x: x["productId"])
                ).to_list()

            for product in productsToCheck:
                ardProductId = wc.getProductIdFromLocalSourceFile(product["productId"])
                generatedProductPath = self.getPathFromProductId(self.pathRoots["outputDir"], ardProductId)
                
                if os.path.exists(generatedProductPath):
                    output["products"].append(product)
            
        with self.output().open('w') as out:
            out.write(wc.getFormattedJson(output))

    def output(self):
        return wc.getLocalDatedStateTarget(self.pathRoots["processingRootDir"], self.runDate, "PreviousProcessedProductsList.json")

    def getPathFromProductId(self, root, productId):
        year = productId[4:8]
        month = productId[8:10]
        day = productId[10:12]

        return os.path.join(os.path.join(os.path.join(os.path.join(root, year), month), day), productId)