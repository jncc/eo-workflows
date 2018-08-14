import luigi
import logging
import json
import os
import datetime
import workflow_common.common as wc
from os.path import join
from process_s1_range_with_retries.GetPreviousProductsList import GetPreviousProductsList
from luigi.util import inherits

log = logging.getLogger('luigi-interface')

@inherits(GetPreviousProductsList)
class GetPreviousProcessedProductsList(luigi.Task):
    pathRoots = luigi.DictParameter()

    def requires(self):
        t = []
        t.append(self.clone(GetPreviousProductsList))

        return t

    def run(self):
        previousProcessedProducts = {
            "products": []
        }

        with self.input()[0].open('r') as previousProductsFile:
            previousProducts = json.load(previousProductsFile)

            for product in previousProducts["products"]:
                ardProductId = wc.getProductIdFromLocalSourceFile(product["productId"])
                generatedProductPath = self.getPathFromProductId(self.pathRoots["outputDir"], ardProductId)
                
                if os.path.exists(generatedProductPath):
                    previousProcessedProducts["products"].append(product)
            
        with self.output().open('w') as out:
            out.write(json.dumps(previousProcessedProducts))

    def output(self):
        outputFolder = self.pathRoots["processingDir"]
        return wc.getLocalStateTarget(outputFolder, "PreviousProcessedProductsList.json")

    def getPathFromProductId(self, root, productId):
        year = productId[4:8]
        month = productId[8:10]
        day = productId[10:12]

        return os.path.join(os.path.join(os.path.join(os.path.join(root, year), month), day), productId)