import luigi
import logging
import json
import os
import workflow_common.common as wc
from functional import seq
from os.path import join
from process_s1_range_with_retries.SubmitJobs import SubmitJobs
from process_s1_range_with_retries.GetCurrentlyProcessingJobsList import GetCurrentlyProcessingJobsList
from luigi.util import requires

log = logging.getLogger('luigi-interface')

@requires(SubmitJobs)
class GenerateProductsList(luigi.Task):
    pathRoots = luigi.DictParameter()
    runDate = luigi.DateParameter()

    def run(self):
        with self.input().open('r') as submittedProductsFile:
            submittedProducts = json.load(submittedProductsFile)

            productsList = submittedProducts["submittedProducts"]
            productsList.extend(submittedProducts["incompleteProducts"])

            productsList = (seq(productsList)
                .distinct_by(lambda x: x["productId"])
                ).to_list()

            output = {
                "products": productsList
            }

            with self.output().open('w') as out:
                out.write(json.dumps(output, indent=4))

    def output(self):
        outputFolder = os.path.join(self.pathRoots["processingRootDir"], os.path.join(str(self.runDate), "states"))
        return wc.getLocalStateTarget(outputFolder, "ProductsList.json")