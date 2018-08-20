import luigi
import workflow_common.common as wc
import logging
import json
import os
import datetime
from process_s1_range_with_retries.GetS1ProductsByDateAndPolygon import GetS1ProductsByDateAndPolygon
from functional import seq
from luigi.util import requires
from os.path import join

log = logging.getLogger('luigi-interface')

@requires(GetS1ProductsByDateAndPolygon)
class GetNewProductsList(luigi.Task):
    pathRoots = luigi.DictParameter()
    runDate = luigi.DateParameter()

    def run(self):
        newProducts = {
            "queryWindow": {
                "start": str(self.runDate),
                "end": str(self.runDate)
            },
            "products": []
        }

        with self.input().open('r') as rawFile:
            rawData = json.load(rawFile)
            entries = seq(rawData["products"])

            newProducts["queryWindow"]["start"] = str(self.runDate)
            newProducts["queryWindow"]["end"] = str(self.runDate)

            if not entries.empty():
                newProducts["products"] = (
                    entries.map(lambda x: {
                        "productId": x["productId"],
                        "initialRunDate": x["initialRunDate"],
                        "filepath": x["filepath"],
                        "jobId": x["jobId"]
                    })).to_list()

        with self.output().open('w') as out:
            out.write(json.dumps(newProducts, indent=4))

    def output(self):
        outputFolder = os.path.join(self.pathRoots["processingRootDir"], os.path.join(str(self.runDate), "states"))
        return wc.getLocalStateTarget(outputFolder, "NewProductsList.json")