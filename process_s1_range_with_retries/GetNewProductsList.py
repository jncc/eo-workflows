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
    startDate = luigi.DateParameter()
    endDate = luigi.DateParameter()

    def run(self):
        newProducts = {
            "queryWindow": {
                "start": str(self.startDate),
                "end": str(self.endDate)
            },
            "products": []
        }

        with self.input().open('r') as rawFile:
            rawData = json.load(rawFile)
            entries = seq(rawData["products"])

            newProducts["queryWindow"]["start"] = str(self.startDate)
            newProducts["queryWindow"]["end"] = str(self.endDate)

            if not entries.empty():
                newProducts["products"] = (
                    entries.map(lambda x: {
                        "productId": x["productId"],
                        "startDate": x["startDate"],
                        "endDate": x["endDate"],
                        "filepath": x["filepath"],
                        "jobId": x["jobId"]
                    })).to_list()

        with self.output().open('w') as out:
            out.write(json.dumps(newProducts))

    def output(self):
        outputFolder = self.pathRoots["processingDir"]
        return wc.getLocalStateTarget(outputFolder, "NewProductsList.json")