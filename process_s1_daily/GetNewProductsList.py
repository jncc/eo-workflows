import luigi
import workflow_common.common as wc
import logging
import json
import os
import datetime
from process_s1_daily.GetS1ProductsByDateAndPolygon import GetS1ProductsByDateAndPolygon
from functional import seq
from luigi.util import requires
from os.path import join

log = logging.getLogger('luigi-interface')

@requires(GetS1ProductsByDateAndPolygon)
class GetNewProductsList(luigi.Task):
    pathRoots = luigi.DictParameter()
    runDate = luigi.DateParameter()

    def run(self):
        output = {
            "queryWindow": {
                "start": str(self.runDate),
                "end": str(self.runDate)
            },
            "products": []
        }

        with self.input().open('r') as rawFile:
            rawData = json.load(rawFile)
            entries = seq(rawData["products"])

            output["queryWindow"]["start"] = str(self.runDate)
            output["queryWindow"]["end"] = str(self.runDate)

            if not entries.empty():
                output["products"] = (
                    entries.map(lambda x: {
                        "productId": x["productId"],
                        "initialRunDate": x["initialRunDate"],
                        "filepath": x["filepath"],
                        "jobId": x["jobId"]
                    })).to_list()

        with self.output().open('w') as out:
            out.write(wc.getFormattedJson(output))

    def output(self):
        return wc.getLocalDatedStateTarget(self.pathRoots["processingRootDir"], self.runDate, "NewProductsList.json")