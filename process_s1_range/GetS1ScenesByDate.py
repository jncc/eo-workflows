import luigi
import logging
import json
import os
import datetime
import process_s1_basket.common as wc
import workflow_common.polygon as polygon
import feedparser
import requests
import copy
import urllib
from math import ceil
from datetime import timedelta, datetime
from dateutil import parser as dateparser
from os.path import join
from urllib.parse import urlencode

log = logging.getLogger('luigi-interface')

CEDA_OPENSEARCH_URL = 'http://opensearch.ceda.ac.uk/opensearch/json?'
class GetS1ScenesByDate(luigi.Task):
    pathRoots = luigi.DictParameter()
    startDate = luigi.DateParameter()
    endDate = luigi.DateParameter()
    maxScenes = luigi.IntParameter()

    def getBaseQuery(self):

        query = {
            "mission": "sentinel-1",
            "productType": "GRD",
            "geometry": polygon.getSearchPolygon(),
            "startDate": self.startDate,
            "endDate": self.endDate
        }

        return query

    def queryCEDA(self, query):
        qs = urlencode(query)
        url = CEDA_OPENSEARCH_URL + qs

        log.info("URL: " + url)

        result = requests.get(url)
        jsonResult = result.json()

        if jsonResult == None or jsonResult["totalResults"] == 0:
            log.exception("Read CEDA feed failed")
            raise TypeError("No result returned")

        return jsonResult

    def run(self):
        output = {
            'rawList': ["/home/vagrant/workspace/S1_processing/s1_raw/S1A_IW_GRDH_1SDV_20171101T180600_20171101T180625_019075_020438_0920.zip",
            "/home/vagrant/workspace/S1_processing/s1_raw/S1A_IW_GRDH_1SDV_20171105T173312_20171105T173337_019133_0205FC_60B5.zip"]
        }

        query = self.getBaseQuery()
        result = self.queryCEDA(query)

        totalResults = int(result["totalResults"])
        # if total records exceeds maxScenes shrink the window
        if totalResults > self.maxScenes:
            msg = "Found " + str(totalResults) + " results, however cannot process more than " + str(self.maxScenes) + " results at a time"
            log.exception(msg)
            raise Exception(msg)

        log.info("total results %d", totalResults)
        filePaths = list()
        for row in result["rows"]:
            filePaths.append(os.path.join(row["file"]["directory"], row["file"]["data_file"]))
        
        output = {
            "rawList": filePaths
        }

        with self.output().open('w') as out:
            out.write(json.dumps(output))

    def output(self):
        outputFolder = self.pathRoots["processingDir"]
        return wc.getLocalStateTarget(outputFolder, "getS1ScenesByDate.json")