from __future__ import division
from collections import OrderedDict
import os
import csv
import sys

import re


class Extractor:
    def __init__(self, top_directory, target_directory):
        self.top_directory = top_directory
        self.target_directory = target_directory
        self.stagesRows = None
        self.stagesTasksList = []
        self.stagesCompletionList = []

    def buildstagesCompletionList(self,file_):
        f = open(file_, "r")
        stages = csv.DictReader(f)
        for item in stages:
            self.stagesCompletionList.append({
                "stageId": item["Stage ID"],
                "completionTime": item["Completion Time"]
            })

    def mergeList(self):
        targetList = []
        z = OrderedDict({})
        for item in self.stagesCompletionList:
            for sub_item in self.stagesTasksList:
                if item["stageId"] == sub_item["stageId"]:
                    z = sub_item.copy()
                    z["completionTime"] = item["completionTime"]
                    targetList.append(z)
        return targetList

    def produceFile(self, finalList):
        f = open(self.target_directory + '/summary.csv', 'w')
        headers = finalList[0].keys()
        writer = csv.writer(f, delimiter=',', lineterminator='\n')
        writer.writerow(headers)
        for item in finalList:
            writer.writerow(item.values())

    def runSingle(self, file_):
        tasks_file = file_+"/tasks_1.csv"
        stages_file = file_+"/stages_1.csv"
        f = open(tasks_file, "r")
        self.stagesRows = self.orderStages(csv.DictReader(f))
        f.close()
        self.buildstagesTasksList()
        self.buildstagesCompletionList(stages_file)
        self.produceFile(self.mergeList())

    def run(self):
        pattern = re.compile('application_([0-9]+)_([0-9]+)_dir')
        for f in os.listdir(self.top_directory):
            if pattern.match(f):
                self.runSingle(self.top_directory+"/"+f)




    """Checks the existence of the given file path"""

    def fileValidation(self, filename):
        if not (os.path.exists(filename)):
            print("The file " + filename + " does not exists")
            exit(-1)

    """Orders the stages dict by 'Stage Id'"""

    def orderStages(self, stages):
        return sorted(stages, key=lambda x: x["Stage ID"])

    def computeStagesTasksDetails(self, stageId, batch):
        shuffleBatch = []
        normalBatch = []
        bytesBatch = []
        for item in batch:
            normalBatch.append(item[0])
            shuffleBatch.append(item[1])
            bytesBatch.append(item[2])
        maxTask = max(normalBatch)
        maxShuffle = max(shuffleBatch)
        avgTask = reduce(lambda x, y: x + y, normalBatch) / len(normalBatch)
        avgShuffle = reduce(lambda x, y: x + y, shuffleBatch) / len(shuffleBatch)
        maxBytes = max(bytesBatch)
        avgBytes = reduce(lambda x, y: x + y, bytesBatch) / len(bytesBatch)
        targetDict = OrderedDict({})
        targetDict["stageId"] = stageId
        targetDict["nTask"] = len(batch)
        targetDict["maxTask"] = maxTask
        targetDict["avgTask"] = avgTask
        targetDict["SHmax"] = maxShuffle
        targetDict["SHavg"] = avgShuffle
        targetDict["Bmax"] = maxBytes
        targetDict["Bavg"] = avgBytes
        return targetDict

    def buildstagesTasksList(self):
        batch = []
        lastRow = None
        for row in self.stagesRows:
            if lastRow != None and lastRow["Stage ID"] != row["Stage ID"]:
                self.stagesTasksList.append(self.computeStagesTasksDetails(lastRow["Stage ID"], batch))
                batch = []
            if row["Shuffle Write Time"] == "NOVAL":
                batch.append([int(row["Executor Run Time"]), -1, -1])
            else:
                batch.append([int(row["Executor Run Time"]), int(row["Shuffle Write Time"]), int(row["Shuffle Bytes Written"])])

            lastRow = row
        self.stagesTasksList.append(self.computeStagesTasksDetails(lastRow["Stage ID"], batch))


def main():
    args = sys.argv
    if len(args) != 3:
        print("Required args: [TOP_DIRECTORY] [TARGET_DIRECTORY]")
        exit(-1)
    else:
        extractor = Extractor(str(args[1]), str(args[2]))
        extractor.run()


if __name__ == '__main__':
    main()
