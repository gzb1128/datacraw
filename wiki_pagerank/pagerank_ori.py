# encoding: utf-8
import os
import numpy as np
import logging
import xml.etree.cElementTree as ET
import mwparserfromhell as wikiparser

__author__ = "gzb1128"
__email__ = "gzb1128@foxmail.com"


logging.basicConfig(level=logging.INFO,
                    format="%(asctime)s - %(levelname)s - %(message)s")
baseXml = "{http://www.mediawiki.org/xml/export-0.10/}"


def pageParser(path: str):
    logging.info("loading page {}".format(path))
    tree = ET.parse(path)
    root = tree.getroot()
    # 过滤siteinfo
    for page in root[1:]:
        title = page.find(baseXml + "title")
        revision = page.find(baseXml + "revision")
        text = revision.find(baseXml + "text")
        wikiCode = wikiparser.parse(text.text)
        filter = wikiCode.ifilter(
            matches=lambda x: isinstance(x, wikiparser.nodes.Wikilink))
        for link in filter:
            logging.debug(title.text + " -> " + str(link.title))
            cur = nameMap.get(title.text, 0)
            nex = nameMap.get(str(link.title), 0)
            # not indexed, ignore
            if cur == 0 or nex == 0:
                continue
            if nex not in edge:
                edge[nex] = []
            if cur not in edgeCnt:
                edgeCnt[cur] = 0
            edgeCnt[cur] += 1
            edge[nex].append(cur)


def indexParser(path: str):
    logging.info("loading index {}".format(path))
    global nameCnt
    cnt = 0
    with open(path, encoding="utf-8") as f:
        buffer = f.readline()
        while(buffer != None and buffer != ""):
            cnt += 1
            start = buffer.find(":", buffer.find(":") + 1)
            name = buffer[start + 1: -1]
            # id start from 1
            nameCnt = nameCnt + 1
            # name和id映射
            nameMap[name] = nameCnt
            buffer = f.readline()


def calcPageRank():
    oldScore = np.ones([nameCnt + 1], dtype=float)
    global curScore
    curScore = np.zeros([nameCnt + 1], dtype=float)
    # walk from edge
    for i in range(nameCnt):
        if i not in edge:
            continue
        for pre in edge[i]:
            curScore[i] += oldScore[pre] / edgeCnt[pre]
        curScore[i] *= PR_alpha

    # random walk
    for i in range(nameCnt):
        curScore[i] += (1.0 - PR_alpha) / nameCnt
    oldScore = curScore


# config
iteTime = 10
# random walk alpha
PR_alpha = 0.9
# variables
osTree = os.walk("../../data")
indexFiles = []
pageFiles = []
nameMap = {}
nameCnt = 0
# arange name into index
curScore = []
edge = {}
edgeCnt = {}

for path, dir_list, file_list in osTree:
    for fileName in file_list:
        name = os.path.join(path, fileName)
        if(name.find("index") > 0):
            indexFiles.append(name)
        else:
            pageFiles.append(name)

logging.info("into indexParser")
for index in indexFiles:
    indexParser(index)
logging.info("pass indexParser, nameCnt is {}".format(nameCnt))
logging.info("into pageParser")
for page in pageFiles:
    pageParser(page)

logging.info("into calcPageRank")
for time in range(iteTime):
    calcPageRank()

# release mem
edge = {}
edgeCnt = {}

collect = []
logging.info("collecting pagerank")
for name in nameMap.keys():
    collect.append([name, curScore[nameMap[name]]])
collect.sort(key=(lambda x: x[1]), reverse=True)

with open("pagerank.txt", "w") as f:
    for item in collect:
        line = "{}\t{:6f}\n".format(item[0], item[1])
        f.write(line)
