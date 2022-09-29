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


class PageRankCli:
    def __init__(self, dir: str, PR_alpha = 0.9, iteTime = 10) -> None:
        # config
        if isinstance(iteTime, int):
            self.iteTime = iteTime
        else:
            self.iteTime = 10
        # random walk alpha
        if isinstance(PR_alpha, float):
            self.PR_alpha = PR_alpha
        else:
            self.PR_alpha = 0.9
        # variables
        self.dir = dir
        self.indexFiles = []
        self.pageFiles = []
        self.nameMap = {}
        self.nameCnt = 0
        # arange name into index
        self.curScore = []
        self.edge = {}
        self.edgeCnt = {}
        self.fin = []

    def prepareFiles(self):
        osTree = os.walk(self.dir)
        for path, _, file_list in osTree:
            for fileName in file_list:
                name = os.path.join(path, fileName)
                if(name.find("index") > 0):
                    self.indexFiles.append(name)
                else:
                    self.pageFiles.append(name)

        logging.info("into indexParser")
        for index in self.indexFiles:
            self.indexParser(index)
        logging.info("pass indexParser, nameCnt is {}".format(self.nameCnt))
        logging.info("into pageParser")
        for page in self.pageFiles:
            self.pageParser(page)
        # release mem
        self.indexFiles = []
        self.pageFiles = []

    def indexParser(self, path: str):
        logging.info("loading index {}".format(path))
        with open(path, encoding="utf-8") as f:
            buffer = f.readline()
            while(buffer != None and buffer != ""):
                start = buffer.find(":", buffer.find(":") + 1)
                name = buffer[start + 1: -1]
                # id start from 1
                self.nameCnt = self.nameCnt + 1
                # name和id映射
                self.nameMap[name] = self.nameCnt
                buffer = f.readline()

    def pageParser(self, path: str):
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
                cur = self.nameMap.get(title.text, 0)
                nex = self.nameMap.get(str(link.title), 0)
                # not indexed, ignore
                if cur == 0 or nex == 0:
                    continue
                if nex not in self.edge:
                    self.edge[nex] = []
                if cur not in self.edgeCnt:
                    self.edgeCnt[cur] = 0
                self.edgeCnt[cur] += 1
                self.edge[nex].append(cur)

    def iteCalc(self):
        logging.info("into calcPageRank")
        self.curScore = np.ones([self.nameCnt + 1], dtype=float)
        for _ in range(self.iteTime):
            self.calcPageRank()

    def calcPageRank(self):
        oldScore = self.curScore
        self.curScore = np.zeros([self.nameCnt + 1], dtype=float)
        # walk from edge
        pool = 0.0
        for i in range(self.nameCnt):
            # 无出边的点，吸收值
            if i not in self.edgeCnt:
                pool += oldScore[i]
            if i not in self.edge:
                continue
            for pre in self.edge[i]:
                self.curScore[i] += oldScore[pre] / self.edgeCnt[pre]
            self.curScore[i] *= self.PR_alpha

        # random walk
        for i in range(self.nameCnt):
            self.curScore[i] += (1.0 - self.PR_alpha) + pool * self.PR_alpha / self.nameCnt

    def collect(self):
        # release mem
        self.edge = {}
        self.edgeCnt = {}

        logging.info("collecting pagerank")
        for name in self.nameMap.keys():
            self.fin.append([name, self.curScore[self.nameMap[name]]])
        self.fin.sort(key=(lambda x: x[1]), reverse=True)

    '''
        return list as [[name, score]...]
    '''
    def run(self) -> list:
        self.prepareFiles()
        self.iteCalc()
        self.collect()
        return self.fin


cli = PageRankCli("../../data", iteTime = 20)

with open("pagerank.txt", "w") as f:
    for item in cli.run():
        line = "{}\t{:6f}\n".format(item[0], item[1])
        f.write(line)
