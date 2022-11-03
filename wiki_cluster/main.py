# encoding utf-8

from filecmp import cmp
from pickletools import optimize
import nltk
from nltk.corpus import stopwords
from nltk.stem import PorterStemmer
import pandas
import logging
import math

class UnionFindSet:
    def __init__(self, titles: set) -> None:
        self.father = {}
        for title in titles:
            self.father[title] = title

    def find(self, x: str) -> str:
        if(self.father[x] == x):
            return x
        else:
            self.father[x] = self.find(self.father[x])
            return self.father[x]

    def union(self, i: str, j: str) -> None:
        fx = self.find(i)
        fy = self.find(j)
        if(fx != fy):
            self.father[fx] = fy

class Distance:
    '''L1 norm distance'''

    def calc(self, a: dict, b: dict) -> float:
        c = a.keys() | b.keys()
        dis = 0.0
        for feature in c:
            tmp = 0.0
            if(feature in a):
                tmp += a[feature]
            if(feature in b):
                tmp -= b[feature]
            if(tmp < 0):
                tmp = -tmp
            dis += tmp
        return dis

class DistanceCos:
    '''cos distance'''
    
    def calc(self, a: dict, b: dict) -> float:
        c = a.keys() & b.keys()
        dis = 0.0
        for feature in c:
            dis += a[feature] * b[feature]
        # 过滤全0
        if(dis < 1e-9 and dis > -(1e-9)):
            return 1.0
        tmp = 0.0
        for feature in a.keys():
            tmp += a[feature] * a[feature]
        dis = dis / math.sqrt(tmp)
        tmp = 0.0
        for feature in b.keys():
            tmp += b[feature] * b[feature]
        dis = dis / math.sqrt(tmp)
        return 1.0 - dis

class ClusterProcessor:
    def __init__(self, func: Distance, cut: float, p_cut: int) -> None:
        '''init clustering algorithm distance function'''
        self.func = func
        self.cut = cut # neighbor distance
        self.p_cut = p_cut # least expected neighbor number

    def analysis(self, page_vector: dict) -> list:
        '''cluster algorithm, return a map with cluster_id as key, list of title in this cluster as value'''
        logging.info("into Cluster analysis")
        dis = {}
        p = {}
        '''calc dis, local density p'''
        for title_i in page_vector:
            table_i = page_vector[title_i]
            p[title_i] = 0
            dis[title_i] = {}
            for title_j in page_vector:
                if(title_i == title_j):
                    continue
                table_j = page_vector[title_j]
                dij = self.func.calc(table_i, table_j)
                dis[title_i][title_j] = dij
                debug_file.write("{} and {} dis {}\n".format(title_i, title_j, dij))
                if(dij <= self.cut):
                    p[title_i] += 1
            if(p[title_i] >= self.p_cut):
                print("{} p is {}".format(title_i, p[title_i]))

        '''union cluster'''
        logging.info("into union cluster")
        unionFindSet = UnionFindSet(page_vector.keys())
        island_pages = set()
        for title_i in page_vector:
            dis_table = dis[title_i]
            p_i = p[title_i]
            island = True
            for title_j in page_vector:
                if(title_i == title_j):
                    continue
                if(p[title_j] >= self.p_cut and dis_table[title_j] <= self.cut):
                    if(p_i >= self.p_cut):
                        #合并簇
                        unionFindSet.union(title_i, title_j)
                    else:
                        #标记非离群点
                        island = False
            if(p_i < self.p_cut and island):
                island_pages.add(title_i)
        island_cnt = 0
        for title in island_pages:
            page_vector.pop(title)
            island_cnt += 1
        logging.info("clean island points, pop {} pages".format(island_cnt))
        
        cluster_name = {}
        logging.info("into cluster find")
        for title_i in page_vector:
            if(p[title_i] >= self.p_cut):
                cluster_name[title_i] = unionFindSet.find(title_i)
                continue
            dis_table = dis[title_i]
            cluster_dis = float(1e9)
            for title_j in page_vector:
                #weight point
                if(title_i == title_j or p[title_j] < self.p_cut):
                    continue
                if(dis_table[title_j] < cluster_dis):
                    cluster_dis = dis_table[title_j]
                    cluster_name[title_i] = unionFindSet.find(title_j)

        logging.info("into cluster collect")
        cluster_result = {}
        for title in page_vector:
            name = unionFindSet.find(cluster_name[title])
            if(name not in cluster_result):
                cluster_result[name] = []
            cluster_result[name].append(title)
        return cluster_result


class ClusterMain:
    def __init__(self, page_cnt: int, feature_num: int, df_min: int, df_max: int, processor: ClusterProcessor) -> None:
        nltk.download('punkt')
        nltk.download('stopwords')
        self.page_cnt = page_cnt
        self.feature_num = feature_num
        self.df_min = df_min
        self.df_max = df_max
        self.ps = PorterStemmer()
        self.stop_words = set(stopwords.words('english'))
        self.processor = processor

    def page_selector(self) -> list:
        '''select page from csv, return a list with [id, title, text] as item'''
        logging.info("into page selector")
        stream = pandas.read_csv(
            "../../data/wiki.csv", sep='\t', lineterminator='\n')
        select = []
        cnt = 0
        for page in stream.values:
            cnt += 1
            select.append([page[0], page[2], page[3]])
            if(cnt >= self.page_cnt):
                break
        logging.info("selected pages number is " + str(cnt))
        return select

    def feature_selector(self, pages: list) -> dict:
        '''feature selector by idf, return a map with feature as key, idf as value'''
        logging.info("into feature selector")
        # word frequency by page
        df = {}
        for item in pages:
            page = item[2]
            table = {}
            sentences = nltk.tokenize.sent_tokenize(page, language="english")
            for sentence in sentences:
                tokens = nltk.tokenize.word_tokenize(sentence)
                tokens = filter(lambda x: x not in self.stop_words, tokens)
                for item in tokens:
                    '''stem analysis'''
                    token = self.ps.stem(item)
                    if(token not in table):
                        table[token] = 1
                        if(token not in df):
                            df[token] = 0
                        df[token] += 1
        feature = []
        for key in df:
            '''select small df to make big idf'''
            if(df[key] >= self.df_min and df[key] <= self.df_max):
                feature.append([key, df[key]])
        feature.sort(key=lambda x: x[1])
        if(len(feature) < self.feature_num):
            self.feature_num = len(feature)

        '''collect'''
        ret = {}
        for index in range(0, self.feature_num):
            item = feature[index]
            ret[item[0]] = math.log2(self.page_cnt/item[1])
            # debug_file.write("{} {} {}\n".format(item[0], item[1], ret[item[0]]))
        logging.info("selected feature number is " + str(self.feature_num))
        return ret

    def vector_generator(self, pages: list) -> dict:
        '''generate page vector dict with title as key, vector table as item'''
        logging.info("into vector generator")
        # word frequency by page
        ret = {}
        for item in pages:
            page = item[2]
            ''' tf idf generator '''
            table = {}
            sentences = nltk.tokenize.sent_tokenize(page, language="english")
            for sentence in sentences:
                tokens = nltk.tokenize.word_tokenize(sentence)
                tokens = filter(lambda x: x not in self.stop_words, tokens)
                for token in tokens:
                    '''stem analysis'''
                    token = self.ps.stem(token)
                    if(token not in self.features):
                        continue
                    if(token not in table):
                        table[token] = 0
                    ''' tf '''
                    table[token] += 1
            debug_cnt = 0
            for token in table:
                ''' multi idf '''
                debug_cnt += 1
                # debug_file.write("item {} token {} df {} idf {}\n".format(item[1], token, table[token], table[token] * self.features[token]))
                table[token] *= self.features[token]
            ret[item[1]] = table
        return ret

    def run(self) -> list:
        '''main process stream, return analysis ans'''
        self.pages = self.page_selector()
        self.features = self.feature_selector(self.pages)
        self.page_vector = self.vector_generator(self.pages)
        return self.processor.analysis(self.page_vector)


debug_file = open("debug.txt","w+")
logging.basicConfig(level=logging.INFO,
                    format="%(asctime)s - %(levelname)s - %(message)s")
task = ClusterMain(page_cnt=2000,
                   feature_num=3000, df_min=20, df_max=500, processor=ClusterProcessor(DistanceCos(), cut=0.6, p_cut=6))

'''
    cut p_cut
    0.6 6
    0.6 5
'''

cluster_result = task.run()
with open("cluster_result.txt","w+") as f:
    cnt = 0
    for cluster_name in cluster_result:
        table = cluster_result[cluster_name]
        cnt += 1
        for title in table:
            f.write("cluster id\t" + str(cnt) + "\t" + title + '\n')
