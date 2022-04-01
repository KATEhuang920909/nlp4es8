# -*- coding: utf-8 -*-
# @Time    : 2022/3/30 20:43
# @Author  : huangkai
# @File    : search_vector.py


import sys

sys.path.append('../')
sys.path.append('../utils')

from ir.config import Config
from utils.logger_config import base_logger
from utils.args import FLAGS


class Search(object):
    def __init__(self):
        base_logger.info("Searching ...")
        self.config = Config(FLAGS.env)
        self.es = self.config.es

    def searchAnswer(self, question_vector, ):
        # temp={"_knn_search": {
        #     "knn": {
        #         "field": "image_vector",
        #         "query_vector": [0.3, 0.1, 1.2],
        #         "k": 10,
        #         "num_candidates": 100
        #     },
        #     "_source": ["name", "file_type"]
        # }}
        body = {
            "query": {
                "knn": {
                    "query": question_vector,
                    "fields": ["embedding"],  # 在question字段中匹配查询
                    "type": "most_fields",
                    "k": 10,
                    "num_candidates": 100
                }},
            "_source": ["embedding", "document"]
        }

        # es相关配置

        index_name = self.config.index_name

        res = self.es.search(index=index_name, body=body, request_timeout=30)

        topn = res['hits']['hits']

        result = []
        for data in topn:
            result.append(
                (
                    data['_source']['document'],
                    data['_source']['embedding']
                )
            )
        return result


if __name__ == '__main__':
    import pandas as pd

    with open('../data/dev.query.txt', encoding='utf8') as f:
        data = f.readline()
        data = [k.strip().split("\t") for k in data]
    # embedding 数据
    with open("../data/query_embedding") as f:
        for line in f:
            sp_line = line.strip('\n').split("\t")
            index, embedding = sp_line
            embedding = embedding.split(',')
            embedding = [float(k) for k in embedding]
    # sample = data[-1]
    search = Search()
    #
    config = Config(FLAGS.env)
    # search = Search()
    result = search.searchAnswer(embedding, )
    print(embedding)
    # for data in result:
    #     print("sub_question_id:%s  primary_question_id:%s  " % (data[0], data[1]))
