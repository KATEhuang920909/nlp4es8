#!/usr/bin/env python
# -*- coding: utf-8 -*-


import sys

sys.path.append('../')
sys.path.append('../utils')

import warnings

warnings.filterwarnings("ignore")

from ir.config import Config
from elasticsearch import helpers
import pandas as pd
from utils.args import FLAGS
# from utils.data_helper import Data
from utils.logger_config import base_logger


class Index(object):
    def __init__(self):
        base_logger.info("Indexing ...")

    @staticmethod
    def data_convert():
        base_logger.info("convert sql data into single doc")

        questions = {}

        # 获取数据
        # dt = Data(FLAGS.env)
        # df = dt.read_all_Question()
        df = pd.read_csv('faq_sub_question.csv', sep=',', error_bad_lines=False,encoding='utf-8')
        for key, value in df.iterrows():
            if not (value[0] or value[1] or value[2].strip()):
                continue
            questions[key] = {'sub_question_id': int(value[0]), 'primary_question_id': int(value[1]),
                              'question': value[2]}

        return questions

    @staticmethod
    def create_index(config):
        base_logger.info("creating %s index ..." % config.index_name)
        request_body = {
            # 设置索引主分片数，每个主分片的副本数，默认分别是5和1
            "settings": {
                "number_of_shards": 5,
                "number_of_replicas": 1
            },
            "mappings": {

                "properties": {
                    "sub_question_id": {
                        "type": "long",
                        "index": "false"
                    },
                    "question": {
                        "type": "text",
                        "analyzer": "index_ansj",
                        "search_analyzer": "query_ansj",
                        "index": "true"
                    }
                }
            }
        }

        try:
            # 若存在index，先删除index
            config.es.indices.delete(index=config.index_name, ignore=[400, 404])
            res = config.es.indices.create(index=config.index_name, body=request_body)

            base_logger.info(res)
            base_logger.info("Indices are created successfully")
        except Exception as e:
            base_logger.warning(e)

    @staticmethod
    def bulk_index(questions, bulk_size, config):
        base_logger.info("Bulk index for question")
        count = 1
        actions = []
        for question_index, question in questions.items():
            action = {
                "_index": config.index_name,
                "_id": question_index,
                "_source": question
            }
            actions.append(action)
            count += 1

            if len(actions) % bulk_size == 0:
                helpers.bulk(config.es, actions)
                actions = []

        if len(actions) > 0:
            helpers.bulk(config.es, actions)
            base_logger.info("Bulk index: %s" % str(count))


if __name__ == '__main__':
    config = Config(FLAGS.env)
    index = Index()
    questions = index.data_convert()
    print(questions[0])
    index.create_index(config)
    index.bulk_index(questions, bulk_size=10000, config=config)
