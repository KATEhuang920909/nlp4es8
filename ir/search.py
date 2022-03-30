#!/usr/bin/env python
# -*- coding: utf-8 -*-



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

    def searchAnswer(self, question, config):

        body = {
            "query": {
                "multi_match": {
                    "query": question,
                    "fields": ["question"],  # 在question字段中匹配查询
                    "type": "most_fields",
                }
            }
        }

        # es相关配置

        index_name = self.config.index_name

        for i in range(10):
            try:
                res = self.es.search(index=index_name, body=body, request_timeout=30)

                topn = res['hits']['hits']

                result = []
                for data in topn:
                    result.append(
                        (
                            data['_source']['sub_question_id'],
                            data['_source']['primary_question_id'],
                            data['_source']['question']
                        )
                    )
                return result
            except Exception as e:
                base_logger.warning(e)
                base_logger.warning("Try again!")
                continue

        base_logger.info("ReadTimeOutError may not be covered!")
        return []


if __name__ == '__main__':
    from utils.data_helper import GetFeatures

    # config = Config(FLAGS.env)
    search = Search()
    gf = GetFeatures()
    # # top10问题检测
    # from tqdm import tqdm
    # import time
    # import pandas as pd
    # from sklearn.metrics.pairwise import cosine_similarity
    #
    # start = time.time()
    # df = pd.read_csv('../ir/sub_1026_1027.csv')
    # match_score = []  # 是否为新问题
    # sub_question = []  # 最匹配子问题
    # faq_question_id = []  # 最匹配主问题id
    # questions = df.question.values.tolist()  # [:5]
    # base_logger.info('question vector success')
    # feature_matrix = gf.get_bert_matrix(questions)
    # result = pd.DataFrame()
    # for i in tqdm(range(len(questions))):
    #     results = search.searchAnswer(questions[i], config)
    #     if len(results) > 0:
    #         result2vec = gf.get_bert_matrix([data[2] for data in results])
    #         score_list = []
    #         for vec in result2vec:
    #             score = cosine_similarity(feature_matrix[i].reshape(1, -1), vec.reshape(1, -1))[0][0]
    #             score_list.append(score)
    #         best_results = ["", "", 0]
    #         for j in range(len(results)):
    #             question = results[j][2]
    #             primary_id = results[j][1]
    #             score = score_list[j]
    #             if score > best_results[2]:
    #                 best_results[0] = question
    #                 best_results[1] = primary_id
    #                 best_results[2] = score
    #         sub_question.append(best_results[0])
    #         faq_question_id.append(best_results[1])
    #         match_score.append(best_results[2])
    #     else:
    #         sub_question.append("")
    #         faq_question_id.append("")
    #         match_score.append(0)
    #     #print(df.shape,len(faq_question_id))
    # result['sub_question_id'] = df['question_id']
    # result["primary_question_id"] = faq_question_id
    # result["question"] = df['question']
    # result['sub_question'] = sub_question
    # result["match_score"] = match_score
    # end = time.time()
    # print('所用时长：%s' % (end - start))
    #
    # result.to_csv('original_result.csv', index=False)
    from tqdm import tqdm

    config = Config(FLAGS.env)
    search = Search()
    while True:
        query = input()
        result = search.searchAnswer(query, config)
        for data in result:
            print("sub_question_id:%s  primary_question_id:%s  question:%s" % (data[0], data[1], data[2]))
