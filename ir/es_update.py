# -*- coding: utf-8 -*-
# @Time    : 2020/11/9 16:54
# @Author  : huangkai21

"""
update es total sub question
"""
from ir.search import Search
from ir.index import Index
from ir.config import Config
from utils.args import FLAGS


class Es_Update():

    def update_total_sub_question(self, ):
        # 同步ES库
        self.index = Index()
        self.search = Search()
        self.config = Config(env=FLAGS.env)
        questions = self.index.data_convert()
        self.index.create_index(self.config)
        self.index.bulk_index(questions, bulk_size=10000, config=self.config)


if __name__ == '__main__':
    es_update = Es_Update()
    es_update.update_total_sub_question()
