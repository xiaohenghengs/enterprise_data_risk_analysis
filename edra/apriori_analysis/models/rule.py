from functools import partial
from multiprocessing.dummy import Pool as ThreadPool

from edra.apriori_analysis.models import sqlCreateRules
from utils.database_operate import DataBaseOperate
from utils.utils import listOfGroups


class Rule:
    def __init__(self, item_id=None, rule=None, conclusion=None, number_of_items_occurrences=None,
                 degree_of_confidence=None, coverage=None, promotion_degree=None, utilization=None):
        self.__item_id = item_id
        self.__rule = rule
        self.__conclusion = conclusion
        self.__number_of_items_occurrences = number_of_items_occurrences
        self.__degree_of_confidence = degree_of_confidence
        self.__coverage = coverage
        self.__promotion_degree = promotion_degree
        self.__utilization = utilization
        self.__list = list()

    @property
    def item_id(self):
        return self.__item_id

    @item_id.setter
    def item_id(self, item_id):
        self.__item_id = item_id

    @property
    def rule(self):
        return self.__rule

    @rule.setter
    def rule(self, rule):
        self.__rule = rule

    @property
    def conclusion(self):
        return self.__conclusion

    @conclusion.setter
    def conclusion(self, conclusion):
        self.__conclusion = conclusion

    @property
    def number_of_items_occurrences(self):
        return self.__number_of_items_occurrences

    @number_of_items_occurrences.setter
    def number_of_items_occurrences(self, number_of_items_occurrences):
        self.__number_of_items_occurrences = number_of_items_occurrences

    @property
    def degree_of_confidence(self):
        return self.__degree_of_confidence

    @degree_of_confidence.setter
    def degree_of_confidence(self, degree_of_confidence):
        self.__degree_of_confidence = degree_of_confidence

    @property
    def coverage(self):
        return self.__coverage

    @coverage.setter
    def coverage(self, coverage):
        self.__coverage = coverage

    @property
    def promotion_degree(self):
        return self.__promotion_degree

    @promotion_degree.setter
    def promotion_degree(self, promotion_degree):
        self.__promotion_degree = promotion_degree

    @property
    def utilization(self):
        return self.__utilization

    @utilization.setter
    def utilization(self, utilization):
        self.__utilization = utilization

    @staticmethod
    def createRule(table_name):
        with DataBaseOperate() as db:
            db.execute_sql(sqlCreateRules(table_name))

    def addList(self, record):
        self.__list.append(record)

    def toList(self):
        return [self.__item_id, self.__rule, self.__conclusion, self.__number_of_items_occurrences,
                self.__degree_of_confidence, self.__coverage, self.__promotion_degree, self.__utilization]

    def save(self, table_name):
        save_list = listOfGroups(self.__list, 2000)
        pool = ThreadPool()
        pool.map(partial(self.doSave, table_name=table_name), save_list)
        pool.close()
        pool.join()

    @staticmethod
    def doSave(save_list, table_name):
        with DataBaseOperate() as db:
            db.executemany_sql("""INSERT INTO %s (item_id,
                                                  rule,
                                                  conclusion,
                                                  number_of_items_occurrences,
                                                  degree_of_confidence,
                                                  coverage,
                                                  promotion_degree,
                                                  utilization)
                                    VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                                """ % table_name, save_list)
