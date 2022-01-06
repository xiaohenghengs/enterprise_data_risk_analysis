from utils.database_operate import DataBaseOperate
from edra.rules_analysis.models import sqlCreateDataRules


class DataRules:
    def __init__(self, rule_type=None, data_id=None, rule_id=None, score=None):
        self.__rule_type = rule_type
        self.__data_id = data_id
        self.__rule_id = rule_id
        self.__score = score
        self.records = list()

    @property
    def rule_type(self):
        return self.__rule_type

    @rule_type.setter
    def rule_type(self, rule_type):
        self.__rule_type = rule_type

    @property
    def data_id(self):
        return self.__data_id

    @data_id.setter
    def data_id(self, data_id):
        self.__data_id = data_id

    @property
    def rule_id(self):
        return self.__rule_id

    @rule_id.setter
    def rule_id(self, rule_id):
        self.__rule_id = rule_id

    @property
    def score(self):
        return self.__score

    @score.setter
    def score(self, score):
        self.__score = score

    def addList(self, record):
        self.records.append(record)

    def toList(self):
        return [self.__rule_type, self.__data_id, self.__rule_id, self.__score]

    @staticmethod
    def createTable():
        with DataBaseOperate() as db:
            db.execute_sql(sqlCreateDataRules())

    def save(self):
        with DataBaseOperate() as db:
            db.executemany_sql("""INSERT INTO data_rules (rule_type,
                                                          data_id,
                                                          rule_id,
                                                          score)
                                  VALUES (?, ?, ?, ?)
                                """, self.records)
