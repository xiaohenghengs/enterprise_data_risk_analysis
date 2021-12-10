from pandas import *

from conf import target_table_name
from utils.sqlite_operate import SqliteOperate


class Attribute:
    def __init__(self, batch):
        self.__batch = batch
        self.__db_object = SqliteOperate()
        self.__columns = self.getColumns()

    def getAll(self):
        return [list(x) for x in self.__db_object.query_all(
            'SELECT %s FROM %s' % (','.join(self.__columns), target_table_name))]

    def getColumns(self):
        self.__columns = list(self.__db_object.get_index_dict(target_table_name).keys())
        return self.__columns

    def removeColumn(self, column):
        self.__columns.remove(column)

    def getMostCountColumnWithColumns(self):
        all_data = self.getAll()
        df = DataFrame(all_data, columns=self.__columns)
        attributes = list()
        for column in self.__columns:
            grouped = df.groupby(column).size()
            dict_group = dict(grouped)
            max_count = grouped.max()
            attributes.append([{'column': column, 'attr': x, 'count': dict_group[x]} for x in dict_group.keys() if
                               dict_group[x] == max_count][0])
        attributes = sorted(attributes, key=lambda x: x['count'], reverse=True)
        return attributes[0], len(self.__columns)

    def saveDropColumn(self, attribute):
        sql = '''INSERT INTO drop_columns (batch, _column, attribute, count) 
                 VALUES (?, ?, ?, ?)'''
        self.__db_object.execute_sql(
            sql, (self.__batch, attribute['column'], attribute['attr'], str(attribute['count']),))
