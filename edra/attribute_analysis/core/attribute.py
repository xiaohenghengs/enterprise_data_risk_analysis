from pandas import *

from conf import table
from utils.database_operate import DataBaseOperate


class Attribute:
    def __init__(self, ids, max_num, min_num, ignore_columns):
        self.__target_table_name = table['target']
        self.__ids = ids
        self.__max_num = max_num
        self.__min_num = min_num
        self.__ignore_columns = ignore_columns
        self.__columns = self.getColumns()
        self.__mostAttributes = list()

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        pass

    def getAllDataFrame(self):
        sql = 'SELECT %s FROM %s WHERE ID in (%s) ' % (
            ','.join(self.__columns), self.__target_table_name, self.__ids)
        for attrs in self.__mostAttributes:
            sql += ' and %s = "%s"' % (attrs['column'], attrs['attr'])
        with DataBaseOperate() as dbo:
            return DataFrame([list(x) for x in dbo.query_all(sql)], columns=self.__columns)

    def getColumns(self):
        with DataBaseOperate() as dbo:
            self.__columns = [x for x in list(dbo.get_index_dict(self.__target_table_name).keys())]
        return self.__columns

    def attributesFilter(self):
        while True:
            df = self.getAllDataFrame()
            attributes = list()
            columns = [x for x in self.__columns if x not in self.__ignore_columns]
            for column in columns:
                grouped = df.groupby(column).size()
                dict_group = dict(grouped)
                max_count = grouped.max()
                attributes.append([{'column': column, 'attr': x, 'count': dict_group[x]} for x in dict_group if
                                   dict_group[x] == max_count][0])
            if attributes:
                attributes = sorted(attributes, key=lambda x: x['count'], reverse=True)
                mostCountAttribute = attributes[0]
                self.__mostAttributes.append(mostCountAttribute)
                count = mostCountAttribute['count']
                if self.__min_num <= count <= self.__max_num:
                    return self.getAllDataFrame()['ID'].values.tolist()
                elif count < self.__min_num:
                    # 如果结果小于最小数量，返回上一次分析的数据id
                    return df['ID'].values.tolist()
                self.__ignore_columns.append(mostCountAttribute['column'])
            else:
                return df['ID'].values.tolist()
