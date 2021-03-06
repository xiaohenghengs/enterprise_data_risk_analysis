import os

import jaydebeapi

from conf import oceanbase, root_path

base_info = oceanbase


class DataBaseOperate:
    def __init__(self):
        driver = base_info['driver']
        url = base_info['url']
        user = base_info['user']
        password = base_info['password']
        jar = os.path.join(root_path, base_info['jar'])
        self.connect = jaydebeapi.connect(jclassname=driver, url=url, driver_args=[str(user), str(password)], jars=jar)
        self.cursor = self.connect.cursor()

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.cursor.close()
        self.connect.close()

    def get_index_dict(self, table_name=None):
        """
        获取数据库对应表中的字段名
        """
        if table_name:
            self.execute_sql('select * from %s limit 1' % table_name)
        index_dict = dict()
        index = 0
        for desc in self.cursor.description:
            index_dict[desc[0]] = index
            index = index + 1
        return index_dict

    def execute_sql(self, sql):
        try:
            self.cursor.execute(sql)
        except Exception as e:
            raise e

    def query_all(self, sql):
        self.execute_sql(sql)
        return self.cursor.fetchall()

    def query_one(self, sql):
        self.execute_sql(sql)
        return self.cursor.fetchone()

    def query_all_with_column(self, sql: object) -> object:
        data = self.query_all(sql)
        index_dict = self.get_index_dict()
        res = []
        for d in data:
            res_i = dict()
            for index_i in index_dict:
                res_i[index_i] = d[index_dict[index_i]]
            res.append(res_i)
        return res

    def query_one_with_column(self, sql):
        data = self.query_one(sql)
        index_dict = self.get_index_dict()
        res = dict()
        for index_i in index_dict:
            res[index_i] = data[index_dict[index_i]]
        return res

    def executemany_sql(self, sql, data_list):
        """
        EXAMPLE
        :param sql:'insert into table_name (column1,column2,column3,...) values (?, ?, ?, ...);'
        :param data_list: [[content1,content2,content3], [content1,content2,content3]]
        """
        try:
            self.cursor.executemany(sql, data_list)
        except Exception as e:
            raise Exception(e)
