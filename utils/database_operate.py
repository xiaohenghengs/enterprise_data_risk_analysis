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
