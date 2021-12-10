import os
import sqlite3

from conf import task_name


def dict_factory(cursor, row):
    d = {}
    for idx, col in enumerate(cursor.description):
        d[col[0]] = row[idx]
    return d


class SqliteOperate:
    def __init__(self, need_column_name=False):
        """
        Sqlite数据库操作
        <p>
        """
        parent_path = os.path.dirname(os.path.dirname(os.path.realpath(__file__)))  # 父文件夹的绝对路径
        self.connect = sqlite3.connect(os.path.join(parent_path, 'database', task_name + '.db'))
        if need_column_name:
            self.connect.row_factory = dict_factory
        self.cursor = self.connect.cursor()

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.connect.close()

    def get_index_dict(self, table_name=None):
        """
        获取数据库对应表中的字段名
        """
        if table_name:
            self.query_one('select * from %s limit 1' % table_name)
        index_dict = dict()
        index = 0
        for desc in self.cursor.description:
            index_dict[desc[0]] = index
            index = index + 1
        return index_dict

    def execute_sql(self, sql, data_list=None):
        if data_list is None:
            data_list = []
        try:
            self.cursor.execute(sql, data_list)
            self.connect.commit()
        except Exception as e:
            self.connect.rollback()
            raise Exception(e)

    def query_all(self, sql, data_list=None):
        self.execute_sql(sql, data_list)
        return self.cursor.fetchall()

    def query_one(self, sql):
        self.execute_sql(sql)
        return self.cursor.fetchone()

    def query_many(self, sql, size):
        self.execute_sql(sql)
        return self.cursor.fetchmany(size=size)

    def executemany_sql(self, sql, data_list):
        """
        EXAMPLE
        :param sql:'insert into table_name (column1,column2,column3,...) values (?, ?, ?, ...);'
        :param data_list: [(content1,content2,content3), (content1,content2,content3)]
        """
        try:
            self.cursor.executemany(sql, data_list)
            self.connect.commit()
        except Exception as e:
            self.connect.rollback()
            raise Exception(e)

    def execute_script(self, script):
        self.cursor.executescript(script)

    def rename_table(self, ori_name, new_name):
        self.cursor.execute('ALTER TABLE %s RENAME TO %s' % (ori_name, new_name))
