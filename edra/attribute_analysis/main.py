import sys

sys.path.append(r'../../../enterprise_data_risk_analysis')
import numpy

from conf import target_table_name, other_table_name
from edra.attribute_analysis import createOtherData
from edra.attribute_analysis.core.attribute import Attribute
from utils.sqlite_operate import SqliteOperate
from utils.logging_operate import LoggingOperate

logger = LoggingOperate('attribute_analysis')

db = SqliteOperate()


def dataMove(data):
    """
    move data to other table
    :param data:
    :return:
    """
    all_ori_columns = db.get_index_dict(other_table_name)
    # insert other table
    insert_sql = 'INSERT INTO %s (%s)VALUES (%s)' % (
        other_table_name, str.join(',', all_ori_columns), ','.join(['?'] * len(all_ori_columns)))
    db.executemany_sql(insert_sql, data)
    # remove data from target table
    data = numpy.array(data)
    ids = data[:, 0]
    remove_sql = 'DELETE FROM %s WHERE id = ?' % target_table_name
    db.executemany_sql(remove_sql, [(x,) for x in ids])


if __name__ == '__main__':
    data_length = int(input('单个目标表的数据长度：'))
    columns_length = int(input('单个目标表的剩余字段个数：'))
    batch = 1
    while True:
        logger.info('>>>>>>Attribute Analysis Start!')
        data_model = Attribute(batch)
        data_model.removeColumn('id')
        while True:
            __most_count, __columns_length = data_model.getMostCountColumnWithColumns()
            # set double cut-off condition
            if (__columns_length < columns_length) or (__most_count['count'] < data_length):
                break
            logger.info('>>>batch:%s, column info:%s' % (str(batch), str(__most_count),))
            data_model.saveDropColumn(__most_count)
            most_column = __most_count['column']
            # query data with most count column
            sql = 'SELECT * FROM %s WHERE %s != "%s"' % (
                target_table_name, most_column, __most_count['attr'])
            other_data = db.query_all(sql)
            if len(other_data) > 0:
                dataMove(other_data)
            data_model.removeColumn(most_column)
        logger.info('>>>batch:%s is done, start next batch' % batch)
        # rename target table with drop column table id
        db.rename_table('target_data', 'target_data_' + str(batch))
        other_length = db.query_one('SELECT count(*) FROM other_data')
        # set cut-off condition
        if other_length[0] < data_length:
            break
        db.rename_table('other_data', 'target_data')
        createOtherData()
        batch += 1
    logger.info('>>>>>>Attribute Analysis Done!')
