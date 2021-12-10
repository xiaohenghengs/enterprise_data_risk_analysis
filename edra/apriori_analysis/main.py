import sys

sys.path.append(r'../../../enterprise_data_risk_analysis')
import time
from functools import partial
from multiprocessing.dummy import Pool as ThreadPool

from conf import target_table_name as target_data
from edra.apriori_analysis.core.association import Association
from utils.sqlite_operate import SqliteOperate
from utils.logging_operate import LoggingOperate

logger = LoggingOperate("apriori_analysis")

db = SqliteOperate()
db_with_column = SqliteOperate(True)


def getAnalysisColumns(bat, table_name):
    ignore_columns = [x[0] for x in db.query_all('SELECT _column FROM drop_columns WHERE batch = %s' % str(bat))]
    columns = list(db.get_index_dict(table_name))
    return [x for x in columns if x not in ignore_columns and x != 'id']


def analysisTargetData():
    batch = [x[0] for x in db.query_all('SELECT batch FROM drop_columns group by batch')]
    for bat in batch:
        target_table_name = target_data + '_' + str(bat)
        wait_analysis_columns = getAnalysisColumns(bat, target_table_name)
        sql = 'SELECT %s FROM %s' % (','.join(wait_analysis_columns), target_table_name)
        wait_analysis_data = [[i + ':' + str(x[i]) for i in x.keys()] for x in db_with_column.query_all(sql)]
        doAssociation(wait_analysis_data, bat)


def doAssociation(wait_analysis_data, batch):
    analysis_start = time.time()
    association = Association(wait_analysis_data, float(10000 / len(wait_analysis_data)), 0.8)
    rules = association.generateResult()
    analysis_end = time.time()
    logger.info(">>>>>>关联分析用时：{}s".format(analysis_end - analysis_start))
    logger.info(">>>本次关联分析生成%s条规则" % len(rules))
    filtering_start = time.time()
    pool = ThreadPool()
    pool.map(partial(association.filteringAndDecodingResult, rule_len=1, target_len=100), rules)
    pool.close()
    pool.join()
    filtering_end = time.time()
    current_data = association.final_result
    logger.info(">>>过滤和解码规则用时：{}s".format(filtering_end - filtering_start))
    logger.info(">>>本次过滤和解码返回%s条规则" % len(current_data))
    if len(current_data) > 0:
        insert_sql = """
                    INSERT INTO rules (batch, rule,conclusion, number_of_items_occurrences,
                     degree_of_confidence, coverage, promotion_degree,utilization)
                    values (""" + str(batch) + ", ?, ?, ?, ?, ?, ?, ?)"
        current_data = [tuple(x) for x in current_data]
        db.executemany_sql(insert_sql, current_data)


if __name__ == '__main__':
    analysisTargetData()
