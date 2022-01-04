import sys

sys.path.append(r'../../../../enterprise_data_risk_analysis')
from functools import partial
from multiprocessing.dummy import Pool as ThreadPool
from threading import Thread

from conf import table
from edra.apriori_analysis.core.association import Association
from edra.apriori_analysis.models.rule import Rule
from utils.database_operate import DataBaseOperate
from utils.logging_operate import LoggingOperate
from utils.utils import listOfGroups
import tensorflow as tf

logger = LoggingOperate("apriori")

RULES_HS_CODE = ['CKSL_DECILE', 'MYLAJ_DECILE', 'FOBDJ_DECILE', 'MZ_2_DECILE', 'JZ_DECILE', 'CKY']

RULES_ENTERPRISE_SCALE = ['ZZMDGDQSZ_DM', 'YSFS_DM', 'ZYG_DM', 'HGCJFS_DM', 'QYGBZ', 'HZDWDQ_DM', 'HGGQKA_DM']

table_name_hs_code = 'rules_hs_code_left'
table_name_enterprise_scale = 'rules_enterprise_scale_left'


def createRulesTable():
    """
    创建规则表
    :return:
    """
    rule = Rule()
    rule.createRule(table_name_hs_code)
    rule.createRule(table_name_enterprise_scale)


def initTask():
    """
    初始化apriori任务
    :return:
    """
    with DataBaseOperate() as db:
        tasks = db.query_all('select id, cksp_dm_length, zmy_unit from attribute_items')
    task_list = listOfGroups(tasks, 5)
    threads = []
    for task in task_list:
        threads.append(Thread(target=runAprioriTask, args=(task,)))
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join()


def runAprioriTask(tasks):
    for task in tasks:
        item_id = task[0]
        cksp_dm_length = task[1]
        zmy_unit = task[2]
        # 根据item_id查询数据id
        with DataBaseOperate() as db:
            hs_code = db.query_all_with_column(sqlQueryData(RULES_HS_CODE, item_id, cksp_dm_length, zmy_unit))
            enterprise_scale = db.query_all_with_column(
                sqlQueryData(RULES_ENTERPRISE_SCALE, item_id, cksp_dm_length, zmy_unit))
            wait_analysis_data_hs = [[i + ':' + str(x[i]) for i in x] for x in hs_code]
            wait_analysis_data_scale = [[i + ':' + str(x[i]) for i in x] for x in enterprise_scale]
            result_rules_hs = doAssociation(wait_analysis_data_hs)
            logger.info("》》》》》》item id : %s ，分析得出hs编码规则 %s 条" % (item_id, len(result_rules_hs)))
            result_rules_scale = doAssociation(wait_analysis_data_scale)
            logger.info("》》》》》》item id : %s ，分析得出企业规模规则 %s 条" % (item_id, len(result_rules_scale)))
            doSaveRules(result_rules_hs, item_id, table_name_hs_code)
            doSaveRules(result_rules_scale, item_id, table_name_enterprise_scale)


def sqlQueryData(columns, item_id, length, unit):
    key_condition = 'LEFT(CKSP_DM, %s) as CKSP_DM, TRUNCATE(ZMY / %s, 0) as ZMY, ' % (
        length if length == '10' else str(int(length) + 2), unit if unit == '10000' else str(int(unit) / 10))
    return """
            SELECT %s 
            FROM %s a 
            WHERE EXISTS(SELECT 1 FROM attribute_items_details b WHERE b.ITEMS_ID = '%s' AND a.ID = b.DATA_ID)
        """ % (key_condition + ','.join(columns), table['target'], item_id)


def handleDataLeft():
    """
    处理剩下的数据
    :return:
    """
    hs_code = queryDataLeft(RULES_HS_CODE)
    enterprise_scale = queryDataLeft(RULES_ENTERPRISE_SCALE)
    wait_analysis_data_hs = [[i + ':' + str(x[i]) for i in x] for x in hs_code]
    wait_analysis_data_scale = [[i + ':' + str(x[i]) for i in x] for x in enterprise_scale]
    result_rules_hs = doAssociation(wait_analysis_data_hs)
    logger.info("》》》》》》分析得出hs编码规则 %s 条" % (len(result_rules_hs)))
    result_rules_scale = doAssociation(wait_analysis_data_scale)
    logger.info("》》》》》》分析得出企业规模规则 %s 条" % (len(result_rules_scale)))
    doSaveRules(result_rules_hs, '', table_name_hs_code)
    doSaveRules(result_rules_scale, '', table_name_enterprise_scale)


def queryDataLeft(columns):
    sql = """
                SELECT LEFT(CKSP_DM, 8) AS CKSP_DM, TRUNCATE(ZMY / 100000000, 0) AS ZMY, %s
                FROM %s c
                WHERE NOT EXISTS(SELECT 1 FROM attribute_items_details a WHERE c.ID = a.DATA_ID)
            """ % (','.join(columns), table['target'])
    with DataBaseOperate() as db:
        return db.query_all_with_column(sql)


def doAssociation(wait_analysis_data):
    association = Association(wait_analysis_data, float(2000 / len(wait_analysis_data)), 0.8)
    rules = association.generateResult()
    pool = ThreadPool()
    pool.map(partial(association.filteringAndDecodingResult, rule_len=1, target_len=100), rules)
    pool.close()
    pool.join()
    final_result = association.final_result
    return final_result


def doSaveRules(rules, item_id, table_name):
    r = Rule()
    for rule in rules:
        r.item_id = item_id
        r.rule = rule[0]
        r.conclusion = rule[1]
        r.number_of_items_occurrences = rule[2]
        r.degree_of_confidence = rule[3]
        r.coverage = rule[4]
        r.promotion_degree = rule[5]
        r.utilization = rule[6]
        r.addList(r.toList())
    r.save(table_name)


if __name__ == '__main__':
    createRulesTable()
    handleDataLeft()
