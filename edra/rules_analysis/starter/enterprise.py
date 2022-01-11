import sys

sys.path.append(r'../../../../enterprise_data_risk_analysis')
import threading
import pandas as pd

from collections import Counter
from threading import Thread
from conf import table
from edra.apriori_analysis.starter.apriori import RULES_HS_CODE, RULES_ENTERPRISE_SCALE
from edra.rules_analysis.starter.rules import queryAllAttributeItems
from edra.rules_analysis.starter.rules import rule_types
from utils.database_operate import DataBaseOperate
from utils.utils import listOfGroups
from utils.logging_operate import LoggingOperate

logger = LoggingOperate('enterprise')
data_column = RULES_HS_CODE + RULES_ENTERPRISE_SCALE


def loadAllCustomsCode():
    with DataBaseOperate() as db:
        sql_query_customs_code = 'SELECT HGQY_DM FROM %s GROUP BY HGQY_DM' % table['target']
        return [x[0] for x in db.query_all(sql_query_customs_code)]


def loadDataIdByCustomsCode(customs_code):
    with DataBaseOperate() as db:
        sql_query_data_id = "SELECT id FROM %s WHERE HGQY_DM = '%s'" % (table['target'], customs_code)
        return [str(x[0]) for x in db.query_all(sql_query_data_id)]


def loadRulesByDataIds(data_ids, rule_type):
    with DataBaseOperate() as db:
        sql_query_data_rules = "SELECT data_id, rule_id, score FROM data_rules WHERE DATA_ID in (%s) AND RULE_TYPE = '%s'" % (
            ','.join(data_ids), rule_type)
        return db.query_all_with_column(sql_query_data_rules)


def doAnalysis(customs):
    thread_name = threading.current_thread().name
    global enterprises
    for index, customs_code in enumerate(customs):
        logger.info('》》》线程：%s，正在分析第 %d 家企业' % (thread_name, index))
        enterprise_info = list()
        data_ids = loadDataIdByCustomsCode(customs_code)
        data_length = len(data_ids)
        enterprise_info.append(customs_code)
        enterprise_info.append(data_length)
        for rule_type in rule_types:
            rules = loadRulesByDataIds(data_ids, rule_type)
            if rules:
                df = pd.DataFrame(rules)
                eq_1 = df[df['SCORE'].astype(float) == 1]
                enterprise_info.append(str(round(len(eq_1) / data_length, 2)))
                between_1_07 = df[
                    (df['SCORE'].astype(float) < 1) & (df['SCORE'].astype(float) >= 0.7)]
                enterprise_info.append(str(round(len(between_1_07) / data_length, 2)))
                less_07 = df[df['SCORE'].astype(float) < 0.7]
                enterprise_info.append(str(round(len(less_07) / data_length, 2)))
                abnormal_data_rules = df[df['SCORE'].astype(float) < 1]
                no_match_attr_counter = handleAbnormalRule(abnormal_data_rules, rule_type)
                attribute = dict()
                for counter in no_match_attr_counter:
                    attribute[counter] = round(no_match_attr_counter[counter] / data_length, 2)
                enterprise_info.append(str(attribute))
            else:
                enterprise_info.append('-')  # probability of score=1
                enterprise_info.append('-')  # probability of 1>score>=0.7
                enterprise_info.append('-')  # probability of score<0.7
                enterprise_info.append('-')  # 异常属性概率
        enterprises.append(enterprise_info)


def handleAbnormalRule(frame, rule_type):
    global attribute_items
    no_match_attribute = list()
    for data_rule in frame.values:
        data_id = data_rule[0]
        rule_id = data_rule[1]
        sql_query_rule = "SELECT ITEM_ID, CONCAT(RULE, '&', CONCLUSION) AS rule FROM %s WHERE id = %s" % (
            rule_type, rule_id)
        with DataBaseOperate() as db:
            rule = db.query_one(sql_query_rule)
            item_id = rule[0]
            rule_info = rule[1]
            items = attribute_items[item_id]
            length = items['CKSP_DM_LENGTH']
            unit = items['ZMY_UNIT']
            key_condition = 'LEFT(CKSP_DM, %s) AS CKSP_DM, TRUNCATE(ZMY / %s, 0) AS ZMY, ' % (
                length if length == '10' else str(int(length) + 2), unit if unit == '10000' else str(int(unit) / 10))
            sql_query_data_info = "SELECT %s FROM %s WHERE id = %s" % (
                key_condition + ','.join(data_column), table['target'], data_id)
            info = db.query_one_with_column(sql_query_data_info)
        data_attribute = [str(column) + ':' + str(info[column]) for column in info]
        r = rule_info.split('&')
        no_match_attribute.extend([x for x in r if x.strip() not in data_attribute])
        logger.info('rule_type: %s ,data_id: %s ,rule_id: %s , no_match_attribute: %s' % (
            rule_type, data_id, rule_id, no_match_attribute))
    no_match_attribute = [x.split(':')[0] for x in no_match_attribute]
    return Counter(no_match_attribute)


attribute_items = dict()
enterprises = list()
if __name__ == '__main__':
    all_customs_code = loadAllCustomsCode()
    attribute_items = {x['ID']: {'CKSP_DM_LENGTH': x['CKSP_DM_LENGTH'], 'ZMY_UNIT': x['ZMY_UNIT']} for x in
                       queryAllAttributeItems()}
    customs_group = listOfGroups(all_customs_code, 250)
    threads = []
    for group in customs_group:
        threads.append(Thread(target=doAnalysis, args=(group,)))
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join()
    data_frame = pd.DataFrame(enterprises, columns=(
        '海关企业代码', '明细条数', '企业规模：SCORE=1', '企业规模：1>SCORE>=0.7', '企业规模：SCORE<0.7', '企业规模：异常属性概率', '商品编码：SCORE=1',
        '商品编码：1>SCORE>=0.7', '商品编码：SCORE<0.7', '商品编码：异常属性概率'))
    data_frame.to_excel('分析结果.xlsx', index=False)
