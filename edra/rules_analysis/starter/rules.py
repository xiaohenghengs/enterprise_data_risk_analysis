import sys

sys.path.append(r'../../../../enterprise_data_risk_analysis')
import threading
from threading import Thread

from conf import table
from edra.apriori_analysis.starter.apriori import RULES_HS_CODE, RULES_ENTERPRISE_SCALE
from edra.rules_analysis.core.rules import RulesHandler
from edra.rules_analysis.models.data_rules import DataRules
from utils.database_operate import DataBaseOperate
from utils.logging_operate import LoggingOperate
from utils.utils import listOfGroups

logger = LoggingOperate('data_rule')

rule_types = ['rules_enterprise_scale', 'rules_hs_code']

data_column = RULES_HS_CODE + RULES_ENTERPRISE_SCALE


def runMatchTask(attr_items):
    thread_name = threading.current_thread().name
    for item_info in attr_items:
        item_id = item_info['ID']
        sql_query_data_id = "select DATA_ID from attribute_items_details where ITEMS_ID = '%s'" % item_id
        logger.info('》》》线程：%s，查询分类下对应的数据id：%s' % (thread_name, sql_query_data_id))
        with DataBaseOperate() as database:
            data_ids = database.query_all(sql_query_data_id)
        logger.info('》》》线程：%s，查询结果返回 %d 条数据，开始分线程处理，每个线程处理500条数据' % (thread_name, len(data_ids)))
        rules_obj = RulesHandler(item_id, rule_types)
        rules_obj.loadAllRulesNormal()
        data_groups = listOfGroups(data_ids, 500)
        __threads = []
        for dg in data_groups:
            __threads.append(Thread(target=doDataMatch, args=(dg, rules_obj, item_info,)))
        for __thread in __threads:
            __thread.start()
        for __thread in __threads:
            __thread.join()


def doDataMatch(data_ids, rules_obj, item_info):
    thread_name = threading.current_thread().name
    data_ids = [x[0] for x in data_ids]
    length = item_info['CKSP_DM_LENGTH']
    unit = item_info['ZMY_UNIT']
    key_condition = 'ID, LEFT(CKSP_DM, %s) AS CKSP_DM, TRUNCATE(ZMY / %s, 0) AS ZMY, ' % (
        length if length == '10' else str(int(length) + 2), unit if unit == '10000' else str(int(unit) / 10))
    sql_query_data_info = "SELECT %s FROM %s WHERE id in (%s)" % (
        key_condition + ','.join(data_column), table['target'], ','.join(data_ids))
    logger.info('》》》线程：%s，查询全部数据信息：%s' % (thread_name, sql_query_data_info))
    with DataBaseOperate() as data:
        d = data.query_all_with_column(sql_query_data_info)
    data_rules = DataRules()
    for rule_type in rule_types:
        logger.info('》》》线程：%s，开始进行 %s 的规则匹配' % (thread_name, rule_type))
        for info in d:
            data_attribute = [str(column) + ':' + str(info[column]) for column in info]
            logger.info('》》》线程：%s，数据属性：%s' % (thread_name, str(data_attribute)))
            highest = rules_obj.matchHighest(data_attribute, rule_type)
            logger.info('》》》线程：%s，规则：%s，最高分匹配规则结果：%s' % (thread_name, rule_type, str(highest)))
            if highest:
                data_rules.data_id = info['ID']
                data_rules.rule_type = rule_type
                data_rules.rule_id = highest['rule_id']
                data_rules.score = highest['score']
                data_rules.addList(data_rules.toList())
    logger.info('》》》线程：%s，数据匹配完成，匹配分析数据 %d 条，开始保存数据' % (thread_name, len(data_rules.records)))
    data_rules.save()
    logger.info('》》》线程：%s，数据保存成功！' % thread_name)


def queryAllAttributeItems():
    with DataBaseOperate() as db:
        return db.query_all_with_column('select ID,CKSP_DM_LENGTH,ZMY_UNIT from attribute_items')


if __name__ == '__main__':
    DataRules.createTable()
    attribute_items = queryAllAttributeItems()
    logger.info('》》》》》》》》》查询到 %d 个分类，开始分线程处理，每个线程处理10个分类' % len(attribute_items))
    items_groups = listOfGroups(attribute_items, 10)
    threads = []
    for group in items_groups:
        threads.append(Thread(target=runMatchTask, args=(group,)))
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join()
