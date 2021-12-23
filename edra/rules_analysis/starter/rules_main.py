import sys

sys.path.append(r'../../../../enterprise_data_risk_analysis')
from threading import Thread
import threading
from conf import target_table_name, other_table_name
from edra.rules_analysis.core.rules import RulesHandler
from utils.logging_operate import LoggingOperate
from utils.sqlite_operate import SqliteOperate

sqlite = SqliteOperate()
logger = LoggingOperate('rules_analysis_rules_main')
data_rules = list()


def doMatchAndSave(curr_table):
    global data_rules
    thread_name = threading.current_thread().name
    logger.info('>>>>>>%s start!' % thread_name)
    rule_handler = RulesHandler()
    sqlite_with_column = SqliteOperate(True)
    t_data = sqlite_with_column.query_all('SELECT * FROM %s' % curr_table)
    for d in t_data:
        target_data = list()
        for column in d:
            if column == 'id':
                continue
            target_data.append(str(column) + ':' + str(d[column]))
        most_matched = rule_handler.matchHighest(target_data)
        data_rules.append((curr_table, d['id'], most_matched['rule_id'], most_matched['score'],))
    logger.info('>>>%s Done!' % thread_name)


def doSave(save_info):
    sql = '''INSERT INTO data_rules (table_name, data_id, rule_id, score) VALUES (?,?,?,?)'''
    sqlite.executemany_sql(sql, save_info)


if __name__ == '__main__':
    # query all wait analysis table name
    table_names = [x[0] for x in sqlite.query_all(
        "SELECT tbl_name FROM sqlite_master WHERE tbl_name like '%s' GROUP BY tbl_name" % (target_table_name + '%'))]
    table_names.append(other_table_name)
    threads = []
    for table_name in table_names:
        threads.append(Thread(target=doMatchAndSave, args=(table_name,)))
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join()
    logger.info('>>>>>>start saving data!')
    save_once = list()
    for data in data_rules:
        save_once.append(data)
        if len(save_once) == 20000:
            doSave(save_once)
            save_once = list()
    if len(save_once) > 0:
        doSave(save_once)
