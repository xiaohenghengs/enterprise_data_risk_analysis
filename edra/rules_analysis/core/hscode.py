from collections import Counter

from utils.sqlite_operate import SqliteOperate


class HsCodeHandler:
    def __init__(self):
        self.sqlite = SqliteOperate()
        self.sqlite_with_columns = SqliteOperate(True)
        self.data_ids = []
        self.hs_codes = self.getTargetHsCode()

    def getTargetHsCode(self):
        self.data_ids = [x[0] for x in self.sqlite.query_all('SELECT data_id FROM data_rules WHERE score < 1')]
        return [x[0] for x in self.sqlite.query_all(
            'SELECT CKSP_DM FROM raw_data WHERE id in (%s) group by CKSP_DM' % ','.join(self.data_ids))]

    def handleSingleHsData(self, ids):
        data_rules = self.sqlite_with_columns.query_all(
            'SELECT data_id,rule_id,score FROM data_rules WHERE data_id IN (%s) AND score < 1' % ','.join(ids))
        s1 = 0
        s2 = 0
        abnormal_attr = list()
        for dr in data_rules:
            if float(dr['score']) >= 0.9:
                s1 += 1
            else:
                s2 += 1
            rule_id = str(dr['rule_id'])
            if 'Batch' in rule_id:
                batch = rule_id.split('_')[1]
                drop_columns = self.sqlite.query_all(
                    'SELECT _column,attribute FROM drop_columns WHERE batch = %s' % batch)
                rules = [str(x[0]) + ':' + str(x[1]) for x in drop_columns]
            else:
                rule = self.sqlite_with_columns.query_one(
                    'SELECT rule,conclusion,batch FROM rules WHERE id = %s' % rule_id)
                drop_columns = self.sqlite.query_all(
                    'SELECT _column,attribute FROM drop_columns WHERE batch = %s' % rule['batch'])
                rules = [str(x[0]) + ':' + str(x[1]) for x in drop_columns] + rule['rule'].split('&') + rule[
                    'conclusion'].split('&')
            target = self.sqlite_with_columns.query_one('SELECT * FROM raw_data WHERE id = %s' % dr['data_id'])
            target_data = list()
            for column in target:
                if column == 'id':
                    continue
                target_data.append(str(column) + ':' + str(target[column]))
            abnormal_attr.extend([x for x in rules if x not in target_data])
        abnormal_attr = [x.split(':')[0] for x in abnormal_attr]
        abnormal_attr_counter = Counter(abnormal_attr)
        counter_dict = dict()
        for counter in abnormal_attr_counter:
            counter_dict[counter] = round(abnormal_attr_counter[counter] / len(abnormal_attr), 2)
        return s1, s2, counter_dict
