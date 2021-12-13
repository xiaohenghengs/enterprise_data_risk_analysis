from utils.sqlite_operate import SqliteOperate


class RulesHandler:
    def __init__(self):
        self.sqlite = SqliteOperate()
        self.rules = self.loadAllRules()

    def loadAllRules(self):
        batch = self.sqlite.query_all('SELECT batch FROM drop_columns GROUP BY batch')
        rules = dict()
        for bat in batch:
            drop_columns_rules = self.makeDropColumnsRules(bat)
            rule_conclusion = self.sqlite.query_all('SELECT id,rule,conclusion FROM rules WHERE batch = %s' % bat)
            if len(rule_conclusion) > 0:
                this_batch_rules = {str(x[0]): drop_columns_rules + str(x[1]).split('&') + (str(x[2]).split('&')) for x
                                    in rule_conclusion}
            else:
                this_batch_rules = {'Batch_' + str(bat[0]): drop_columns_rules}
            rules.update(this_batch_rules)
        return rules

    def makeDropColumnsRules(self, batch):
        drop_columns = self.sqlite.query_all('SELECT _column,attribute FROM drop_columns WHERE batch = %s' % batch)
        # make up drop columns into rules format
        return [str(x[0]) + ':' + str(x[1]) for x in drop_columns]

    def matchHighest(self, data):
        rule_match_info = list()
        for rule_id in self.rules.keys():
            rule = self.rules[rule_id]
            match_attribute = [x for x in rule if x.strip() in data]
            score = len(match_attribute) / len(rule)
            __dict = {'rule_id': rule_id, 'score': score}
            if score == 1:
                return __dict
            rule_match_info.append(__dict)
        return sorted(rule_match_info, key=lambda x: x['score'], reverse=True)[0]
