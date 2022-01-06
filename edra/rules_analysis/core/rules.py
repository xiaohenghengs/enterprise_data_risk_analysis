from utils.database_operate import DataBaseOperate


class RulesHandler:
    def __init__(self, item_id, rules_list):
        self.rules = {}
        self.item_id = item_id
        self.rules_list_normal = rules_list

    def loadAllRulesNormal(self):
        with DataBaseOperate() as db:
            for rules_lst in self.rules_list_normal:
                self.rules[rules_lst] = db.query_all_with_column(
                    """SELECT 
                        r.ID, CONCAT(r.RULE, '&', r.CONCLUSION) AS rule
                       FROM 
                        %s r 
                       JOIN attribute_items a ON r.ITEM_ID = a.ID WHERE ITEM_ID = '%s'
                    """ % (rules_lst, self.item_id)
                )

    def matchHighest(self, data, rule_type):
        rule_match_info = list()
        for rule in self.rules[rule_type]:
            rule_id = rule['ID']
            rule = str(rule['rule']).split('&')
            match_attribute = [x for x in rule if x.strip() in data]
            score = (len(match_attribute) + 2) / (len(rule) + 2)
            __dict = {'rule_id': rule_id, 'score': score}
            if score == 1:
                return __dict
            rule_match_info.append(__dict)
        if rule_match_info:
            return sorted(rule_match_info, key=lambda x: x['score'], reverse=True)[0]
        else:
            return None
