from utils.database_operate import DataBaseOperate


class RulesHandler:
    def __init__(self, item_id, normal):
        self.rules = {}
        self.item_id = item_id
        self.rules_list_normal = normal

    def loadAllRulesNormal(self):
        with DataBaseOperate() as db:
            for normal in self.rules_list_normal:
                self.rules[normal] = db.query_all_with_column(
                    """SELECT 
                        r.id, CONCAT(r.RULE, '&', r.CONCLUSION) AS rule
                       FROM 
                        %s r 
                       JOIN attribute_items a ON r.ITEM_ID = a.ID WHERE ITEM_ID = '%s'
                    """ % (normal, self.item_id)
                )

    def matchHighest(self, data):
        rule_match_info = list()
        for rule in self.rules:
            rule_id = rule['id']
            rule = rule['rule']
            match_attribute = [x for x in rule if x.strip() in data]
            score = len(match_attribute) / len(rule)
            __dict = {'rule_id': rule_id, 'score': score}
            if score == 1:
                return __dict
            rule_match_info.append(__dict)
        return sorted(rule_match_info, key=lambda x: x['score'], reverse=True)[0]
