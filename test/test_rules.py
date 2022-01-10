import unittest

import edra.rules_analysis.starter.enterprise as enterprise
from conf import table
from edra.rules_analysis.core.rules import RulesHandler
from edra.rules_analysis.starter.rules import queryAllAttributeItems
from utils.database_operate import DataBaseOperate
from utils.logging_operate import LoggingOperate


class RuleHandlerTest(unittest.TestCase):
    def setUp(self) -> None:
        self.logger = LoggingOperate('rule_handler_test')
        self.item_id = 'e5fcdcce6df211eca4f1ebf2c57c8f4d'
        self.data_id = '487016'
        self.normal = ['rules_enterprise_scale', 'rules_hs_code']
        self.rulesHandler = RulesHandler(self.item_id, self.normal)

    def tearDown(self) -> None:
        pass

    def test_loadAllRulesNormal(self):
        self.rulesHandler.loadAllRulesNormal()
        self.assertIsInstance(self.rulesHandler.rules, dict)

    def test_matchHighest(self):
        self.test_loadAllRulesNormal()
        with DataBaseOperate() as db:
            data = db.query_all_with_column(
                "SELECT ID, ZZMDGDQSZ_DM, YSFS_DM, ZYG_DM, HGCJFS_DM, QYGBZ, HZDWDQ_DM, HGGQKA_DM FROM %s WHERE id = '%s'" % (
                    table['target'], self.data_id))
            highest = self.rulesHandler.matchHighest([str(column) + ':' + str(data[0][column]) for column in data[0]],
                                                     'rules_enterprise_scale')
            self.logger.info(highest)
            self.assertIsInstance(highest, dict)

    def test_loadAllEnterprise(self):
        enterprises = enterprise.loadAllCustomsCode()
        self.assertIsNotNone(enterprises)
        return enterprises[10086]

    def test_loadDataIdByCustomCode(self):
        custom_code = self.test_loadAllEnterprise()
        data_ids = enterprise.loadDataIdByCustomsCode(custom_code)
        self.assertIsNotNone(data_ids)
        return data_ids

    def test_loadRulesByDataIds(self):
        ids = self.test_loadDataIdByCustomCode()
        rules_data = enterprise.loadRulesByDataIds(ids, self.normal[0])
        self.assertIsNotNone(rules_data)

    def test_doAnalysis(self):
        enterprise.enterprises = list()
        enterprise.attribute_items = {x['ID']: {'CKSP_DM_LENGTH': x['CKSP_DM_LENGTH'], 'ZMY_UNIT': x['ZMY_UNIT']} for x
                                      in queryAllAttributeItems()}
        enterprise.doAnalysis(['32023631C2'])
        self.assertIsNotNone(enterprise.enterprises)


if __name__ == '__main__':
    unittest.main()
