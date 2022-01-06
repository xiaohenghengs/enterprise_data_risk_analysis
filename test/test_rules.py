import unittest

from conf import table
from edra.rules_analysis.core.rules import RulesHandler
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


if __name__ == '__main__':
    unittest.main()
