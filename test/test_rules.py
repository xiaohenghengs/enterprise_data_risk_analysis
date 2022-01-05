import unittest

from edra.rules_analysis.core.rules import RulesHandler
from utils.logging_operate import LoggingOperate


class RuleHandlerTest(unittest.TestCase):
    def setUp(self) -> None:
        self.logger = LoggingOperate('rule_handler_test')
        self.item_id = '5ab0acc0691211ec812861810bda09ce'
        self.normal = ['rules_enterprise_scale', 'rules_hs_code']
        self.rulesHandler = RulesHandler(self.item_id, self.normal)

    def tearDown(self) -> None:
        pass

    def test_loadAllRulesNormal(self):
        self.rulesHandler.loadAllRulesNormal()
        self.assertIsInstance(self.rulesHandler.rules, dict)


if __name__ == '__main__':
    unittest.main()
