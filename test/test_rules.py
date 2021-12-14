import unittest

from edra.rules_analysis.core.hscode import HsCodeHandler
from edra.rules_analysis.core.rules import RulesHandler
from utils.logging_operate import LoggingOperate
from utils.sqlite_operate import SqliteOperate


class RuleHandlerTest(unittest.TestCase):
    def setUp(self) -> None:
        self.logger = LoggingOperate('rule_handler_test')
        self.rulesHandler = RulesHandler()
        self.hsHandler = HsCodeHandler()
        self.sqlite = SqliteOperate()
        self.sqlite_with_columns = SqliteOperate(True)

    def tearDown(self) -> None:
        pass

    def test_loadAllRules(self):
        self.logger.info(self.rulesHandler.rules)

    def test_matchHighest(self):
        data = self.sqlite_with_columns.query_one('SELECT * FROM target_data_1')
        target_data = list()
        for column in data:
            if column == 'id':
                continue
            target_data.append(str(column) + ':' + str(data[column]))
        self.rulesHandler.matchHighest(target_data)

    def test_getTargetDataRules(self):
        hs_codes = self.hsHandler.getTargetHsCode()
        self.assertIsNotNone(hs_codes)
        return hs_codes

    def test_handleSingleHsData(self):
        hs_codes = (7228309000,)
        data_ids = self.sqlite.query_all("SELECT id FROM raw_data WHERE CKSP_DM = '%s'" % hs_codes)
        self.hsHandler.handleSingleHsData([str(x[0]) for x in data_ids])


if __name__ == '__main__':
    unittest.main()
