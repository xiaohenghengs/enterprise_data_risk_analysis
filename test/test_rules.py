import unittest

from edra.rules_analysis.core.rules import RulesHandler
from utils.logging_operate import LoggingOperate
from utils.sqlite_operate import SqliteOperate


class RuleHandlerTest(unittest.TestCase):
    def setUp(self) -> None:
        self.logger = LoggingOperate('rule_handler_test')
        self.rulesHandler = RulesHandler()
        self.sqlite = SqliteOperate(True)

    def tearDown(self) -> None:
        pass

    def test_loadAllRules(self):
        self.logger.info(self.rulesHandler.rules)

    def test_matchHighest(self):
        data = self.sqlite.query_one('SELECT * FROM target_data_1')
        target_data = list()
        for column in data:
            if column == 'id':
                continue
            target_data.append(str(column) + ':' + str(data[column]))
        self.rulesHandler.matchHighest(target_data)


if __name__ == '__main__':
    unittest.main()
