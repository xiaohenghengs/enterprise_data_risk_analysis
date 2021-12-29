import unittest

from edra.attribute_analysis.core.attribute import Attribute


class AttributeTest(unittest.TestCase):
    def setUp(self) -> None:
        self.__length = 10
        self.__unit = 10000
        self.__cksp_dm = 4907009011
        self.__zmy = 22697

    def tearDown(self) -> None:
        pass

    def test_attributesFilter(self):
        with Attribute(
                {'max_num': 20000, 'min_num': 10000, 'length': self.__length, 'unit': self.__unit},
                ['ID', 'HGQY_DM']) as attribute:
            ids = attribute.attributesFilter(self.__cksp_dm, self.__zmy)
            self.assertLessEqual(len(ids), 20000)
            self.assertGreaterEqual(len(ids), 10000)

    def test_resultLessThanMin(self):
        with Attribute(
                {'max_num': 20000, 'min_num': 14395, 'length': self.__length, 'unit': self.__unit},
                ['ID', 'HGQY_DM']) as attribute:
            ids = attribute.attributesFilter(self.__cksp_dm, self.__zmy)
            self.assertGreaterEqual(len(ids), 20000)

    def test_outOfColumns(self):
        with Attribute(
                {'max_num': 100, 'min_num': 0, 'length': self.__length, 'unit': self.__unit},
                ['ID', 'HGQY_DM']) as attribute:
            ids = attribute.attributesFilter(self.__cksp_dm, self.__zmy)
            self.assertLessEqual(len(ids), 200)


if __name__ == '__main__':
    unittest.main()
