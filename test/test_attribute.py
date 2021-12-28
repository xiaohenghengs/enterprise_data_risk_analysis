import unittest

from edra.attribute_analysis.core.aggregation import Aggregation
from edra.attribute_analysis.core.attribute import Attribute


class AttributeTest(unittest.TestCase):
    def setUp(self) -> None:
        self.__max_num = 20000
        self.__min_num = 10000
        self.__ids = Aggregation(self.__max_num, self.__min_num).getAttributeIds(4907009011, 22697)

    def tearDown(self) -> None:
        pass

    def test_attributesFilter(self):
        with Attribute(self.__ids, self.__max_num, self.__min_num, ['ID', 'HGQY_DM']) as attribute:
            ids = attribute.attributesFilter()
            self.assertLessEqual(len(ids), self.__max_num)
            self.assertGreaterEqual(len(ids), self.__min_num)

    def test_resultLessThanMin(self):
        with Attribute(self.__ids, 20000, 14395, ['ID', 'HGQY_DM']) as attribute:
            ids = attribute.attributesFilter()
            self.assertGreaterEqual(len(ids), self.__max_num)

    def test_outOfColumns(self):
        with Attribute(self.__ids, 100, 0, ['ID', 'HGQY_DM']) as attribute:
            ids = attribute.attributesFilter()
            self.assertLessEqual(len(ids), 200)


if __name__ == '__main__':
    unittest.main()
