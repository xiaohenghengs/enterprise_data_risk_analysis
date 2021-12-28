import unittest

from edra.attribute_analysis.core.aggregation import Aggregation


class aggregationTest(unittest.TestCase):
    def setUp(self) -> None:
        self.aggregation = Aggregation(300000, 10000)

    def tearDown(self) -> None:
        pass

    def test_getItems(self):
        self.aggregation.getItems()

    def test_getAttrItem(self):
        items = self.aggregation.attributeItems()
        self.assertIsNotNone(items)
        return items


if __name__ == '__main__':
    unittest.main()
