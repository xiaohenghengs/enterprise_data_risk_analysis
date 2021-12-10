import unittest

from edra.attribute_analysis.core.attribute import Attribute


class DataModelTest(unittest.TestCase):
    def setUp(self) -> None:
        self.__data_model = Attribute(1)

    def tearDown(self) -> None:
        pass

    def test_getColumns(self):
        self.assertIsNotNone(self.__data_model.getColumns())

    def test_getMostCountColumnWithColumns(self):
        return self.__data_model.getMostCountColumnWithColumns()

    def test_saveDropColumn(self):
        __most_count, __column_length = self.test_getMostCountColumnWithColumns()
        self.__data_model.saveDropColumn(__most_count)


if __name__ == '__main__':
    unittest.main()
