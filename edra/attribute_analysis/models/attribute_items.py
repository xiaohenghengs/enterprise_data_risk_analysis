from threading import Thread

from edra.attribute_analysis.models import sqlCreateAttributeItems, sqlCreateAttributeItemsDetails
from utils.database_operate import DataBaseOperate
from utils.utils import listOfGroups


class AttributeItem:
    """
    属性集合详情实体
    """

    def __init__(self, self_id=None, cksp_dm=None, zmy=None, count=None, cksp_dm_length=None, zmy_unit=None,
                 data_ids=None):
        self.__self_id = self_id
        self.__cksp_dm = cksp_dm
        self.__zmy = zmy
        self.__count = count
        self.__cksp_dm_length = cksp_dm_length
        self.__zmy_unit = zmy_unit
        self.__data_ids = data_ids
        self.__list = list()

    @property
    def self_id(self):
        return self.__self_id

    @self_id.setter
    def self_id(self, self_id):
        self.__self_id = self_id

    @property
    def cksp_dm(self):
        return self.__cksp_dm

    @cksp_dm.setter
    def cksp_dm(self, cksp_dm):
        self.__cksp_dm = cksp_dm

    @property
    def zmy(self):
        return self.__zmy

    @zmy.setter
    def zmy(self, zmy):
        self.__zmy = zmy

    @property
    def count(self):
        return self.__count

    @count.setter
    def count(self, count):
        self.__count = count

    @property
    def cksp_dm_length(self):
        return self.__cksp_dm_length

    @cksp_dm_length.setter
    def cksp_dm_length(self, cksp_dm_length):
        self.__cksp_dm_length = cksp_dm_length

    @property
    def zmy_unit(self):
        return self.__zmy_unit

    @zmy_unit.setter
    def zmy_unit(self, zmy_unit):
        self.__zmy_unit = zmy_unit

    @property
    def data_ids(self):
        return self.__data_ids

    @data_ids.setter
    def data_ids(self, data_ids):
        self.__data_ids = data_ids

    @staticmethod
    def createAttributeItems():
        with DataBaseOperate() as db:
            db.execute_sql(sqlCreateAttributeItems())
            db.execute_sql(sqlCreateAttributeItemsDetails())

    def addList(self, record):
        self.__list.append(record)

    def toList(self):
        return [self.__self_id, self.__cksp_dm, self.__zmy, self.__count, self.__cksp_dm_length, self.__zmy_unit,
                self.__data_ids]

    def save(self):
        with DataBaseOperate() as db:
            for item in self.__list:
                db.execute_sql(
                    """
                        INSERT INTO attribute_items (id, cksp_dm, zmy, count, cksp_dm_length, zmy_unit)
                        VALUES ('%s', %s, %s, %s, %s, %s)
                    """ % tuple(item[0:6]))
                data_ids = item[-1]
                ids_list = listOfGroups(data_ids, 500)
                threads = []
                for ids in ids_list:
                    threads.append(Thread(target=self.saveDetails, args=(item[0], ids,)))
                for thread in threads:
                    thread.start()
                for thread in threads:
                    thread.join()

    @staticmethod
    def saveDetails(item_id, ids):
        set_item = [[item_id, str(x[0])] for x in ids]
        with DataBaseOperate() as db:
            db.executemany_sql("""INSERT INTO attribute_items_details (items_id, data_id)
                                    VALUES (?, ?)
                                """, set_item)
