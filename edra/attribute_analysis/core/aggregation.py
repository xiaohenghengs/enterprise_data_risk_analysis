import uuid

from conf import table
from edra.attribute_analysis.models.attribute_items import AttributeItem
from utils.database_operate import DataBaseOperate
from utils.logging_operate import LoggingOperate

logger = LoggingOperate("aggregation")


class Aggregation:
    """
        待分析数据聚合
    """

    def __init__(self, max_num, min_num):
        self.__max_num = max_num
        self.__min_num = min_num
        self.__cksp_dm_length = 10
        self.__zmy_unit = 10000
        self.__unit_switch = False
        self.__length_switch = True

    def getItems(self):
        logger.info("》》》》》》开始全部数据属性分类")
        AttributeItem.createAttributeItems()
        while True:
            logger.info("》》》参数：length %s ；unit %s" % (self.__cksp_dm_length, self.__zmy_unit))
            if self.__cksp_dm_length == 0:
                logger.info("》》》Done！")
                break
            items = self.attributeItems()
            if items:
                logger.info("》》》查询到 %s 个分类" % len(items))
                attributeItem = AttributeItem()
                for index, item in enumerate(items):
                    count = item[2]
                    cksp_dm = item[0]
                    zmy = item[1]
                    attributeItem.self_id = str(uuid.uuid1()).replace('-', '')
                    attributeItem.cksp_dm = cksp_dm
                    attributeItem.zmy = zmy
                    attributeItem.count = count
                    attributeItem.cksp_dm_length = self.__cksp_dm_length
                    attributeItem.zmy_unit = self.__zmy_unit
                    data_ids = self.getAttributeItems(['ID'], cksp_dm, zmy)
                    if count <= self.__max_num:
                        attributeItem.data_ids = data_ids
                    else:
                        logger.info("》》》商品代码：%s，企业规模：%s ，分类数据集数： %s ，超出阈值，开始属性筛选" % (cksp_dm, str(zmy), str(count)))
                        # attribute analysis
                        from edra.attribute_analysis.core.attribute import Attribute
                        attributeItem.data_ids = Attribute(
                            {'max_num': self.__max_num, 'min_num': self.__min_num, 'length': self.__cksp_dm_length,
                             'unit': self.__zmy_unit}, ['ID', 'HGQY_DM']).attributesFilter(cksp_dm, zmy)
                        logger.info("》》》属性筛选剩 %s 个数据" % (len(attributeItem.data_ids)))
                    attributeItem.addList(attributeItem.toList())
                attributeItem.save()
            else:
                logger.info("》》》没有符合阈值的分类数据，开始 KEY 属性退位")
                if self.__unit_switch:
                    self.__zmy_unit = self.__zmy_unit * 10
                    self.__unit_switch = False
                    self.__length_switch = True
                elif self.__length_switch:
                    self.__cksp_dm_length = self.__cksp_dm_length - 2
                    self.__length_switch = False
                    self.__unit_switch = True
                logger.info("》》》退位后参数：length=%s；unit=%s" % (self.__cksp_dm_length, self.__zmy_unit))

    def attributeItems(self):
        sql = """
            select tt.*
            from (
                     select t.CKSP_DM, t.ZMY, count(*) as COUNT
                     from (
                              select ID,
                                     left(c.CKSP_DM, %s)        as CKSP_DM,
                                     TRUNCATE(c.ZMY / %s, 0) as ZMY
                              from %s c
                              where not exists (select 1 from attribute_items_details a where a.DATA_ID = C.ID)
                          ) t
                     group by t.CKSP_DM, t.ZMY
                 ) tt
            where tt.count >= %s
            """ % (self.__cksp_dm_length, self.__zmy_unit, table['target'], self.__min_num)
        with DataBaseOperate() as db:
            return db.query_all(sql)

    def getAttributeItems(self, columns, cksp_dm, zmy, length=None, unit=None, conditions=None):
        with DataBaseOperate() as db:
            sql = """select %s
                     from %s
                     where left(CKSP_DM, %s) = '%s'
                     and TRUNCATE(ZMY / %s, 0) = %s
                """ % (','.join(columns), table['target'], length if length else self.__cksp_dm_length, cksp_dm,
                       unit if unit else self.__zmy_unit, zmy)
            if conditions:
                sql += conditions
            return db.query_all(sql)
