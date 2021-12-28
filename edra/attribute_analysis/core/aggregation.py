from conf import table
from edra.attribute_analysis.core.attribute import Attribute
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
        self.__ids = list()
        self.__unit_switch = True
        self.__length_switch = False

    def getItems(self):
        logger.info("》》》》》》开始全部数据属性分类")
        AttributeItem.createAttributeItems()
        while True:
            logger.info("》》》参数：length=%s；unit=%s" % (self.__cksp_dm_length, self.__zmy_unit))
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
                    attributeItem.cksp_dm = cksp_dm
                    attributeItem.zmy = zmy
                    attributeItem.count = count
                    attributeItem.cksp_dm_length = self.__cksp_dm_length
                    attributeItem.zmy_unit = self.__zmy_unit
                    data_ids = self.getAttributeIds(cksp_dm, zmy)
                    if count <= self.__max_num:
                        attributeItem.data_ids = data_ids
                    else:
                        logger.info("》》》商品代码：%s，企业规模：%s ，分类数据集数： %s ，超出阈值，开始属性筛选" % (cksp_dm, str(zmy), str(count)))
                        # attribute analysis
                        ids = Attribute(data_ids, self.__max_num, self.__min_num, ['ID', 'HGQY_DM']).attributesFilter()
                        logger.info("》》》属性筛选剩 %s 个数据" % (len(ids)))
                        attributeItem.data_ids = ','.join([str(x) for x in ids])
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
                              where ID NOT IN (%s)
                          ) t
                     group by t.CKSP_DM, t.ZMY
                 ) tt
            where tt.count >= %s
            """ % (self.__cksp_dm_length, self.__zmy_unit, table['target'], ','.join(self.__ids) if self.__ids else 0,
                   self.__min_num)
        with DataBaseOperate() as db:
            return db.query_all(sql)

    def getAttributeIds(self, cksp_dm, zmy):
        with DataBaseOperate() as db:
            ids = db.query_all("""
                        select ID
                        from %s c
                        where left(c.CKSP_DM, %s) = '%s'
                          and TRUNCATE(c.ZMY / %s, 0) = %s
                        """ % (table['target'], self.__cksp_dm_length, cksp_dm, self.__zmy_unit, zmy))
            ids = [str(x[0]) for x in ids]
            self.__ids.extend(ids)
        return ','.join(ids)
