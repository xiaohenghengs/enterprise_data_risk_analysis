import pandas as pd

from edra.rules_analysis.core.hscode import HsCodeHandler
from utils.logging_operate import LoggingOperate
from utils.sqlite_operate import SqliteOperate

sqlite = SqliteOperate()
hsCode_handler = HsCodeHandler()
logger = LoggingOperate('rules_analysis_hsCode_main')

if __name__ == '__main__':
    hs_code = hsCode_handler.hs_codes
    result = list()
    for hs in hs_code:
        single_result = list()
        data_ids = sqlite.query_all("SELECT id FROM raw_data WHERE CKSP_DM = '%s'" % hs)
        ids = [str(x[0]) for x in data_ids]
        len_ids = len(ids)
        s1, s2, abnormal_attr_counter = hsCode_handler.handleSingleHsData(ids)
        single_result.append(str(hs))
        single_result.append(str(len_ids))
        single_result.append(str(round(s1 / len_ids, 2)))
        single_result.append(str(round(s2 / len_ids, 2)))
        attribute = dict()
        for counter in abnormal_attr_counter:
            attribute[counter] = round(abnormal_attr_counter[counter] / len_ids, 2)
        single_result.append(str(attribute))
        result.append(single_result)
    data_frame = pd.DataFrame(result, columns=('商品编码', '明细条数', '1>SCORE>=0.9', 'SCORE<0.9', '异常属性概率'))
    data_frame.to_excel('分析结果.xlsx', index=False)
