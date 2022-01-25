import pandas as pd

from utils.database_operate import DataBaseOperate


def loadSupplierInfo():
    """
    获取全部供应商
    :return: supplier_info
    """
    global main_price_array, hs_code_array, customs_code_array
    with DataBaseOperate() as db:
        main_price = db.query_all(
            """
                SELECT GFNSRSBH, DATE_FORMAT(CKRQ_1, '%Y%m') AS NY, ROUND(SUM(JSJE), 4) AS ZMY
                FROM customs_declaration_v1
                GROUP BY GFNSRSBH, DATE_FORMAT(CKRQ_1, '%Y%m') limit 500
            """)
        main_price_array = pd.DataFrame(main_price, columns=['GFNSRSBH', 'NY', 'ZMY'])
        hs_code = db.query_all(
            """
                SELECT GFNSRSBH,
                       DATE_FORMAT(CKRQ_1, '%Y%m') AS NY,
                       LEFT(SPDM, 2)               AS HS,
                       COUNT(*)                    AS COUNT,
                       ROUND(SUM(JSJE), 4)         AS ZMY
                FROM customs_declaration_v1
                GROUP BY GFNSRSBH, DATE_FORMAT(CKRQ_1, '%Y%m'), LEFT(SPDM, 2) limit 500
            """)
        hs_code_array = pd.DataFrame(hs_code, columns=['GFNSRSBH', 'NY', 'HS', 'COUNT', 'ZMY'])
        customs_code = db.query_all(
            """
                SELECT GFNSRSBH,
                       DATE_FORMAT(CKRQ_1, '%Y%m') AS NY,
                       HGQYDM,
                       COUNT(*)                    AS COUNT,
                       ROUND(SUM(JSJE), 4)         AS ZMY
                FROM customs_declaration_v1
                GROUP BY GFNSRSBH, DATE_FORMAT(CKRQ_1, '%Y%m'), HGQYDM limit 500
            """)
        customs_code_array = pd.DataFrame(customs_code, columns=['GFNSRSBH', 'NY', 'HGQYDM', 'COUNT', 'ZMY'])


global main_price_array, hs_code_array, customs_code_array
# 供货方企业初始化
if __name__ == '__main__':
    loadSupplierInfo()
    supplier = list()
    for mp in main_price_array.values:
        row = list(mp)
        gfnsrsbh = mp[0]
        ny = mp[1]
        target_hs_code_array = hs_code_array[(hs_code_array.GFNSRSBH == gfnsrsbh) & (hs_code_array.NY == ny)]
        target_hs_code = target_hs_code_array[target_hs_code_array.ZMY.argmax():]
        row.append(target_hs_code.HS.values[0])
        row.append(target_hs_code.COUNT.values[0])
        row.append(target_hs_code.ZMY.values[0])
        target_customs_code_array = customs_code_array[
            (customs_code_array.GFNSRSBH == gfnsrsbh) & (customs_code_array.NY == ny)]
        target_customs_code = target_customs_code_array[target_customs_code_array.ZMY.argmax():]
        row.append(target_customs_code.HGQYDM.values[0])
        row.append(target_customs_code.COUNT.values[0])
        row.append(target_customs_code.ZMY.values[0])
        supplier.append(row)
