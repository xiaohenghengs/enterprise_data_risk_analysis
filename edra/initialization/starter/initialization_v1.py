import sys

sys.path.append(r'../../../../enterprise_data_risk_analysis')
import numpy
import threading
from threading import Thread
from utils.logging_operate import LoggingOperate
from utils.database_operate import DataBaseOperate
from edra.initialization.models.target_customs_declaration_v1 import CustomsDeclarationTarget
from utils.dynamic_range_quantile import DynamicRangeQuantile
from utils.utils import listOfGroups

logger = LoggingOperate('initialization_v1')


def initCustomsDeclarationTarget():
    global init_quantile_cksl, init_quantile_mylaj_sb, init_quantile_ckjhje, init_quantile_jsje, init_quantile_zsse, \
        init_quantile_ytse, init_quantile_mylaj, init_quantile_sl1, init_quantile_sl2, init_quantile_sbsl, \
        init_quantile_mz_2, init_quantile_jz, init_quantile_fobdj, total_trade_price
    # create target table if not exist
    CustomsDeclarationTarget.createTable()
    with DataBaseOperate() as db:
        # init quantile class
        quantile_columns_data = db.query_all(
            """
                SELECT round(CKSL, 4)        as CKSL,
                       round(MYLAJ_SB, 4)    as MYLAJ_SB,
                       round(CKJHJE, 4)      as CKJHJE,
                       round(JSJE, 4)        as JSJE,
                       round(ZSSE, 4)        as ZSSE,
                       round(YTSE, 4)        as YTSE,
                       round(MYLAJ, 4)       as MYLAJ,
                       round(SL1, 4)         as SL1,
                       round(SL2, 4)         as SL2,
                       round(SBSL, 4)        as SBSL,
                       round(MZ_2, 4)        as MZ_2,
                       round(JZ, 4)          as JZ,
                       round(MYLAJ / SL1, 4) as FOBDJ
                FROM customs_declaration_v1
            """)
    data_frame = numpy.array(quantile_columns_data)
    init_quantile_cksl = DynamicRangeQuantile(list(data_frame[:, 0]), 10, 4, None, 2)
    init_quantile_mylaj_sb = DynamicRangeQuantile(list(data_frame[:, 1]), 10, 4, None, 2)
    init_quantile_ckjhje = DynamicRangeQuantile(list(data_frame[:, 2]), 10, 4, None, 2)
    init_quantile_jsje = DynamicRangeQuantile(list(data_frame[:, 3]), 10, 4, None, 2)
    init_quantile_zsse = DynamicRangeQuantile(list(data_frame[:, 4]), 10, 4, None, 2)
    init_quantile_ytse = DynamicRangeQuantile(list(data_frame[:, 5]), 10, 4, None, 2)
    init_quantile_mylaj = DynamicRangeQuantile(list(data_frame[:, 6]), 10, 4, None, 2)
    init_quantile_sl1 = DynamicRangeQuantile(list(data_frame[:, 7]), 10, 4, None, 2)
    init_quantile_sl2 = DynamicRangeQuantile(list(data_frame[:, 8]), 10, 4, None, 2)
    init_quantile_sbsl = DynamicRangeQuantile(list(data_frame[:, 9]), 10, 4, None, 2)
    init_quantile_mz_2 = DynamicRangeQuantile(list(data_frame[:, 10]), 10, 4, None, 2)
    init_quantile_jz = DynamicRangeQuantile(list(data_frame[:, 11]), 10, 4, None, 2)
    fob = [x for x in list(data_frame[:, 12]) if x]
    init_quantile_fobdj = DynamicRangeQuantile(fob, 10, 4, None, 2)
    with DataBaseOperate() as db:
        # query data group by enterprise
        customs_enterprise_codes = db.query_all(
            """
            SELECT HGQYDM, DATE_FORMAT(CKRQ_1, '%Y') AS YEAR
            FROM customs_declaration_v1
            GROUP BY HGQYDM, DATE_FORMAT(CKRQ_1, '%Y')
            """)
        logger.info('????????????????????????????????????????????? %d ?????????' % len(customs_enterprise_codes))
        # query total trade price by enterprise
        total_trade_price = db.query_all(
            """
            SELECT HGQYDM, DATE_FORMAT(CKRQ_1, '%Y') AS YEAR, ROUND(SUM(MYLAJ), 4) AS ZMY
            FROM customs_declaration_v1
            WHERE CKRQ_1 IS NOT NULL
            GROUP BY HGQYDM, DATE_FORMAT(CKRQ_1, '%Y')
            """)
        total_trade_price = numpy.array(total_trade_price)
        logger.info('?????????????????????????????????????????????????????????????????????')
        customs_group = listOfGroups(customs_enterprise_codes, 150)
        threads = []
        for customs in customs_group:
            threads.append(Thread(target=threadInitTarget, args=(customs,)))
        for thread in threads:
            thread.start()
        for thread in threads:
            thread.join()


def threadInitTarget(codes_year):
    thread_name = threading.current_thread().name
    logger.info('??????????????????%s start!' % thread_name)
    for index, code_year in enumerate(codes_year):
        code = code_year[0]
        year = code_year[1]
        logger.info('?????????thread???%s ?????? %d ?????????????????????????????????%s' % (thread_name, index, code_year))
        with DataBaseOperate() as db:
            # query data by customs code
            sql = """
                    SELECT  HGQYDM, SBNY, PC, TSJGDM, GLH, CKFPH, BGDH, SPDM, SPMC, JLDW, CKSL, MYLAJ_SB, CKJHJE, DRRQ, 
                            NSRSBH, DAH, JLDWDM, ZGLH, SZ, JHPZH, GFNSRSBH, KPRQ, JSJE, ZSSL, ZSSE, TSL, YTSE, HYD_SJ,
                            HYD_DSJ, HYD_QXJ, CKRQ_1, MYLAJ, RMB, SL1, SL2, SBSL, MZ_2, JZ, ZZMDGDQSZ_DM, YSFS_DM,
                            ZYG_DM, HGCJFS_DM, JHFS_DM, HZDWDQ_DM, HGGQKA_DM, SBDW_DM, SBDWMC,
                            TIMESTAMPDIFF(MONTH, DATE_FORMAT(CKRQ_1, '%Y-%m-%d'), CONCAT(LEFT(SBNY, 4), '-', RIGHT(SBNY, 2), '-01')) AS DIFF,
                            round(MYLAJ / SL1, 4) as FOBDJ, DATE_FORMAT(CKRQ_1, '%Y') AS CKN
                    FROM customs_declaration_v1
                    WHERE HGQYDM = '""" + code + """'"""
            if year:
                sql += " AND DATE_FORMAT(CKRQ_1, '%Y') = '" + year + "'"
            else:
                sql += ' AND CKRQ_1 IS NULL'
            details = db.query_all(sql)
        logger.info('?????????thread???%s ??????????????????????????? %d ???????????????????????????????????????' % (thread_name, len(details)))
        details_group = listOfGroups(details, 1000)
        threads = []
        for details in details_group:
            threads.append(Thread(target=saveTarget, args=(details,)))
        for thread in threads:
            thread.start()
        for thread in threads:
            thread.join()


def saveTarget(details):
    thread_name = threading.current_thread().name
    details_frame = numpy.array(details)
    customs = CustomsDeclarationTarget()
    for detail in details_frame:
        customs.hgqydm = detail[0]
        zmy_frame = total_trade_price[total_trade_price[:, 0] == detail[0]][:, 1:3]
        customs.sbny = detail[1]
        customs.pc = detail[2]
        customs.tsjgdm = detail[3]
        customs.glh = detail[4]
        customs.ckfph = detail[5]
        customs.bgdh = detail[6]
        customs.spdm = detail[7]
        customs.spmc = detail[8]
        customs.jldw = detail[9]
        customs.cksl_decile = init_quantile_cksl.intervalNum(detail[10])
        customs.mylaj_sb_decile = init_quantile_mylaj_sb.intervalNum(detail[11])
        customs.ckjhje_decile = init_quantile_ckjhje.intervalNum(detail[12])
        customs.drrq = detail[13]
        customs.nsrsbh = detail[14]
        customs.dah = detail[15]
        customs.jldwdm = detail[16]
        customs.zglh = detail[17]
        customs.sz = detail[18]
        customs.jhpzh = detail[19]
        customs.gfnsrsbh = detail[20]
        customs.kprq = detail[21]
        customs.jsje_decile = init_quantile_jsje.intervalNum(detail[22])
        customs.zssl = detail[23]
        customs.zsse_decile = init_quantile_zsse.intervalNum(detail[24])
        customs.tsl = detail[25]
        customs.ytse_decile = init_quantile_ytse.intervalNum(detail[24])
        customs.hyd_sj = detail[27]
        customs.hyd_dsj = detail[28]
        customs.hyd_qxj = detail[29]
        customs.ckrq_1 = detail[30]
        customs.mylaj = detail[31]
        customs.mylaj_decile = init_quantile_mylaj.intervalNum(detail[31])
        customs.rmb = detail[32]
        customs.sl1_decile = init_quantile_sl1.intervalNum(detail[33])
        customs.sl2_decile = init_quantile_sl2.intervalNum(detail[34])
        customs.sbsl_decile = init_quantile_sbsl.intervalNum(detail[35])
        customs.mz_2_decile = init_quantile_mz_2.intervalNum(detail[36])
        customs.jz_decile = init_quantile_jz.intervalNum(detail[37])
        customs.zzmdgdqsz_dm = detail[38]
        customs.ysfs_dm = detail[39]
        customs.zyg_dm = detail[40]
        customs.hgcjfs_dm = detail[41]
        customs.jhfs_dm = detail[42]
        customs.hzdwdq_dm = detail[43]
        customs.hggqka_dm = detail[44]
        customs.sbdw_dm = detail[45]
        customs.sbdwmc = detail[46]
        customs.ck_sb_diff = detail[47]
        customs.fobdj_decile = init_quantile_fobdj.intervalNum(detail[48]) if detail[48] else ''
        customs.zmy = float(zmy_frame[zmy_frame[:, 0] == detail[49]][:, 1]) if detail[49] else 0
        customs.addList(customs.toList())
    customs.save()
    logger.info('?????????thread???%s ????????????????????????????????????' % thread_name)


global init_quantile_cksl, init_quantile_mylaj_sb, init_quantile_ckjhje, init_quantile_jsje, init_quantile_zsse, \
    init_quantile_ytse, init_quantile_mylaj, init_quantile_sl1, init_quantile_sl2, init_quantile_sbsl, \
    init_quantile_mz_2, init_quantile_jz, init_quantile_fobdj, total_trade_price
if __name__ == '__main__':
    initCustomsDeclarationTarget()
