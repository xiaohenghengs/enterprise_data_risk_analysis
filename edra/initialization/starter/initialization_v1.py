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
    global init_quantile_cksl, init_quantile_mylaj_sb, init_quantile_ckjhje, init_quantile_jsje, init_quantile_zsse, init_quantile_ytse, init_quantile_mylaj, init_quantile_sl1, init_quantile_sl2, init_quantile_sbsl, init_quantile_mz_2, init_quantile_jz, init_quantile_fobdj
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
                FROM customs_declaration_v1 limit 500
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
        customs_enterprise_codes = db.query_all('SELECT HGQYDM FROM customs_declaration_v1 GROUP BY HGQYDM')
        logger.info('》》》》》》》》》查询获取全部 %d 家企业' % len(customs_enterprise_codes))
        customs_group = listOfGroups(customs_enterprise_codes, 75)
        threads = []
        for customs in customs_group:
            threads.append(Thread(target=threadInitTarget, args=(customs,)))
        for thread in threads:
            thread.start()
        for thread in threads:
            thread.join()


def threadInitTarget(customs_enterprise_codes):
    thread_name = threading.current_thread().name
    logger.info('》》》》》》%s start!' % thread_name)
    global init_quantile_cksl, init_quantile_mylaj_sb, init_quantile_ckjhje, init_quantile_jsje, init_quantile_zsse, init_quantile_ytse, init_quantile_mylaj, init_quantile_sl1, init_quantile_sl2, init_quantile_sbsl, init_quantile_mz_2, init_quantile_jz, init_quantile_fobdj
    for index, code in enumerate(customs_enterprise_codes):
        code = code[0]
        logger.info('》》》thread：%s ，第 %d 家企业，海关企业代码：%s' % (thread_name, index, code))
        with DataBaseOperate() as db:
            # query data by customs code
            details = db.query_all(
                """
                    SELECT  HGQYDM, SBNY, PC, TSJGDM, GLH, CKFPH, BGDH, SPDM, SPMC, JLDW, CKSL, MYLAJ_SB, CKJHJE, DRRQ, 
                            NSRSBH, DAH, JLDWDM, ZGLH, SZ, JHPZH, GFNSRSBH, KPRQ, JSJE, ZSSL, ZSSE, TSL, YTSE, HYD_SJ,
                            HYD_DSJ, HYD_QXJ, CKRQ_1, MYLAJ, RMB, SL1, SL2, SBSL, MZ_2, JZ, ZZMDGDQSZ_DM, YSFS_DM,
                            ZYG_DM, HGCJFS_DM, JHFS_DM, HZDWDQ_DM, HGGQKA_DM, SBDW_DM, SBDWMC,
                            TIMESTAMPDIFF(MONTH, DATE_FORMAT(CKRQ_1, '%Y-%m-%d'), CONCAT(LEFT(SBNY, 4), '-', RIGHT(SBNY, 2), '-01')) AS DIFF,
                            round(MYLAJ / SL1, 4) as FOBDJ, DATE_FORMAT(CKRQ_1, '%Y') AS CKN
                    FROM customs_declaration_v1 
                    WHERE HGQYDM = '""" + code + "'")
            logger.info('》》》thread：%s ，查询到该企业含有 %d 条报关单明细，开始明细分析' % (thread_name, len(details)))
            zmy = db.query_all("""
                                  SELECT DATE_FORMAT(CKRQ_1, '%Y') AS YEAR, 
                                         ROUND(SUM(MYLAJ), 4) AS ZMY
                                  FROM customs_declaration_v1
                                  WHERE HGQYDM = '""" + code + """'
                                    AND CKRQ_1 IS NOT NULL
                                  GROUP BY DATE_FORMAT(CKRQ_1, '%Y')
                                """)
            details_frame = numpy.array(details)
            zmy_frame = numpy.array(zmy)
        customs = CustomsDeclarationTarget()
        for detail in details_frame:
            customs.hgqydm = detail[0]
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
        logger.info('》》》thread：%s ，目标明细数据保存成功！' % thread_name)


global init_quantile_cksl, init_quantile_mylaj_sb, init_quantile_ckjhje, init_quantile_jsje, init_quantile_zsse, init_quantile_ytse, init_quantile_mylaj, init_quantile_sl1, init_quantile_sl2, init_quantile_sbsl, init_quantile_mz_2, init_quantile_jz, init_quantile_fobdj
if __name__ == '__main__':
    initCustomsDeclarationTarget()
