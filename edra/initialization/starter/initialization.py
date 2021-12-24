import sys

sys.path.append(r'../../../../enterprise_data_risk_analysis')
import threading
from threading import Thread

import numpy

from conf import table
from edra.initialization.models.target_customs_declaration import CustomsDeclarationTarget
from utils.database_operate import DataBaseOperate
from utils.logging_operate import LoggingOperate
from utils.quantile import Quantile

columns = ['HGQY_DM', 'CKSL', 'MYLAJ', 'MYLAJ / CKSL AS FOBDJ', 'MZ_2', 'JZ', 'CKNY', 'ZZMDGDQSZ_DM', 'YSFS_DM',
           'ZYG_DM', 'HGCJFS_DM', 'JHFS_DM', 'YFJSFS_DM', 'BFJSFS_DM', 'ZFJSFS_DM', 'QYGBZ', 'HZDWDQ_DM', 'HGGQKA_DM',
           'CKSP_DM']
quantile_columns = ['CKSL', 'MYLAJ', 'MYLAJ / CKSL as FOBDJ', 'ifnull(MZ_2,-1)', 'ifnull(JZ,-1)']

logger = LoggingOperate('initialization')

global init_quantile_cksl, init_quantile_mylaj, init_quantile_fobdj, init_quantile_mz, init_quantile_jz


def initCustomsDeclarationTarget():
    global init_quantile_cksl, init_quantile_mylaj, init_quantile_fobdj, init_quantile_mz, init_quantile_jz
    # create target table if not exist
    CustomsDeclarationTarget.createTable()
    with DataBaseOperate() as qcd_db:
        # init quantile class
        quantile_columns_data = qcd_db.query_all(
            'SELECT %s FROM %s' % (','.join(quantile_columns), table['basic']))
        data_frame = numpy.array(quantile_columns_data)
        init_quantile_cksl = Quantile(list(data_frame[:, 0]))
        init_quantile_mylaj = Quantile(list(data_frame[:, 1]))
        init_quantile_fobdj = Quantile(list(data_frame[:, 2]))
        mz_list = [x for x in list(data_frame[:, 3]) if x != -1]
        init_quantile_mz = Quantile(mz_list)
        jz_list = [x for x in list(data_frame[:, 4]) if x != -1]
        init_quantile_jz = Quantile(jz_list)
    with DataBaseOperate() as cec_db:
        # query data group by enterprise
        customs_enterprise_codes = cec_db.query_all('SELECT HGQY_DM FROM %s GROUP BY HGQY_DM' % table['basic'])
        logger.info('》》》》》》》》》查询获取全部 %d 家企业' % len(customs_enterprise_codes))
        customs_group = listOfGroups(customs_enterprise_codes, 250)
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
    global init_quantile_cksl, init_quantile_mylaj, init_quantile_fobdj, init_quantile_mz, init_quantile_jz
    for index, code in enumerate(customs_enterprise_codes):
        code = code[0]
        logger.info('》》》thread：%s ，第 %d 家企业，海关企业代码：%s' % (thread_name, index, code))
        with DataBaseOperate() as d_db:
            # query data by customs code
            details = d_db.query_all(
                """
                SELECT %s
                FROM %s
                WHERE HGQY_DM = '%s'
                """ % (','.join(columns), table['basic'], code)
            )
            logger.info('》》》thread：%s ，查询到该企业含有 %d 条报关单明细，开始明细分析' % (thread_name, len(details)))
            cky = d_db.query_all(
                """
                SELECT LEFT(CKNY,4) AS CKY,SUM(MYLAJ) AS ZMY
                FROM %s
                WHERE HGQY_DM = '%s'
                GROUP BY CKY
                """ % (table['basic'], code)
            )
            details_frame = numpy.array(details)
            zmy_frame = numpy.array(cky)
            customs = CustomsDeclarationTarget()
            for detail in details_frame:
                ckny = detail[6]
                customs.hgqy_dm = detail[0]
                customs.cksl_decile = init_quantile_cksl.decileIntervalNum(detail[1]) if detail[1] else ''
                customs.mylaj_decile = init_quantile_mylaj.decileIntervalNum(detail[2]) if detail[2] else ''
                customs.fobdj_decile = init_quantile_fobdj.decileIntervalNum(detail[3]) if detail[3] else ''
                customs.mz_2_decile = init_quantile_mz.decileIntervalNum(detail[4]) if detail[4] else ''
                customs.jz_decile = init_quantile_jz.decileIntervalNum(detail[5]) if detail[5] else ''
                customs.cky = ckny[4:]
                year = ckny[0:4]
                customs.zzmdgdqsz_dm = detail[7] if detail[7] else ''
                customs.ysfs_dm = detail[8] if detail[8] else ''
                customs.zyg_dm = detail[9] if detail[9] else ''
                customs.hgcjfs_dm = detail[10] if detail[10] else ''
                customs.jhfs_dm = detail[11] if detail[11] else ''
                customs.yfjsfs_dm = detail[12] if detail[12] else ''
                customs.bfjsfs_dm = detail[13] if detail[13] else ''
                customs.zfjsfs_dm = detail[14] if detail[14] else ''
                customs.qygbz = detail[15] if detail[15] else ''
                customs.hzdwdq_dm = detail[16] if detail[16] else ''
                customs.hggqka_dm = detail[17] if detail[17] else ''
                customs.cksp_dm = detail[18] if detail[18] else ''
                customs.zmy = float(zmy_frame[zmy_frame[:, 0] == year][:, 1])
                customs.addList(customs.toList())
            customs.save()
            logger.info('》》》thread：%s ，目标明细数据保存成功！' % thread_name)


def listOfGroups(init_list, children_list_len):
    list_of_groups = zip(*(iter(init_list),) * children_list_len)
    end_list = [list(i) for i in list_of_groups]
    count = len(init_list) % children_list_len
    end_list.append(init_list[-count:]) if count != 0 else end_list
    return end_list


if __name__ == '__main__':
    initCustomsDeclarationTarget()
