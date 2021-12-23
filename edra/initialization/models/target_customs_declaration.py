from edra.initialization.models import sqlCreateTableCustomsDeclaration
from utils.database_operate import DataBaseOperate


class CustomsDeclarationTarget:
    """
    报关单明细目标数据实体
    """

    def __init__(self, hgqy_dm=None, cksl_decile=None, mylaj_decile=None, fobdj_decile=None, mz_2_decile=None,
                 jz_decile=None, cky=None, zzmdgdqsz_dm=None, ysfs_dm=None, zyg_dm=None, hgcjfs_dm=None, jhfs_dm=None,
                 yfjsfs_dm=None, bfjsfs_dm=None, zfjsfs_dm=None, qygbz=None, hzdwdq_dm=None, hggqka_dm=None, zmy=None):
        self.__hgqy_dm = hgqy_dm
        self.__cky = cky
        self.__zmy = zmy
        self.__cksl_decile = cksl_decile
        self.__mylaj_decile = mylaj_decile
        self.__fobdj_decile = fobdj_decile
        self.__mz_2_decile = mz_2_decile
        self.__jz_decile = jz_decile
        self.__zzmdgdqsz_dm = zzmdgdqsz_dm
        self.__ysfs_dm = ysfs_dm
        self.__zyg_dm = zyg_dm
        self.__hgcjfs_dm = hgcjfs_dm
        self.__jhfs_dm = jhfs_dm
        self.__yfjsfs_dm = yfjsfs_dm
        self.__bfjsfs_dm = bfjsfs_dm
        self.__zfjsfs_dm = zfjsfs_dm
        self.__qygbz = qygbz
        self.__hzdwdq_dm = hzdwdq_dm
        self.__hggqka_dm = hggqka_dm
        self.__list = list()

    @property
    def hgqy_dm(self):
        return self.__hgqy_dm

    @hgqy_dm.setter
    def hgqy_dm(self, hgqy_dm):
        self.__hgqy_dm = hgqy_dm

    @property
    def cksl_decile(self):
        return self.__cksl_decile

    @cksl_decile.setter
    def cksl_decile(self, cksl_decile):
        self.__cksl_decile = cksl_decile

    @property
    def mylaj_decile(self):
        return self.__mylaj_decile

    @mylaj_decile.setter
    def mylaj_decile(self, mylaj_decile):
        self.__mylaj_decile = mylaj_decile

    @property
    def fobdj_decile(self):
        return self.__fobdj_decile

    @fobdj_decile.setter
    def fobdj_decile(self, fobdj_decile):
        self.__fobdj_decile = fobdj_decile

    @property
    def mz_2_decile(self):
        return self.__mz_2_decile

    @mz_2_decile.setter
    def mz_2_decile(self, mz_2_decile):
        self.__mz_2_decile = mz_2_decile

    @property
    def jz_decile(self):
        return self.__jz_decile

    @jz_decile.setter
    def jz_decile(self, jz_decile):
        self.__jz_decile = jz_decile

    @property
    def cky(self):
        return self.__cky

    @cky.setter
    def cky(self, cky):
        self.__cky = cky

    @property
    def zzmdgdqsz_dm(self):
        return self.__zzmdgdqsz_dm

    @zzmdgdqsz_dm.setter
    def zzmdgdqsz_dm(self, zzmdgdqsz_dm):
        self.__zzmdgdqsz_dm = zzmdgdqsz_dm

    @property
    def ysfs_dm(self):
        return self.__ysfs_dm

    @ysfs_dm.setter
    def ysfs_dm(self, ysfs_dm):
        self.__ysfs_dm = ysfs_dm

    @property
    def zyg_dm(self):
        return self.__zyg_dm

    @zyg_dm.setter
    def zyg_dm(self, zyg_dm):
        self.__zyg_dm = zyg_dm

    @property
    def hgcjfs_dm(self):
        return self.__hgcjfs_dm

    @hgcjfs_dm.setter
    def hgcjfs_dm(self, hgcjfs_dm):
        self.__hgcjfs_dm = hgcjfs_dm

    @property
    def jhfs_dm(self):
        return self.__jhfs_dm

    @jhfs_dm.setter
    def jhfs_dm(self, jhfs_dm):
        self.__jhfs_dm = jhfs_dm

    @property
    def yfjsfs_dm(self):
        return self.__yfjsfs_dm

    @yfjsfs_dm.setter
    def yfjsfs_dm(self, yfjsfs_dm):
        self.__yfjsfs_dm = yfjsfs_dm

    @property
    def bfjsfs_dm(self):
        return self.__bfjsfs_dm

    @bfjsfs_dm.setter
    def bfjsfs_dm(self, bfjsfs_dm):
        self.__bfjsfs_dm = bfjsfs_dm

    @property
    def zfjsfs_dm(self):
        return self.__zfjsfs_dm

    @zfjsfs_dm.setter
    def zfjsfs_dm(self, zfjsfs_dm):
        self.__zfjsfs_dm = zfjsfs_dm

    @property
    def qygbz(self):
        return self.__qygbz

    @qygbz.setter
    def qygbz(self, qygbz):
        self.__qygbz = qygbz

    @property
    def hzdwdq_dm(self):
        return self.__hzdwdq_dm

    @hzdwdq_dm.setter
    def hzdwdq_dm(self, hzdwdq_dm):
        self.__hzdwdq_dm = hzdwdq_dm

    @property
    def hggqka_dm(self):
        return self.__hggqka_dm

    @hggqka_dm.setter
    def hggqka_dm(self, hggqka_dm):
        self.__hggqka_dm = hggqka_dm

    @property
    def zmy(self):
        return self.__zmy

    @zmy.setter
    def zmy(self, zmy):
        self.__zmy = zmy

    @staticmethod
    def createTable():
        with DataBaseOperate() as db:
            db.execute_sql(sqlCreateTableCustomsDeclaration())

    def addList(self, record):
        self.__list.append(record)

    def toList(self):
        return [self.__hgqy_dm, self.__cky, self.__zmy, self.__cksl_decile, self.__mylaj_decile, self.__fobdj_decile,
                self.__mz_2_decile, self.__jz_decile, self.__zzmdgdqsz_dm, self.__ysfs_dm, self.__zyg_dm,
                self.__hgcjfs_dm, self.__jhfs_dm, self.__yfjsfs_dm, self.__bfjsfs_dm, self.__zfjsfs_dm, self.__qygbz,
                self.__hzdwdq_dm, self.__hggqka_dm]

    def save(self):
        with DataBaseOperate() as db:
            db.executemany_sql(
                """
                    INSERT INTO customs_declaration_target (hgqy_dm, cky, zmy, cksl_decile, mylaj_decile, fobdj_decile,
                                    mz_2_decile,jz_decile, zzmdgdqsz_dm, ysfs_dm, zyg_dm, hgcjfs_dm, jhfs_dm, yfjsfs_dm,
                                    bfjsfs_dm, zfjsfs_dm, qygbz, hzdwdq_dm, hggqka_dm)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """, self.__list)
