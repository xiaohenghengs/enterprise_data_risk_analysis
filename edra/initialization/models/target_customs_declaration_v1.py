from edra.initialization.models import sqlCreateTableCustomsDeclarationV1
from utils.database_operate import DataBaseOperate


class CustomsDeclarationTarget:
    def __init__(self, hgqydm=None, sbny=None, pc=None, tsjgdm=None, glh=None, ckfph=None, bgdh=None, spdm=None,
                 spmc=None, jldw=None, cksl_decile=None, mylaj_sb_decile=None, ckjhje_decile=None, drrq=None,
                 nsrsbh=None, dah=None, jldwdm=None, zglh=None, sz=None, jhpzh=None, gfnsrsbh=None, kprq=None,
                 jsje_decile=None, zssl=None, zsse_decile=None, tsl=None, ytse_decile=None, hyd_sj=None,
                 hyd_dsj=None, hyd_qxj=None, ckrq_1=None, mylaj_decile=None, rmb=None, sl1_decile=None,
                 sl2_decile=None, sbsl_decile=None, mz_2_decile=None, jz_decile=None, zzmdgdqsz_dm=None, ysfs_dm=None,
                 zyg_dm=None, hgcjfs_dm=None, jhfs_dm=None, hzdwdq_dm=None, hggqka_dm=None, sbdw_dm=None, sbdwmc=None,
                 ck_sb_diff=None, zmy=None, fobdj_decile=None, mylaj=None):
        self.__hgqydm = hgqydm
        self.__sbny = sbny
        self.__pc = pc
        self.__tsjgdm = tsjgdm
        self.__glh = glh
        self.__ckfph = ckfph
        self.__bgdh = bgdh
        self.__spdm = spdm
        self.__spmc = spmc
        self.__jldw = jldw
        self.__cksl_decile = cksl_decile
        self.__mylaj_sb_decile = mylaj_sb_decile
        self.__ckjhje_decile = ckjhje_decile
        self.__drrq = drrq
        self.__nsrsbh = nsrsbh
        self.__dah = dah
        self.__jldwdm = jldwdm
        self.__zglh = zglh
        self.__sz = sz
        self.__jhpzh = jhpzh
        self.__gfnsrsbh = gfnsrsbh
        self.__kprq = kprq
        self.__jsje_decile = jsje_decile
        self.__zssl = zssl
        self.__zsse_decile = zsse_decile
        self.__tsl = tsl
        self.__ytse_decile = ytse_decile
        self.__hyd_sj = hyd_sj
        self.__hyd_dsj = hyd_dsj
        self.__hyd_qxj = hyd_qxj
        self.__ckrq_1 = ckrq_1
        self.__mylaj_decile = mylaj_decile
        self.__rmb = rmb
        self.__sl1_decile = sl1_decile
        self.__sl2_decile = sl2_decile
        self.__sbsl_decile = sbsl_decile
        self.__mz_2_decile = mz_2_decile
        self.__jz_decile = jz_decile
        self.__zzmdgdqsz_dm = zzmdgdqsz_dm
        self.__ysfs_dm = ysfs_dm
        self.__zyg_dm = zyg_dm
        self.__hgcjfs_dm = hgcjfs_dm
        self.__jhfs_dm = jhfs_dm
        self.__hzdwdq_dm = hzdwdq_dm
        self.__hggqka_dm = hggqka_dm
        self.__sbdw_dm = sbdw_dm
        self.__sbdwmc = sbdwmc
        self.__ck_sb_diff = ck_sb_diff
        self.__zmy = zmy
        self.__fobdj_decile = fobdj_decile
        self.__mylaj = mylaj
        self.records = list()

    @property
    def hgqydm(self):
        return self.__hgqydm

    @hgqydm.setter
    def hgqydm(self, hgqydm):
        self.__hgqydm = hgqydm

    @property
    def sbny(self):
        return self.__sbny

    @sbny.setter
    def sbny(self, sbny):
        self.__sbny = sbny

    @property
    def pc(self):
        return self.__pc

    @pc.setter
    def pc(self, pc):
        self.__pc = pc

    @property
    def tsjgdm(self):
        return self.__tsjgdm

    @tsjgdm.setter
    def tsjgdm(self, tsjgdm):
        self.__tsjgdm = tsjgdm

    @property
    def glh(self):
        return self.__glh

    @glh.setter
    def glh(self, glh):
        self.__glh = glh

    @property
    def ckfph(self):
        return self.__ckfph

    @ckfph.setter
    def ckfph(self, ckfph):
        self.__ckfph = ckfph

    @property
    def bgdh(self):
        return self.__bgdh

    @bgdh.setter
    def bgdh(self, bgdh):
        self.__bgdh = bgdh

    @property
    def spdm(self):
        return self.__spdm

    @spdm.setter
    def spdm(self, spdm):
        self.__spdm = spdm

    @property
    def spmc(self):
        return self.__spmc

    @spmc.setter
    def spmc(self, spmc):
        self.__spmc = spmc

    @property
    def jldw(self):
        return self.__jldw

    @jldw.setter
    def jldw(self, jldw):
        self.__jldw = jldw

    @property
    def cksl_decile(self):
        return self.__cksl_decile

    @cksl_decile.setter
    def cksl_decile(self, cksl_decile):
        self.__cksl_decile = cksl_decile

    @property
    def mylaj_sb_decile(self):
        return self.__mylaj_sb_decile

    @mylaj_sb_decile.setter
    def mylaj_sb_decile(self, mylaj_sb_decile):
        self.__mylaj_sb_decile = mylaj_sb_decile

    @property
    def ckjhje_decile(self):
        return self.__ckjhje_decile

    @ckjhje_decile.setter
    def ckjhje_decile(self, ckjhje_decile):
        self.__ckjhje_decile = ckjhje_decile

    @property
    def drrq(self):
        return self.__drrq

    @drrq.setter
    def drrq(self, drrq):
        self.__drrq = drrq

    @property
    def nsrsbh(self):
        return self.__nsrsbh

    @nsrsbh.setter
    def nsrsbh(self, nsrsbh):
        self.__nsrsbh = nsrsbh

    @property
    def dah(self):
        return self.__dah

    @dah.setter
    def dah(self, dah):
        self.__dah = dah

    @property
    def jldwdm(self):
        return self.__jldwdm

    @jldwdm.setter
    def jldwdm(self, jldwdm):
        self.__jldwdm = jldwdm

    @property
    def zglh(self):
        return self.__zglh

    @zglh.setter
    def zglh(self, zglh):
        self.__zglh = zglh

    @property
    def sz(self):
        return self.__sz

    @sz.setter
    def sz(self, sz):
        self.__sz = sz

    @property
    def jhpzh(self):
        return self.__jhpzh

    @jhpzh.setter
    def jhpzh(self, jhpzh):
        self.__jhpzh = jhpzh

    @property
    def gfnsrsbh(self):
        return self.__gfnsrsbh

    @gfnsrsbh.setter
    def gfnsrsbh(self, gfnsrsbh):
        self.__gfnsrsbh = gfnsrsbh

    @property
    def kprq(self):
        return self.__kprq

    @kprq.setter
    def kprq(self, kprq):
        self.__kprq = kprq

    @property
    def jsje_decile(self):
        return self.__jsje_decile

    @jsje_decile.setter
    def jsje_decile(self, jsje_decile):
        self.__jsje_decile = jsje_decile

    @property
    def zssl(self):
        return self.__zssl

    @zssl.setter
    def zssl(self, zssl):
        self.__zssl = zssl

    @property
    def zsse_decile(self):
        return self.__zsse_decile

    @zsse_decile.setter
    def zsse_decile(self, zsse_decile):
        self.__zsse_decile = zsse_decile

    @property
    def tsl(self):
        return self.__tsl

    @tsl.setter
    def tsl(self, tsl):
        self.__tsl = tsl

    @property
    def ytse_decile(self):
        return self.__ytse_decile

    @ytse_decile.setter
    def ytse_decile(self, ytse_decile):
        self.__ytse_decile = ytse_decile

    @property
    def hyd_sj(self):
        return self.__hyd_sj

    @hyd_sj.setter
    def hyd_sj(self, hyd_sj):
        self.__hyd_sj = hyd_sj

    @property
    def hyd_dsj(self):
        return self.__hyd_dsj

    @hyd_dsj.setter
    def hyd_dsj(self, hyd_dsj):
        self.__hyd_dsj = hyd_dsj

    @property
    def hyd_qxj(self):
        return self.__hyd_qxj

    @hyd_qxj.setter
    def hyd_qxj(self, hyd_qxj):
        self.__hyd_qxj = hyd_qxj

    @property
    def ckrq_1(self):
        return self.__ckrq_1

    @ckrq_1.setter
    def ckrq_1(self, ckrq_1):
        self.__ckrq_1 = ckrq_1

    @property
    def mylaj_decile(self):
        return self.__mylaj_decile

    @mylaj_decile.setter
    def mylaj_decile(self, mylaj_decile):
        self.__mylaj_decile = mylaj_decile

    @property
    def rmb(self):
        return self.__rmb

    @rmb.setter
    def rmb(self, rmb):
        self.__rmb = rmb

    @property
    def sl1_decile(self):
        return self.__sl1_decile

    @sl1_decile.setter
    def sl1_decile(self, sl1_decile):
        self.__sl1_decile = sl1_decile

    @property
    def sl2_decile(self):
        return self.__sl2_decile

    @sl2_decile.setter
    def sl2_decile(self, sl2_decile):
        self.__sl2_decile = sl2_decile

    @property
    def sbsl_decile(self):
        return self.__sbsl_decile

    @sbsl_decile.setter
    def sbsl_decile(self, sbsl_decile):
        self.__sbsl_decile = sbsl_decile

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
    def sbdw_dm(self):
        return self.__sbdw_dm

    @sbdw_dm.setter
    def sbdw_dm(self, sbdw_dm):
        self.__sbdw_dm = sbdw_dm

    @property
    def sbdwmc(self):
        return self.__sbdwmc

    @sbdwmc.setter
    def sbdwmc(self, sbdwmc):
        self.__sbdwmc = sbdwmc

    @property
    def ck_sb_diff(self):
        return self.__ck_sb_diff

    @ck_sb_diff.setter
    def ck_sb_diff(self, ck_sb_diff):
        self.__ck_sb_diff = ck_sb_diff

    @property
    def zmy(self):
        return self.__zmy

    @zmy.setter
    def zmy(self, zmy):
        self.__zmy = zmy

    @property
    def fobdj_decile(self):
        return self.__fobdj_decile

    @fobdj_decile.setter
    def fobdj_decile(self, fobdj_decile):
        self.__fobdj_decile = fobdj_decile

    @property
    def mylaj(self):
        return self.__mylaj

    @mylaj.setter
    def mylaj(self, mylaj):
        self.__mylaj = mylaj

    @staticmethod
    def createTable():
        with DataBaseOperate() as db:
            db.execute_sql(sqlCreateTableCustomsDeclarationV1())

    def addList(self, record):
        self.records.append(record)

    def toList(self):
        return [self.__hgqydm, self.__sbny, self.__pc, self.__tsjgdm, self.__glh, self.__ckfph, self.__bgdh,
                self.__spdm, self.__spmc, self.__jldw, self.__cksl_decile, self.__mylaj_sb_decile, self.__ckjhje_decile,
                self.__drrq, self.__nsrsbh, self.__dah, self.__jldwdm, self.__zglh, self.__sz, self.__jhpzh,
                self.__gfnsrsbh, self.__kprq, self.__jsje_decile, self.__zssl, self.__zsse_decile,
                self.__tsl, self.__ytse_decile, self.__hyd_sj, self.__hyd_dsj, self.__hyd_qxj, self.__ckrq_1,
                self.__mylaj_decile, self.__rmb, self.__sl1_decile, self.__sl2_decile, self.__sbsl_decile,
                self.__mz_2_decile, self.__jz_decile, self.__zzmdgdqsz_dm, self.__ysfs_dm, self.__zyg_dm,
                self.__hgcjfs_dm, self.__jhfs_dm, self.__hzdwdq_dm, self.__hggqka_dm, self.__sbdw_dm, self.__sbdwmc,
                self.__ck_sb_diff, self.__zmy, self.__fobdj_decile, self.mylaj]

    def save(self):
        with DataBaseOperate() as db:
            db.executemany_sql(
                """
                    INSERT INTO customs_declaration_v1_target (hgqydm, sbny, pc, tsjgdm, glh, ckfph, bgdh, spdm, spmc,
                                           jldw, cksl_decile, mylaj_sb_decile, ckjhje_decile, drrq, nsrsbh, dah,
                                           jldwdm, zglh, sz, jhpzh, gfnsrsbh, kprq, jsje_decile,
                                           zssl, zsse_decile, tsl, ytse_decile, hyd_sj, hyd_dsj,
                                           hyd_qxj, ckrq_1, mylaj_decile, rmb, sl1_decile, sl2_decile,
                                           sbsl_decile, mz_2_decile, jz_decile, zzmdgdqsz_dm, ysfs_dm, zyg_dm,
                                           hgcjfs_dm, jhfs_dm, hzdwdq_dm, hggqka_dm, sbdw_dm, sbdwmc, ck_sb_diff, zmy,
                                           fobdj_decile, mylaj)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 
                            ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """, self.records)
