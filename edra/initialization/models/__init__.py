from conf import table


def sqlCreateTableCustomsDeclaration():
    return """
    CREATE TABLE IF NOT EXISTS %s
        (
            ID           BIGINT NOT NULL AUTO_INCREMENT PRIMARY KEY,
            HGQY_DM      varchar(64),
            CKSP_DM      varchar(64),
            CKY          varchar(64),
            ZMY          varchar(64),
            CKSL_DECILE  varchar(64),
            MYLAJ_DECILE varchar(64),
            FOBDJ_DECILE varchar(64),
            MZ_2_DECILE  varchar(64),
            JZ_DECILE    varchar(64),
            ZZMDGDQSZ_DM varchar(64),
            YSFS_DM      varchar(64),
            ZYG_DM       varchar(64),
            HGCJFS_DM    varchar(64),
            JHFS_DM      varchar(64),
            YFJSFS_DM    varchar(64),
            BFJSFS_DM    varchar(64),
            ZFJSFS_DM    varchar(64),
            QYGBZ        varchar(64),
            HZDWDQ_DM    varchar(64),
            HGGQKA_DM    varchar(64)
        )
    """ % table['target']
