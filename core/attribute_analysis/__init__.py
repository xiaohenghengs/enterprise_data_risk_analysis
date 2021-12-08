from conf import other_table_name
from libraries.sqlite_operate import SqliteOperate

db = SqliteOperate()


def createOtherData():
    db.execute_sql(
        """
        create table %s
        (
            id                       integer not null primary key autoincrement unique,
            CKNY                     varchar(255),
            HGGQKA_DM                varchar(255),
            HZDWDQ_DM                varchar(255),
            CKSP_DM                  varchar(255),
            MYGDQSZ_DM               varchar(255),
            DYJLDW_DM                varchar(255),
            CJHGHBSZ_DM              varchar(255),
            YSFS_DM                  varchar(255),
            ZMXZ_DM                  varchar(255),
            JHFS_DM                  varchar(255),
            ZYG_DM                   varchar(255),
            HGCJFS_DM                varchar(255),
            YFJSFS_DM                varchar(255),
            YFHGHBSZ_DM              varchar(255),
            BFJSFS_DM                varchar(255),
            BFHGHBSZ_DM              varchar(255),
            ZFJSFS_DM                varchar(255),
            ZFHGHBSZ_DM              varchar(255),
            SBJLDW_DM                varchar(255),
            LXBZ                     varchar(255),
            QYGBZ                    varchar(255),
            MYGDQSZ_ZZMDGDQSZ        integer,
            CKSL_DECILE              varchar(10),
            MYLAJ_DECILE             varchar(10),
            CJZJ_DECILE              varchar(10),
            SBSL_1_DECILE            varchar(10),
            SBDJ_DECILE              varchar(10),
            YFHL_DECILE              varchar(10),
            BFHL_DECILE              varchar(10),
            ZFHL_DECILE              varchar(10),
            TSL_DECILE               varchar(10),
            MZ_2_DECILE              varchar(10),
            JZ_DECILE                varchar(10),
            TSSBSL_CKSL_DECILE       varchar(10),
            TSSBRMBLAJ_RMBLAJ_DECILE varchar(10),
            TSSBMYLAJ_MYLAJ_DECILE   varchar(10),
            TYSL_CKSL_DECILE         varchar(10),
            TYRMBLAJ_RMBLAJ_DECILE   varchar(10),
            TYMYLAJ_MYLAJ_DECILE     varchar(10),
            DLSBSL_CKSL_DECILE       varchar(10),
            DLSBRMBLAJ_RMBLAJ_DECILE varchar(10),
            DLSBMYLAJ_MYLAJ_DECILE   varchar(10),
            HGSPMC_EXTEND            integer
        )
        """ % other_table_name
    )


if __name__ == '__main__':
    db.execute_sql(
        """
            CREATE TABLE drop_columns
            (
                id        integer primary key autoincrement not null unique,
                batch     integer                           null,
                _column   varchar(64)                       null,
                attribute varchar(64)                       null,
                count     varchar(64)                       null
            )
        """
    )
