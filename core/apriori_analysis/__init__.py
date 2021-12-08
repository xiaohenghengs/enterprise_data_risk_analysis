from conf import task_name
from libraries.sqlite_operate import SqliteOperate

db = SqliteOperate(task_name)

if __name__ == '__main__':
    db.execute_sql(
        """
            create table rules
            (
                id                          integer primary key autoincrement not null unique,
                drop_columns_id             integer                           not null,
                rule                        varchar(512)                      null,
                conclusion                  varchar(64)                       null,
                number_of_items_occurrences int                               null,
                degree_of_confidence        varchar(64)                       null,
                coverage                    varchar(64)                       null,
                promotion_degree            varchar(64)                       null,
                utilization                 varchar(64)                       null
            )
        """
    )
    db.execute_sql("create index rules_rule_index on rules (rule)")
