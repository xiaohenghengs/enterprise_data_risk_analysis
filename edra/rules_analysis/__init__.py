from utils.sqlite_operate import SqliteOperate

db = SqliteOperate()

if __name__ == '__main__':
    db.execute_sql(
        """
            CREATE TABLE data_rules
            (
                id         integer primary key autoincrement not null unique,
                table_name varchar(64)                       null,
                data_id    integer                           null,
                rule_id    integer                           null,
                score      varchar(64)                       null
            )
        """
    )
