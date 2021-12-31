def sqlCreateRules(table_name):
    return """
            CREATE TABLE IF NOT EXISTS %s
            (
                ID                          BIGINT(20)   NOT NULL AUTO_INCREMENT,
                ITEM_ID                     VARCHAR(64)  NULL,
                RULE                        LONGTEXT     NULL,
                CONCLUSION                  LONGTEXT     NULL,
                NUMBER_OF_ITEMS_OCCURRENCES INT          NULL,
                DEGREE_OF_CONFIDENCE        VARCHAR(64)  NULL,
                COVERAGE                    VARCHAR(64)  NULL,
                PROMOTION_DEGREE            VARCHAR(64)  NULL,
                UTILIZATION                 VARCHAR(64)  NULL,
                PRIMARY KEY (`ID`)
            );
        """ % table_name
