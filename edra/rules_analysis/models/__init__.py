def sqlCreateDataRules():
    return """
            CREATE TABLE IF NOT EXISTS data_rules
            (
                ID         BIGINT(20)   NOT NULL AUTO_INCREMENT,
                RULE_TYPE  VARCHAR(64)  NULL,
                DATA_ID    VARCHAR(64)  NULL,
                RULE_ID    VARCHAR(64)  NULL,
                SCORE      VARCHAR(64)  NULL,
                PRIMARY KEY (`ID`)
            )
        """
