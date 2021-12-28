def sqlCreateAttributeItems():
    return """
    CREATE TABLE IF NOT EXISTS attribute_items
        (
            ID              BIGINT NOT NULL AUTO_INCREMENT PRIMARY KEY,
            CKSP_DM         VARCHAR(64),
            ZMY             VARCHAR(64),
            COUNT           VARCHAR(64),
            CKSP_DM_LENGTH  VARCHAR(64),
            ZMY_UNIT        VARCHAR(64),
            DATA_IDS        LONGTEXT
        );
    """
