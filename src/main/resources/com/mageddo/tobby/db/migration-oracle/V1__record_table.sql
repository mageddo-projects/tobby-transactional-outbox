CREATE TABLE TTO_RECORD (
    IDT_TTO_RECORD VARCHAR2(36) NOT NULL PRIMARY KEY,
    NAM_TOPIC VARCHAR2(255),
    NUM_PARTITION NUMBER(20, 0),
    TXT_KEY CLOB,
    TXT_VALUE CLOB,
    TXT_HEADERS CLOB,
    DAT_CREATED TIMESTAMP DEFAULT CAST(SYS_EXTRACT_UTC(SYSTIMESTAMP) AS TIMESTAMP) NOT NULL
);

CREATE INDEX TTO_RECORD_IDX1 ON TTO_RECORD(DAT_CREATED);

CREATE VIEW TTO_RECORD_VW AS (
    SELECT
        R.*
--        ,UTL_RAW.CAST_TO_VARCHAR2(UTL_ENCODE.BASE64_DECODE(UTL_RAW.CAST_TO_RAW(TXT_KEY))) TXT_KEY_DECODED,
--        UTL_RAW.CAST_TO_VARCHAR2(UTL_ENCODE.BASE64_DECODE(UTL_RAW.CAST_TO_RAW(TXT_VALUE))) TXT_VALUE_DECODE
    FROM TTO_RECORD R
);

CREATE TABLE TTO_PARAMETER(
    IDT_TTO_PARAMETER VARCHAR2(255) NOT NULL PRIMARY KEY,
    VAL_PARAMETER VARCHAR2(4000),
    DAT_CREATED TIMESTAMP DEFAULT CAST(SYS_EXTRACT_UTC(SYSTIMESTAMP) AS TIMESTAMP) NOT NULL,
    DAT_UPDATED TIMESTAMP
);

CREATE TABLE TTO_RECORD_PROCESSED(
    IDT_TTO_RECORD VARCHAR2(36),
    DAT_CREATED TIMESTAMP DEFAULT CAST(SYS_EXTRACT_UTC(SYSTIMESTAMP) AS TIMESTAMP) NOT NULL,
    CONSTRAINT TTO_RECORD_PROCESSED_PK PRIMARY KEY (IDT_TTO_RECORD)
);

CREATE INDEX TTO_RECORD_PROCESSED_IDX1 ON TTO_RECORD_PROCESSED(DAT_CREATED);
