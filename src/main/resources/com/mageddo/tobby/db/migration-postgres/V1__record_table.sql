CREATE TABLE TTO_RECORD (
    IDT_TTO_RECORD VARCHAR(36) NOT NULL PRIMARY KEY,
    NAM_TOPIC VARCHAR(255),
    NUM_PARTITION NUMERIC(20, 0),
    TXT_KEY TEXT,
    TXT_VALUE TEXT,
    TXT_HEADERS TEXT,
    DAT_CREATED TIMESTAMP NOT NULL DEFAULT TIMEZONE('UTC', NOW())
);

CREATE INDEX TTO_RECORD_IDX1 ON TTO_RECORD(DAT_CREATED);
CREATE INDEX TTO_RECORD_IDX2 ON TTO_RECORD(TXT_KEY);

CREATE VIEW TTO_RECORD_VW AS (
    SELECT
        R.*,
        CONVERT_FROM(DECODE(TXT_KEY, 'BASE64'), 'UTF8') TXT_KEY_DECODED,
        CONVERT_FROM(DECODE(TXT_VALUE, 'BASE64'), 'UTF8') TXT_VALUE_DECODED
    FROM TTO_RECORD R
);

CREATE TABLE TTO_PARAMETER(
    IDT_TTO_PARAMETER VARCHAR(255) NOT NULL PRIMARY KEY,
    VAL_PARAMETER TEXT,
    DAT_CREATED TIMESTAMP NOT NULL DEFAULT TIMEZONE('UTC', NOW()),
    DAT_UPDATED TIMESTAMP
);

CREATE TABLE TTO_RECORD_PROCESSED(
    IDT_TTO_RECORD VARCHAR(36),
    DAT_CREATED TIMESTAMP NOT NULL DEFAULT TIMEZONE('UTC', NOW()),
    CONSTRAINT TTO_RECORD_PROCESSED_PK PRIMARY KEY (IDT_TTO_RECORD)
);