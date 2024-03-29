CREATE TABLE TTO_RECORD (
    IDT_TTO_RECORD VARCHAR(36) NOT NULL PRIMARY KEY,
    NAM_TOPIC VARCHAR(255),
    NUM_PARTITION NUMERIC(3, 0),
    IND_STATUS VARCHAR(8),
    IND_AGENT VARCHAR(8),
    TXT_KEY TEXT,
    TXT_VALUE TEXT,
    TXT_HEADERS TEXT,
    DAT_CREATED TIMESTAMP NOT NULL DEFAULT TIMEZONE('UTC', NOW()),
    DAT_SENT TIMESTAMP
);

CREATE INDEX TTO_RECORD_IDX1 ON TTO_RECORD(DAT_CREATED);
CREATE INDEX TTO_RECORD_IDX2 ON TTO_RECORD(TXT_KEY);
CREATE INDEX TTO_RECORD_IDX3 ON TTO_RECORD(IND_STATUS, DAT_CREATED);

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
    NAM_TOPIC VARCHAR(255),
    NUM_PARTITION NUMERIC(20, 0),
    IND_STATUS VARCHAR(4),
    TXT_KEY TEXT,
    TXT_VALUE TEXT,
    TXT_HEADERS TEXT,
    DAT_CREATED TIMESTAMP NOT NULL DEFAULT TIMEZONE('UTC', NOW()),
    CONSTRAINT TTO_RECORD_PROCESSED_PK PRIMARY KEY (IDT_TTO_RECORD)
);

CREATE INDEX TTO_RECORD_PROCESSED_IDX1 ON TTO_RECORD_PROCESSED(DAT_CREATED);
