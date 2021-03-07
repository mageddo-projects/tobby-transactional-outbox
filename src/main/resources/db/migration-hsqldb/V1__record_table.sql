CREATE TABLE TTO_RECORD (
    IDT_TTO_RECORD VARCHAR(36) NOT NULL PRIMARY KEY,
    NAM_TOPIC VARCHAR(255),
    NUM_PARTITION NUMERIC(20, 0),
    TXT_KEY LONGVARCHAR,
    TXT_RECORD_VALUE LONGVARCHAR,
    JSN_HEADERS LONGVARCHAR,
    DAT_CREATED TIMESTAMP DEFAULT CURRENT_TIMESTAMP AT TIME ZONE INTERVAL '0:00' HOUR TO MINUTE NOT NULL
);

CREATE INDEX TTO_RECORD_IDX1 ON TTO_RECORD(DAT_CREATED);
CREATE INDEX TTO_RECORD_IDX2 ON TTO_RECORD(TXT_KEY);

CREATE TABLE PARAMETER(
    IDT_PARAMETER VARCHAR(255) NOT NULL PRIMARY KEY,
    VAL_PARAMETER LONGVARCHAR,
    DAT_CREATED TIMESTAMP DEFAULT CURRENT_TIMESTAMP AT TIME ZONE INTERVAL '0:00' HOUR TO MINUTE NOT NULL,
    DAT_UPDATED TIMESTAMP
);
