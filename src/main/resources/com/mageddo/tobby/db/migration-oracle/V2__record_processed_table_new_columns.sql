ALTER TABLE TTO_RECORD_PROCESSED ADD NAM_TOPIC VARCHAR2(255);
ALTER TABLE TTO_RECORD_PROCESSED ADD NUM_PARTITION NUMBER(20, 0);
ALTER TABLE TTO_RECORD_PROCESSED ADD TXT_KEY VARCHAR2(4000);
ALTER TABLE TTO_RECORD_PROCESSED ADD TXT_VALUE CLOB;
ALTER TABLE TTO_RECORD_PROCESSED ADD TXT_HEADERS CLOB;
