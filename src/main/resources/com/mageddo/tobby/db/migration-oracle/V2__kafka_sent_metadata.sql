ALTER TABLE TTO_RECORD ADD COLUMN NUM_SENT_PARTITION NUMBER(3, 0);
ALTER TABLE TTO_RECORD ADD COLUMN NUM_SENT_OFFSET NUMBER(20, 0);
ALTER TABLE TTO_RECORD_PROCESSED ADD COLUMN NUM_SENT_PARTITION NUMERIC(3, 0);
ALTER TABLE TTO_RECORD_PROCESSED ADD COLUMN NUM_SENT_OFFSET NUMERIC(20, 0);
