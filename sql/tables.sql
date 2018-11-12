-- SMS PARTITIONED TABLE
CREATE TABLE sms
(
    dt timestamp(6) without time zone NOT NULL,
    ctn character varying(12) NOT NULL,
    b_phone_num character varying(30) NOT NULL,
    sms_length smallint,
    direction_key smallint NOT NULL
) PARTITION BY RANGE (dt);
-- PARTITIONS OF SMS BY DT RANGE
CREATE TABLE sms_y2018m11 PARTITION OF sms 
FOR VALUES FROM ('2018-11-01') TO ('2018-12-01');
-- INDEXES OF PARTITON SMS
CREATE INDEX CONCURRENTLY ctn_sms_y2018m11 on sms_y2018m11 using btree (ctn);

-- NETWORK PARTITIONED TABLE
CREATE TABLE network
(
    dt timestamp(6) without time zone NOT NULL,
    ctn character varying(18)  NOT NULL,
    total_flux bigint,
    up_flux bigint,
    down_flux bigint,
    elapse_duration integer,
    calling_cell_id character varying(255),
    end_dt timestamp(6) without time zone
) PARTITION BY RANGE (dt);

-- PARTITIONS OF NETWORK BY DT RANGE
CREATE TABLE network_y2018m11 PARTITION OF network
FOR VALUES FROM ('2018-11-01') TO ('2018-12-01');
-- INDEXES OF PARTITON NETWORK
CREATE INDEX CONCURRENTLY ctn_network_y2018m11 on network_y2018m11 using btree (ctn);


-- VOICE PARTITIONED TABLE

CREATE TABLE voice
(
    dt timestamp(6) without time zone NOT NULL,
    ctn character varying(12),
    b_phone_num character varying(30),
    duration_nval smallint,
    calling_cell_id character varying(255),
    called_cell_id character varying(255),
    direction_key smallint
) PARTITION BY RANGE (dt);
-- PARTITIONS OF VOICE BY DT RANGE
CREATE TABLE voice_y2018m11 PARTITION OF voice
FOR VALUES FROM ('2018-11-01') TO ('2018-12-01');
-- INDEXES OF PARTITON NETWORK
CREATE INDEX CONCURRENTLY ctn_voice_y2018m11 on voice_y2018m11 using btree (ctn);