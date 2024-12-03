
CREATE TABLE test_history_text (
	itemid                   bigint                                    NOT NULL,
	clock                    integer         DEFAULT '0'               NOT NULL,
	value                    text                                      NOT NULL,
	ns                       integer         DEFAULT '0'               NOT NULL,
	PRIMARY KEY (itemid,clock,ns)
);

INSERT INTO test_history_text (itemid,clock,value,ns) VALUES
	 (1,21131,'ces1',21321),
	 (2,21321,'ces2',12321);
