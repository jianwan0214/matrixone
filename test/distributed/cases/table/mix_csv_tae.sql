-- env prepare statement
drop table if exists statement_info;
drop table if exists statement_info_csv;
drop table if exists statement_info_tae;

-- check only tae
-- @bvt:issue#6812
CREATE EXTERNAL TABLE IF NOT EXISTS `statement_info_tae`( `statement_id` VARCHAR(36) NOT NULL COMMENT "statement uniq id", `transaction_id` VARCHAR(36) NOT NULL COMMENT "txn uniq id", `session_id` VARCHAR(36) NOT NULL COMMENT "session uniq id", `account` VARCHAR(1024) NOT NULL COMMENT "account name", `user` VARCHAR(1024) NOT NULL COMMENT "user name", `host` VARCHAR(1024) NOT NULL COMMENT "user client ip", `database` VARCHAR(1024) NOT NULL COMMENT "what database current session stay in.", `statement` TEXT NOT NULL COMMENT "sql statement", `statement_tag` TEXT NOT NULL COMMENT "note tag in statement(Reserved)", `statement_fingerprint` TEXT NOT NULL COMMENT "note tag in statement(Reserved)", `node_uuid` VARCHAR(36) NOT NULL COMMENT "node uuid, which node gen this data.", `node_type` VARCHAR(1024) NOT NULL COMMENT "node type in MO, val in [DN, CN, LOG]", `request_at` Datetime(6) NOT NULL COMMENT "request accept datetime", `response_at` Datetime(6) NOT NULL COMMENT "response send datetime", `duration` BIGINT UNSIGNED DEFAULT "0" COMMENT "exec time, unit: ns", `status` VARCHAR(1024) NOT NULL COMMENT "sql statement running status, enum: Running, Success, Failed", `err_code` VARCHAR(1024) DEFAULT "0" COMMENT "error code info", `error` TEXT NOT NULL COMMENT "error message", `exec_plan` JSON NOT NULL COMMENT "statement execution plan", `rows_read` BIGINT DEFAULT "0" COMMENT "rows read total", `bytes_scan` BIGINT DEFAULT "0" COMMENT "bytes scan total", `stats` JSON NOT NULL COMMENT "global stats info in exec_plan", `statement_type` VARCHAR(1024) NOT NULL COMMENT "statement type, val in [Insert, Delete, Update, Drop Table, Drop User, ...]", `query_type` VARCHAR(1024) NOT NULL COMMENT "query type, val in [DQL, DDL, DML, DCL, TCL]", `role_id` BIGINT DEFAULT "0" COMMENT "role id", `sql_source_type` TEXT NOT NULL COMMENT "sql statement source type") infile{"filepath"="$resources/external_table_file/mix/tae/*"} FIELDS TERMINATED BY ',' ENCLOSED BY '"' LINES TERMINATED BY '\n' IGNORE 0 lines;
select count(1) as cnt from statement_info_tae;
-- @bvt:issue

-- check only csv
CREATE EXTERNAL TABLE IF NOT EXISTS `statement_info_csv`( `statement_id` VARCHAR(36) NOT NULL COMMENT "statement uniq id", `transaction_id` VARCHAR(36) NOT NULL COMMENT "txn uniq id", `session_id` VARCHAR(36) NOT NULL COMMENT "session uniq id", `account` VARCHAR(1024) NOT NULL COMMENT "account name", `user` VARCHAR(1024) NOT NULL COMMENT "user name", `host` VARCHAR(1024) NOT NULL COMMENT "user client ip", `database` VARCHAR(1024) NOT NULL COMMENT "what database current session stay in.", `statement` TEXT NOT NULL COMMENT "sql statement", `statement_tag` TEXT NOT NULL COMMENT "note tag in statement(Reserved)", `statement_fingerprint` TEXT NOT NULL COMMENT "note tag in statement(Reserved)", `node_uuid` VARCHAR(36) NOT NULL COMMENT "node uuid, which node gen this data.", `node_type` VARCHAR(1024) NOT NULL COMMENT "node type in MO, val in [DN, CN, LOG]", `request_at` Datetime(6) NOT NULL COMMENT "request accept datetime", `response_at` Datetime(6) NOT NULL COMMENT "response send datetime", `duration` BIGINT UNSIGNED DEFAULT "0" COMMENT "exec time, unit: ns", `status` VARCHAR(1024) NOT NULL COMMENT "sql statement running status, enum: Running, Success, Failed", `err_code` VARCHAR(1024) DEFAULT "0" COMMENT "error code info", `error` TEXT NOT NULL COMMENT "error message", `exec_plan` JSON NOT NULL COMMENT "statement execution plan", `rows_read` BIGINT DEFAULT "0" COMMENT "rows read total", `bytes_scan` BIGINT DEFAULT "0" COMMENT "bytes scan total", `stats` JSON NOT NULL COMMENT "global stats info in exec_plan", `statement_type` VARCHAR(1024) NOT NULL COMMENT "statement type, val in [Insert, Delete, Update, Drop Table, Drop User, ...]", `query_type` VARCHAR(1024) NOT NULL COMMENT "query type, val in [DQL, DDL, DML, DCL, TCL]", `role_id` BIGINT DEFAULT "0" COMMENT "role id", `sql_source_type` TEXT NOT NULL COMMENT "sql statement source type") infile{"filepath"="$resources/external_table_file/mix/csv/*"} FIELDS TERMINATED BY ',' ENCLOSED BY '"' LINES TERMINATED BY '\n' IGNORE 0 lines;
select count(1) as cnt from statement_info_csv;

-- check mix csv & tae
CREATE EXTERNAL TABLE IF NOT EXISTS `statement_info`( `statement_id` VARCHAR(36) NOT NULL COMMENT "statement uniq id", `transaction_id` VARCHAR(36) NOT NULL COMMENT "txn uniq id", `session_id` VARCHAR(36) NOT NULL COMMENT "session uniq id", `account` VARCHAR(1024) NOT NULL COMMENT "account name", `user` VARCHAR(1024) NOT NULL COMMENT "user name", `host` VARCHAR(1024) NOT NULL COMMENT "user client ip", `database` VARCHAR(1024) NOT NULL COMMENT "what database current session stay in.", `statement` TEXT NOT NULL COMMENT "sql statement", `statement_tag` TEXT NOT NULL COMMENT "note tag in statement(Reserved)", `statement_fingerprint` TEXT NOT NULL COMMENT "note tag in statement(Reserved)", `node_uuid` VARCHAR(36) NOT NULL COMMENT "node uuid, which node gen this data.", `node_type` VARCHAR(1024) NOT NULL COMMENT "node type in MO, val in [DN, CN, LOG]", `request_at` Datetime(6) NOT NULL COMMENT "request accept datetime", `response_at` Datetime(6) NOT NULL COMMENT "response send datetime", `duration` BIGINT UNSIGNED DEFAULT "0" COMMENT "exec time, unit: ns", `status` VARCHAR(1024) NOT NULL COMMENT "sql statement running status, enum: Running, Success, Failed", `err_code` VARCHAR(1024) DEFAULT "0" COMMENT "error code info", `error` TEXT NOT NULL COMMENT "error message", `exec_plan` JSON NOT NULL COMMENT "statement execution plan", `rows_read` BIGINT DEFAULT "0" COMMENT "rows read total", `bytes_scan` BIGINT DEFAULT "0" COMMENT "bytes scan total", `stats` JSON NOT NULL COMMENT "global stats info in exec_plan", `statement_type` VARCHAR(1024) NOT NULL COMMENT "statement type, val in [Insert, Delete, Update, Drop Table, Drop User, ...]", `query_type` VARCHAR(1024) NOT NULL COMMENT "query type, val in [DQL, DDL, DML, DCL, TCL]", `role_id` BIGINT DEFAULT "0" COMMENT "role id", `sql_source_type` TEXT NOT NULL COMMENT "sql statement source type") infile{"filepath"="$resources/external_table_file/mix/*/*"} FIELDS TERMINATED BY ',' ENCLOSED BY '"' LINES TERMINATED BY '\n' IGNORE 0 lines;
-- @bvt:issue#6812
select count(*) as cnt from statement_info;
select count(1) as cnt from statement_info;
-- @bvt:issue
select count(1) as cnt from statement_info where __mo_filepath like "%/csv/%";
select count(statement_id) as cnt from statement_info where __mo_filepath like "%/csv/%";
select count(1) as cnt from statement_info where __mo_filepath like '%csv%' group by __mo_filepath order by cnt;

-- @bvt:issue#6812
select count(1) as cnt from statement_info where __mo_filepath like "%/tae/%";
select count(*) as cnt from statement_info where __mo_filepath like "%/tae/%";
select count(1) as cnt from statement_info group by __mo_filepath order by cnt;

select count(statement_id) as cnt from statement_info where __mo_filepath like "%/tae/%";
-- @bvt:issue
