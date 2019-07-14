SET SESSION hive.compression_codec = 'snappy';
CREATE SCHEMA IF NOT EXISTS hive.tpch_sf100;
USE hive.tpch_sf100;
CREATE TABLE IF NOT EXISTS lineitem AS SELECT * from tpch.sf100.lineitem;

/* Enable Aria Scan */
SET SESSION aria_scan = true;
SET SESSION hive.aria_scan_enabled = true;

/* These will run with Aria scan enabled */
SELECT SUM(extendedprice) FROM lineitem WHERE suppkey = 12345;
SELECT COUNT(*), SUM(extendedprice), SUM(quantity) FROM lineitem WHERE partkey BETWEEN 10000000 AND 10200000 AND suppkey BETWEEN 500000 AND 510000;
SELECT sum (partkey) FROM lineitem WHERE quantity < 10;
SELECT MAX(orderkey), MAX(partkey), MAX(suppkey) FROM lineitem WHERE suppkey > 0;
SELECT COUNT(*) FROM lineitem WHERE partkey < 19000000 AND suppkey < 900000 AND quantity < 45 AND extendedprice < 9000;
SELECT COUNT(*), MAX(partkey) FROM lineitem WHERE comment LIKE '%fur%' AND partkey + 1 < 19000000 AND suppkey + 1 < 100000;
SELECT COUNT(*), SUM(extendedprice) FROM lineitem WHERE shipmode LIKE '%AIR%' AND shipinstruct LIKE '%PERSON';
SELECT COUNT(*), SUM(extendedprice) FROM lineitem WHERE shipmode LIKE '%AIR%';

/* Disable filter reordering */
SET SESSION hive.filter_reordering_enabled = false;
SELECT COUNT(*) FROM lineitem WHERE partkey < 19000000 AND suppkey < 900000 AND quantity < 45 AND extendedprice < 9000;
SELECT COUNT(*), MAX(partkey) FROM lineitem WHERE comment LIKE '%fur%' AND partkey + 1 < 19000000 AND suppkey + 1 < 100000;

/* Disable Aria scan */
SET SESSION aria_scan = false;
SET SESSION hive.aria_scan_enabled = false;

/* Repeat the queries without Aria */
SELECT SUM(extendedprice) FROM lineitem WHERE suppkey = 12345;
SELECT COUNT(*), SUM(extendedprice), SUM(quantity) FROM lineitem WHERE partkey BETWEEN 10000000 AND 10200000 AND suppkey BETWEEN 500000 AND 510000;
SELECT sum (partkey) FROM lineitem WHERE quantity < 10;
SELECT MAX(orderkey), MAX(partkey), MAX(suppkey) FROM lineitem WHERE suppkey > 0;
SELECT COUNT(*) FROM lineitem WHERE partkey < 19000000 AND suppkey < 900000 AND quantity < 45 AND extendedprice < 9000;
SELECT COUNT(*), MAX(partkey) FROM lineitem WHERE comment LIKE '%fur%' AND partkey + 1 < 19000000 AND suppkey + 1 < 100000;
SELECT COUNT(*), SUM(extendedprice) FROM lineitem WHERE shipmode LIKE '%AIR%' AND shipinstruct LIKE '%PERSON';
SELECT COUNT(*), SUM(extendedprice) FROM lineitem WHERE shipmode LIKE '%AIR%';
