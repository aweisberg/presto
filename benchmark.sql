SET SESSION hive.compression_codec = 'snappy';
CREATE SCHEMA IF NOT EXISTS hive.tpch_sf100;
USE hive.tpch_sf100;

CREATE TABLE IF NOT EXISTS lineitem AS SELECT * from tpch.sf100.lineitem;

-- Column index of numeric/date columns to column value as long.
CREATE TABLE IF NOT EXISTS lineitem_map AS
SELECT
    orderkey AS l_orderkey,
    linenumber AS l_linenumber,
    map(
        ARRAY[2,
        3,
        5,
        6,
        7,
        8,
        11,
        12,
        13,
        IF(receiptdate > commitdate, 100, 101),
        IF (returnflag = 'R', 102, 103),
        104],
        ARRAY[partkey,
        suppkey,
        CAST(quantity AS BIGINT),
        CAST(extendedprice AS BIGINT),
        CAST(discount * 100 AS BIGINT),
        CAST(tax * 100 AS BIGINT),
        DATE_DIFF('day', CAST('1970-1-1' AS DATE), shipdate),
        DATE_DIFF('day', CAST('1970-1-1' AS DATE), commitdate),
        DATE_DIFF('day', CAST('1970-1-1' AS DATE), receiptdate),
        DATE_DIFF('day', commitdate, receiptdate),
        CAST(extendedprice AS BIGINT),
        IF (extendedprice > 5000, CAST(extendedprice * discount AS BIGINT), NULL)]
    ) AS ints,
    map(
        ARRAY['9',
        '10',
        '13',
        '14',
        '15'],
        ARRAY[returnflag,
        linestatus,
        shipmode,
        shipinstruct,
        COMMENT]
    ) AS strs
FROM tpch.sf100.lineitem;

-- 1.  orderkey
-- 2.  partkey
-- 3.  suppkey
-- 4.  linenumber
-- 5. quantity
-- 6. extendedprice
-- 7. discount
-- 8.  tax
-- 9.  returnflag
-- 10.  linestatus
-- 11.  shipdate
-- 12.  commitdate
-- 13.  receiptdate
-- 14.  shipinstruct
-- 15.  shipmode
-- 16.  comment

CREATE TABLE IF NOT EXISTS lineitem_struct AS
SELECT
    l.orderkey AS l_orderkey,
    linenumber AS l_linenumber,
    CAST(
        ROW (
            l.partkey,
            l.suppkey,
            extendedprice,
            discount,
            quantity,
            shipdate,
            receiptdate,
            commitdate,
            l.comment
        ) AS ROW(
            l_partkey BIGINT,
            l_suppkey BIGINT,
            l_extendedprice DOUBLE,
            l_discount DOUBLE,
            l_quantity DOUBLE,
            l_shipdate DATE,
            l_receiptdate DATE,
            l_commitdate DATE,
            l_comment VARCHAR(44)
        )
    ) AS l_shipment,
    CASE
        WHEN S.nationkey = C.nationkey THEN NULL
        ELSE CAST(
            ROW(
                S.NATIONKEY,
                C.NATIONKEY,
                CASE
                    WHEN (S.NATIONKEY IN (6, 7, 19) AND C.NATIONKEY IN (6, 7, 19)) THEN 1
                    ELSE 0
                END,
                CASE
                    WHEN s.nationkey = 24 AND c.nationkey = 10 THEN 1
                    ELSE 0
                END,
                CASE
                    WHEN p.comment LIKE '%fur%' OR p.comment LIKE '%care%' THEN ROW(
                        o.orderdate,
                        l.shipdate,
                        l.partkey + l.suppkey,
                        CONCAT(p.comment, l.comment)
                    )
                    ELSE NULL
                END
            ) AS ROW (
                s_nation BIGINT,
                c_nation BIGINT,
                is_inside_eu int,
                is_restricted int,
                license ROW (applydate DATE, grantdate DATE, filing_no BIGINT, COMMENT VARCHAR)
            )
        )
    END AS l_export
FROM tpch.sf100.lineitem l,
    tpch.sf100.orders o,
    tpch.sf100.customer c,
    tpch.sf100.supplier s,
    tpch.sf100.part p
WHERE
    l.orderkey = o.orderkey
    AND l.partkey = p.partkey
    AND l.suppkey = s.suppkey
    AND c.custkey = o.custkey;

USE hive.tpch_sf100;

/* Enable Aria Scan */
SET SESSION aria_scan = true;
SET SESSION hive.aria_scan_enabled = true;

/* These will run with Aria scan enabled */
SELECT COUNT(*)
FROM lineitem
WHERE partkey BETWEEN 1000000 AND 2000000 AND suppkey BETWEEN 100000 AND 200000 AND extendedprice > 0;

SELECT COUNT(*)
FROM lineitem_struct
WHERE l_shipment.l_partkey BETWEEN 1000000 AND 2000000 AND l_shipment.l_suppkey BETWEEN 100000 AND 200000 AND l_shipment.l_extendedprice > 0 ;

SELECT COUNT(*), SUM(l_shipment.l_extendedprice)
FROM lineitem_struct
WHERE l_shipment.l_partkey BETWEEN 1000000 AND 2000000 AND l_shipment.l_suppkey BETWEEN 100000 AND 200000;

SELECT COUNT(*), SUM(l_shipment.l_extendedprice), COUNT(l_export.license.filing_no)
FROM lineitem_struct
WHERE l_shipment.l_suppkey BETWEEN 200000 AND 400000 AND l_shipment.l_quantity < 10;

SELECT SUM(l_shipment.l_extendedprice), COUNT(l_export.license.filing_no)
FROM lineitem_struct
WHERE l_shipment.l_partkey BETWEEN 200000 AND 400000 AND l_shipment.l_quantity < 10 AND l_export.s_nation IN (1, 3, 6) AND l_export.c_nation = 11;

SELECT COUNT(*)
FROM lineitem_map
WHERE ints[2] BETWEEN 1000000 AND 2000000 AND ints[3] BETWEEN 100000 AND 200000;

SELECT COUNT(*), SUM(ints[6])
FROM lineitem_map
WHERE ints[2] BETWEEN 1000000 AND 2000000 AND ints[3] BETWEEN 100000 AND 200000;

SELECT COUNT(*), SUM(ints[6])
FROM lineitem_map
WHERE ints[2] BETWEEN 1000000 AND 2000000 AND ints[3] BETWEEN 100000 AND 200000 AND strs['13'] = 'AIR';

SELECT COUNT(*), SUM(ints[6])
FROM lineitem_map
WHERE ints[2] + 1 BETWEEN 1000000 AND 2000000 AND ints[3] + 1 BETWEEN 100000 AND 200000 AND strs['15'] LIKE '%theodol%';

/* Disable Aria scan */
SET SESSION aria_scan = false;
SET SESSION hive.aria_scan_enabled = false;

/* Repeat the queries without Aria */
SELECT COUNT(*)
FROM lineitem
WHERE partkey BETWEEN 1000000 AND 2000000 AND suppkey BETWEEN 100000 AND 200000 AND extendedprice > 0;

SELECT COUNT(*)
FROM lineitem_struct
WHERE l_shipment.l_partkey BETWEEN 1000000 AND 2000000 AND l_shipment.l_suppkey BETWEEN 100000 AND 200000 AND l_shipment.l_extendedprice > 0 ;

SELECT COUNT(*), SUM(l_shipment.l_extendedprice)
FROM lineitem_struct
WHERE l_shipment.l_partkey BETWEEN 1000000 AND 2000000 AND l_shipment.l_suppkey BETWEEN 100000 AND 200000;

SELECT COUNT(*), SUM(l_shipment.l_extendedprice), COUNT(l_export.license.filing_no)
FROM lineitem_struct
WHERE l_shipment.l_suppkey BETWEEN 200000 AND 400000 AND l_shipment.l_quantity < 10;

SELECT SUM(l_shipment.l_extendedprice), COUNT(l_export.license.filing_no)
FROM lineitem_struct
WHERE l_shipment.l_partkey BETWEEN 200000 AND 400000 AND l_shipment.l_quantity < 10 AND l_export.s_nation IN (1, 3, 6) AND l_export.c_nation = 11;

SELECT COUNT(*)
FROM lineitem_map
WHERE ints[2] BETWEEN 1000000 AND 2000000 AND ints[3] BETWEEN 100000 AND 200000;

SELECT COUNT(*), SUM(ints[6])
FROM lineitem_map
WHERE ints[2] BETWEEN 1000000 AND 2000000 AND ints[3] BETWEEN 100000 AND 200000;

SELECT COUNT(*), SUM(ints[6])
FROM lineitem_map
WHERE ints[2] BETWEEN 1000000 AND 2000000 AND ints[3] BETWEEN 100000 AND 200000 AND strs['13'] = 'AIR';

SELECT COUNT(*), SUM(ints[6])
FROM lineitem_map
WHERE ints[2] + 1 BETWEEN 1000000 AND 2000000 AND ints[3] + 1 BETWEEN 100000 AND 200000 AND strs['15'] LIKE '%theodol%';

