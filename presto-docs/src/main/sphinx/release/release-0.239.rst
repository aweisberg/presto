=============
Release 0.239
=============

General Changes
_______________
* Fix  :func:`classification_miss_rate` and :func:`classification_fall_out` functions (:pr:`14740`).
* Fix NPE in `/v1/thread` end point.
* Fix an issue where the property ``ignore_stats_calculator_failures`` would not be honored for certain queries that had filters pushed down to the table scan.
* Fix missing query completion events for queries which fail prior to dispatching.
* Fix potential infinite loop when the setting ``use_legacy_scheduler`` is set to false.
* Add aggregation function SET_UNION.
* Add local disk spilling support for `ORDER BY` syntax.
* Add local disk spilling support for aggregation functions with `ORDER BY` or `DISTINCT` syntax.
* Add optimization for cursor projection & filter by extract and compute common subexpressions among all projections & filter first. This optimization can be turned off by session property ``optimize_common_sub_expressions``.
* Add support for 2 security modes for views. The default `DEFINER` security mode is the same as the previous behavior. Tables referenced in the view are accessed using the permissions of the view owner (the **creator** or.
* Add support for warning on unfiltered partition keys using `partition-keys-to-warn-on-no-filtering` system property.
* Add support to optimize min/max only metadata query. This is controlled by existing config ``optimizer.optimize-metadata-queries`` and session property ``optimize_metadata_queries``. Note that enabling this config/session property might change query result if there are metadata that refers to empty data, e.g. empty hive partition.
* Remove experimental feature to perform grouped execution for eligible table scans and its associated configuration property ``experimental.grouped-execution-for-elligible-table-scans`` and session property ``grouped_execution_for_eligible_table_scans``.
* Definer** of the view) rather than the user executing the query. In the `INVOKER` security mode, tables referenced in the view are accessed using the permissions of the query user (the **invoker** of the view).
* Enable ``dynamic-schedule-for-grouped-execution`` by default.  In future releases, we will remove this property, and grouped execution will always use dynamic scheduling.
* Enable ``grouped-execution-for-aggregation`` and ``experimental.grouped-execution-for-eligible-table-scans`` by default.
* Enable async page transport with non-blocking IO by default. This can be disabled by setting ``exchange.async-page-transport-enabled`` configuration property to false.
* Introduce new configuration property ``grouped-execution-enabled`` and session property ``grouped_execution`` to turn grouped execution on or off.  This property is true by default.  If set to false, it is equivalent to setting all of ``grouped-execution-for-aggregation``, ``grouped-execution-for-join``, and ``experimental.grouped-execution-for-eligible-table-scans`` to false.  In future releases we will remove these other properties and only have a single switch for enabling and disabling grouped execution.
* Reverting Reliable Resource Group Versioning.
* Specify allowed roles for HTTP endpoints.
* The worker page's thread snapshot UI does not work (no stack trace displayed on click) when there is active query load (tested under Chrome). This patch fixes an uninitialized variable in client JS that was causing this UI behavior.
* Update JTS to 1.17.0. This changes the implementation of ST_Buffer: the output might change by a small (1e-10) amount.

SPI Changes
___________
* Move `DistinctLimitNode` to `presto-spi` module for connectors to push down.

Elasticsearch Changes
_____________________
* Add configurations to improve concurrency in Elasticsearch.
* Support Elasticsearch numeric keyword.
* Support composite publish_address in Elasticsearch.

Hive Changes
____________
* Add support for caching the Glue metastore.

JDBC Changes
____________
* Implemented DatabaseMetaData.getClientInfoProperties API.

Pinot Changes
_____________
* Add Pinot SQL endpoint support.
* Pushdown DistinctLimitNode to Pinot Query in SQL mode.
