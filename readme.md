## Install
```bash
go build -o slowlog2csv main.go
brew install duckdb
```

## Usage

* step 1: start a tidb-server(port 4000) 
  * e.g. `tiup playground nightly --db 1 --pd 1 --kv 1`
  * ※ The `slowlog2csv` command will be use tidb-server to parse plan data `tidb_decode_plan(...base64_data...)` and get the execution plan
* step 2: convert slowlog file to csv file
  * `slowlog2csv <slowlog file>`
  * e.g. `./slowlog2csv slowlog.log`
  * the output file will be `slowlog.csv`
* step 3: analysis and insights

## Analysis and insights QueryTime data via duckdb
> target columns: "QueryTime", "ParseTime", "CompileTime", "RewriteTime", "OptimizeTime", "WaitTS", "CopTime", "ProcessTime", "WaitTime"
```SQL
CREATE TABLE slowlog AS SELECT * FROM read_csv("/pathto/slowlog.csv", max_line_size=400971520, auto_detect=true);
SELECT
    round(sum(ParseTime)/sum(QueryTime)*100,2) as ParseTimePercent,
    round(sum(CompileTime)/sum(QueryTime)*100,2) as CompileTimePercent,
    round(sum(RewriteTime)/sum(QueryTime)*100,2) as RewriteTimePercent,
    round(sum(OptimizeTime)/sum(QueryTime)*100,2) as OptimizeTimePercent,
    round(sum(WaitTS)/sum(QueryTime)*100,2) as WaitTSPercent,
    round(sum(CopTime)/sum(QueryTime)*100,2) as CopTimePercent,
    round(sum(ProcessTime)/sum(QueryTime)*100,2) as ProcessTimePercent,
    round(sum(WaitTime)/sum(QueryTime)*100,2) as WaitTimePercent 
FROM slowlog;

```
A sample output (The sum may be greater than 100% because of the summation of concurrency):
```
┌──────────────────┬────────────────────┬────────────────────┬─────────────────────┬───────────────┬────────────────┬────────────────────┬─────────────────┐
│ ParseTimePercent │ CompileTimePercent │ RewriteTimePercent │ OptimizeTimePercent │ WaitTSPercent │ CopTimePercent │ ProcessTimePercent │ WaitTimePercent │
│      double      │       double       │       double       │       double        │    double     │     double     │       double       │     double      │
├──────────────────┼────────────────────┼────────────────────┼─────────────────────┼───────────────┼────────────────┼────────────────────┼─────────────────┤
│             0.05 │               3.19 │               0.13 │                3.01 │          0.03 │         178.95 │             136.73 │            0.53 │
└──────────────────┴────────────────────┴────────────────────┴─────────────────────┴───────────────┴────────────────┴────────────────────┴─────────────────┘
```
* If ParseTimePercent or CompileTimePercent is large, you may need to check if there are long SQL statements.
* If WaitTSPercent is large, you may need to check if TiDB and PD's CPU resources are bottlenecked.
* If CopTimePercent or ProcessTimePercent is large, there may be heavy statements occupying tikv's resources (usually TableFullScan/IndexFullScan/TableRowIDScan).
* If WaitTimePercent is large, it may be caused by other queries occupying tikv's resources.

## Exporting the top-10 statements by NumCopTasks via duckdb
```SQL
CREATE TABLE slowlog AS SELECT * FROM read_csv("/pathto/slowlog.csv", max_line_size=400971520, auto_detect=true, types = {'NumCopTasks': 'INTEGER', 'TotalKeys': 'INTEGER', 'ProcessKeys': 'INTEGER'});
CREATE TABLE top_slowlog_statements AS SELECT digest,sql,count(*),sum(NumCopTasks),sum(QueryTime),sum(TotalKeys),any_value(plan)  FROM slowlog GROUP BY digest,sql ORDER BY 4 DESC LIMIT 10;
COPY top_slowlog_statements TO '/pathto/top-NumCopTasks-slowlog-statements.csv' ;
```

## Exporting the top-10 statements by QueryTime via duckdb
```SQL
CREATE TABLE slowlog AS SELECT * FROM read_csv("/pathto/slowlog.csv", max_line_size=400971520, auto_detect=true, types = {'NumCopTasks': 'INTEGER', 'TotalKeys': 'INTEGER', 'ProcessKeys': 'INTEGER'});
CREATE TABLE top_slowlog_statements AS SELECT digest,sql,count(*),sum(NumCopTasks),sum(QueryTime),sum(TotalKeys),any_value(plan)  FROM slowlog GROUP BY digest,sql ORDER BY 5 DESC LIMIT 10;
COPY top_slowlog_statements TO '/pathto/top-QueryTime-slowlog-statements.csv' ;
```
## Exporting the top-10 statements by TotalKeys via duckdb
```SQL
CREATE TABLE slowlog AS SELECT * FROM read_csv("/pathto/slowlog.csv", max_line_size=400971520, auto_detect=true, types = {'NumCopTasks': 'INTEGER', 'TotalKeys': 'INTEGER', 'ProcessKeys': 'INTEGER'});
CREATE TABLE top_slowlog_statements AS SELECT digest,sql,count(*),sum(NumCopTasks),sum(QueryTime),sum(TotalKeys),any_value(plan)  FROM slowlog GROUP BY digest,sql ORDER BY 6 DESC LIMIT 10;
COPY top_slowlog_statements TO '/pathto/top-TotalKeys-slowlog-statements.csv' ;
```

## Exporting the top-10 statements by MVCC_Versions via duckdb
(If TotalKeys is much larger than ProcessKeys, you may be able to use the in-memory-engine)
```SQL
CREATE TABLE slowlog AS SELECT * FROM read_csv("/pathto/slowlog.csv", max_line_size=400971520, auto_detect=true, types = {'NumCopTasks': 'INTEGER', 'TotalKeys': 'INTEGER', 'ProcessKeys': 'INTEGER'});
CREATE TABLE top_slowlog_statements AS SELECT digest,sql,count(*),sum(TotalKeys-ProcessKeys) as MVCC_Versions,sum(TotalKeys),sum(ProcessKeys),sum(NumCopTasks),sum(QueryTime),any_value(plan)  FROM slowlog GROUP BY digest,sql ORDER BY 4 DESC LIMIT 10;
COPY top_slowlog_statements TO '/pathto/top-MVCCVersions-slowlog-statements.csv' ;
```

## Deeper analysis and insights execution plan data via duckdb
This script is suitable for the Poc phase of the full analysis of the scenario.
It is recommended to set [tidb_slow_log_threshold](https://docs.pingcap.com/tidb/dev/tidb-configuration-file#tidb_slow_log_threshold) to 0 in order to get all the query slowlog, and then analyze various table access methods, review the design of primary keys and indexes by this script.

```SQL
CREATE TABLE slowlog AS SELECT * FROM read_csv("./tidb-slow-with-results.csv", auto_detect=true, max_line_size=400971520);

CREATE TABLE statements AS
    select
        ROW_NUMBER() OVER () AS statementID,
        digest as digestID ,
        Sql,
        Plan
    FROM slowlog;

create table plan_lines as
    select
        statementID, digestID, REGEXP_SPLIT_TO_ARRAY(plan, E'\n') as lines
    from statements;

create table plan_line_maps as
    select  statementID, digestID, unnest(map_entries(map(range(len( lines )) ,  lines ))) as linemap
    from plan_lines ;

create table plan_split_cols as
    select statementID, digestID, linemap.key as lineNumber, REGEXP_SPLIT_TO_ARRAY(linemap.value, E'\t') AS cols
    from plan_line_maps;

CREATE TABLE plancsv AS
    select
        plan_split_cols.statementID as statementID,
        plan_split_cols.digestID as digestID,
        format('{:011d}-{:04d}', plan_split_cols.statementID, lineNumber) as sortID,
        regexp_replace( regexp_replace(regexp_replace(cols[2], '_\d+\s*', ''), '^\W+', ''), '\s+', '') as type,
        lineNumber,
        length(regexp_extract(cols[2], '^\W+')) as depth,
        regexp_replace(cols[2], '^ ', '.') as planId,
        cols[3] as task,
        cols[4] as estRows,
        regexp_replace(cols[5], '\s*$', '') as operatorInfo,
        TRY_CAST(cols[6] AS INTEGER) as actRows,
        TRY_CAST(cols[6] AS INTEGER) as digestExecCountXactRows,
        regexp_replace(cols[7], '\s*$', '') as executionInfo,
        cols[8] as memory,
        cols[9] as disk,
        regexp_extract_all(cols[5], 'table:(\w+)', 1)[1] as tablename,
        regexp_extract_all(cols[5], 'index:([\w\(,\)]+)', 1)[1] as indexname,
        TRY_CAST(regexp_extract_all(cols[7], 'loops:(\d+)', 1)[1] AS INTEGER) as loops,
        TRY_CAST(regexp_extract_all(cols[7], 'cop_task:\s*\{.*num:\s*(\d+)', 1)[1] AS INTEGER) as cop_task_num,
        TRY_CAST(regexp_extract_all(cols[7], 'tikv_task:\{.*tasks:\s*(\d+)', 1)[1] AS INTEGER) as tikv_task_num,
        TRY_CAST(regexp_extract_all(cols[7], 'tiflash_task:\{.*tasks:\s*(\d+)', 1)[1] AS INTEGER) as tiflash_task_num,
        TRY_CAST(regexp_extract_all(cols[7], 'total_process_keys:\s*(\d+)', 1)[1] AS INTEGER) as total_process_keys,
        TRY_CAST(regexp_extract_all(cols[7], 'total_keys:\s*(\d+)', 1)[1] AS INTEGER) as total_keys
    from plan_split_cols left join statements on plan_split_cols.statementID = statements.statementID
    where lineNumber <> 0;


CREATE TABLE table_access_methods AS
    SELECT 
        tablename,
        SUM(CASE WHEN type IN (
            'Point_Get' , 'TableRangeScan', 'Batch_Point_Get',
            'Point_Get(Build)', 'Batch_Point_Get(Build)', 'TableRangeScan(Build)',
            'Point_Get(Probe)', 'Batch_Point_Get(Probe)', 'TableRangeScan(Probe)',
        ) THEN 1 ELSE 0 END) AS ViaPrimaryKeyAccessSum,
        SUM(CASE WHEN type IN ('TableRowIDScan', 'TableRowIDScan(Probe)') THEN 1 ELSE 0 END) AS ViaIndexAccessSum,
        SUM(CASE WHEN type IN ('TableFullScan') THEN 1 ELSE 0 END) AS ViaTableFullScanSum,
    FROM plancsv
    WHERE tablename <> ''
    GROUP BY tablename
    ORDER by ViaPrimaryKeyAccessSum , ViaIndexAccessSum DESC;

COPY table_access_methods TO '/pathto/table_access_methods.csv' ;
```


