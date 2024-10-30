inputfile : slow_log_file.log

to csv : ./slowlog2csv 

output : slow_log_with_results.csv

duckdb:
D CREATE TABLE slowlog AS SELECT * FROM read_csv("/Users/du-zhigang/go/src/github.com/dulao5/slowlog2csv/slow_log_with_results.csv", max_line_size=4000971520, auto_detect=true);
D create table slowlog_statements AS select digest,sql,count(*),sum(NumCopTasks),sum(RequestCount),sum(ProcessKeys),any_value(plan)  from slowlog group by digest,sql order by 2 desc;
D copy slowlog_statements to '/Users/du-zhigang/Downloads/super-studio-slowlog-statements.csv' ;

