package main

import (
	"bufio"
	"database/sql"
	"encoding/csv"
	"fmt"
	"os"
	"regexp"
	"strings"

	_ "github.com/go-sql-driver/mysql"
)

type LogRecord struct {
	LogTime                   string
	Hostname                  string
	Tidb                      string
	Db                        string
	TxnStartTs                string
	ConnID                    string
	QueryTime                 string
	ParseTime                 string
	CompileTime               string
	RewriteTime               string
	OptimizeTime              string
	WaitTS                    string
	CopTime                   string
	ProcessTime               string
	WaitTime                  string
	RequestCount              string
	ProcessKeys               string
	TotalKeys                 string
	GetSnapshotTime           string
	RocksdbDeleteSkippedCount string
	RocksdbKeySkippedCount    string
	RocksdbBlockCacheHitCount string
	DbName                    string
	IsInternal                string
	Digest                    string
	Stats                     string
	NumCopTasks               string
	CopProcAvg                string
	CopProcP90                string
	CopProcMax                string
	CopProcAddr               string
	CopWaitAvg                string
	CopWaitP90                string
	CopWaitMax                string
	CopWaitAddr               string
	MemMax                    string
	Prepared                  string
	PlanFromCache             string
	PlanFromBinding           string
	HasMoreResults            string
	KvTotal                   string
	PdTotal                   string
	BackoffTotal              string
	WriteSqlResponseTotal     string
	ResultRows                string
	Succ                      string
	IsExplicitTxn             string
	IsSyncStatsFailed         string
	PlanDigest                string
	Sql                       string
	Plan                      string
	RpcNum                    string
	RpcTime                   string
	ResultOneLine             string
}

func main() {
	// 打开日志文件
	logFile, err := os.Open("slow_log_file.log")
	if err != nil {
		fmt.Println("Error opening log file:", err)
		return
	}
	defer logFile.Close()

	// 创建CSV文件
	csvFile, err := os.Create("slow_log_with_results.csv")
	if err != nil {
		fmt.Println("Error creating CSV file:", err)
		return
	}
	defer csvFile.Close()

	writer := csv.NewWriter(csvFile)
	defer writer.Flush()

	// 写入CSV表头
	headers := []string{"LogTime", "Hostname", "Tidb", "Db", "TxnStartTs", "ConnID", "QueryTime", "ParseTime", "CompileTime", "RewriteTime", "OptimizeTime", "WaitTS", "CopTime", "ProcessTime", "WaitTime", "RequestCount", "ProcessKeys", "TotalKeys", "GetSnapshotTime", "RocksdbDeleteSkippedCount", "RocksdbKeySkippedCount", "RocksdbBlockCacheHitCount", "DbName", "IsInternal", "Digest", "Stats", "NumCopTasks", "CopProcAvg", "CopProcP90", "CopProcMax", "CopProcAddr", "CopWaitAvg", "CopWaitP90", "CopWaitMax", "CopWaitAddr", "MemMax", "Prepared", "PlanFromCache", "PlanFromBinding", "HasMoreResults", "KvTotal", "PdTotal", "BackoffTotal", "WriteSqlResponseTotal", "ResultRows", "Succ", "IsExplicitTxn", "IsSyncStatsFailed", "PlanDigest", "Sql", "Plan", "RpcNum", "RpcTime", "ResultOneLine"}
	writer.Write(headers)

	scanner := bufio.NewScanner(logFile)
	buf := make([]byte, bufio.MaxScanTokenSize)
	scanner.Buffer(buf, 100*bufio.MaxScanTokenSize)
	var record LogRecord

	// first line
	reFirstLine := regexp.MustCompile(`^([0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9]{2}:[0-9]{2}:[0-9]{2}.*;slowlog\s)?# Time:\s*(.*)`)

	// normal key: value
	reKeyValue := regexp.MustCompile(`^# (\w+): (.*)`)

	reCopTime := regexp.MustCompile(`Cop_time: ([^ ]+)`)
	reProcessTime := regexp.MustCompile(`Process_time: ([^ ]+)`)
	reWaitTime := regexp.MustCompile(`Wait_time: ([^ ]+)`)
	reRequestCount := regexp.MustCompile(`Request_count: ([^ ]+)`)
	reProcessKeys := regexp.MustCompile(`Process_keys: ([^ ]+)`)
	reTotalKeys := regexp.MustCompile(`Total_keys: ([^ ]+)`)
	reGetSnapshotTime := regexp.MustCompile(`Get_snapshot_time: ([^ ]+)`)
	reRocksdbDeleteSkippedCount := regexp.MustCompile(`Rocksdb_delete_skipped_count: ([^ ]+)`)
	reRocksdbKeySkippedCount := regexp.MustCompile(`Rocksdb_key_skipped_count: ([^ ]+)`)
	reRocksdbBlockCacheHitCount := regexp.MustCompile(`Rocksdb_block_cache_hit_count: ([^ ]+)`)

	reCopProcAvg := regexp.MustCompile(`Cop_proc_avg: ([^ ]+)`)
	reCopProcP90 := regexp.MustCompile(`Cop_proc_p90: ([^ ]+)`)
	reCopProcMax := regexp.MustCompile(`Cop_proc_max: ([^ ]+)`)
	reCopProcAddr := regexp.MustCompile(`Cop_proc_addr: ([^ ]+)`)

	reCopWaitAvg := regexp.MustCompile(`Cop_proc_avg: ([^ ]+)`)
	reCopWaitP90 := regexp.MustCompile(`Cop_proc_p90: ([^ ]+)`)
	reCopWaitMax := regexp.MustCompile(`Cop_proc_max: ([^ ]+)`)
	reCopWaitAddr := regexp.MustCompile(`Cop_proc_addr: ([^ ]+)`)

	rePlan := regexp.MustCompile(`^# Plan: tidb_decode_plan\(['"](.*)['"]\)`)

	for scanner.Scan() {
		line := scanner.Text()

		if matches := reFirstLine.FindStringSubmatch(line); len(matches) > 0 {
			if record.LogTime != "" {
				writeRecord(writer, record)
			}

			// 初始化变量
			record = LogRecord{}

			if matches[1] != "" {
				parts := strings.Split(line, ";")
				record.LogTime = parts[0]
				record.Hostname = parts[1]
				record.Tidb = parts[2]
				if len(parts) > 3 {
					record.Db = parts[3]
				}
			} else {
				record.LogTime = matches[2]
			}

			continue
		}

		if rePlan.MatchString(line) {
			matches := rePlan.FindStringSubmatch(line)
			planBase64 := matches[1]
			record.Plan = executeSQL(fmt.Sprintf("select tidb_decode_plan('%s');", planBase64))
			continue
		}

		if strings.HasPrefix(line, "# Cop_time:") {
			if matches := reCopTime.FindStringSubmatch(line); len(matches) > 0 {
				record.CopTime = matches[1]
			}
			if matches := reProcessTime.FindStringSubmatch(line); len(matches) > 0 {
				record.ProcessTime = matches[1]
			}
			if matches := reWaitTime.FindStringSubmatch(line); len(matches) > 0 {
				record.WaitTime = matches[1]
			}
			if matches := reRequestCount.FindStringSubmatch(line); len(matches) > 0 {
				record.RequestCount = matches[1]
			}
			if matches := reProcessKeys.FindStringSubmatch(line); len(matches) > 0 {
				record.ProcessKeys = matches[1]
			}
			if matches := reTotalKeys.FindStringSubmatch(line); len(matches) > 0 {
				record.TotalKeys = matches[1]
			}
			if matches := reGetSnapshotTime.FindStringSubmatch(line); len(matches) > 0 {
				record.GetSnapshotTime = matches[1]
			}
			if matches := reRocksdbDeleteSkippedCount.FindStringSubmatch(line); len(matches) > 0 {
				record.RocksdbDeleteSkippedCount = matches[1]
			}
			if matches := reRocksdbKeySkippedCount.FindStringSubmatch(line); len(matches) > 0 {
				record.RocksdbKeySkippedCount = matches[1]
			}
			if matches := reRocksdbBlockCacheHitCount.FindStringSubmatch(line); len(matches) > 0 {
				record.RocksdbBlockCacheHitCount = matches[1]
			}
			continue
		}

		if strings.HasPrefix(line, "# Cop_proc_avg:") {
			if matches := reCopProcAvg.FindStringSubmatch(line); len(matches) > 0 {
				record.CopProcAvg = matches[1]
			}
			if matches := reCopProcP90.FindStringSubmatch(line); len(matches) > 0 {
				record.CopProcP90 = matches[1]
			}
			if matches := reCopProcMax.FindStringSubmatch(line); len(matches) > 0 {
				record.CopProcMax = matches[1]
			}
			if matches := reCopProcAddr.FindStringSubmatch(line); len(matches) > 0 {
				record.CopProcAddr = matches[1]
			}
			continue
		}

		if strings.HasPrefix(line, "# Cop_wait_avg:") {
			if matches := reCopWaitAvg.FindStringSubmatch(line); len(matches) > 0 {
				record.CopWaitAvg = matches[1]
			}
			if matches := reCopWaitP90.FindStringSubmatch(line); len(matches) > 0 {
				record.CopWaitP90 = matches[1]
			}
			if matches := reCopWaitMax.FindStringSubmatch(line); len(matches) > 0 {
				record.CopWaitMax = matches[1]
			}
			if matches := reCopWaitAddr.FindStringSubmatch(line); len(matches) > 0 {
				record.CopWaitAddr = matches[1]
			}
			continue
		}

		if matches := reKeyValue.FindStringSubmatch(line); len(matches) > 0 {
			key := matches[1]
			value := matches[2]

			switch key {
			case "Txn_start_ts":
				record.TxnStartTs = value
			case "Conn_ID":
				record.ConnID = value
			case "Query_time":
				record.QueryTime = value
			case "Parse_time":
				record.ParseTime = value
			case "Compile_time":
				record.CompileTime = value
			case "Rewrite_time":
				record.RewriteTime = value
			case "Optimize_time":
				record.OptimizeTime = value
			case "Wait_TS":
				record.WaitTS = value
			case "DB":
				record.DbName = value
			case "Is_internal":
				record.IsInternal = value
			case "Digest":
				record.Digest = value
			case "Stats":
				record.Stats = value
			case "Num_cop_tasks":
				record.NumCopTasks = value
			case "Mem_max":
				record.MemMax = value
			case "Prepared":
				record.Prepared = value
			case "Plan_from_cache":
				record.PlanFromCache = value
			case "Plan_from_binding":
				record.PlanFromBinding = value
			case "Has_more_results":
				record.HasMoreResults = value
			case "KV_total":
				record.KvTotal = value
			case "PD_total":
				record.PdTotal = value
			case "Backoff_total":
				record.BackoffTotal = value
			case "Write_sql_response_total":
				record.WriteSqlResponseTotal = value
			case "Result_rows":
				record.ResultRows = value
			case "Succ":
				record.Succ = value
			case "IsExplicitTxn":
				record.IsExplicitTxn = value
			case "IsSyncStatsFailed":
				record.IsSyncStatsFailed = value
			case "Plan_digest":
				record.PlanDigest = value
			}

			continue
		} else if strings.HasSuffix(line, ";") {
			if strings.HasPrefix(line, "use ") {
				// we can ignore it here
				// https://github.com/pingcap/tidb/blob/e41a47cab6cf99d869a4250695dd09351ba5658c/pkg/executor/slow_query.go#L638-L644
				continue
			} else {
				record.Sql = line
			}

		}
	}

	if record.LogTime != "" {
		writeRecord(writer, record)
	}

	if err := scanner.Err(); err != nil {
		fmt.Println("Error reading log file:", err)
	}

}

// writeRecord 写入记录到 CSV
func writeRecord(writer *csv.Writer, record LogRecord) {
	writer.Write([]string{
		record.LogTime,
		record.Hostname,
		record.Tidb,
		record.Db,
		record.TxnStartTs,
		record.ConnID,
		record.QueryTime,
		record.ParseTime,
		record.CompileTime,
		record.RewriteTime,
		record.OptimizeTime,
		record.WaitTS,
		record.CopTime,
		record.ProcessTime,
		record.WaitTime,
		record.RequestCount,
		record.ProcessKeys,
		record.TotalKeys,
		record.GetSnapshotTime,
		record.RocksdbDeleteSkippedCount,
		record.RocksdbKeySkippedCount,
		record.RocksdbBlockCacheHitCount,
		record.DbName,
		record.IsInternal,
		record.Digest,
		record.Stats,
		record.NumCopTasks,
		record.CopProcAvg,
		record.CopProcP90,
		record.CopProcMax,
		record.CopProcAddr,
		record.CopWaitAvg,
		record.CopWaitP90,
		record.CopWaitMax,
		record.CopWaitAddr,
		record.MemMax,
		record.Prepared,
		record.PlanFromCache,
		record.PlanFromBinding,
		record.HasMoreResults,
		record.KvTotal,
		record.PdTotal,
		record.BackoffTotal,
		record.WriteSqlResponseTotal,
		record.ResultRows,
		record.Succ,
		record.IsExplicitTxn,
		record.IsSyncStatsFailed,
		record.PlanDigest,
		record.Sql,
		record.Plan,
		record.RpcNum,
		record.RpcTime,
		record.ResultOneLine,
	})
}

// executeSQL 执行给定的 SQL 查询并返回结果拼接成的一行字符串
func executeSQL(query string) string {
	dsn := "root:@tcp(127.0.0.1:4000)/"
	db, err := sql.Open("mysql", dsn)
	if err != nil {
		fmt.Println("Error connecting to the database:", err)
		return ""
	}
	defer db.Close()

	rows, err := db.Query(query)
	if err != nil {
		fmt.Println("Error executing query:", err)
		return ""
	}
	defer rows.Close()

	var resultOneLine string
	columns, _ := rows.Columns()
	values := make([]sql.RawBytes, len(columns))
	scanArgs := make([]interface{}, len(values))
	for i := range values {
		scanArgs[i] = &values[i]
	}

	for rows.Next() {
		err := rows.Scan(scanArgs...)
		if err != nil {
			fmt.Println("Error scanning row:", err)
			return ""
		}

		for _, col := range values {
			resultOneLine += string(col) + "\n"
		}
	}

	if err := rows.Err(); err != nil {
		fmt.Println("Error iterating over rows:", err)
		return ""
	}

	return strings.TrimSpace(resultOneLine)
}
