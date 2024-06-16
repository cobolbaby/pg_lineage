package main

import (
	"database/sql"
	"flag"
	"fmt"
	"net/url"
	"os"
	"strings"

	"pg_lineage/internal/erd"
	"pg_lineage/internal/lineage"
	"pg_lineage/pkg/depgraph"
	"pg_lineage/pkg/log"

	_ "github.com/lib/pq"
	"github.com/neo4j/neo4j-go-driver/v4/neo4j"
	"github.com/spf13/viper"
)

type DataSource struct {
	Alias string
	Name  string
	DB    *sql.DB
}

type QueryStore struct {
	Query     string
	Calls     int64
	TotalTime float64
	MinTime   float64
	MaxTime   float64
	MeanTime  float64
}

type Config struct {
	Postgres struct {
		DSN   string `mapstructure:"dsn"`
		Alias string `mapstructure:"alias"`
	} `mapstructure:"postgres"`
	Neo4j struct {
		URL      string `mapstructure:"url"`
		User     string `mapstructure:"user"`
		Password string `mapstructure:"password"`
	} `mapstructure:"neo4j"`
	Log log.LoggerConfig
}

var config Config

var PG_QUERY_STORE = `
	SELECT 
		s.query, s.calls, s.total_time, s.min_time, s.max_time, s.mean_time
	FROM 
		pg_stat_statements s
	JOIN
		pg_database d ON d.oid = s.dbid
	WHERE
		d.datname = '%s'
		AND calls > 10
	ORDER BY
		s.mean_time DESC
	Limit 1000;
`

func initConfig(cfgFile string) error {
	if cfgFile != "" {
		// Use config file from the flag.
		viper.SetConfigFile(cfgFile)
	} else {
		viper.SetConfigName("config") // name of config file (without extension)
		viper.SetConfigType("yaml")   // 设置配置文件类型
		// viper.AddConfigPath("$HOME/.dkron") // call multiple times to add many search paths
		viper.AddConfigPath("./config") // call multiple times to add many search paths
	}

	// 如果有相应的环境变量设置，则使用环境变量的值覆盖配置文件中的值
	viper.SetEnvPrefix("LINEAGE")
	replacer := strings.NewReplacer("-", "_")
	viper.SetEnvKeyReplacer(replacer)
	viper.AutomaticEnv() // read in environment variables that match

	// 读取配置文件
	err := viper.ReadInConfig()
	if err != nil {
		fmt.Println("Error reading config file:", err)
		return err
	}

	// 将配置文件内容解析到结构体中
	err = viper.Unmarshal(&config)
	if err != nil {
		fmt.Println("Error parsing config file:", err)
		return err
	}

	return nil
}

func init() {
	configFile := flag.String("c", "./config/config.yaml", "path to config.yaml")
	flag.Parse()

	if err := initConfig(*configFile); err != nil {
		fmt.Println("initConfig err: ", err)
		os.Exit(1)
	}

	if err := log.InitLogger(&config.Log); err != nil {
		fmt.Println(err)
		os.Exit(1)
	}
}

func main() {
	log.Infof("log level: %s, log file: %s", config.Log.Level, config.Log.Path)

	db, err := sql.Open("postgres", config.Postgres.DSN)
	if err != nil {
		log.Fatal("sql.Open err: ", err)
	}
	defer db.Close()

	uri, _ := url.Parse(config.Postgres.DSN)
	ds := &DataSource{
		Alias: config.Postgres.Alias,
		Name:  strings.TrimPrefix(uri.Path, "/"),
		DB:    db,
	}

	driver, err := neo4j.NewDriver(config.Neo4j.URL, neo4j.BasicAuth(config.Neo4j.User, config.Neo4j.Password, ""))
	if err != nil {
		log.Fatal("neo4j.NewDriver err: ", err)
	}
	// Handle driver lifetime based on your application lifetime requirements  driver's lifetime is usually
	// bound by the application lifetime, which usually implies one driver instance per application
	defer driver.Close()

	// Sessions are short-lived, cheap to create and NOT thread safe. Typically create one or more sessions
	// per request in your web application. Make sure to call Close on the session when done.
	// For multi-database support, set sessionConfig.DatabaseName to requested database
	// Session config will default to write mode, if only reads are to be used configure session for
	// read mode.
	session := driver.NewSession(neo4j.SessionConfig{})
	defer session.Close()

	// 每次都是重建整张图，避免重复写入，后期可以考虑优化
	if err := lineage.ResetGraph(session); err != nil {
		log.Fatal("ResetGraph err: ", err)
	}
	if err := erd.ResetGraph(session); err != nil {
		log.Fatal("ResetGraph err: ", err)
	}

	// 支持获取pg_stat_statements中的sql语句
	querys, err := ds.DB.Query(fmt.Sprintf(PG_QUERY_STORE, ds.Name))
	if err != nil {
		log.Fatal("db.Query err: ", err)
	}
	defer querys.Close()

	m := make(map[string]*erd.RelationShip)

	for querys.Next() {

		var qs QueryStore
		_ = querys.Scan(&qs.Query, &qs.Calls, &qs.TotalTime, &qs.MinTime, &qs.MaxTime, &qs.MeanTime)

		// 生成血缘图
		// 一个 udf 会生成一颗 sqlTree，且不能将多个 udf 的 sqlTree 做合并，所以需要循环写入所有的 sqlTree
		generateTableLineage(&qs, ds, session)

		// 为了避免重复插入，写入前依赖 MAP 特性做一次去重，并且最后一次性入库
		// r := generateTableJoinRelation(&qs, ds, session)
		// maps.Copy(m, r)

		// 扩展别的图.
	}

	// 一次性入库...
	if err := erd.CreateGraph(session, m); err != nil {
		log.Errorf("ERD err: %s ", err)
	}

	// 查询所有表的使用信息，更新图数据库中的节点信息
	completeLineageGraphInfo(ds, session)

}

// 生成一张 JOIN 图
// 可以推导出关联关系的有 IN / JOIN
func generateTableJoinRelation(qs *QueryStore, ds *DataSource, session neo4j.Session) map[string]*erd.RelationShip {
	log.Debugf("generateTableJoinRelation sql: %s", qs.Query)

	var m map[string]*erd.RelationShip

	if udf, err := lineage.IdentifyFuncCall(qs.Query); err == nil {
		m, _ = erd.HandleUDF4ERD(ds.DB, udf)
	} else {
		m, _ = erd.Parse(qs.Query)
	}

	n := make(map[string]*erd.RelationShip)
	for kk, vv := range m {
		// 过滤掉临时表
		if vv.SColumn == nil || vv.TColumn == nil || vv.SColumn.Schema == "" || vv.TColumn.Schema == "" {
			continue
		}
		n[kk] = vv
	}
	fmt.Printf("GetRelationShip: #%d\n", len(n))
	for _, v := range n {
		fmt.Printf("%s\n", v.ToString())
	}

	return n
}

// 生成表血缘关系图
func generateTableLineage(qs *QueryStore, ds *DataSource, session neo4j.Session) {

	// 一个 udf 会生成一颗 Tree
	var sqlTree *depgraph.Graph

	udf, err := lineage.IdentifyFuncCall(qs.Query)
	if err == nil {
		sqlTree, err = lineage.HandleUDF4Lineage(ds.DB, udf)
	} else {
		sqlTree, err = lineage.Parse(qs.Query)
	}
	if err != nil {
		log.Errorf("Parse err: %s", err)
		return
	}

	udf.Calls = qs.Calls

	log.Debugf("UDF Graph: %+v", sqlTree)
	for i, layer := range sqlTree.TopoSortedLayers() {
		log.Debugf("UDF Graph %d: %s\n", i, strings.Join(layer, ", "))
	}

	// 设置所属命名空间，避免节点冲突
	sqlTree.SetNamespace(ds.Alias)

	if err := lineage.CreateGraph(session, sqlTree.ShrinkGraph(), udf); err != nil {
		log.Errorf("UDF CreateGraph err: %s ", err)
	}
}

func completeLineageGraphInfo(ds *DataSource, session neo4j.Session) {

	rows, err := ds.DB.Query(`
		SELECT relname, schemaname, seq_scan, seq_tup_read, 
			COALESCE(idx_scan, 0), COALESCE(idx_tup_fetch, 0), COALESCE(obj_description(relid), '') as comment
		FROM pg_stat_user_tables
		WHERE schemaname !~ '^pg_temp_' AND schemaname !~ '_del$'
			AND schemaname NOT IN ('sync', 'sync_his', 'partman', 'debug')
	`)
	if err != nil {
		log.Fatalf("Unable to execute query: %v\n", err)
	}
	defer rows.Close()

	for rows.Next() {
		var relname, schemaName, comment string
		var seqScan, seqTupRead, idxScan, idxTupFetch int64
		err := rows.Scan(&relname, &schemaName, &seqScan, &seqTupRead, &idxScan, &idxTupFetch, &comment)
		if err != nil {
			log.Fatalf("Error scanning row: %v\n", err)
		}

		err = lineage.CompleteLineageGraphInfo(session, &lineage.Record{
			Database:    ds.Alias,
			SchemaName:  schemaName,
			RelName:     relname,
			SeqScan:     seqScan,
			SeqTupRead:  seqTupRead,
			IdxScan:     idxScan,
			IdxTupFetch: idxTupFetch,
			Comment:     comment,
		})
		if err != nil {
			log.Fatalf("Error updating Neo4j: %v\n", err)
		}

	}

	log.Info("Data updated successfully in Neo4j.")
}
