package main

import (
	"database/sql"
	"encoding/json"
	"flag"
	"fmt"
	"os"
	"pg_lineage/internal/lineage"
	C "pg_lineage/pkg/config"
	"pg_lineage/pkg/depgraph"
	"pg_lineage/pkg/log"

	"github.com/go-openapi/strfmt"
	grafanaclient "github.com/grafana/grafana-openapi-client-go/client"
	grafanasearch "github.com/grafana/grafana-openapi-client-go/client/search"
	_ "github.com/lib/pq"
	"github.com/neo4j/neo4j-go-driver/v4/neo4j"
)

var config C.Config

func init() {
	configFile := flag.String("c", "./config.yaml", "path to config.yaml")
	flag.Parse()

	var err error
	if config, err = C.InitConfig(*configFile); err != nil {
		fmt.Println("InitConfig err: ", err)
		os.Exit(1)
	}
	if err = log.InitLogger(&config.Log); err != nil {
		fmt.Println(err)
		os.Exit(1)
	}
}

func main() {
	grafanaCfg := &grafanaclient.TransportConfig{
		// Host is the doman name or IP address of the host that serves the API.
		Host: config.Grafana.Host,
		// BasePath is the URL prefix for all API paths, relative to the host root.
		BasePath: "/api",
		// Schemes are the transfer protocols used by the API (http or https).
		Schemes: []string{"https"},
		// APIKey is an optional API key or service account token.
		// APIKey: os.Getenv("API_ACCESS_TOKEN"),
		// BasicAuth is optional basic auth credentials.
		// BasicAuth: url.UserPassword("admin", "admin"),
		// OrgID provides an optional organization ID.
		// OrgID is only supported with BasicAuth since API keys are already org-scoped.
		// OrgID: 1,
		// TLSConfig provides an optional configuration for a TLS client
		// TLSConfig: &tls.Config{},
		// NumRetries contains the optional number of attempted retries
		NumRetries: 3,
		// RetryTimeout sets an optional time to wait before retrying a request
		// RetryTimeout: 0,
		// RetryStatusCodes contains the optional list of status codes to retry
		// Use "x" as a wildcard for a single digit (default: [429, 5xx])
		RetryStatusCodes: []string{"420", "5xx"},
		// HTTPHeaders contains an optional map of HTTP headers to add to each request
		HTTPHeaders: map[string]string{},
		// Debug:       true,
	}

	client := grafanaclient.NewHTTPClientWithConfig(strfmt.Default, grafanaCfg)

	neo4jDriver, err := neo4j.NewDriver(config.Neo4j.URL, neo4j.BasicAuth(config.Neo4j.User, config.Neo4j.Password, ""))
	if err != nil {
		log.Fatal("connectToNeo4j err: ", err)
	}
	defer neo4jDriver.Close()

	typeVar := "dash-db"
	pageVar := int64(1)
	limitVar := int64(100)

	for {
		// 执行搜索查询
		params := grafanasearch.NewSearchParams().
			WithType(&typeVar).WithPage(&pageVar).WithLimit(&limitVar)
		dashboards, err := client.Search.Search(params)
		if err != nil {
			log.Fatalf("Error searching dashboards: %v", err)
		}

		// 处理查询结果
		if len(dashboards.Payload) == 0 {
			// 如果没有更多结果，退出循环
			break
		}

		// 打印当前页的结果
		for _, dashboardItem := range dashboards.Payload {
			// 获取完整的仪表板配置
			dashboardFullWithMeta, err := client.Dashboards.GetDashboardByUID(dashboardItem.UID)
			if err != nil {
				log.Fatalf("Error getting dashboard by UID: %v", err)
			}

			var dashboard lineage.Dashboard
			dashboardJSON, err := json.Marshal(dashboardFullWithMeta.Payload.Dashboard)
			if err != nil {
				log.Fatalf("Error marshalling dashboard JSON: %v", err)
			}
			log.Debug(string(dashboardJSON))

			// 将 JSON 数据反序列化为 Dashboard 结构体
			err = json.Unmarshal(dashboardJSON, &dashboard)
			if err != nil {
				log.Fatalf("Error unmarshalling dashboard JSON: %v", err)
			}

			// 读取特定数据，例如仪表板标题和面板信息
			log.Debugf("Dashboard Title: %s\n", dashboard.Title)
			for _, panel := range dashboard.Panels {
				if panel.Datasource == nil {
					continue
				}

				var db *sql.DB

				// TODO:获取数据源详情，新建数据库连接
				if datasourceName, ok := panel.Datasource.(string); ok {
					datasource, err := client.Datasources.GetDataSourceByName(datasourceName)
					if err != nil {
						log.Fatalf("Error getting datasource by name: %v", err)
						continue
					}
					log.Debugf("Datasource Name: %s, Datasource Type: %s\n", datasource.Payload.Name, datasource.Payload.Type)

					// 判断是否为 PG，非 PG 跳过
					if datasource.Payload.Type != "postgres" {
						continue
					}

					db, err = sql.Open("postgres", datasource.Payload.URL)
					if err != nil {
						log.Fatal("sql.Open err: ", err)
					}
					defer db.Close()
				}

				log.Debugf("Panel ID: %d, Panel Type: %s, Panel Title: %s\n", panel.ID, panel.Type, panel.Title)

				var dependencies []*lineage.Table

				for _, t := range panel.Targets {
					var r []*lineage.Table

					if t.RawSQL != "" { // custom raw SQL query
						log.Debugf("Panel Datasource: %s, Panel RawSQL: %s\n", panel.Datasource, t.RawSQL)
						r, _ = parseRawSQL(t.RawSQL, db)
					}
					if t.Query != "" { // influxdb query
						log.Debugf("Panel Datasource: %s, Panel Query: %s\n", panel.Datasource, t.Query)
						r, _ = parseRawSQL(t.Query, db)
					}

					if len(r) > 0 {
						dependencies = append(dependencies, r...)
					}
				}

				if len(dependencies) > 0 {
					generateDashboardLineage(neo4jDriver, &panel, &dashboard, dependencies)
				}
			}
		}

		// 增加页码，准备下一页查询
		pageVar++
	}

}

// TODO:访问数据源，获取Function的定义
func parseRawSQL(rawsql string, db *sql.DB) ([]*lineage.Table, error) {

	// 一个 udf 会生成一颗 Tree
	var sqlTree *depgraph.Graph

	udf, err := lineage.IdentifyFuncCall(rawsql)
	if err == nil {
		sqlTree, err = lineage.HandleUDF4Lineage(db, udf)
	} else {
		sqlTree, err = lineage.Parse(rawsql)
	}
	if err != nil {
		return nil, err
	}

	var depTables []*lineage.Table

	for _, v := range sqlTree.ShrinkGraph().GetNodes() {
		if r, ok := v.(*lineage.Table); ok {
			depTables = append(depTables, r)
		}
	}

	return depTables, nil
}

func generateDashboardLineage(driver neo4j.Driver, p *lineage.Panel, d *lineage.Dashboard, dependencies []*lineage.Table) error {

	// Sessions are short-lived, cheap to create and NOT thread safe. Typically create one or more sessions
	// per request in your web application. Make sure to call Close on the session when done.
	// For multi-database support, set sessionConfig.DatabaseName to requested database
	// Session config will default to write mode, if only reads are to be used configure session for
	// read mode.
	session := driver.NewSession(neo4j.SessionConfig{})
	defer session.Close()

	// 开始事务
	tx, err := session.BeginTransaction()
	if err != nil {
		return fmt.Errorf("failed to begin transaction: %w", err)
	}
	defer tx.Close()

	if _, err := lineage.CreatePanelNode(tx, p, d); err != nil {
		return fmt.Errorf("failed to insert Panel node: %w", err)
	}

	// 插入表节点并创建边
	for _, r := range dependencies {
		if err := lineage.CreatePanelEdge(tx, p, d, r); err != nil {
			return fmt.Errorf("failed to create relationship: %w", err)
		}
	}

	// 提交事务
	if err = tx.Commit(); err != nil {
		return fmt.Errorf("failed to commit transaction: %w", err)
	}

	return nil
}
