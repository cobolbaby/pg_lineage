package main

import (
	"database/sql"
	"encoding/json"
	"flag"
	"fmt"
	"net/url"
	"os"
	"regexp"
	"sync"

	"pg_lineage/internal/lineage"
	writer "pg_lineage/internal/lineage-writer"
	"pg_lineage/internal/service"
	C "pg_lineage/pkg/config"
	"pg_lineage/pkg/depgraph"
	"pg_lineage/pkg/log"

	"github.com/go-openapi/strfmt"
	grafanaclient "github.com/grafana/grafana-openapi-client-go/client"
	grafanasearch "github.com/grafana/grafana-openapi-client-go/client/search"
	"github.com/grafana/grafana-openapi-client-go/models"
	_ "github.com/lib/pq"
)

type DataSourceCache struct {
	ds   map[string]*models.DataSource
	mu   sync.Mutex
	rule *regexp.Regexp
}

var (
	config  C.Config
	dsCache *DataSourceCache
)

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

	dsCache = &DataSourceCache{
		ds:   make(map[string]*models.DataSource),
		rule: regexp.MustCompile(config.Service.Grafana.DsMatchRules),
	}
}

func main() {

	neo4jDriver, err := writer.InitNeo4jDriver(&config.Storage.Neo4j)
	if err != nil {
		log.Error(err)
	}
	if neo4jDriver != nil {
		defer neo4jDriver.Close()
	}

	pgDriver, err := writer.InitPGClient(&config.Storage.Postgres)
	if err != nil {
		log.Error(err)
	}
	if pgDriver != nil {
		defer pgDriver.Close()
	}

	wm := writer.InitWriterManager(&writer.WriterContext{
		Neo4jDriver: neo4jDriver, // 或 nil
		PgDriver:    pgDriver,    // 或 nil
	})

	grafanaSvc, err := initGrafanaClient(&config.Service.Grafana)
	if err != nil {
		log.Fatal(err)
	}

	pgSvc, err := writer.InitPGClient(&config.Service.Postgres)
	if err != nil {
		log.Fatal("sql.Open err: ", err)
	}
	defer pgSvc.Close()

	err = processDashboards(grafanaSvc, pgSvc, wm)
	if err != nil {
		log.Fatal(err)
	}
}

func initGrafanaClient(c *C.GrafanaService) (*grafanaclient.GrafanaHTTPAPI, error) {
	grafanaCfg := &grafanaclient.TransportConfig{
		Host:             c.Host,
		BasePath:         "/api",
		Schemes:          []string{"https"},
		BasicAuth:        url.UserPassword(c.User, c.Password),
		OrgID:            c.OrgID,
		NumRetries:       3,
		RetryStatusCodes: []string{"420", "5xx"},
		HTTPHeaders:      map[string]string{},
		// Debug:            true,
	}

	return grafanaclient.NewHTTPClientWithConfig(strfmt.Default, grafanaCfg), nil
}

func processDashboards(client *grafanaclient.GrafanaHTTPAPI, db *sql.DB, m *writer.WriterManager) error {
	typeVar := "dash-db"
	pageVar := int64(1)
	limitVar := int64(100)
	dashIds := config.Service.Grafana.DashboardIDs

	for {
		params := grafanasearch.NewSearchParams().
			WithType(&typeVar).
			WithPage(&pageVar).
			WithLimit(&limitVar)

		// fix: Grafana 8.x 版本还不支持 Uid 检索
		if len(dashIds) > 0 {
			params = params.WithDashboardIds(dashIds)
		}

		dashboards, err := client.Search.Search(params)
		if err != nil {
			return fmt.Errorf("error searching dashboards: %v", err)
		}

		if len(dashboards.Payload) == 0 {
			break
		}

		for _, dashboardItem := range dashboards.Payload {
			err := processDashboardItem(client, db, m, dashboardItem)
			if err != nil {
				log.Errorf("Error processing dashboard item: %v", err)
			}
		}

		pageVar++
	}

	return nil
}

func processDashboardItem(client *grafanaclient.GrafanaHTTPAPI, db *sql.DB, m *writer.WriterManager, dashboardItem *models.Hit) error {
	dashboardFullWithMeta, err := client.Dashboards.GetDashboardByUID(dashboardItem.UID)
	if err != nil {
		return fmt.Errorf("error getting dashboard by UID: %v", err)
	}

	var dashboard *service.DashboardFullWithMeta
	dashboardJSON, err := json.Marshal(dashboardFullWithMeta.Payload)
	if err != nil {
		return fmt.Errorf("error marshalling dashboard JSON: %v", err)
	}

	if err = json.Unmarshal(dashboardJSON, &dashboard); err != nil {
		return fmt.Errorf("error unmarshalling dashboard JSON: %v", err)
	}

	log.Debugf("Dashboard Title: %s", dashboard.Dashboard.Title)
	for _, panel := range dashboard.Dashboard.Panels {
		if panel.Datasource == nil {
			continue
		}

		var datasource *models.DataSource

		switch ds := panel.Datasource.(type) {
		case string:
			datasource, err = getDatasourceByName(client, ds)
		case map[string]any:
			datasource, err = getDatasourceByObject(client, ds)
		default:
			log.Errorf("Unknown datasource type: %T", panel.Datasource)
			continue
		}

		if err != nil {
			// TODO:支持数据源是变量类型, 以及混合数据源
			log.Errorf("Error getting datasource %s: %v", panel.Datasource, err)
			continue
		}

		log.Debugf("Datasource Name: %s, Datasource Type: %s, Datasource Database: %s", datasource.Name, datasource.Type, datasource.Database)

		// 非 PG 数据源只记录点
		if datasource.Type != "postgres" {
			var dependencies []*service.Table

			if err := m.CreateGraphGrafana(
				panel, dashboard, config.Service.Grafana, dependencies, config.Service.Postgres); err != nil {
				log.Errorf("Error creating panel graph: %v", err)
			}

			continue
		}

		// 匹配特定数据源
		if !dsCache.rule.MatchString(datasource.URL) {
			continue
		}

		log.Debugf("Panel ID: %d, Panel Type: %s, Panel Title: %s", panel.ID, panel.Type, panel.Title)

		dependencies, err := getPanelDependencies(panel, db)
		if err != nil {
			log.Errorf("Error getting panel dependencies: %v", err)
			continue
		}

		if err := m.CreateGraphGrafana(
			panel, dashboard, config.Service.Grafana, dependencies, config.Service.Postgres); err != nil {
			log.Errorf("Error creating panel graph: %v", err)
		}
	}

	return nil
}

func getDatasourceByName(client *grafanaclient.GrafanaHTTPAPI, name string) (*models.DataSource, error) {
	dsCache.mu.Lock()
	defer dsCache.mu.Unlock()

	if cachedDatasource, found := dsCache.ds[name]; found {
		return cachedDatasource, nil
	}

	ds, err := client.Datasources.GetDataSourceByName(name)
	if err != nil {
		return nil, err
	}
	dsCache.ds[name] = ds.Payload

	return ds.Payload, nil
}

func getDatasourceByObject(client *grafanaclient.GrafanaHTTPAPI, input map[string]any) (*models.DataSource, error) {
	if input["uid"] == nil || input["type"] == nil {
		return nil, fmt.Errorf("invalid datasource object")
	}

	uid, ok := input["uid"].(string)
	if !ok {
		return nil, fmt.Errorf("invalid uid type")
	}

	dsCache.mu.Lock()
	defer dsCache.mu.Unlock()

	if cachedDatasource, found := dsCache.ds[uid]; found {
		return cachedDatasource, nil
	}

	ds, err := client.Datasources.GetDataSourceByUID(uid)
	if err != nil {
		return nil, err
	}
	dsCache.ds[uid] = ds.Payload

	return ds.Payload, nil
}

func getPanelDependencies(panel *service.Panel, db *sql.DB) ([]*service.Table, error) {
	var dependencies []*service.Table

	for _, t := range panel.Targets {
		var r []*service.Table

		if t.RawSQL != "" {
			log.Debugf("Panel Datasource: %s, Panel RawSQL: %s", panel.Datasource, t.RawSQL)
			r, _ = parseRawSQL(t.RawSQL, db)
		}
		if t.Query != "" {
			log.Debugf("Panel Datasource: %s, Panel Query: %s", panel.Datasource, t.Query)
			r, _ = parseRawSQL(t.Query, db)
		}

		if len(r) > 0 {
			dependencies = append(dependencies, r...)
		}
	}

	return dependencies, nil
}

func parseRawSQL(rawsql string, db *sql.DB) ([]*service.Table, error) {
	var sqlTree *depgraph.Graph

	udf, err := lineage.IdentifyFuncCall(rawsql)
	// TODO:引入 AI for lineage
	if err == nil {
		sqlTree, err = lineage.HandleUDF4Lineage(db, udf)
	} else {
		sqlTree, err = lineage.Parse(rawsql)
	}
	if err != nil {
		return nil, err
	}

	sqlTree.SetNamespace(config.Service.Postgres.Label)

	var depTables []*service.Table
	for _, v := range sqlTree.ShrinkGraph().GetNodes() {
		if r, ok := v.(*service.Table); ok {

			// TODO: Graph 中仍然存在临时节点, Why?
			if r.SchemaName == "" {
				log.Warnf("Invalid r.SchemaName: %+v", r)
				continue
			}

			r.Database = sqlTree.GetNamespace()

			depTables = append(depTables, r)
		}
	}

	return depTables, nil
}
