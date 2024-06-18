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
	C "pg_lineage/pkg/config"
	"pg_lineage/pkg/depgraph"
	"pg_lineage/pkg/log"

	"github.com/go-openapi/strfmt"
	grafanaclient "github.com/grafana/grafana-openapi-client-go/client"
	grafanasearch "github.com/grafana/grafana-openapi-client-go/client/search"
	"github.com/grafana/grafana-openapi-client-go/models"
	_ "github.com/lib/pq"
	"github.com/neo4j/neo4j-go-driver/v4/neo4j"
)

type DataSourceCache struct {
	cache map[string]*models.DataSource
	mu    sync.Mutex
}

var (
	dsMatchRule *regexp.Regexp
	config      C.Config
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
	dsMatchRule = regexp.MustCompile(config.Grafana.DataSourceMatchRules)
}

func main() {
	client, err := initGrafanaClient()
	if err != nil {
		log.Fatal(err)
	}

	neo4jDriver, err := initNeo4jDriver()
	if err != nil {
		log.Fatal(err)
	}
	defer neo4jDriver.Close()

	db, err := sql.Open("postgres", config.Postgres.DSN)
	if err != nil {
		log.Fatal("sql.Open err: ", err)
	}
	defer db.Close()

	err = processDashboards(client, neo4jDriver, db)
	if err != nil {
		log.Fatal(err)
	}
}

func initGrafanaClient() (*grafanaclient.GrafanaHTTPAPI, error) {
	grafanaCfg := &grafanaclient.TransportConfig{
		Host:             config.Grafana.Host,
		BasePath:         "/api",
		Schemes:          []string{"https"},
		BasicAuth:        url.UserPassword(config.Grafana.User, config.Grafana.Password),
		OrgID:            config.Grafana.OrgID,
		NumRetries:       3,
		RetryStatusCodes: []string{"420", "5xx"},
		HTTPHeaders:      map[string]string{},
		// Debug:            true,
	}

	return grafanaclient.NewHTTPClientWithConfig(strfmt.Default, grafanaCfg), nil
}

func initNeo4jDriver() (neo4j.Driver, error) {
	return neo4j.NewDriver(config.Neo4j.URL, neo4j.BasicAuth(config.Neo4j.User, config.Neo4j.Password, ""))
}

func processDashboards(client *grafanaclient.GrafanaHTTPAPI, neo4jDriver neo4j.Driver, db *sql.DB) error {
	typeVar := "dash-db"
	pageVar := int64(1)
	limitVar := int64(100)

	dsCache := &DataSourceCache{
		cache: make(map[string]*models.DataSource),
	}

	for {
		params := grafanasearch.NewSearchParams().WithType(&typeVar).WithPage(&pageVar).WithLimit(&limitVar)
		dashboards, err := client.Search.Search(params)
		if err != nil {
			return fmt.Errorf("error searching dashboards: %v", err)
		}

		if len(dashboards.Payload) == 0 {
			break
		}

		for _, dashboardItem := range dashboards.Payload {
			err := processDashboardItem(client, neo4jDriver, db, dashboardItem, dsCache)
			if err != nil {
				log.Errorf("Error processing dashboard item: %v", err)
			}
		}

		pageVar++
	}

	return nil
}

func processDashboardItem(client *grafanaclient.GrafanaHTTPAPI, neo4jDriver neo4j.Driver, db *sql.DB, dashboardItem *models.Hit, cache *DataSourceCache) error {
	dashboardFullWithMeta, err := client.Dashboards.GetDashboardByUID(dashboardItem.UID)
	if err != nil {
		return fmt.Errorf("error getting dashboard by UID: %v", err)
	}

	var dashboard *lineage.DashboardFullWithMeta
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

		if datasourceName, ok := panel.Datasource.(string); ok {
			cache.mu.Lock()
			var found bool
			if datasource, found = cache.cache[datasourceName]; !found {
				ds, err := client.Datasources.GetDataSourceByName(datasourceName)
				if err != nil {
					cache.mu.Unlock()
					log.Errorf("Error getting datasource by name %s: %v", datasourceName, err)
					continue
				}
				datasource = ds.Payload
				cache.cache[datasourceName] = ds.Payload
			}
			cache.mu.Unlock()

			log.Debugf("Datasource Name: %s, Datasource Type: %s, Datasource Database: %s", datasource.Name, datasource.Type, datasource.Database)

			if datasource.Type != "postgres" || datasource.Database != "bdc" {
				continue
			}
			if !dsMatchRule.MatchString(datasource.URL) {
				continue
			}

		} else {
			log.Errorf("Datasource is %v, not a string", panel.Datasource)
			continue
		}

		log.Debugf("Panel ID: %d, Panel Type: %s, Panel Title: %s", panel.ID, panel.Type, panel.Title)

		dependencies, err := getPanelDependencies(panel, db)
		if err != nil {
			log.Errorf("Error getting panel dependencies: %v", err)
			continue
		}

		if len(dependencies) > 0 {
			lineage.CreatePanelGraph(neo4jDriver.NewSession(neo4j.SessionConfig{}), panel, dashboard, dependencies)
		}
	}

	return nil
}

func getPanelDependencies(panel *lineage.Panel, db *sql.DB) ([]*lineage.Table, error) {
	var dependencies []*lineage.Table

	for _, t := range panel.Targets {
		var r []*lineage.Table

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

func parseRawSQL(rawsql string, db *sql.DB) ([]*lineage.Table, error) {
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

	// TODO:操作得前置
	sqlTree.SetNamespace(config.Postgres.Alias)

	var depTables []*lineage.Table
	for _, v := range sqlTree.ShrinkGraph().GetNodes() {
		if r, ok := v.(*lineage.Table); ok {
			r.Database = sqlTree.GetNamespace()

			depTables = append(depTables, r)
		}
	}

	return depTables, nil
}
