package main

import (
	"database/sql"
	"encoding/json"
	"flag"
	"fmt"
	"net/url"
	"os"
	"regexp"
	"strings"
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
	"github.com/neo4j/neo4j-go-driver/v4/neo4j"
)

// ServiceProvider 封装服务依赖
type ServiceProvider struct {
	Grafana       GrafanaBundle
	PG            map[string]*PGBundle
	WriterManager *writer.WriterManager
}

type GrafanaBundle struct {
	Client *grafanaclient.GrafanaHTTPAPI
	Config *C.GrafanaService
}

type PGBundle struct {
	Client *sql.DB
	Config *C.PostgresService
}

type DataSourceCache struct {
	ds map[string]*models.DataSource
	mu sync.Mutex
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
		ds: make(map[string]*models.DataSource),
	}
}

func main() {
	// 初始化 WriterManager（内部根据 config.Storage 决定用哪种存储）
	wm := mustInitWriterManager(&config.Storage)
	defer wm.Close() // WriterManager 内部封装资源关闭逻辑

	// 初始化 Grafana 客户端
	grafanaClient := mustInitGrafanaClient(&config.Service.Grafana)

	// 初始化所有 PG 客户端并关联配置
	pgMap := mustInitPGClients(config.Service.Postgres)
	defer closePGClients(pgMap)

	// 构造 ServiceProvider，聚合服务资源
	sp := &ServiceProvider{
		Grafana: GrafanaBundle{
			Client: grafanaClient,
			Config: &config.Service.Grafana,
		},
		PG:            pgMap,
		WriterManager: wm,
	}

	// 核心处理
	if err := processDashboards(sp); err != nil {
		log.Fatal(err)
	}
}

func mustInitWriterManager(cfg *C.StorageConfig) *writer.WriterManager {
	var neo4jDriver neo4j.Driver
	var pgDriver *sql.DB
	var err error

	if cfg.Neo4j.Enabled {
		neo4jDriver, err = writer.InitNeo4jDriver(&cfg.Neo4j)
		if err != nil {
			log.Fatal(err)
		}
	}

	if cfg.Postgres.Enabled {
		pgDriver, err = writer.InitPGClient(&cfg.Postgres)
		if err != nil {
			log.Fatal(err)
		}
	}

	return writer.InitWriterManager(&writer.WriterContext{
		Neo4jDriver: neo4jDriver,
		PgDriver:    pgDriver,
	})
}

func mustInitGrafanaClient(cfg *C.GrafanaService) *grafanaclient.GrafanaHTTPAPI {
	grafanaCfg := &grafanaclient.TransportConfig{
		Host:             cfg.Host,
		BasePath:         "/api",
		Schemes:          []string{"https"},
		BasicAuth:        url.UserPassword(cfg.User, cfg.Password),
		OrgID:            cfg.OrgID,
		NumRetries:       3,
		RetryStatusCodes: []string{"420", "5xx"},
		HTTPHeaders:      map[string]string{},
		// Debug:            true,
	}

	return grafanaclient.NewHTTPClientWithConfig(strfmt.Default, grafanaCfg)
}

func mustInitPGClients(pgConfigs []C.PostgresService) map[string]*PGBundle {
	pgMap := make(map[string]*PGBundle)
	for _, pgConf := range pgConfigs {
		db, err := writer.InitPGClient(&pgConf)
		if err != nil {
			log.Fatalf("failed to init pg client for label %s: %v", pgConf.Label, err)
		}
		pgMap[pgConf.Label] = &PGBundle{
			Client: db,
			Config: &pgConf,
		}
	}
	return pgMap
}

func closePGClients(pgMap map[string]*PGBundle) {
	for label, pg := range pgMap {
		if err := pg.Client.Close(); err != nil {
			log.Warnf("failed to close PG client for %s: %v", label, err)
		}
	}
}

func processDashboards(sp *ServiceProvider) error {
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

		dashboards, err := sp.Grafana.Client.Search.Search(params)
		if err != nil {
			return fmt.Errorf("error searching dashboards: %v", err)
		}

		if len(dashboards.Payload) == 0 {
			break
		}

		for _, dashboardItem := range dashboards.Payload {
			err := processDashboardItem(sp, dashboardItem)
			if err != nil {
				log.Errorf("Error processing dashboard item: %v", err)
			}
		}

		pageVar++
	}

	return nil
}

func processDashboardItem(sp *ServiceProvider, dashboardItem *models.Hit) error {
	dashboardFullWithMeta, err := sp.Grafana.Client.Dashboards.GetDashboardByUID(dashboardItem.UID)
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

	for _, panel := range dashboard.Dashboard.Panels {
		processPanelRecursive(sp, dashboard, panel)
	}

	return nil
}

func processPanelRecursive(sp *ServiceProvider, dashboard *service.DashboardFullWithMeta, panel *service.Panel) {
	if panel.Collapsed && len(panel.Panels) > 0 {
		for _, child := range panel.Panels {
			processPanelRecursive(sp, dashboard, child)
		}
		return
	}

	// 没有数据源的 Panel 直接跳过
	if panel.Datasource == nil {
		return
	}

	// 格式化 panel.Title
	if panel.Title == "" {
		panel.Title = "Untitled Panel"
	}
	panel.Title = fmt.Sprintf("%s %d-%d", panel.Title, dashboard.Dashboard.ID, panel.ID)

	processDatasourceForPanel(sp, panel, dashboard)
}

func processDatasourceForPanel(sp *ServiceProvider, panel *service.Panel, dashboard *service.DashboardFullWithMeta) {
	// 获取 panel 所依赖的数据源及表信息
	depMap, err := getPanelDependencies(sp, panel, dashboard)
	if err != nil {
		log.Errorf("Error getting dependencies for panel %d: %v", panel.ID, err)
		return
	}

	if len(depMap) == 0 {
		notCareDB := C.PostgresService{}
		if err := sp.WriterManager.CreateGraphGrafana(panel, dashboard, *sp.Grafana.Config, nil, notCareDB); err != nil {
			log.Errorf("Error creating graph for panel %d: %v", panel.ID, err)
		}
	}

	for ds, dependencies := range depMap {
		if err := sp.WriterManager.CreateGraphGrafana(panel, dashboard, *sp.Grafana.Config, dependencies, *ds); err != nil {
			log.Errorf("Error creating graph for panel %d: %v", panel.ID, err)
		}
	}
}
func resolveDatasource(client *grafanaclient.GrafanaHTTPAPI, ds any, dashboard *service.DashboardFullWithMeta) ([]*models.DataSource, error) {
	switch v := ds.(type) {
	case string:
		if strings.HasPrefix(v, "${") {
			return nil, fmt.Errorf("template variable %s not resolved", v)
		}
		dsObj, err := getDatasourceByName(client, v)
		if err != nil {
			return nil, err
		}
		return []*models.DataSource{dsObj}, nil

	case map[string]any:
		uid, ok := v["uid"].(string)
		if !ok {
			return nil, fmt.Errorf("invalid uid type")
		}

		if !strings.HasPrefix(uid, "${") {
			dsObj, err := getDatasourceByUid(client, uid)
			if err != nil {
				return nil, err
			}
			return []*models.DataSource{dsObj}, nil
		}

		varName := strings.TrimSuffix(strings.TrimPrefix(uid, "${"), "}")
		templatingVar := findTemplatingVariable(dashboard, varName)
		if templatingVar == nil {
			return nil, fmt.Errorf("template variable '%s' not found in dashboard", varName)
		}

		dsNames := extractDatasourceName(client, templatingVar)
		if len(dsNames) == 0 {
			return nil, fmt.Errorf("template variable '%s' has no matched datasource", varName)
		}

		var result []*models.DataSource
		for _, name := range dsNames {
			dsObj, err := getDatasourceByName(client, name)
			if err != nil {
				return nil, fmt.Errorf("error retrieving datasource '%s': %w", name, err)
			}
			result = append(result, dsObj)
		}
		return result, nil

	default:
		return nil, fmt.Errorf("unknown datasource type: %T", ds)
	}
}

func findTemplatingVariable(dashboard *service.DashboardFullWithMeta, name string) *service.TemplateVar {
	for _, t := range dashboard.Dashboard.Templating.List {
		if t.Name == name {
			return &t
		}
	}
	return nil
}

func extractDatasourceName(client *grafanaclient.GrafanaHTTPAPI, t *service.TemplateVar) []string {
	// 如果没有 regex，无法进行匹配
	if t.Regex == "" {
		return nil
	}

	// 获取所有数据源
	datasources, err := client.Datasources.GetDataSources()
	if err != nil {
		return nil
	}

	// fix: Go 的正则库 不支持 正则两边的斜杠 /.../，所以需要在代码中去掉首尾的斜杠
	re, err := regexp.Compile(strings.Trim(t.Regex, "/"))
	if err != nil {
		return nil
	}

	// 匹配所有符合的名称
	var matched []string
	for _, ds := range datasources.Payload {
		if re.MatchString(ds.Name) {
			matched = append(matched, ds.Name)
		}
	}

	return matched
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

func getDatasourceByUid(client *grafanaclient.GrafanaHTTPAPI, uid string) (*models.DataSource, error) {
	if uid == "" {
		return nil, fmt.Errorf("uid is empty")
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

func getPanelDependencies(sp *ServiceProvider, panel *service.Panel, dashboard *service.DashboardFullWithMeta) (map[*C.PostgresService][]*service.Table, error) {
	result := make(map[*C.PostgresService][]*service.Table)

	for _, t := range panel.Targets {
		// 跳过无数据源的 Target
		if t.Datasource == nil {
			continue
		}

		// 获取所有解析出的数据源
		dsList, err := resolveDatasource(sp.Grafana.Client, t.Datasource, dashboard)
		if err != nil {
			log.Warnf("Skipping unresolved datasource: %v", err)
			continue
		}

		for _, ds := range dsList {
			// 只处理 Postgres 数据源
			if ds.Type != "postgres" {
				log.Debugf("Datasource %s is not postgres, skipping", ds.Name)
				continue
			}

			// fix: 将 Name 转换为小写，适配 viper 配置文件的读取规则
			dsName := strings.ToLower(ds.Name)

			dbInstanceName, ok := sp.Grafana.Config.Datasources[dsName]
			if !ok {
				log.Warnf("Datasource %s not found in Grafana datasources", dsName)
				continue
			}

			pgBundle, ok := sp.PG[dbInstanceName]
			if !ok || pgBundle.Client == nil {
				log.Warnf("Datasource %s not found in PG clients", dbInstanceName)
				continue
			}

			var tables []*service.Table
			if t.RawSQL != "" {
				tables, _ = parseRawSQL(t.RawSQL, pgBundle)
			} else if t.Query != "" {
				tables, _ = parseRawSQL(t.Query, pgBundle)
			}

			if len(tables) > 0 {
				result[pgBundle.Config] = append(result[pgBundle.Config], tables...)
			}
		}
	}

	return result, nil
}

func parseRawSQL(rawsql string, pgBundle *PGBundle) ([]*service.Table, error) {
	var sqlTree *depgraph.Graph

	udf, err := lineage.IdentifyFuncCall(rawsql)
	// TODO:引入 AI for lineage
	if err == nil {
		sqlTree, err = lineage.HandleUDF4Lineage(pgBundle.Client, udf)
	} else {
		sqlTree, err = lineage.Parse(rawsql)
	}
	if err != nil {
		return nil, err
	}

	sqlTree.SetNamespace(pgBundle.Config.Label)

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
