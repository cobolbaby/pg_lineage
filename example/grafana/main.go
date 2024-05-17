package main

import (
	"encoding/json"
	"fmt"
	"log"

	"github.com/go-openapi/strfmt"
	grafanaclient "github.com/grafana/grafana-openapi-client-go/client"
	grafanadashboard "github.com/grafana/grafana-openapi-client-go/client/dashboards"
	grafanasearch "github.com/grafana/grafana-openapi-client-go/client/search"
)

type Dashboard struct {
	ID     int `json:"id"`
	Panels []struct {
		Datasource any    `json:"datasource"`
		ID         int    `json:"id"`
		Title      string `json:"title"`
		Type       string `json:"type"`
		Targets    []struct {
			Query    string `json:"query"`
			RawQuery bool   `json:"rawQuery"`
			RawSQL   string `json:"rawSql"`
		} `json:"targets,omitempty"`
		Description string `json:"description,omitempty"`
	} `json:"panels"`
	SchemaVersion int   `json:"schemaVersion"`
	Tags          []any `json:"tags"`
	Templating    struct {
		List []struct {
			AllValue any `json:"allValue"`
			Current  struct {
				Text  any `json:"text"`
				Value any `json:"value"`
			} `json:"current"`
			Datasource     any    `json:"datasource"`
			Definition     string `json:"definition"`
			Hide           int    `json:"hide"`
			IncludeAll     bool   `json:"includeAll"`
			Index          int    `json:"index"`
			Label          any    `json:"label"`
			Multi          bool   `json:"multi"`
			Name           string `json:"name"`
			Options        []any  `json:"options"`
			Query          string `json:"query"`
			Refresh        int    `json:"refresh"`
			Regex          string `json:"regex"`
			SkipURLSync    bool   `json:"skipUrlSync"`
			Sort           int    `json:"sort"`
			TagValuesQuery string `json:"tagValuesQuery"`
			Tags           []any  `json:"tags"`
			TagsQuery      string `json:"tagsQuery"`
			Type           string `json:"type"`
			UseTags        bool   `json:"useTags"`
		} `json:"list"`
	} `json:"templating"`
	Time struct {
		From string `json:"from"`
		To   string `json:"to"`
	} `json:"time"`
	Timezone  string `json:"timezone"`
	Title     string `json:"title"`
	UID       string `json:"uid"`
	Variables struct {
		List []any `json:"list"`
	} `json:"variables"`
	Version int `json:"version"`
}

func main() {
	cfg := &grafanaclient.TransportConfig{
		// Host is the doman name or IP address of the host that serves the API.
		Host: "grafana8.itc.inventec.net",
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

	client := grafanaclient.NewHTTPClientWithConfig(strfmt.Default, cfg)

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
			dashboardFullWithMeta, err := client.Dashboards.GetDashboardByUIDWithParams(
				grafanadashboard.NewGetDashboardByUIDParams().WithUID(dashboardItem.UID))
			if err != nil {
				log.Fatalf("Error getting dashboard by UID: %v", err)
			}

			// 将 JSON 数据反序列化为 Dashboard 结构体
			var dashboardConfig Dashboard
			dashboardJSON, err := json.Marshal(dashboardFullWithMeta.Payload.Dashboard)
			if err != nil {
				log.Fatalf("Error marshalling dashboard JSON: %v", err)
			}

			// fmt.Println(string(dashboardJSON))

			err = json.Unmarshal(dashboardJSON, &dashboardConfig)
			if err != nil {
				log.Fatalf("Error unmarshalling dashboard JSON: %v", err)
			}

			// 读取特定数据，例如仪表板标题和面板信息
			fmt.Printf("Dashboard Title: %s\n", dashboardConfig.Title)
			for _, panel := range dashboardConfig.Panels {
				if panel.Datasource == nil {
					continue
				}

				fmt.Printf("Panel ID: %d, Panel Type: %s, Panel Title: %s\n", panel.ID, panel.Type, panel.Title)
				for _, t := range panel.Targets {
					if t.RawSQL != "" { // custom raw SQL query
						fmt.Printf("Panel Datasource: %s, Panel RawSQL: %s\n", panel.Datasource, t.RawSQL)
					}
					if t.Query != "" { // influxdb query
						fmt.Printf("Panel Datasource: %s, Panel Query: %s\n", panel.Datasource, t.Query)
					}

					// TODO:增加 SQL 解析，然后入库
				}
			}
		}

		// 增加页码，准备下一页查询
		pageVar++
	}

}
