package service

import (
	"strconv"

	"github.com/grafana/grafana-openapi-client-go/models"
)

type Panel struct {
	Datasource  any    `json:"datasource"`
	Description string `json:"description,omitempty"`
	ID          int    `json:"id"`
	Targets     []struct {
		Query    string `json:"query"`
		RawQuery bool   `json:"rawQuery"`
		RawSQL   string `json:"rawSql"`
	} `json:"targets,omitempty"`
	Title string `json:"title"`
	Type  string `json:"type"`
}

type Dashboard struct {
	ID         int      `json:"id"`
	Panels     []*Panel `json:"panels"`
	Tags       []string `json:"tags"`
	Templating struct {
		List []struct {
			Datasource any    `json:"datasource,omitempty"`
			Label      string `json:"label"`
			Query      any    `json:"query,omitempty"`
			Type       string `json:"type"`
		} `json:"list"`
	} `json:"templating"`
	Time struct {
		From string `json:"from"`
		To   string `json:"to"`
	} `json:"time"`
	Timezone string `json:"timezone"`
	Title    string `json:"title"`
	UID      string `json:"uid"`
	Version  int    `json:"version"`
}

type DashboardFullWithMeta struct {
	// dashboard
	Dashboard Dashboard `json:"dashboard,omitempty"`

	// meta
	Meta *models.DashboardMeta `json:"meta,omitempty"`
}

func (p *Panel) GetID() string {
	return strconv.Itoa(p.ID)
}

func (p *Panel) IsTemp() bool {
	return false
}
