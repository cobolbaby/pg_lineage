package config

import (
	"fmt"
	"strings"

	"github.com/spf13/viper"
)

type Config struct {
	Storage StorageConfig `mapstructure:"storage"` // 用于埋点数据的存储
	Log     LogConfig     `mapstructure:"log"`
	Service ServiceConfig `mapstructure:"service"`
}

type StorageConfig struct {
	Neo4j    Neo4jService    `mapstructure:"neo4j"`
	Postgres PostgresService `mapstructure:"postgres"`
}

type LogConfig struct {
	Level string `mapstructure:"level"`
	Path  string `mapstructure:"path"`
}

type ServiceConfig struct {
	Postgres []PostgresService `mapstructure:"postgres"`
	Grafana  GrafanaService    `mapstructure:"grafana"`
}

type Neo4jService struct {
	URL      string `mapstructure:"url"`
	User     string `mapstructure:"user"`
	Password string `mapstructure:"password"`
	Enabled  bool   `mapstructure:"enabled"`
}

type PostgresService struct {
	Zone    string `mapstructure:"zone"`
	DSN     string `mapstructure:"dsn"`
	DBName  string `mapstructure:"dbname"`
	Label   string `mapstructure:"label"`
	Enabled bool   `mapstructure:"enabled"`
	Type    string `mapstructure:"type"`
}

type GrafanaService struct {
	Zone         string            `mapstructure:"zone"`
	Host         string            `mapstructure:"host"`
	User         string            `mapstructure:"user"`
	Password     string            `mapstructure:"password"`
	OrgID        int64             `mapstructure:"org_id"`
	DashboardIDs []int64           `mapstructure:"dashids"`
	Datasources  map[string]string `json:"datasources"`
}

func InitConfig(cfgFile string) (Config, error) {

	var config Config

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
	viper.SetEnvPrefix("PG_LINEAGE")
	replacer := strings.NewReplacer("-", "_")
	viper.SetEnvKeyReplacer(replacer)
	viper.AutomaticEnv() // read in environment variables that match

	// 读取配置文件
	err := viper.ReadInConfig()
	if err != nil {
		fmt.Println("Error reading config file:", err)
		return config, err
	}

	// 将配置文件内容解析到结构体中
	err = viper.Unmarshal(&config)
	if err != nil {
		fmt.Println("Error parsing config file:", err)
		return config, err
	}

	return config, nil
}
