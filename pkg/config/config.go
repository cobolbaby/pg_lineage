package config

import (
	"fmt"
	"pg_lineage/pkg/log"
	"strings"

	"github.com/spf13/viper"
)

type Config struct {
	PostgreSQL struct {
		DSN   string `mapstructure:"dsn"`
		Alias string `mapstructure:"alias"`
	} `mapstructure:"postgresql"`
	Neo4j struct {
		URL      string `mapstructure:"url"`
		User     string `mapstructure:"user"`
		Password string `mapstructure:"password"`
	} `mapstructure:"neo4j"`
	Log     log.LoggerConfig
	Grafana struct {
		Host string `mapstructure:"host"`
	}
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
