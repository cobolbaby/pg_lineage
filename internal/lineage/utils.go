package lineage

import (
	"database/sql"
	"errors"
	"fmt"
	"pg_lineage/internal/service"
	"pg_lineage/pkg/log"
	"regexp"
)

var (
	PLPGSQL_UNHANLED_COMMANDS   = regexp.MustCompile(`(?i)set\s+(time zone|enable_)(.*?);`)
	PLPGSQL_GET_FUNC_DEFINITION = `
		SELECT nspname, proname, pg_get_functiondef(p.oid) as definition
		FROM pg_proc p JOIN pg_namespace n ON n.oid = p.pronamespace
		WHERE nspname = '%s' and proname = '%s'
		LIMIT 1;
	`

	PG_FuncCallPattern1 = regexp.MustCompile(`(?is)^\s*(select|call)\s+(\w+)\.(\w+)\((.*)\)\s*(;)?`)
	PG_FuncCallPattern2 = regexp.MustCompile(`(?is)^\s*select\s+(.*)from\s+(\w+)\.(\w+)\((.*)\)\s*(as\s+(.*))?\s*(;)?`)
)

func IdentifyFuncCall(sql string) (*service.Udf, error) {

	// 正则匹配，忽略大小写
	// select dw.func_insert_?()
	// call   dw.func_insert_?()
	// select * from dw.func_insert_?()

	if r := PG_FuncCallPattern1.FindStringSubmatch(sql); r != nil {
		log.Debug("FuncCallPattern1:", r[1], r[2], r[3])
		return &service.Udf{
			Type:       "plpgsql",
			SchemaName: r[2],
			ProcName:   r[3],
		}, nil
	}
	if r := PG_FuncCallPattern2.FindStringSubmatch(sql); r != nil {
		log.Debug("FuncCallPattern2:", r[1], r[2], r[3])
		return &service.Udf{
			Type:       "plpgsql",
			SchemaName: r[2],
			ProcName:   r[3],
		}, nil
	}

	return &service.Udf{}, errors.New("not a function call")
}

// 过滤部分关键词
func FilterUnhandledCommands(content string) string {
	// 字符串过滤，https://github.com/pganalyze/libpg_query/issues/125
	// return PLPGSQL_UNHANLED_COMMANDS.ReplaceAllString(content, "")
	return content
}

// 获取相关定义
func GetUDFDefinition(db *sql.DB, udf *service.Udf) (string, error) {

	rows, err := db.Query(fmt.Sprintf(PLPGSQL_GET_FUNC_DEFINITION, udf.SchemaName, udf.ProcName))
	if err != nil {
		return "", err
	}
	defer rows.Close()

	var nspname string
	var proname string
	var definition string

	for rows.Next() {
		err := rows.Scan(&nspname, &proname, &definition)
		switch err {
		case sql.ErrNoRows:
			log.Warn("No rows were returned")
		case nil:
			log.Infof("Query Data = (%s, %s)\n", nspname, proname)
		default:
			log.Fatalf("rows.Scan err: %s", err)
		}
	}

	return definition, nil
}
