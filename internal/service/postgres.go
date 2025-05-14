package service

import (
	"strings"
	"time"
)

const (
	REL_PERSIST     = "p"
	REL_PERSIST_NOT = "t"
)

type Owner struct {
	Username string
	Nickname string
	ID       string
}

type Table struct {
	ID             string
	Database       string
	SchemaName     string
	RelName        string
	RelPersistence string
	RelKind        string
	Columns        []string
	Size           int64
	Owner          *Owner
	CreateTime     time.Time
	Tags           []string
	Calls          int64
	SeqScan        int64
	SeqTupRead     int64
	IdxScan        int64
	IdxTupFetch    int64
	Comment        string
}

func (r *Table) GetID() string {
	if r.ID != "" {
		return r.ID
	}

	if r.SchemaName != "" {
		return r.SchemaName + "." + r.RelName
	} else {
		switch r.RelName {
		case "pg_namespace", "pg_class", "pg_attribute", "pg_type":
			r.SchemaName = "pg_catalog"
			return r.SchemaName + "." + r.RelName
		default:
			return r.RelName
		}
	}
}

func (r *Table) IsTemp() bool {
	return strings.HasPrefix(r.SchemaName, "pg_temp_") || r.RelPersistence == REL_PERSIST_NOT ||
		r.SchemaName == ""
}

type Udf struct {
	ID         string
	Database   string
	SchemaName string
	ProcName   string
	Type       string
	SrcID      string
	DestID     string
	Owner      *Owner
	Calls      int64
	Comment    string
}

func (o *Udf) GetID() string {
	if o.ID != "" {
		return o.ID
	}

	if o.SchemaName == "" {
		o.SchemaName = "public"
	}
	return o.SchemaName + "." + o.ProcName
}
