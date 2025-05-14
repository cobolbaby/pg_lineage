package writer

import (
	"database/sql"
	"errors"
	"fmt"
	"pg_lineage/internal/service"
	"pg_lineage/pkg/config"
	"pg_lineage/pkg/log"
)

type PGLineageWriter struct {
	db *sql.DB // 在 init 时初始化好的连接池
}

func InitPGClient(c *config.PostgresService) (*sql.DB, error) {
	if c == nil {
		return nil, fmt.Errorf("postgres config is nil")
	}

	db, err := sql.Open("postgres", c.DSN)
	if err != nil {
		return nil, fmt.Errorf("sql.Open err: %w", err)
	}

	// 可选：ping 测试连接
	if err := db.Ping(); err != nil {
		db.Close()
		return nil, fmt.Errorf("db.Ping err: %w", err)
	}

	return db, nil
}

func (p *PGLineageWriter) Init(ctx *WriterContext) error {
	if ctx.PgDriver == nil {
		return errors.New("Postgres DB not provided")
	}
	p.db = ctx.PgDriver
	return nil
}

func (w *PGLineageWriter) ResetGraph() error {

	tx, err := w.db.Begin()
	if err != nil {
		return err
	}
	defer rollbackOnError(tx, err)

	smt := `
		DELETE FROM manager.data_lineage_node WHERE service = 'grafana';
		DELETE FROM manager.data_lineage_relationship WHERE down_node_name like '%:grafana:%';
	`

	if _, err = tx.Exec(smt); err != nil {
		return err
	}

	return tx.Commit()
}

func (w *PGLineageWriter) WriteDashboardNode(d *service.DashboardFullWithMeta, s config.GrafanaService) error {
	tx, err := w.db.Begin()
	if err != nil {
		return err
	}
	defer rollbackOnError(tx, err)

	smt := fmt.Sprintf(`
		INSERT INTO manager.data_lineage_node(
			node_name, site, service, domain, node, attribute, type, cdt, udt, author
		) VALUES (
			'%s:grafana:%s:%s>%s', 
			'%s', 'grafana', '%s', '%s>%s',
			jsonb_build_object(
				'created', '%s',
				'updated', '%s',
				'created_by', '%s',
				'updated_by', '%s',
				'dashboard_title', '%s',
				'dashboard_uid', '%s'
			),
			'dashboard', now(), now(), 'ITC180012'
		)
		ON CONFLICT (node_name) DO NOTHING;`,
		s.Zone, s.Host, d.Meta.FolderTitle, d.Dashboard.Title,
		s.Zone, s.Host, d.Meta.FolderTitle, d.Dashboard.Title,
		d.Meta.Created.String(),
		d.Meta.Updated.String(),
		d.Meta.CreatedBy,
		d.Meta.UpdatedBy,
		d.Dashboard.Title,
		d.Dashboard.UID,
	)

	log.Debug(smt)

	if _, err = tx.Exec(smt); err != nil {
		return err
	}

	return tx.Commit()
}

func (w *PGLineageWriter) WritePanelNode(p *service.Panel, d *service.DashboardFullWithMeta, s config.GrafanaService) error {
	tx, err := w.db.Begin()
	if err != nil {
		return err
	}
	defer rollbackOnError(tx, err)

	// 直接使用 Debugf 中的 SQL INSERT，和原来你写的逻辑一样
	smt := fmt.Sprintf(`
		INSERT INTO manager.data_lineage_node(
			node_name, site, service, domain, node, attribute, type, cdt, udt, author)
		VALUES (
			'%s:grafana:%s:%s>%s>%s', 
			'%s', 'grafana', '%s', '%s>%s>%s', 
			jsonb_build_object(
				'created', '%s',
				'updated', '%s',
				'created_by', '%s',
				'updated_by', '%s',
				'panel_type', '%s',
				'panel_title', '%s',
				'dashboard_uid', '%s',
				'dashboard_title', '%s',
				'panel_description', regexp_replace('%s', '^0x', '')
			),
			'dashboard-panel', now(), now(), 'ITC180012'
		)
		ON CONFLICT (node_name) DO NOTHING;`,
		s.Zone, s.Host, d.Meta.FolderTitle, d.Dashboard.Title, p.Title, // node_name
		s.Zone, s.Host, d.Meta.FolderTitle, d.Dashboard.Title, p.Title, // domain, node
		d.Meta.Created.String(),
		d.Meta.Updated.String(),
		d.Meta.CreatedBy,
		d.Meta.UpdatedBy,
		p.Type,
		p.Title,
		d.Dashboard.UID,
		d.Dashboard.Title,
		p.Description,
	)

	log.Debug(smt)

	if _, err = tx.Exec(smt); err != nil {
		return err
	}

	return tx.Commit()
}

func (w *PGLineageWriter) WriteTable2PanelEdge(p *service.Panel, d *service.DashboardFullWithMeta, s config.GrafanaService, dependencies []*service.Table, ds config.PostgresService) error {
	tx, err := w.db.Begin()
	if err != nil {
		return err
	}
	defer rollbackOnError(tx, err)

	for _, t := range dependencies {

		smt := fmt.Sprintf(`
			INSERT INTO manager.data_lineage_relationship(
				up_node_name, 
				down_node_name, 
				type, 
				attribute, 
				cdt, 
				udt, 
				name, 
				author
			) VALUES (
				'%s:postgresql:%s:%s.%s.%s', 
				'%s:grafana:%s:%s>%s>%s', 
				'data_logic', 
				'{}', 
				now(), 
				now(), 
				md5('%s:postgresql:%s:%s.%s.%s' || '_' || '%s:grafana:%s:%s>%s>%s' || '_' || '{}'::varchar), 
				'ITC180012'
			)
			ON CONFLICT (name) DO NOTHING;`,
			ds.Zone, t.Database, ds.DBName, t.SchemaName, t.RelName,
			s.Zone, s.Host, d.Meta.FolderTitle, d.Dashboard.Title, p.Title,
			ds.Zone, t.Database, ds.DBName, t.SchemaName, t.RelName,
			s.Zone, s.Host, d.Meta.FolderTitle, d.Dashboard.Title, p.Title,
		)

		log.Debug(smt)

		if _, err = tx.Exec(smt); err != nil {
			return err
		}

	}

	return tx.Commit()
}

func (w *PGLineageWriter) WriteDash2PanelEdge(p *service.Panel, d *service.DashboardFullWithMeta, s config.GrafanaService) error {
	tx, err := w.db.Begin()
	if err != nil {
		return err
	}
	defer rollbackOnError(tx, err)

	smt := fmt.Sprintf(`
		INSERT INTO manager.data_lineage_relationship(
			up_node_name, 
			down_node_name, 
			type, 
			attribute, 
			cdt, 
			udt, 
			name, 
			author
		) VALUES (
			'%s:grafana:%s:%s>%s', 
			'%s:grafana:%s:%s>%s>%s', 
			'data_logic', 
			'{}', 
			now(),
			now(),
			md5('%s:grafana:%s:%s>%s' || '_' || '%s:grafana:%s:%s>%s>%s' || '_' || '{}'::varchar), 
			'ITC180012'
		)
		ON CONFLICT (name) DO NOTHING;`,
		s.Zone, s.Host, d.Meta.FolderTitle, d.Dashboard.Title,
		s.Zone, s.Host, d.Meta.FolderTitle, d.Dashboard.Title, p.Title,
		s.Zone, s.Host, d.Meta.FolderTitle, d.Dashboard.Title,
		s.Zone, s.Host, d.Meta.FolderTitle, d.Dashboard.Title, p.Title,
	)

	log.Debug(smt)

	if _, err = tx.Exec(smt); err != nil {
		return err
	}

	return tx.Commit()
}

func rollbackOnError(tx *sql.Tx, err error) {
	if p := recover(); p != nil {
		tx.Rollback()
		panic(p)
	}
	if err != nil {
		tx.Rollback()
	}
}

// 创建图中节点
func (w *PGLineageWriter) WriteTableNode(r *service.Table, s config.PostgresService) error {
	tx, err := w.db.Begin()
	if err != nil {
		return err
	}
	defer rollbackOnError(tx, err)

	// 调整 Debugf 输出的 INSERT 语句
	smt := fmt.Sprintf(`
		INSERT INTO manager.data_lineage_node(
			node_name, site, service, domain, node, attribute, type, cdt, udt, author)
		VALUES (
			'%s:postgresql:%s:%s.%s.%s',
			'%s', 'postgresql', '%s', '%s.%s.%s',
			jsonb_build_object(
				'site', '%s',
				'owner', '???',
				'database', '%s',
				'schema', '%s',
				'tablename', '%s',
				'relpersistence', '%s',
				'calls', %d
			),
			'postgresql-table', now(), now(), 'ITC180012'
		)
		ON CONFLICT (node_name) DO UPDATE
		SET udt = now(),
			attribute = jsonb_set(
				EXCLUDED.attribute,
				'{calls}',
				to_jsonb( ((EXCLUDED.attribute->>'calls')::bigint + %d) )
		);`,
		s.Zone, r.Database, s.DBName, r.SchemaName, r.RelName,
		s.Zone, r.Database, s.DBName, r.SchemaName, r.RelName,
		s.Zone, s.DBName, r.SchemaName, r.RelName, r.RelPersistence, r.Calls,
		r.Calls,
	)

	log.Debug(smt)

	if _, err = tx.Exec(smt); err != nil {
		return err
	}

	return tx.Commit()
}

// 创建图中边
func (w *PGLineageWriter) WriteFuncEdge(r *service.Udf, s config.PostgresService) error {

	return nil
}

func (w *PGLineageWriter) CompleteTableNode(r *service.Table, s config.PostgresService) error {
	tx, err := w.db.Begin()
	if err != nil {
		return err
	}
	defer rollbackOnError(tx, err)

	// 更新后的 Debugf 输出，包含更多字段的处理
	smt := fmt.Sprintf(`
		INSERT INTO manager.data_lineage_node(
			node_name, site, service, domain, node, attribute, type, cdt, udt, author)
		VALUES (
			'%s:postgresql:%s:%s.%s.%s',
			'%s', 'postgresql', '%s', '%s.%s.%s',
			jsonb_build_object(
				'site', '%s',
				'owner', '???',
				'database', '%s',
				'schema', '%s',
				'tablename', '%s',
				'relpersistence', '%s',
				'calls', %d,
				'seq_scan', %d,
				'seq_tup_read', %d,
				'idx_scan', %d,
				'idx_tup_fetch', %d,
				'comment', regexp_replace('%s', '^0x', '')
			),
			'postgresql-table', now(), now(), 'ITC180012'
		)
		ON CONFLICT (node_name) DO UPDATE SET
			udt = now(),
			attribute = EXCLUDED.attribute || jsonb_build_object(
				'calls', %d,
				'seq_scan', %d,
				'seq_tup_read', %d,
				'idx_scan', %d,
				'idx_tup_fetch', %d,
				'comment', regexp_replace('%s', '^0x', '')
		);`,
		s.Zone, r.Database, s.DBName, r.SchemaName, r.RelName,
		s.Zone, r.Database, s.DBName, r.SchemaName, r.RelName,
		s.Zone, r.Database, r.SchemaName, r.RelName, r.RelPersistence,
		r.Calls, r.SeqScan, r.SeqTupRead, r.IdxScan, r.IdxTupFetch,
		r.Comment,
		r.Calls, r.SeqScan, r.SeqTupRead, r.IdxScan, r.IdxTupFetch,
		r.Comment,
	)

	log.Debug(smt)

	if _, err = tx.Exec(smt); err != nil {
		return err
	}

	return tx.Commit()

}
