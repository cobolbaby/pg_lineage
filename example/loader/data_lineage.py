import concurrent.futures
import datetime
import logging
import time

import pandas as pd
import psycopg2
import yaml
from neo4j import GraphDatabase, basic_auth

logging.basicConfig(level=logging.INFO)

def load_config():
    with open("./config.yaml", "r") as f:
        return yaml.safe_load(f)

config = load_config()

def get_pg_conn():
    return psycopg2.connect(config['storage']['postgres']['dsn'])

neo4j_conf = config['storage']['neo4j']
neo_driver = GraphDatabase.driver(
    neo4j_conf['url'], auth=basic_auth(neo4j_conf['user'], neo4j_conf['password'])
)

# 查询最近 7 天的节点
def fetch_nodes():
    sql = '''
        SELECT node_name, type,
            attribute || jsonb_build_object(
                'service', service,
                'domain', domain,
                'site', site,
                'author', coalesce(author, '-'),
                'node', node
            ) AS attribute
        FROM manager.data_lineage_node
        WHERE cdt > current_date - interval '7 day'
    '''
    with get_pg_conn() as conn:
        return pd.read_sql(sql, conn)

# 查询最近 7 天的关系
def fetch_relationships():
    sql = '''
        SELECT a.*
        FROM manager.data_lineage_relationship a
        JOIN manager.data_lineage_node b ON a.up_node_name = b.node_name
        JOIN manager.data_lineage_node c ON a.down_node_name = c.node_name
        WHERE b.cdt > current_date - interval '7 day'
           OR c.cdt > current_date - interval '7 day'
    '''
    with get_pg_conn() as conn:
        return pd.read_sql(sql, conn)

# 写入节点
def write_nodes(tx, df):
    start_time = time.time()

    for _, row in df.iterrows():
        query = f"""
            MERGE (n:`{row['type']}` {{ name: $name }})
            SET n += $props
        """
        tx.run(query, name=row['node_name'], props=row['attribute'])

    duration = time.time() - start_time
    logging.info(f"[Nodes] Wrote {len(df)} rows in {duration:.2f}s")

# 写入关系
def write_relationships(tx, df):
    start_time = time.time()

    for _, row in df.iterrows():
        query = f"""
            MATCH (a {{ name: $up_name }})
            MATCH (b {{ name: $down_name }})
            MERGE (a)-[r:`{row['type']}`]->(b)
            SET r += $props
        """
        tx.run(query, up_name=row['up_node_name'], down_name=row['down_node_name'], props=row['attribute'])
        
    duration = time.time() - start_time
    logging.info(f"[Relationships] Wrote {len(df)} rows in {duration:.2f}s")

def batch_write_nodes(tx, df):
    start_time = time.time()

    query = """
    UNWIND $rows AS row
    MERGE (n:datamap:`%s` { name: row.name })
    SET n += row.props
    """ % df.iloc[0]['type']

    data = [{'name': row['node_name'], 'props': row['attribute']} for _, row in df.iterrows()]
    tx.run(query, rows=data)

    duration = time.time() - start_time
    logging.info(f"[Nodes] Wrote {len(df)} rows in {duration:.2f}s")

def batch_write_relationships(tx, df):
    start_time = time.time()

    try:
        grouped = df.groupby('type')

        total_rows = 0
        for rel_type, group in grouped:
            query = f"""
            UNWIND $rows AS row
            MATCH (a:datamap {{ name: row.up_name }})
            MATCH (b:datamap {{ name: row.down_name }})
            MERGE (a)-[r:`{rel_type}`]->(b)
            SET r += row.props
            """

            data = [
                {
                    'up_name': row['up_node_name'],
                    'down_name': row['down_node_name'],
                    'props': row['attribute']
                }
                for _, row in group.iterrows()
            ]

            tx.run(query, rows=data)
            total_rows += len(group)

        duration = time.time() - start_time
        logging.info(f"[Relationships] Wrote {total_rows} rows in {duration:.2f}s")
    
    except Exception as e:
        duration = time.time() - start_time
        logging.error(f"[Relationships] Failed to write relationships after {duration:.2f}s: {str(e)}")
        raise

def create_datamap_index():
    with neo_driver.session() as session:
        # https://neo4j.com/docs/cypher-manual/current/indexes/search-performance-indexes/overview/
        session.run("CREATE INDEX idx_datamap_node_name IF NOT EXISTS FOR (n:datamap) ON (n.name);")

# 批量执行事务
def batch_write(data, write_fn, batch_size=500, max_workers=5):
    chunks = [data.iloc[i:i+batch_size] for i in range(0, len(data), batch_size)]
    def worker(chunk):
        with neo_driver.session() as session:
            session.execute_write(write_fn, chunk)
    with concurrent.futures.ThreadPoolExecutor(max_workers=max_workers) as executor:
        executor.map(worker, chunks)

if __name__ == '__main__':
    start = datetime.datetime.now()

    try:

        nodes = fetch_nodes()
        relationships = fetch_relationships()

        # batch_write(nodes, write_nodes, batch_size=500, max_workers=4)
        # batch_write(relationships, write_relationships, batch_size=500, max_workers=2)
        
        # 逐条写入的总耗时
        # Used time: 2:18:35.098832

        create_datamap_index()
        batch_write(nodes, batch_write_nodes, batch_size=1000, max_workers=4)
        batch_write(relationships, batch_write_relationships, batch_size=1000, max_workers=1)

        # 批量 + 索引的总耗时
        # Total time: 0:01:26.340983
    
    except Exception as e:
        logging.error(e)

    logging.info("Total time: %s", datetime.datetime.now() - start)
