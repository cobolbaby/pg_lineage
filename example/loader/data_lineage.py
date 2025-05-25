import concurrent.futures
import datetime

import pandas as pd
import psycopg2
import yaml
from neo4j import GraphDatabase, basic_auth


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
    for _, row in df.iterrows():
        query = f"""
            MERGE (n:`{row['type']}` {{ name: $name }})
            SET n += $props
        """
        tx.run(query, name=row['node_name'], props=row['attribute'])

# 写入关系
def write_relationships(tx, df):
    for _, row in df.iterrows():
        query = f"""
            MATCH (a {{ name: $up_name }})
            MATCH (b {{ name: $down_name }})
            MERGE (a)-[r:`{row['type']}`]->(b)
            SET r += $props
        """
        tx.run(query, up_name=row['up_node_name'], down_name=row['down_node_name'], props=row['attribute'])

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

    nodes = fetch_nodes()
    relationships = fetch_relationships()

    batch_write(nodes, write_nodes, batch_size=500, max_workers=5)
    batch_write(relationships, write_relationships, batch_size=500, max_workers=2)

    print("Used time:", datetime.datetime.now() - start)
