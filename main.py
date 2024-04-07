import os
import connection
import sqlparse
import pandas as pd

if __name__ == '__main__':
    pd.set_option('display.max_columns', None)
    pd.set_option('display.max_rows', None)
    
    # connecton datasource
    conf = connection.config('marketplace_prod')
    conn, engine = connection.get_conn(conf, 'dataSource')
    cursor = conn.cursor()

    # connecton dataWarehouse
    conf_dwh = connection.config('dwh')
    conn_dwh, engine_dwh = connection.get_conn(conf_dwh, 'dataWarehouse')
    cursor_dwh = conn_dwh.cursor()
    
    # get query string
    path_query = os.getcwd()+'/query/'
    query = sqlparse.format(
        open(path_query+'query.sql', 'r').read(), strip_comment=True
    ).strip()
    dwh_design = sqlparse.format(
        open(path_query+'dwh_design.sql', 'r').read(), strip_comment=True
    ).strip()
    
    try:
        # get data
        print('[info] service etl is running..')
        df = pd.read_sql(query, engine)
             
        # create schema
        cursor_dwh.execute(dwh_design)
        conn_dwh.commit()
           
        # ingest data to dwh
        df.to_sql('dim_orders', engine_dwh, schema='galang_dwh',
                  if_exists='append', index=False)
        
        print('[info] service etl is success...')
    except Exception as e:
        print('[info] service etl is failes')
        print(str(e))
