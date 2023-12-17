import pandas as pd
from sqlalchemy import create_engine
import time
import psycopg2
from psycopg2.extras import execute_values
from io import StringIO
import csv



df = pd.read_csv("klines.csv")

def to_sql(): 
    # Example: 'postgresql://username:password@localhost:5432/your_database'
    engine = create_engine('postgresql://postgres:postgres@localhost:5432/postgres')

    start_time = time.time() 

    df.to_sql(
        name="test", 
        con=engine,  
        if_exists="replace", 
        index=False 
    )

    total_time = time.time() - start_time 
    print(f"Insert time: {total_time} seconds") 
    
# to_sql()
#  Insert time: 16.5913827419281 seconds    


def copy_expert():
    conn = psycopg2.connect(
        dbname='postgres',
        user='postgres',
        password='postgres',
        host='localhost',
        port='5432'
    )

    start_time = time.time() 

    sio = StringIO()
    df.to_csv(sio, index=None, header=None)
    sio.seek(0)
    with conn.cursor() as cur:
        cur.copy_expert(
            sql="""
            COPY test (symbol, open, close, high, low, volume, time) FROM STDIN WITH CSV""",
            file=sio
        )
        conn.commit()

    total_time = time.time() - start_time 
    print(f"Insert time: {total_time} seconds") 

# copy_expert()
# Insert time: 2.0030646324157715 seconds


def copy_expert_csv():
    conn = psycopg2.connect(
        dbname='postgres',
        user='postgres',
        password='postgres',
        host='localhost',
        port='5432'
    )

    start_time = time.time() # get start time before insert

    sio = StringIO()
    writer = csv.writer(sio)
    writer.writerows(df.values)
    sio.seek(0)
    with conn.cursor() as cur:
        cur.copy_expert(
            sql="""
            COPY test (symbol, open, close, high, low, volume, time) FROM STDIN WITH CSV""",
            file=sio
        )
        conn.commit()

    total_time = time.time() - start_time 
    print(f"Insert time: {total_time} seconds") 

# copy_expert_csv()
# Insert time: 2.1114087104797363 seconds


def to_sql_method_copy():
    def psql_insert_copy(table, conn, keys, data_iter): 
        dbapi_conn = conn.connection
        with dbapi_conn.cursor() as cur:
            s_buf = StringIO()
            writer = csv.writer(s_buf)
            writer.writerows(data_iter)
            s_buf.seek(0)

            columns = ', '.join('"{}"'.format(k) for k in keys)
            if table.schema:
                table_name = '{}.{}'.format(table.schema, table.name)
            else:
                table_name = table.name

            sql = 'COPY {} ({}) FROM STDIN WITH CSV'.format(table_name, columns)
            cur.copy_expert(sql=sql, file=s_buf)

    engine = create_engine('postgresql://postgres:postgres@localhost:5432/postgres')

    start_time = time.time() 
    df.to_sql(
        name="test",
        con=engine,
        if_exists="replace",
        index=False,
        method=psql_insert_copy
    )

    total_time = time.time() - start_time 
    print(f"Insert time: {total_time} seconds") 

# to_sql_method_copy()
# Insert time: 1.869837760925293 seconds
