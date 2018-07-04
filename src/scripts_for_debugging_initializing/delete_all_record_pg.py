'''
connect to postgres and delete all records in the static tables:
source_type, document_type, sources, model_type, models
a function to reset the pg tables in test runs
author: Heling
'''
from amp_poll.ct_poll import ct_pipeline_config as config
import psycopg2

pg_credentials = config.export_db_credentials
pg_DSN = "dbname={} user={} host={} password={}".format(pg_credentials['db'], pg_credentials['user'], pg_credentials['host'], pg_credentials['password'])
#table_name = 'test3'

with psycopg2.connect(pg_DSN) as pg_conn:
    with pg_conn.cursor() as pg_cur:
        pg_cur.execute('DELETE FROM sabrage.source_type')
        pg_cur.execute('DELETE FROM sabrage.sources')
        pg_cur.execute('DELETE FROM sabrage.document_type')
        pg_cur.execute('DELETE FROM sabrage.model_type')
        pg_cur.execute('DELETE FROM sabrage.models')
        pg_conn.commit()

#pg_conn.rollback()

with psycopg2.connect(pg_DSN) as pg_conn:
    with pg_conn.cursor() as pg_cur:
        pg_cur.execute('DELETE FROM sabrage.documents')
        pg_cur.execute('DELETE FROM sabrage.document_version')
        pg_cur.execute('DELETE FROM sabrage.document_xpath')
        pg_conn.commit()




