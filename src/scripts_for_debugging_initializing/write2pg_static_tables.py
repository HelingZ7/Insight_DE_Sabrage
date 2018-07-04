'''
connect to postgres and write the static tables:
source_type, document_type, sources, model_type, models
other tables will be written during the program
author: Heling
'''
from amp_poll.ct_poll import ct_pipeline_config as config
from amp_data_tools.amp_io.amp_io_helpers import get_now
import psycopg2
#from psycopg2 import sql
#import logging
#from amp_data_tools.amp_io.amp_io_helpers import get_logger

#logger = get_logger(None, file_path='/test.log')

pg_credentials = config.export_db_credentials
pg_DSN = "dbname={} user={} host={} password={}".format(pg_credentials['db'], pg_credentials['user'], pg_credentials['host'], pg_credentials['password'])
#table_name = 'test3'

schema_source_type =    ["id", "name", "notes", "active", "create_by", "create_date", "update_by", "update_date"]
schema_sources =        ["id", "source_type_id", "name", "notes", "active", "create_by", "create_date", "update_by", "update_date"]
schema_document_type =  ["id", "name", "notes", "active", "create_by", "create_date", "update_by", "update_date"]
schema_model_type =     ["id", "name", "notes", "active", "create_by", "create_date", "update_by", "update_date"]
schema_models =         ["id", "model_type_id", "name", "notes", "active", "create_by", "create_date", "update_by", "update_date"]

source_type_values =    (1,'ClinicalTrial', 'test1', 1, 'Heling', get_now("%Y-%m-%d"), 'Heling', get_now("%Y-%m-%d"))
sources_values =        (1, 1, 'ClinicalTrial.gov', 'test1', 1, 'Heling', get_now("%Y-%m-%d"), 'Heling', get_now("%Y-%m-%d"))
document_type_values =  (1,'XML', 'test1', 1, 'Heling', get_now("%Y-%m-%d"), 'Heling', get_now("%Y-%m-%d"))
model_type_values =     (1, 'regex', 'test1', 1, 'Heling', get_now("%Y-%m-%d"), 'Heling', get_now("%Y-%m-%d"))
models_values =         (1, 1, 'regex_sabrage', 'test1', 1, 'Heling', get_now("%Y-%m-%d"), 'Heling', get_now("%Y-%m-%d"))

with psycopg2.connect(pg_DSN) as pg_conn:
    with pg_conn.cursor() as pg_cur:
        pg_cur.execute(build_insert_query('sabrage.source_type', schema_source_type), source_type_values)
        pg_cur.execute(build_insert_query('sabrage.sources', schema_sources), sources_values)
        pg_cur.execute(build_insert_query('sabrage.document_type', schema_document_type), document_type_values)
        pg_cur.execute(build_insert_query('sabrage.model_type', schema_model_type), model_type_values)
        pg_cur.execute(build_insert_query('sabrage.models', schema_models), models_values)
        pg_conn.commit()

#pg_conn.rollback()


def build_insert_query(table_name, schema_type):
    values_format = ', '.join(['%s' for _ in range(len(schema_type))])
    return "INSERT INTO {} ({}) VALUES ({}) ".format(table_name, ', '.join(schema_type), values_format)


