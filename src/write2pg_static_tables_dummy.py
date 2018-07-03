'''
connect to postgres and write the static tables:
source_type, document_type, sources, model_type, models
other tables will be written during the program
author: Heling
'''
from amp_poll.ct_poll import ct_pipeline_config as config
from amp_data_tools.amp_io.amp_io_helpers import get_now
import psycopg2

def build_insert_query(table_name, schema_type):
    values_format = ', '.join(['%s' for _ in range(len(schema_type))])
    return "INSERT INTO {} ({}) VALUES ({}) ".format(table_name, ', '.join(schema_type), values_format)

pg_credentials = config.export_db_credentials
pg_DSN = "dbname={} user={} host={} password={}".format(pg_credentials['db'], pg_credentials['user'], pg_credentials['host'], pg_credentials['password'])
#table_name = 'test3'

schema_source_type =    ["name", "notes", "active", "create_by", "create_date", "update_by", "update_date"]
schema_sources =        ["source_type_id", "name", "notes", "active", "create_by", "create_date", "update_by", "update_date"]
schema_document_type =  ["name", "notes", "active", "create_by", "create_date", "update_by", "update_date"]
schema_model_type =     ["name", "notes", "active", "create_by", "create_date", "update_by", "update_date"]
schema_models =         ["model_type_id", "name", "notes", "active", "create_by", "create_date", "update_by", "update_date"]

source_type_sample = {1: 'clinical trial', 2: 'publications', 3: 'legislation'}
sources_sample = {1: {1: 'US', 2: 'Europe'}, 2: {1: 'pubmed' , 2: 'Scopus'}, 3: {1: 'FDA', 2: 'CFIA'}}
document_type_sample = {1: 'XML', 2: 'html', 3: 'pdf'}
model_type_sample = {1: 'regex_x1', 2: 'regex_x2', 3: 'regex_x3'}
models_samples = {1: {1: 'considering soluble', 2: 'not considering soluble'},
                 2: {1: 'variation_1', 2: 'variation_2'},
                 3: {1: 'variation_3', 2: 'variation_4'}}

c = 9 # current id
with psycopg2.connect(pg_DSN) as pg_conn:
    with pg_conn.cursor() as pg_cur:
        pg_conn.autocommit = True
        for i in range(c,c+3):
            source_type_values = (source_type_sample[i-c+1], 'test', 1, 'Heling', get_now("%Y-%m-%d"), 'Heling', get_now("%Y-%m-%d"))
            document_type_values = (document_type_sample[i-c+1], 'test', 1, 'Heling', get_now("%Y-%m-%d"), 'Heling', get_now("%Y-%m-%d"))
            model_type_values = (model_type_sample[i-c+1], 'test', 1, 'Heling', get_now("%Y-%m-%d"), 'Heling', get_now("%Y-%m-%d"))
            pg_cur.execute(build_insert_query('sabrage.source_type', schema_source_type), source_type_values)
            pg_cur.execute(build_insert_query('sabrage.document_type', schema_document_type), document_type_values)
            pg_cur.execute(build_insert_query('sabrage.model_type', schema_model_type), model_type_values)
            for j in range(c,c+2):
                sources_values =        (i, sources_sample[i-c+1][j-c+1], 'test', 1, 'Heling', get_now("%Y-%m-%d"), 'Heling', get_now("%Y-%m-%d"))
                models_values =         (i, models_samples[i-c+1][j-c+1], 'test', 1, 'Heling', get_now("%Y-%m-%d"), 'Heling', get_now("%Y-%m-%d"))
                pg_cur.execute(build_insert_query('sabrage.sources', schema_sources), sources_values)
                pg_cur.execute(build_insert_query('sabrage.models', schema_models), models_values)
                #pg_conn.commit()

#pg_conn.rollback()





