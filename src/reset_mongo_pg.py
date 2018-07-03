'''
This code is used to delete all intermediate results in MongoDB and Postgres when debugging preprocess steps
otherwise the scripts in preprocess will not skip the files because they are already in postgres,
MongoDB will raise error for duplicated key
'''

from pymongo import MongoClient
from amp_poll.ct_poll import ct_pipeline_config as config
import psycopg2

# empty mongoDB to avoid duplicate key error
client = MongoClient()
db = client.ct_pipeline_enhance_testhz
db.ClinicalTrialsBiomarkerMeasurementClassifier.drop()
db.results_new_pipeline.drop()
db.preprocess_new_pipeline.drop()
db.preprocess_new_pipeline_x2.drop()

# empty documents, document_version, document_xpath in postgres so the code don't skip those files
pg_credentials = config.export_db_credentials
pg_DSN = "dbname={} user={} host={} password={}".format(pg_credentials['db'], pg_credentials['user'],
                                                        pg_credentials['host'], pg_credentials['password'])
with psycopg2.connect(pg_DSN) as pg_conn:
    with pg_conn.cursor() as pg_cur:
        pg_cur.execute('DELETE FROM sabrage.documents')
        pg_cur.execute('DELETE FROM sabrage.document_version')
        pg_cur.execute('DELETE FROM sabrage.document_xpath')
        pg_cur.execute('DELETE FROM sabrage.doc_xpath_attr')
        pg_cur.execute('DELETE FROM sabrage.doc_xpath_attr_raw')
        pg_conn.commit()

