from amp_data_tools.amp_io import MySQLConnector
from amp_apps.config import bmb_mysql_config
from amp_poll.ct_poll import ct_pipeline_config as config
from amp_data_tools.amp_io.amp_io_helpers import get_now
import psycopg2
import pandas as pd
from sqlalchemy import create_engine


def build_insert_query(table_name, schema_type):
    values_format = ', '.join(['%s' for _ in range(len(schema_type))])
    return "INSERT INTO {} ({}) VALUES ({}) ".format(table_name, ', '.join(schema_type), values_format)


def get_unique_with_original_order(input_list):
    # return unique list in the original input order and id of the referring to the new unique sequence
    # that represents the original (non-unique) sequences
    unique_list = []
    unique_list.append(input_list[0])
    id_original = []
    id_original.append(0)
    for i in range(1, len(input_list)):
        if input_list[i] in unique_list:
            new_id = unique_list.index(input_list[i])
            id_original.append(new_id)
        else:
            unique_list.append(input_list[i])
            id_original.append(i)
    return unique_list, id_original


def get_unique_original_order(input_list):
    included = set()
    return [i for i in input_list if i not in included and (included.add(i) or True)]


def get_unique_with_original_order_x2(input_list):
    included = set()
    return [i for i in range(len(input_list)) if input_list[i] not in included and (included.add(input_list[i]) or True)]


if __name__ == "__main__":
        # read targets and target-synonyms table from MySQL
    credentials = bmb_mysql_config.dbs['bmb_rds_staging']
    conn = MySQLConnector(**credentials)
    str_format = "%Y-%m-%d"
    targets = conn.fetchall('Select * from {}'.format('targets'))
    targets_pg = [[i['display_name']+' - ' + str(i['gene_id']), i['protein_name'], i['id'], 1, 'Heling',
                   i['last_update'].strftime(str_format), 'Heling',
                   i['last_update'].strftime(str_format)] for i in targets]

    target_synonyms = conn.fetchall('Select * from {}'.format('target_synonyms'))

    synonyms_pg_all = [i['synonym'] for i in target_synonyms]
    unique_id = get_unique_with_original_order_x2(synonyms_pg_all)
    synonyms_pg = [[target_synonyms[i]['synonym'], target_synonyms[i]['id'], 1, 'Heling',
                    get_now("%Y-%m-%d"), 'Heling', get_now("%Y-%m-%d")]
                   for i in unique_id]

    # write targets and synonyms (unique) table into postgres
    pg_credentials = config.export_db_credentials
    pg_DSN = "dbname={} user={} host={} password={}".format(pg_credentials['db'], pg_credentials['user'],
                                                            pg_credentials['host'], pg_credentials['password'])

    schema_targets = ["name", "name_alt", "notes", "active", "create_by", "create_date", "update_by", "update_date"]
    schema_synonyms = ["name", "notes", "active", "create_by", "create_date", "update_by", "update_date"]
    schema_target_synonym = ["target_id", "synonym_id", "notes", "active", "create_by", "create_date",
                             "update_by", "update_date"]

    query_insert = build_insert_query('sabrage.synonyms', schema_synonyms)

    with psycopg2.connect(pg_DSN) as pg_conn:
        with pg_conn.cursor() as pg_cur:
            pg_conn.autocommit = True
            for i in range(len(synonyms_pg)):
                pg_cur.execute(query_insert, synonyms_pg[i])

    query_insert = build_insert_query('sabrage.targets', schema_targets)
    with psycopg2.connect(pg_DSN) as pg_conn:
        with pg_conn.cursor() as pg_cur:
            pg_conn.autocommit = True
            for i in range(len(targets_pg)):
                pg_cur.execute(query_insert, targets_pg[i])

    unique_id = get_unique_with_original_order_x2(synonyms_pg_all)
    engine = create_engine('postgresql+psycopg2://{}:{}@{}/{}'.format(pg_credentials['user'],
                                                                      pg_credentials['password'],
                                                                      pg_credentials['host'],
                                                                      pg_credentials['db']))
    target_df = pd.read_sql_query('SELECT id, name, notes FROM {}'.format('sabrage.targets'), con=engine)\
        .rename(columns={'name': 'target_name'})

    synonyms_df = pd.read_sql_query('SELECT id, name FROM {}'.format('sabrage.synonyms'), con=engine)\
        .rename(columns={'name': 'synonym_name'})

    target_synonyms_df = pd.DataFrame(list(target_synonyms))[['synonym', 'targets_id']]
    target_df['notes'] = target_df.notes.astype('int64')
    target_synonyms_df_with_target_id = pd.merge(target_synonyms_df, target_df, left_on='targets_id', right_on='notes')\
        .rename(columns={'id': 'target_id'})

    target_synonyms_df_with_target_id_synonyms_id = pd.merge(target_synonyms_df_with_target_id, synonyms_df,
                                                             left_on='synonym', right_on='synonym_name')\
        .rename(columns={'id': 'synonym_id'})

    target_synonyms_pg = target_synonyms_df_with_target_id_synonyms_id[['target_id', 'synonym_id']]
    target_synonyms_pg.loc[:, 'notes'] = target_synonyms_df_with_target_id_synonyms_id\
        .apply(lambda row: row['target_name'] + ': ' + row['synonym_name'], axis=1)

    target_synonyms_pg.loc[:, 'active'] = 1
    target_synonyms_pg.loc[:, 'create_by'] = 'Heling'
    target_synonyms_pg.loc[:, 'create_date'] = get_now("%Y-%m-%d")
    target_synonyms_pg.loc[:, 'update_by'] = 'Heling'
    target_synonyms_pg.loc[:, 'update_date'] = get_now("%Y-%m-%d")
    target_synonyms_pg.to_sql('target_synonym', con=engine, if_exists='append', index_label='id',
                              chunksize=1000, schema='sabrage')
    # wrote in to public - copy to sabrage
    with psycopg2.connect(pg_DSN) as pg_conn:
        with pg_conn.cursor() as pg_cur:
            pg_conn.autocommit = True
            pg_cur.execute('INSERT INTO sabrage.target_synonym SELECT * FROM public.sabrage.target_synonym')

    with psycopg2.connect(pg_DSN) as pg_conn:
        with pg_conn.cursor() as pg_cur:
            pg_conn.autocommit = True
            pg_cur.execute('SELECT id, name FROM {}'.format('sabrage.targets'))
            pd.df = pg_cur.fetchall()
