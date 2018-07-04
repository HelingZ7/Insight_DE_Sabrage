import logging
import multiprocessing
from os import path, walk
from time import time

from amp_data_tools.amp_io.amp_connect.amp_mongo_connect import MongoConnector
from amp_poll.ct_poll import ct_pipeline_config as config
from amp_sor.amp_sor_curate import SORNLPPipeline, SORNLPExporter
from amp_sor.clinical_trial.ct_preprocessing import ClinicalTrialCorpusPreprocessor

import psycopg2
import subprocess
from amp_data_tools.amp_io.amp_io_helpers import get_now
import version_control_config as vc_config
import decimal

logger = logging.getLogger(__name__)


def preprocess(xml_file_paths, bmb_credentials, mongo_credentials, **kwargs):
    preprocessor_type = kwargs.get('preprocessor', ClinicalTrialCorpusPreprocessor)
    logger.info('Preprocess: Utilizing {}'.format(preprocessor_type))
    preprocessor = preprocessor_type(bmb_credentials, mongo_credentials, **kwargs)
    preprocessor.process_corpus(xml_file_paths, **kwargs)


# beginning of code from Heling
def save_git_version(output_credentials):

    # use git version for model version control for now, same git hash is considered to be same model version
    git_version = subprocess.check_output(['git', 'rev-parse', 'HEAD']).strip()
    pg_DSN = "dbname={} user={} host={} password={}".format(output_credentials['db'], output_credentials['user'],
                                                            output_credentials['host'], output_credentials['password'])
    schema_model_version = ["model_id", "version", "notes", "active", "create_by", "create_date",
                            "update_by", "update_date"]
    is_new_model_version = False
    with psycopg2.connect(pg_DSN) as pg_conn:
        with pg_conn.cursor() as pg_cur:
            pg_conn.autocommit = True
            pg_cur.execute('SELECT id FROM sabrage.model_version WHERE notes = \'{}\''.format(git_version))
            model_version_id = pg_cur.fetchone()
            if model_version_id is None:
                # new model version, check if the model has been used before.
                # If yes, version +1 from last record with same model
                # If no, version nubmer = 0
                is_new_model_version = True
                pg_cur.execute('SELECT model_id, version FROM sabrage.model_version WHERE model_id = {} '
                               'ORDER BY id DESC '
                               'LIMIT 1'. format(vc_config.models))
                last_version = pg_cur.fetchone()
                if last_version is None:
                    version = 0
                else:
                    version = last_version[1]+decimal.Decimal('1')
                model_version_values = (vc_config.models, version, git_version, 1, 'Heling', get_now("%Y-%m-%d"),
                                        'Heling', get_now("%Y-%m-%d"))
                values_format = ', '.join(['%s' for _ in range(len(schema_model_version))])
                query = "INSERT INTO {} ({}) VALUES ({}) ".format('sabrage.model_version',
                                                                  ', '.join(schema_model_version), values_format)
                pg_cur.execute(query, model_version_values)
                pg_cur.execute('SELECT id FROM sabrage.model_version WHERE notes = \'{}\''.format(git_version))
                model_version_id = pg_cur.fetchone()
    return model_version_id, is_new_model_version
# end of code from Heling


def run_preprocessing(**kwargs):

    xml_directory_path = kwargs.get('xml_directory_path', config.xml_directory_path)
    input_credentials = kwargs.get('input_credentials', config.bmb_credentials)
    output_credentials = kwargs.get('output_credentials', config.reduced_credentials)
    num_workers = kwargs.get('num_workers', multiprocessing.cpu_count())

    model_version_id, is_new_model_version = save_git_version(config.export_db_credentials)
    kwargs['model_version_id'] = model_version_id
    kwargs['is_new_model_version'] = is_new_model_version
    xml_file_paths = [path.join(dp, f) for dp, _, fn in walk(xml_directory_path) for f in fn]
    num_docs = len(xml_file_paths)
    logger.warning('Preprocessing: Retrieved {} clinical trial xml for analysis'.format(num_docs))

    batch_size = num_docs / num_workers

    jobs = []
    start_time = time()

    for i in range(num_workers):
        # define the starting index for the work
        start_doc = i * batch_size
        # define the ending index: the last thread manages the remainder
        if i == (num_workers - 1):
            end_doc = num_docs
        else:
            end_doc = start_doc + batch_size
        logger.warning('Preprocessing: Starting worker %i:  Range is %i - %i' % (i, start_doc, end_doc))
        kwargs['worker_id'] = i
        worker_args = (xml_file_paths[start_doc:end_doc], input_credentials, output_credentials)
        p = multiprocessing.Process(target=preprocess, args=worker_args, kwargs=kwargs)
        jobs.append(p)
        p.start()

    for j in jobs:
        j.join()
        logger.warn('Preprocessing: %s.exitcode = %s' % (j.name, j.exitcode))
    process_time = (time() - start_time) / 60.0
    logger.warning('Preprocessing: Task complete ({} minutes)'.format(round(process_time, 2)))


def run_nlp_pipeline(**kwargs):
    input_credentials = kwargs.get('input_credentials', config.reduced_credentials)
    output_credentials = kwargs.get('output_credentials', config.model_results_credentials)
    num_workers = kwargs.get('num_workers', multiprocessing.cpu_count())
    classifier_defs = kwargs.get('classifier_defs', config.classifier_defs)

    conn = MongoConnector(**input_credentials)
    logger.info('Clinical Trials NLP Pipeline: Retrieving doc ids for analysis')
    doc_ids = kwargs.pop('doc_ids', None)
    if doc_ids is None:
        query = conn.fetchall({}, collection=input_credentials['collection'], projection={'_id': True})
        doc_ids = [x['_id'] for x in query]

    start_time = time()
    num_docs = len(doc_ids)
    doc_ids.sort()
    logger.warning('Clinical Trials NLP Pipeline: Retrieved {} doc ids for analysis'.format(num_docs))
    batch_size = num_docs / num_workers

    jobs = []
    for i in range(num_workers):
        # define the starting index for the work
        start_doc = i * batch_size
        # define the ending index: the last thread manages the remainder
        if i == (num_workers - 1):
            end_doc = num_docs
        else:
            end_doc = start_doc + batch_size
        logger.warning('Clinical Trials NLP Pipeline: Starting worker %i:  Range is %i - %i' % (i, start_doc, end_doc))
        kwargs['worker_id'] = i
        kwargs['pmids'] = doc_ids[start_doc:end_doc]
        worker_args = (input_credentials, output_credentials, input_credentials['collection'], classifier_defs)
        p = multiprocessing.Process(target=apply_nlp_pipeline, args=worker_args, kwargs=kwargs)
        jobs.append(p)
        p.start()

    for j in jobs:
        j.join()
        logger.warn('NLP Pipeline: %s.exitcode = %s' % (j.name, j.exitcode))
    process_time = (time() - start_time) / 60.0
    logger.warning('NLP Pipeline: Task complete ({} minutes)'.format(round(process_time, 2)))


def apply_nlp_pipeline(input_credentials, output_credentials, reduced_collection, classifier_defs, **kwargs):
    # build the pipeline
    pipeline = SORNLPPipeline(input_credentials, output_credentials, reduced_collection, classifier_defs, **kwargs)
    pipeline.run(**kwargs)


def export(input_credentials, output_credentials, classifier_defs, **kwargs):
    exporter = SORNLPExporter(input_credentials, output_credentials, classifier_defs, **kwargs)
    exporter.run(**kwargs)


# def run_exporter(**kwargs):
#     input_credentials = kwargs.get('input_credentials', config.model_results_credentials)
#     output_credentials = kwargs.pop('output_credentials', config.export_db_credentials)
#     num_workers = kwargs.get('num_workers', multiprocessing.cpu_count())
#     classifier_defs = kwargs.get('classifier_defs', config.classifier_collections)
#     logger.info('Clinical Trials NLP Results Exporting: Retrieving doc_ids for analysis')
#     conn = SORNLPExporter(input_credentials, output_credentials, classifier_defs, **kwargs)
#     doc_ids = conn.get_docs(**kwargs)
#     num_docs = len(doc_ids)
#     logger.warning('Clinical Trials NLP Results: Retrieved {} doc_ids for analysis'.format(num_docs))
#
#     batch_size = num_docs / num_workers
#
#     jobs = []
#     start_time = time()
#
#     for i in range(num_workers):
#         # define the starting index for the work
#         start_doc = i * batch_size
#         # define the ending index: the last thread manages the remainder
#         if i == (num_workers - 1):
#             end_doc = num_docs
#         else:
#             end_doc = start_doc + batch_size
#         logger.warning('Exporting: Starting worker %i:  Range is %i - %i' % (i, start_doc, end_doc))
#         kwargs['worker_id'] = i
#         kwargs['doc_ids'] = doc_ids[start_doc:end_doc]
#         worker_args = (input_credentials, output_credentials, classifier_defs)
#         p = multiprocessing.Process(target=export, args=worker_args, kwargs=kwargs)
#         jobs.append(p)
#         p.start()
#
#     for j in jobs:
#         j.join()
#         logger.warn('Clinical Trials NLP Results: %s.exitcode = %s' % (j.name, j.exitcode))
#     process_time = (time() - start_time) / 60.0
#     logger.warning('Clinical Trials NLP Results: Task complete ({} minutes)'.format(round(process_time, 2)))
