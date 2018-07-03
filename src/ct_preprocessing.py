import logging
import traceback as tb
from os.path import basename

import spacy

from amp_data_tools.amp_data.text_helpers import always_unicode, always_str
from amp_data_tools.amp_io.amp_connect.amp_mongo_connect import MongoConnector
from amp_data_tools.amp_io.amp_io_helpers import read_text_file
from amp_sor.clinical_trial.ct_io import XMLClinicalTrialParser
from amp_sor.clinical_trial.ct_ner.ct_match import ClinicalTrialBmBTargetRegexMatcherSabrage

from amp_poll.ct_poll import ct_pipeline_config as config
import version_control_config as vc_config
from amp_data_tools.amp_io.amp_io_helpers import get_now
import psycopg2
import pandas as pd
from pandas.io.json import json_normalize
from sqlalchemy import create_engine

# import re

logger = logging.getLogger(__name__)

try:
    import re2 as re

    use_re2 = True
    logger.info('Using re2 library.')
except ImportError as import_error:
    import re

    logger.error('Failed to import re2.\n{}'.format(import_error))
    use_re2 = False


class ClinicalTrialCorpusPreprocessor(ClinicalTrialBmBTargetRegexMatcherSabrage):
    # def __init__(self, bmb_credentials, mongo_credentials, included_xpaths, **kwargs):
    def __init__(self, bmb_credentials, mongo_credentials, **kwargs):
        super(ClinicalTrialCorpusPreprocessor, self).__init__(bmb_credentials, **kwargs)
        self.xml_parser = XMLClinicalTrialParser()
        self.space_swapper = re.compile('\s+')
        self.colon_swapper = re.compile(':[ ]+\-')  # removes "bla: - item 1
        self.hyphen_swapper = re.compile('\-')
        self.filters = '\t\n'
        self.en_model = spacy.load('en')
        # serialization info
        self.mongo_conn = MongoConnector(**mongo_credentials)
        self.collection = mongo_credentials['collection']

        self.output_credentials = config.export_db_credentials
        self.vc_config = vc_config
        self.model_version_id = kwargs.get('model_version_id')
        self.is_new_model_version = kwargs.get('is_new_model_version')
        # sections to process
        self.included_xpaths = kwargs.get('included_xpaths', None)
        if not self.included_xpaths:
            logger.info('{}: Preprocessing all clinical trial xpaths'.format(self.name))
        else:
            logger.info('{}: Limiting preprocessing to xpaths {}'.format(self.name, self.included_xpaths))


    def process_corpus(self, xml_file_paths, **kwargs):

        report_at = kwargs.get('report_at', 5000)
        worker_id = kwargs.get('worker_id', 0)

        num_docs = len(xml_file_paths)
        logger.info('Worker {}: Processing {} docs'.format(worker_id, num_docs))
        count = 0
        for xml_file_path in xml_file_paths:
            count += 1
            if count % report_at == 0:
                logger.info('Worker {}: Processed {} out of {} docs'.format(worker_id, count, num_docs))
            try:
                logger.debug('Worker {}: Processing {}'.format(worker_id, xml_file_path))
                self.process(xml_file_path)
            except Exception as e:
                logger.error('{}: Failed to process doc {}\n{}\n{}'.format(self.name, basename(xml_file_path),
                                                                           e, tb.format_exc()))

    def process(self, xml_file_path, **kwargs):

        xml = read_text_file(xml_file_path)

        ct = self.xml_parser.parse(xml, **kwargs)
        ct_num = ct.sor_id
        # results = {'_id': ct.sor_id}

        results = {'_id': int(ct_num.replace('NCT', '')), 'ct_num': ct.sor_id}

        # added document version tracking. Heling
        is_new_doc, doc_id = self.save_document_pg(ct_num)
        print(dir(ct))

        # if is_new_doc == True, insert new document_version version 1.0
        # if is_new_doc ~= True, return ALL existing doc_version_id for comparision later with xpath
        is_new_doc_version = False  # set to False initially. actual test in save_document_xpath_pg
        doc_version_id = self.save_document_version_pg(xml, doc_id, is_new_doc, is_new_doc_version, ct.last_updated)

        # first check - is it a file already processed? same sor_id, last_updated, git version number
        # need to finish check_exsiting_doc
        # retrieve all regex candidates for doc
        # doc_candidates = self.find_doc_matches(ct, **kwargs)
        write_pg_results = False
        results_df = pd.DataFrame()
        for xpath, section in ct.mapping.iteritems():
            if not self.filter_section(section, xpath):
                # added for xpath record tracking. Heling
                is_new_xpath, xpath_id = self.save_document_xpath_pg(xpath, section.text, doc_version_id, is_new_doc,
                                                                     xml, doc_id, ct.last_updated)
                if is_new_xpath or self.is_new_model_version:
                    # process the the xpath if it is a new xpath or the code running has a new model version
                    section_results = self.process_section(section)
                    if section_results:
                        write_pg_results = True
                        results[xpath] = section_results
                        results_df_temp = json_normalize(section_results)
                        results_df_temp.loc[:, 'document_xpath_id'] = xpath_id
                        results_df = results_df.append(results_df_temp, ignore_index=True)

                else:
                    pass

        # write into pg
        if write_pg_results:
            results_df = self.save_doc_xpath_attr_raw(results_df)
            self.save_doc_xpath_attr(results_df)
        else:
            pass

        # serialize resulting json
        self.mongo_conn.insert(results, collection=self.collection)

    # end of edit. Heling

    # beginning of new functions from Heling
    def save_document_pg(self, ct_num):
        # check if the document is already in the database, if yes, return id, if no, insert the record and return id
        # return doc_in_record (bool) and doc_id (id in sabrage.documents)
        output_credentials = self.output_credentials
        pg_DSN = "dbname={} user={} host={} password={}".format(output_credentials['db'], output_credentials['user'],
                                                                output_credentials['host'],
                                                                output_credentials['password'])

        schema_documents = ["document_type_id", "source_id", "name", "notes", "active", "create_by",
                            "create_date", "update_by", "update_date"]

        documents_values = (vc_config.document_type, vc_config.sources, ct_num, 'test', 1, 'Heling',
                            get_now("%Y-%m-%d"), 'Heling', get_now("%Y-%m-%d"))
        values_format = ', '.join(['%s' for _ in range(len(schema_documents))])
        query = "INSERT INTO {} ({}) VALUES ({}) ".format('sabrage.documents', ', '.join(schema_documents),
                                                          values_format)
        with psycopg2.connect(pg_DSN) as pg_conn:
            with pg_conn.cursor() as pg_cur:
                pg_conn.autocommit = True
                pg_cur.execute('SELECT id, name FROM sabrage.documents WHERE name = \'{}\''.format(ct_num))
                doc_id = pg_cur.fetchone()
                is_new_doc = False
                if doc_id is None:
                    pg_cur.execute(query, documents_values)
                    pg_cur.execute('SELECT id, name FROM sabrage.documents WHERE name = \'{}\''.format(ct_num))
                    doc_id = pg_cur.fetchone()
                    is_new_doc = True
        return is_new_doc, doc_id[0]

    def save_document_xpath_pg(self, xpath, text, doc_version_id, is_new_doc, xml, doc_id, last_updated):
        # check if the xpath is already in the database,
        # if yes, check if text is the same,
        #         if yes, return id,
        #         if no, this is a new version of document,
        #                insert new version into document_version table
        #                insert xpath with new document version number into document_xpath table
        # if no, insert the record into document_xpath table and return id
        #
        # return is_new_xpath (bool) and document_xpath_id
        output_credentials = self.output_credentials
        pg_DSN = "dbname={} user={} host={} password={}".format(output_credentials['db'], output_credentials['user'],
                                                                output_credentials['host'],
                                                                output_credentials['password'])
        schema_document_xpath = ["document_version_id", "xpath", "xpath_body", "notes", "active", "create_by",
                                 "create_date", "update_by", "update_date"]
        values_format = ', '.join(['%s' for _ in range(len(schema_document_xpath))])
        query = "INSERT INTO {} ({}) VALUES ({}) ".format('sabrage.document_xpath', ', '.join(schema_document_xpath),
                                                          values_format)
        with psycopg2.connect(pg_DSN) as pg_conn:
            with pg_conn.cursor() as pg_cur:
                pg_conn.autocommit = True
                if is_new_doc:
                    document_xpath_values = (doc_version_id[0], xpath, text, 'test', 1, 'Heling',
                                             get_now("%Y-%m-%d"), 'Heling', get_now("%Y-%m-%d"))
                    pg_cur.execute(query, document_xpath_values)
                    pg_cur.execute('SELECT id, xpath_body FROM sabrage.document_xpath '
                                   'WHERE document_version_id = {} and xpath = \'{}\''.format(doc_version_id[0], xpath))
                    xpath_id_body = pg_cur.fetchone()
                    is_new_xpath = True
                else:
                    # document and xpath already exist, check if the xpath_body is the same in any previous versions
                    for i in range(len(doc_version_id)):
                        pg_cur.execute('SELECT id, xpath_body FROM sabrage.document_xpath '
                                       'WHERE document_version_id = {} and xpath = \'{}\''.format(doc_version_id[i][0],
                                                                                                  xpath))
                        xpath_id_body = pg_cur.fetchone()
                        if xpath_id_body is not None:
                            is_new_xpath = False
                            break
                    else:
                        # new xpath: insert a new version, get new version id, insert the xpath with new version id
                        is_new_xpath = True
                        doc_version_id = self.save_document_version_pg(xml, doc_id, is_new_doc, is_new_xpath,
                                                                       last_updated)
                        document_xpath_values = (doc_version_id, xpath, text, 'test', 1, 'Heling',
                                                 get_now("%Y-%m-%d"), 'Heling', get_now("%Y-%m-%d"))
                        pg_cur.execute(query, document_xpath_values)
                        pg_cur.execute('SELECT id, xpath_body FROM sabrage.document_xpath '
                                       'WHERE document_version_id = {} and xpath = \'{}\''.format(doc_version_id[0][0],
                                                                                                  xpath))
                        xpath_id_body = pg_cur.fetchone()

        return is_new_xpath, xpath_id_body[0]

    def save_document_version_pg(self, xml, doc_id, is_new_doc, is_new_version, last_updated):
        # check if the document_version is already in the database,
        # if yes, return id, if no, insert the record and return id
        # return doc_in_record (bool) and doc_id (id in sabrage.documents_version)
        # important !!!! is_new_doc_version is determined by the text in xpath not the whole xml text
        # this is performed inside save_document_xpath
        # except when is_new_doc is True
        output_credentials = self.output_credentials
        pg_DSN = "dbname={} user={} host={} password={}".format(output_credentials['db'], output_credentials['user'],
                                                                output_credentials['host'],
                                                                output_credentials['password'])
        schema_document_version = ["document_id", "document_body", "version", "notes", "active", "create_by",
                                   "create_date", "update_by", "update_date"]
        values_format = ', '.join(['%s' for _ in range(len(schema_document_version))])
        query = "INSERT INTO {} ({}) VALUES ({}) ".format('sabrage.document_version',
                                                          ', '.join(schema_document_version),
                                                          values_format)
        with psycopg2.connect(pg_DSN) as pg_conn:
            with pg_conn.cursor() as pg_cur:
                pg_conn.autocommit = True
                if is_new_doc:  # version 1
                    document_version_values = (doc_id, xml, 1, last_updated, 1, 'Heling', get_now("%Y-%m-%d"),
                                               'Heling', get_now("%Y-%m-%d"))
                    pg_cur.execute(query, document_version_values)
                    pg_cur.execute('SELECT id FROM sabrage.document_version WHERE document_id = {}'.format(doc_id))
                    # return version id (there is only one, since it is the first record)
                    doc_version_id = pg_cur.fetchone()
                elif is_new_version:  # version = previous version + 1
                    pg_cur.execute('SELECT version FROM sabrage.document_version WHERE document_id = {} '
                                   'ORDER BY id DESC '
                                   'LIMIT 1'.format(doc_id))
                    last_doc_version = pg_cur.fetchone()
                    document_version_values = (doc_id, xml, last_doc_version+1, last_updated, 1,
                                               'Heling', get_now("%Y-%m-%d"), 'Heling', get_now("%Y-%m-%d"))
                    pg_cur.execute(query, document_version_values)
                    pg_cur.execute('SELECT id FROM sabrage.document_version WHERE document_id = {} ORDER BY id DESC '
                           'LIMIT 1'.format(doc_id))
                    doc_version_id = pg_cur.fetchone()  # return the version id of the record inserted
                else:  # return all version ids associated with this doc_id
                    pg_cur.execute('SELECT id FROM sabrage.document_version WHERE document_id = {}'.format(doc_id))
                    doc_version_id = pg_cur.fetchall()  # return the all version ids associated with the doc_id

        return doc_version_id


    def save_doc_xpath_attr_raw(self, results_df):
        pg_credentials = self.output_credentials
        engine = create_engine(
            'postgresql+psycopg2://{}:{}@{}/{}'.format(pg_credentials['user'], pg_credentials['password'],
                                                       pg_credentials['host'], pg_credentials['db']))

        results_df.loc[:, 'model_version_id'] = self.model_version_id
        results_df.loc[:, 'confidence'] = 0
        location_fun = lambda i: re.search('~~\[\[1 .* 1\]\]~~', ' '.join(results_df.ix[i, 'marked'])).span()
        # the pattern is offset 5 digit before and after the match
        results_df.loc[:, 'location'] = [str(location_fun(i)[0] + 5) + '-' + str(location_fun(i)[0] - 5) for i in
                                         range(len(results_df))]

        results_df = results_df.rename(columns={'candidate.text': 'notes', 'candidate.bmb_ids': 'target_id',
                                                'marked': 'matched_text'})
        results_df['target_id']=results_df['target_id'].apply(lambda i: i[0])
        results_df.loc[:, 'active'] = 1
        results_df.loc[:, 'create_by'] = 'Heling'
        results_df.loc[:, 'create_date'] = get_now("%Y-%m-%d")
        results_df.loc[:, 'update_by'] = 'Heling'
        results_df.loc[:, 'update_date'] = get_now("%Y-%m-%d")
        schema_doc_xpath_attr_raw = ["document_xpath_id", "location", "target_id", "matched_text", "confidence",
                                     "model_version_id", "notes", "active", "create_by", "create_date",
                                     "update_by", "update_date"]
        results_df[schema_doc_xpath_attr_raw].to_sql('doc_xpath_attr_raw', con=engine, if_exists='append',
                                                     index=False, schema='sabrage')
        return results_df


    def save_doc_xpath_attr(self, results_df):
        pg_credentials = self.output_credentials
        engine = create_engine(
            'postgresql+psycopg2://{}:{}@{}/{}'.format(pg_credentials['user'], pg_credentials['password'],
                                                       pg_credentials['host'], pg_credentials['db']))
        synonym_id = []
        for i in range(len(results_df)):
            synonyms_match_synonyms = list(pd.read_sql_query('SELECT id FROM {} WHERE name = \'{}\''
                                            .format('sabrage.synonyms', results_df.ix[i, 'notes']), con=engine)['id'])
            if len(synonyms_match_synonyms) == 1:
                target_synonyms_id = list(pd.read_sql_query('SELECT id FROM {} WHERE target_id = {} and synonym_id = {}'
                                                            .format('sabrage.target_synonym',
                                                                    results_df.ix[i, 'target_id'],
                                                                    synonyms_match_synonyms[0]), con=engine)['id'])
                synonym_id.append(target_synonyms_id[0])
            elif len(synonyms_match_synonyms) == 0:
                # missing synonyms
                synonym_id.append(1)
            else:
                target_synonyms_df = pd.read_sql_query('SELECT id, synonym_id FROM {} WHERE target_id = {}'
                                                       .format('sabrage.target_synonym', results_df.ix[i, 'target_id']),
                                                       con=engine)
                synonyms_match_bridge = list(target_synonyms_df['synonym_id'])
                synonym_id_common = set(synonyms_match_synonyms).intersection(set(synonyms_match_bridge))
                synonym_id.append(target_synonyms_df[target_synonyms_df[synonym_id] == synonym_id_common]['id'])

        results_df.loc[:, 'target_synonym_id'] = synonym_id
        #print(synonym_id)
        schema_doc_xpath_attr = ["document_xpath_id", "location", "target_synonym_id", "confidence", "model_version_id",
                                 "notes", "active", "create_by", "create_date", "update_by", "update_date"]
        results_df[schema_doc_xpath_attr].to_sql('doc_xpath_attr', con=engine, if_exists='append',
                                                 index=False, schema='sabrage')

    # end of code from Heling

    def check_existing_doc(self, xml_file_path):
        pass

    def filter_section(self, section, xpath):
        status = True
        if self.included_xpaths is None or xpath in self.included_xpaths:
            status = False
        return status

    def process_section(self, section, remove_ambiguous=True):
        results = []
        text = section.text
        # remove undesired characters
        clean_text = self.clean_text(text)
        if clean_text:
            # tokenize and parse with spaCy
            doc = self.tokenize(clean_text)
            # find potential biomarker candidates
            candidates = self.find_matches(clean_text)
            for candidate in candidates:
                if remove_ambiguous and len(candidate.ids['target ID']) > 1:
                    msg = '{}: Removing candidate due to multiple BmB IDs: {}/{}'
                    logger.debug(msg.format(self.name, always_str(candidate.text), candidate.ids['target ID']))
                    continue
                tokens = self.process_candidate(doc, candidate)
                results.append({'marked': tokens, 'candidate': {
                    'text': candidate.text,
                    'bmb_ids': list(candidate.ids['target ID']),
                }})
            return results

    def process_candidate(self, doc, candidate):
        sent = self.get_candidate_sent(doc, candidate)
        entity_attr = self.extract_entity_attributes(sent, candidate)
        tokens = self.clean_tokens(sent)
        tokens_marked = self.mark(tokens, entity_attr['entity_types'])
        return tokens_marked

    @staticmethod
    def get_candidate_sent(doc, candidate):
        match_start = candidate.location[0]
        sents = [x for x in doc.sents if x.start_char <= match_start <= x.end_char]
        if len(sents) > 1:
            sent = sents[-1]
        else:
            sent = sents[0]
        return sent

    def tokenize(self, text):
        doc = self.en_model(text)
        self.en_model.parser(doc)
        return doc

    def clean_text(self, text):
        # clean_text = always_str(text).translate(maketrans(self.filters, ' ' * len(self.filters)))
        clean_text = self.space_swapper.sub(' ', always_unicode(text))
        clean_text = self.colon_swapper.sub(' ', always_unicode(clean_text))
        clean_text = self.hyphen_swapper.sub(' ', always_unicode(clean_text))
        try:
            clean_text = always_unicode(clean_text)
        except Exception as e:
            logger.warn('clean_text: Failed on text\n{}'.format(e))
            clean_text = None
        return clean_text

    def clean_tokens(self, sent):
        return [self.scrub(x.text).lower() for x in sent]

    @staticmethod
    def scrub(s):
        return ''.join(c for c in s if ord(c) < 128)

    def extract_entity_attributes(self, sent, candidate):
        entity_types = ['O'] * len(sent)

        def update_entity_types(idx, entity_type):
            entity_types[idx] = entity_type

        def process_entity_matches(entity_tag='biomarker'):
            hit_start = candidate.location[0]
            hit_end = candidate.location[1]
            # bmb_id = instance.tgID
            # retrieve the index value
            starts = [x.idx for x in sent]
            ends = starts[1:] + [sent.end_char]
            annotating = False
            for i, x in enumerate(starts):
                if x <= hit_start < ends[i]:
                    annotating = True
                    update_entity_types(i, entity_tag)
                elif annotating and hit_end <= starts[i]:
                    break
                elif annotating:
                    update_entity_types(i, entity_tag)
            if annotating and hit_end == ends[-1]:
                update_entity_types(-1, entity_tag)
            if not annotating:
                msg = '{}: hit location does not align in tokens for {}'.format(self.name)
                raise IndexError(msg.format(candidate.regs[0]))

        process_entity_matches()
        return {'entity_types': entity_types}

    @staticmethod
    def mark(tokens, tags, marker='biomarker'):
        entity_type = 'O'
        marked = []
        for i in range(len(tokens)):
            tag = tags[i]
            if entity_type != tag and entity_type == marker:
                marked.append('1]]~~')

            if entity_type != tag and tag == marker:
                marked.append('~~[[1')

            entity_type = tag
            marked.append(tokens[i])
        if entity_type == marker:
            marked.append('1]]~~')
        return marked
