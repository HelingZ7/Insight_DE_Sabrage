from collections import defaultdict

import enchant

from amp_apps.bmb.bmb_io import BmBEntityParser, BMBTargetParser, BMBDiseaseParser
from amp_data_tools.amp_data.text_helpers import always_unicode
from amp_entity.entity_match import EntityRegexMatcher
import pandas as pd
from sqlalchemy import create_engine
from amp_poll.ct_poll import ct_pipeline_config as config


class BmBRegexMatcher(BmBEntityParser, EntityRegexMatcher):
    def __init__(self, credentials, **kwargs):
        super(BmBRegexMatcher, self).__init__(credentials, **kwargs)
        EntityRegexMatcher.__init__(self, **kwargs)

    def aggregate_synonyms(self, entities):
        raise NotImplementedError('aggregate_synonyms not implemented for {}'.format(self.name))

    def get_search_terms(self, **kwargs):
        # entities = self.get_all(**kwargs)
        entities = self.get_all(max_id=5, **kwargs)
        return entities.values()

    def get_synonym_map(self, addendum_file_path=None, filter_ids=None, synonym_filter=None, **kwargs):
        entities = self.get_search_terms(**kwargs)
        synonyms = self.aggregate_synonyms(entities)

        # add additional synonyms not contained in Gene
        if addendum_file_path is not None:
            with open(addendum_file_path, 'r') as f:
                for line in f:
                    chunks = line.strip().split('\t')
                    synonyms[int(chunks[1])].add(chunks[0].strip().decode('utf-8'))

        # remove undesirable IDs
        if filter_ids is not None:
            for _id in filter_ids:
                if _id in synonyms:
                    synonyms.pop(_id)

        # remove specific synonyms
        self.filter_synonyms(synonyms, synonym_filter)
        return synonyms


# Heling
class BmBRegexMatcherSabrage(BmBEntityParser, EntityRegexMatcher):
    # sabrage pipeline: use this class instead of BmBRegexMatcher.
    # Read target, synonyms, target-synonym from postgres instead of MySQL
    def __init__(self, credentials, **kwargs):
        super(BmBRegexMatcherSabrage, self).__init__(credentials, **kwargs)
        EntityRegexMatcher.__init__(self, **kwargs)
        self.output_credentials = config.export_db_credentials

    def get_search_terms(self, **kwargs):
        entities = self.get_all_sabrage(**kwargs)
        # entities = self.get_all_sabrage(max_id=2000, **kwargs)
        return entities

    def get_all_sabrage(self, **kwargs):

        """ Get all of the BMB entity selected as a dict of ID: BMBObject
        """
        pg_credentials = self.output_credentials
        engine = create_engine(
            'postgresql+psycopg2://{}:{}@{}/{}'.format(pg_credentials['user'], pg_credentials['password'],
                                                       pg_credentials['host'], pg_credentials['db']))

        max_id = kwargs.get('max_id', None)
        if max_id:
            target_df = pd.read_sql_query('SELECT id, name FROM {} LIMIT {}'.format('sabrage.targets', max_id),
                                          con=engine).rename(columns={'id': 'target_id', 'name': 'target_name'})
        else:
            target_df = pd.read_sql_query('SELECT id, name FROM {}'.format('sabrage.targets'),
                                          con=engine).rename(columns={'id': 'target_id', 'name': 'target_name'})

        entities = {target_df.ix[i, 'target_id']: target_df.ix[i, 'target_name'] for i in range(len(target_df))}

        return entities

    def aggregate_synonyms(self, target):
        pg_credentials = self.output_credentials
        engine = create_engine(
            'postgresql+psycopg2://{}:{}@{}/{}'.format(pg_credentials['user'], pg_credentials['password'],
                                                       pg_credentials['host'], pg_credentials['db']))

        target_synonyms_df = pd.read_sql_query(
            'SELECT id, target_id, synonym_id FROM {}'.format('sabrage.target_synonym'),
            con=engine)
        synonyms_df = pd.read_sql_query('SELECT id, name FROM {}'.format('sabrage.synonyms'),
                                        con=engine).rename(columns={'id': 'synonym_id', 'name': 'synonym_name'})
        target_synonyms_df_with_synonyms_name = pd.merge(target_synonyms_df, synonyms_df, left_on='synonym_id',
                                                         right_on='synonym_id')
        target_synonyms_grouped = target_synonyms_df_with_synonyms_name.groupby('target_id')
        synonyms = {target_id: set(target_synonyms_grouped.get_group(target_id)['synonym_name']) for target_id in
                    target.keys()}

        return defaultdict(set, synonyms)

    def get_synonym_map(self, addendum_file_path=None, filter_ids=None, synonym_filter=None, **kwargs):
        entities = self.get_search_terms(**kwargs)
        synonyms = self.aggregate_synonyms(entities)

        # add additional synonyms not contained in Gene
        if addendum_file_path is not None:
            with open(addendum_file_path, 'r') as f:
                for line in f:
                    chunks = line.strip().split('\t')
                    synonyms[int(chunks[1])].add(chunks[0].strip().decode('utf-8'))

        # remove undesirable IDs
        if filter_ids is not None:
            for _id in filter_ids:
                if _id in synonyms:
                    synonyms.pop(_id)

        # remove specific synonyms
        self.filter_synonyms(synonyms, synonym_filter)
        return synonyms


class BmBTargetRegexMatcher(BmBRegexMatcherSabrage, BMBTargetParser):
    ENTITY_LABEL = 'targets'

    def __init__(self, credentials, **kwargs):
        super(BmBTargetRegexMatcher, self).__init__(credentials, **kwargs)
        self.english_checker = enchant.Dict("en_US")
        BMBTargetParser.__init__(self, credentials)

    # Heling changed name with _x1 in the end. Use aggregate_synonyms in BmBRegexMatcherSabrage
    def aggregate_synonyms_x1(self, targets):
        synonyms = defaultdict(set)
        for target in targets:
            for synonym in target.synonyms.get_app_synonyms():
                synonym_stripped = synonym.strip()
                if synonym_stripped:
                    synonyms[target.bmb_id].add(always_unicode(synonym_stripped))
            # if target.gene_symbol is not None and target.gene_symbol.strip():
            #     synonyms[target.bmb_id].add(always_unicode(target.gene_symbol.strip()))
            # if target.protein_name is not None and target.protein_name.strip():
            #     synonyms[target.bmb_id].add(always_unicode(target.protein_name.strip()))
            # synonyms[target.bmb_id].add(always_unicode(target.display_name.strip()))
        return synonyms

    def filter_match(self, text, match):
        """ Apply a set of filters to supplied regex match

        Args:
            text. str. The text used for regex matching
            match. re.MatchObject

        Return:
            bool. Keep the match?
        """
        keep_match = True
        match_text = text[match.regs[0][0]:match.regs[0][1]].strip().lower()
        if match_text in self.tossed_matches:
            keep_match = False
        elif match_text.isdigit():
            keep_match = False
        # elif len(match_text) < 3:
        #     keep_match = False
        elif self.english_checker.check(match_text.replace('-', ' ')):
            keep_match = False
        return keep_match


# class BMBAPITargetRegexMatcher(BMBAPI, BmBTargetRegexMatcher):
#     def __init__(self, **kwargs):
#         super(BMBAPITargetRegexMatcher, self).__init__(bmb_api_config.bmb_api_token, **kwargs)
#         BmBTargetRegexMatcher.__init__(self, **kwargs)

class BmBDiseaseRegexMatcher(BmBRegexMatcher, BMBDiseaseParser):
    ENTITY_LABEL = 'diseases'

    def __init__(self, credentials, **kwargs):
        super(BmBDiseaseRegexMatcher, self).__init__(credentials, **kwargs)
        BMBDiseaseParser.__init__(self, credentials)

    def aggregate_synonyms(self, diseases):
        synonyms = defaultdict(set)
        for disease in diseases:
            for synonym in disease.synonyms.columns.tolist():
                synonym_stripped = synonym.strip()
                if synonym_stripped:
                    synonyms[disease.bmb_id].add(always_unicode(synonym_stripped))
            synonyms[disease.bmb_id].add(always_unicode(disease.name.strip()))
        return synonyms
