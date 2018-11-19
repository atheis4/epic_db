import os
import pandas as pd

from epic_db.models import (Sequela,
                            SequelaSet,
                            SequelaSetVersion)


class XlsProcessor(object):

    _valid_columns = ['cause_id', 'sequela_id', 'sequela_name', 'lvl_5_name']

    def _fill_merged_cells(self):
        self.data.fillna(method='ffill', inplace=True)

    def _rename_columns(self):
        self.data.rename(columns={'Name level 5 hierarchy': 'lvl_5_name'},
                         inplace=True)

    def _drop_unidentified_column(self):
        to_drop = []
        for col in self.data.columns:
            if 'Unnamed' in col:
                to_drop.append(col)
        self.data.drop(labels=to_drop, inplace=True, axis=1)

    def _drop_row_nans(self):
        self.data.dropna(axis=0, subset=['sequela_id'], inplace=True)

    def run(self, path):
        self.data = pd.read_excel(path)
        self._drop_row_nans()
        self._drop_unidentified_column()
        self._rename_columns()
        self._fill_merged_cells()
        return self.data[self._valid_columns]


class JsonConverter(object):

    def __init__(self, dataframe, session=None):
        self.dataframe = dataframe
        self.session = session
        self.most_detailed = dataframe.sequela_id.tolist()
        self.level_5 = dataframe.lvl_5_name.unique().tolist()

    def _create_most_detailed(self, sequela_id, sequela_name, version_id,
                              cause_id):
        return {'sequela_id': sequela_id,
                'sequela_name': sequela_name,
                'sequela_hierarchy_history': {
                    'sequela_set_version_id': version_id,
                    'cause_id': cause_id,
                    'children': None}}

    def _create_aggregate(self, sequela_name):
        return {'sequela_id': None,
                'sequela_name': sequela_name}

    def _get_id_from_name(self, lvl_5_name):
        seq_id = self.session.query(Sequela).filter(
            Sequela.sequela_name == lvl_5_name).first().sequela_id
        return seq_id

    def _create_hierarchy_row(self, lvl_5_id, version_id, cause_id, children):
        return {'sequela_id': lvl_5_id,
                'sequela_set_version_id': version_id,
                'cause_id': cause_id,
                'children': children}

    def create_all_most_detailed(self, version_id=None):
        output = {'sequela': []}
        for _, row in self.dataframe.iterrows():
            row_dict = row.to_dict()
            sequela_id = row_dict['sequela_id']
            sequela_name = row_dict['sequela_name']
            cause_id = row_dict['cause_id']
            output['sequela'].append(
                self._create_most_detailed(sequela_id, sequela_name,
                                           version_id, cause_id))
        return output

    def create_all_aggregates(self):
        output = {'sequela': []}
        for lvl_5 in self.level_5:
            children = self.dataframe[self.dataframe.lvl_5_name == lvl_5]
            output['sequela'].append(self._create_aggregate(lvl_5))
        return output

    def create_hierarchy(self, version_id=None):
        output = {'sequela_hierarchy_history': []}
        for lvl_5 in self.level_5:
            lvl_5_id = self._get_id_from_name(lvl_5)
            this_data = self.dataframe[self.dataframe.lvl_5_name == lvl_5]
            children = [int(child) for child in this_data.sequela_id.tolist()]
            cause_ids = [int(cause) for cause in this_data.cause_id.unique()]
            assert len(cause_ids) == 1, print(cause_ids)
            output['sequela_hierarchy_history'].append(
                self._create_hierarchy_row(lvl_5_id, version_id, cause_ids[0],
                                           children))
        return output
