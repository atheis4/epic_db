import pytest

from epic_db.database import create_db, delete_db, config
from epic_db import models

one_set_two_versions = {1: {
    1: {1: [11, 12, 13], 2: [21, 22, 23]},
    2: {1: [11, 12, 13], 2: [21, 22, 24], 3: []}}}

two_sets_four_versions = {
    1: {1: {1: [11, 12, 13, 14], 2: [21, 22, 23], 3: [], 4: []},
        2: {1: [11, 12, 13], 2: [21, 22, 23], 3: [31, 32], 4: []}},
    2: {3: {1: [11, 12, 13], 3: [33, 34], 5: [], 6: [61, 62, 63]},
        4: {1: [11, 12, 13], 3: [33, 34], 5: [], 6: [61, 62, 63], 7: []}}}


class TestEpicDb(object):

    def __init__(self):
        self._session = config.Session()

    @property
    def session(self):
        return self._session

    @property
    def query(self):
        return self.session.query

    def flush_changes(self):
        self.session.flush()

    def sequela_by_id(self, sequela_id):
        return self.session.query(
            models.Sequela).filter(
                models.Sequela.sequela_id == sequela_id).all()

    def add_data(self, data_dict, gbd_round_id=5, level_0_id=None):
        """Adds entries to the Sequela, SequelaSet, SequealaSetVersion,
        and SequealHierarchHistory tables by parsing a nested dictionary.

        Examples:
            If data_dict = {1: {
                                1: {
                                    1: [11, 12, 13],
                                    2: []},
                                2: {
                                    1: [11, 12, 13],
                                    2: [21, 22],
                                    3: []}
                                }
                            2: {
                                3: {
                                    5: [51, 52],
                                    6: [],
                                    7: []
                                    }
                                }
                            }}

            then 2 sets will be created (set_id = 1 and 2)
            Set 1 will have 2 versions (version_id = 1 and 2)
            and set 2 will have just 1 version (version_id = 3)
            Version 1 will have 2 level 1 sequela, one of which
            is most detailed (sequela_id = 2) and one of which (sequela_id = 1)
            has 3 level 2 most detailed children associated with it
            (sequela_ids 11, 12, and 13)"""

        for set_id in data_dict:
            self.add_set(set_id=set_id)
            self.flush_changes()
            versions = data_dict[set_id]
            for version_id in versions:
                self.add_version(
                    set_id=set_id, version_id=version_id,
                    gbd_round_id=gbd_round_id)
                level_0 = self.sequela_by_id(0)
                if not level_0:
                    level_0 = self.add_sequela(sequela_id=0, name='root')
                    self.flush_changes()
                else:
                    level_0 = level_0[0]
                self.add_hierarchy(
                    set_id, version_id, level_0.sequela_id,
                    level=0, most_detailed=0,
                    parent_id=level_0.sequela_id,
                    path_to_top_parent='0',
                    sequela_name=level_0.sequela_name)
                self.flush_changes()
                level_1_sequela = versions[version_id]
                for sequela_id in level_1_sequela:
                    level_1 = self.sequela_by_id(sequela_id)
                    if not level_1:
                        level_1 = self.add_sequela(sequela_id=sequela_id)
                        self.flush_changes()
                    else:
                        level_1 = level_1[0]
                    level_2_sequela = level_1_sequela[sequela_id]
                    if not level_2_sequela:
                        most_detailed = 1
                    else:
                        most_detailed = 0

                    self.add_hierarchy(
                        set_id, version_id, sequela_id,
                        level=1, most_detailed=most_detailed,
                        parent_id=level_0.sequela_id,
                        path_to_top_parent='0,{}'.format(sequela_id),
                        sequela_name=level_1.sequela_name)
                    self.flush_changes()

                    for child_id in level_2_sequela:
                        level_2 = self.sequela_by_id(child_id)
                        if not level_2:
                            level_2 = self.add_sequela(sequela_id=child_id)
                            self.flush_changes()
                        else:
                            level_2 = level_2[0]
                        self.add_hierarchy(
                            set_id, version_id, child_id,
                            level=2, most_detailed=1,
                            parent_id=sequela_id,
                            sequela_name=level_2.sequela_name)
                        self.flush_changes()

    def add_set(self, set_id=None, name=None, description=None):
        if not name and set_id:
            name = 'set {}'.format(set_id)
        this_set = models.SequelaSet(
            sequela_set_id=set_id,
            sequela_set_name=name,
            sequela_set_description=description)
        self.session.add(this_set)
        return this_set

    def add_version(self, set_id, version_id=None,
                    name=None, description=None,
                    justification=None, gbd_round_id=5):

        if version_id:
            if not name:
                name = 'set version {}'.format(version_id)
            if not description:
                description = 'set {} version {}'.format(set_id, version_id)
        this_version = models.SequelaSetVersion(
            sequela_set_version_id=version_id,
            sequela_set_id=set_id,
            sequela_set_version=name,
            sequela_set_version_description=description,
            sequela_set_version_justification=justification,
            gbd_round_id=gbd_round_id)
        self.session.add(this_version)
        return this_version

    def add_sequela(self, sequela_id=None, name=None):
        if not name and sequela_id:
            name = 'test sequela {}'.format(sequela_id)
        this_sequela = models.Sequela(
            sequela_id=sequela_id,
            sequela_name=name)
        self.session.add(this_sequela)
        return this_sequela

    def add_hierarchy(self, set_id, version_id, sequela_id,
                      level, most_detailed, parent_id,
                      sequela_name, modelable_entity_id=None,
                      cause_id=None, healthstate_id=None,
                      path_to_top_parent=None, sort_order=None):

        this_hierarchy = models.SequelaHierarchyHistory(
            sequela_set_version_id=version_id,
            sequela_set_id=set_id,
            sequela_id=sequela_id,
            level=level, most_detailed=most_detailed,
            parent_id=parent_id,
            path_to_top_parent=path_to_top_parent,
            sort_order=sort_order, sequela_name=sequela_name,
            modelable_entity_id=modelable_entity_id,
            cause_id=cause_id, healthstate_id=healthstate_id)
        self.session.add(this_hierarchy)
        return this_hierarchy


@pytest.fixture(scope='function')
def empty_schema_sqlite():
    create_db()
    yield
    delete_db()


@pytest.fixture(scope='function')
def one_set_two_versions_sqlite():
    create_db()
    test_db = TestEpicDb()
    test_db.add_data(one_set_two_versions)
    yield test_db
    delete_db()


@pytest.fixture(scope='function')
def two_sets_four_versions_sqlite():
    create_db()
    test_db = TestEpicDb()
    test_db.add_data(two_sets_four_versions)
    yield test_db
    delete_db()
