import pytest

from epic_db.constructors import RowConstructor
from epic_db.requests import RequestHandler
from epic_db.models import (Sequela,
                            SequelaSet,
                            SequelaSetVersion,
                            SequelaHierarchyHistory)


def test_model_modify_sequela(two_sets_four_versions_sqlite):
    db = two_sets_four_versions_sqlite
    session = db.session

    seq3 = session.query(Sequela).get(3)
    old_name = seq3.sequela_name
    seq3.sequela_name = 'this is the new name'

    assert old_name != seq3.sequela_name
    assert 'this is the new name' == seq3.sequela_name


def test_constructor_modify_sequela(two_sets_four_versions_sqlite):
    db = two_sets_four_versions_sqlite
    session = db.session

    seq4 = session.query(Sequela).get(4)
    old_name = seq4.sequela_name

    constructor = RowConstructor.get_constructor_by_tablename(
        'sequela')(session)

    row = constructor.get_row({'sequela_id': 4})
    constructor.modify_row(row, {'sequela_name': 'this name is new yo!'})
    session.commit()

    assert old_name != seq4.sequela_name
    assert seq4.sequela_name == 'this name is new yo!'


def test_request_modify_sequela(two_sets_four_versions_sqlite):
    db = two_sets_four_versions_sqlite
    session = db.session

    seq11 = session.query(Sequela).get(11)
    old_name = seq11.sequela_name

    request = {'sequela': [{'sequela_id': 11,
                            'sequela_name': old_name + 'NEW'}]}

    handler = RequestHandler(session)
    handler.process_request(request)
    session.commit()

    assert seq11.sequela_name == old_name + 'NEW'


def test_model_modify_sequela_set(two_sets_four_versions_sqlite):
    db = two_sets_four_versions_sqlite
    session = db.session

    set1 = session.query(SequelaSet).get(1)

    old_name = set1.sequela_set_name
    set1.sequela_set_name = 'DEPRECATED STUFF'

    assert old_name != set1.sequela_set_name
    assert set1.sequela_set_name == 'DEPRECATED STUFF'


def test_constructor_modify_sequela_set(two_sets_four_versions_sqlite):
    db = two_sets_four_versions_sqlite
    session = db.session

    set2 = session.query(SequelaSet).get(2)

    old_name = set2.sequela_set_name

    constructor = RowConstructor.get_constructor_by_tablename(
        'sequela_set')(session)
    row = constructor.get_row({'sequela_set_id': 2})
    constructor.modify_row(row, {'sequela_set_name': 'NEW-NESS'})

    assert old_name != set2.sequela_set_name
    assert set2.sequela_set_name == 'NEW-NESS'


def test_request_modify_sequela_set(two_sets_four_versions_sqlite):
    db = two_sets_four_versions_sqlite
    session = db.session

    set1 = session.query(SequelaSet).get(1)
    set2 = session.query(SequelaSet).get(2)

    old_name1 = set1.sequela_set_name
    old_name2 = set2.sequela_set_name

    request = {'sequela_set': [
                   {'sequela_set_id': 1,
                    'sequela_set_name': old_name2 + 'SWAPPED'},
                   {'sequela_set_id': 2,
                    'sequela_set_name': old_name1 + 'SWAPPED'}]}

    handler = RequestHandler(session)
    handler.process_request(request)
    session.commit()

    assert old_name1 + 'SWAPPED' == set2.sequela_set_name
    assert old_name2 + 'SWAPPED' == set1.sequela_set_name


def test_model_modify_sequela_set_version(two_sets_four_versions_sqlite):
    db = two_sets_four_versions_sqlite
    session = db.session

    version1 = session.query(SequelaSetVersion).get(1)
    old_desc = version1.sequela_set_version_description

    version1.sequela_set_version_description = 'new description'

    assert (session.query(SequelaSetVersion)
            .get(1).sequela_set_version_description) == 'new description'
    assert old_desc != version1.sequela_set_version_description


def test_constructor_modify_sequela_set_version(two_sets_four_versions_sqlite):
    db = two_sets_four_versions_sqlite
    session = db.session

    version1 = session.query(SequelaSetVersion).get(1)
    old_desc = version1.sequela_set_version_description
    old_just = version1.sequela_set_version_justification

    constructor = RowConstructor.get_constructor_by_tablename(
        'sequela_set_version')(session)

    row = constructor.get_row({'sequela_set_version_id': 1})

    column_map = {'sequela_set_version_description': 'Changing this thang',
                  'sequela_set_version_justification': 'BECAUSE',
                  'sequela_set_id': 1}

    constructor.modify_row(row, column_map)
    session.commit()

    assert version1.sequela_set_version_description != old_desc
    assert version1.sequela_set_version_justification != old_just
    assert version1.sequela_set_version_description == 'Changing this thang'
    assert version1.sequela_set_version_justification == 'BECAUSE'


def test_request_modify_sequela_set_version(two_sets_four_versions_sqlite):
    db = two_sets_four_versions_sqlite
    session = db.session

    version1 = session.query(SequelaSetVersion).get(1)
    old_desc = version1.sequela_set_version_description
    old_just = version1.sequela_set_version_justification

    request = {'sequela_set_version': {
                   'sequela_set_id': 1,
                   'sequela_set_version_id': 1,
                   'sequela_set_version_description': 'Changing this thang',
                   'sequela_set_version_justification': 'BECAUSE'}}

    handler = RequestHandler(session)
    handler.process_request(request)
    session.commit()

    assert version1.sequela_set_version_description != old_desc
    assert version1.sequela_set_version_justification != old_just
    assert version1.sequela_set_version_description == 'Changing this thang'
    assert version1.sequela_set_version_justification == 'BECAUSE'


def test_model_modify_shh(two_sets_four_versions_sqlite):
    db = two_sets_four_versions_sqlite
    session = db.session

    seqhh1 = session.query(SequelaHierarchyHistory).get([1, 1])
    old_name = seqhh1.sequela_name
    old_cause = seqhh1.cause_id

    seqhh1.cause_id = 419
    seqhh1.sequela_name = 'modified name'

    assert old_name != seqhh1.sequela_name
    assert old_cause != seqhh1.cause_id


def test_constructor_modify_shh(two_sets_four_versions_sqlite):
    db = two_sets_four_versions_sqlite
    session = db.session

    seqhh23 = session.query(SequelaHierarchyHistory).get([1, 23])
    version1 = session.query(SequelaSetVersion).get(1)

    old_id = seqhh23.cause_id
    old_name = seqhh23.sequela_name

    constructor = RowConstructor.get_constructor_by_tablename(
        'sequela_hierarchy_history')(session)

    row = constructor.get_row({'sequela_set_version_id': 1,
                               'sequela_id': 23})

    column_map = {'sequela_name': 'overwrite name',
                  'cause_id': 666,
                  'sequela_set_version_id': 1}
    constructor.modify_row(row, column_map, sequela_set_version=version1)
    session.commit()

    assert old_id != seqhh23.cause_id
    assert seqhh23.cause_id == 666
    # sequela_name is not currently a modify column for SequelaHierarchyHistory
    assert old_name == seqhh23.sequela_name
    assert seqhh23.sequela_name == 'test sequela 23'


def test_request_modify_shh(two_sets_four_versions_sqlite):
    db = two_sets_four_versions_sqlite
    session = db.session

    seqhh4 = session.query(SequelaHierarchyHistory).get([2, 4])
    old_cause = seqhh4.cause_id
    old_me = seqhh4.modelable_entity_id
    old_hs = seqhh4.healthstate_id

    request = {'sequela_hierarchy_history': [
                  {'sequela_set_version_id': 2,
                   'sequela_id': 4,
                   'cause_id': 419,
                   'modelable_entity_id': 841,
                   'healthstate_id': 1968}]}

    handler = RequestHandler(session)
    handler.process_request(request)
    session.commit()

    assert old_cause != seqhh4.cause_id
    assert old_me != seqhh4.modelable_entity_id
    assert old_hs != seqhh4.healthstate_id
    assert seqhh4.cause_id == 419
    assert seqhh4.modelable_entity_id == 841
    assert seqhh4.healthstate_id == 1968
