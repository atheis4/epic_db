import pytest

from epic_db.constructors import RowConstructor
from epic_db.requests import RequestHandler
from epic_db.models import (Sequela,
                            SequelaSet,
                            SequelaSetVersion,
                            SequelaHierarchyHistory)


def test_model_delete_sequela(two_sets_four_versions_sqlite):
    db = two_sets_four_versions_sqlite
    session = db.session

    seq1 = session.query(Sequela).get(1)
    seq1.delete()

    session.flush()

    assert seq1.last_updated_action == 'DELETE'
    assert seq1.active_end is not None


def test_constructor_delete_sequela(two_sets_four_versions_sqlite):
    db = two_sets_four_versions_sqlite
    session = db.session

    seq3 = session.query(Sequela).get(3)
    constructor = RowConstructor.get_constructor_by_tablename(
        'sequela')(session)

    primary_keys = {'sequela_id': 3}
    row = constructor.get_row(primary_keys)

    constructor.delete_row(row)
    session.flush()

    assert seq3.last_updated_action == 'DELETE'
    assert seq3.active_end is not None


def test_request_delete_one_sequela(two_sets_four_versions_sqlite):
    db = two_sets_four_versions_sqlite
    session = db.session

    seq2 = session.query(Sequela).get(2)

    request = {'sequela': [{'sequela_id': 2, 'is_delete': True}]}

    handler = RequestHandler(session)
    handler.process_request(request)

    session.commit()

    assert seq2.last_updated_action == 'DELETE'
    assert seq2.active_end is not None


def test_request_delete_two_sequela(two_sets_four_versions_sqlite):
    db = two_sets_four_versions_sqlite
    session = db.session

    seqs_to_del = session.query(Sequela).filter(Sequela.sequela_id.in_([2, 4]))

    request = {'sequela': []}
    for seq in seqs_to_del:
        request['sequela'].append({'sequela_id': seq.sequela_id,
                                   'is_delete': True})

    handler = RequestHandler(session)
    handler.process_request(request)

    session.commit()

    for seq in seqs_to_del:
        assert seq.last_updated_action == 'DELETE'
        assert seq.active_end is not None


def test_model_delete_sequela_set(two_sets_four_versions_sqlite):
    db = two_sets_four_versions_sqlite
    session = db.session

    set1 = session.query(SequelaSet).get(1)
    set1.delete()

    session.flush()

    assert set1.last_updated_action == 'DELETE'


def test_constructor_delete_sequela_set(two_sets_four_versions_sqlite):
    db = two_sets_four_versions_sqlite
    session = db.session

    set2 = session.query(SequelaSet).get(2)

    constructor = RowConstructor.get_constructor_by_tablename(
        'sequela_set')(session)

    primary_keys = {'sequela_set_id': 2}
    row = constructor.get_row(primary_keys)

    constructor.delete_row(row)
    session.flush()

    assert set2.last_updated_action == 'DELETE'


def test_request_delete_sequela_set(two_sets_four_versions_sqlite):
    db = two_sets_four_versions_sqlite
    session = db.session

    set1 = session.query(SequelaSet).get(1)

    request = {'sequela_set': {'sequela_set_id': 1, 'is_delete': True}}

    handler = RequestHandler(session)
    handler.process_request(request)

    session.commit()

    assert set1.last_updated_action == 'DELETE'


def test_model_delete_sequela_set_version(two_sets_four_versions_sqlite):
    db = two_sets_four_versions_sqlite
    session = db.session

    setver3 = session.query(SequelaSetVersion).get(3)
    setver3.delete()

    session.flush()

    assert setver3.last_updated_action == 'DELETE'
    assert setver3.end_date is not None


def test_constructor_delete_sequela_set_version(two_sets_four_versions_sqlite):
    db = two_sets_four_versions_sqlite
    session = db.session

    setver2 = session.query(SequelaSetVersion).get(2)

    constructor = RowConstructor.get_constructor_by_tablename(
        'sequela_set_version')(session)

    primary_keys = {'sequela_set_version_id': 2}
    row = constructor.get_row(primary_keys)

    constructor.delete_row(row)
    session.flush()

    assert setver2.last_updated_action == 'DELETE'
    assert setver2.end_date is not None


def test_request_delete_sequela_set_version(two_sets_four_versions_sqlite):
    db = two_sets_four_versions_sqlite
    session = db.session

    setver1 = session.query(SequelaSetVersion).get(1)

    request = {'sequela_set_version': {'sequela_set_version_id': 1,
                                       'is_delete': True}}

    handler = RequestHandler(session)
    handler.process_request(request)

    session.commit()

    assert setver1.last_updated_action == 'DELETE'
    assert setver1.end_date is not None


def test_model_delete_most_detailed_shh(two_sets_four_versions_sqlite):
    db = two_sets_four_versions_sqlite
    session = db.session

    seqhh11 = session.query(SequelaHierarchyHistory).get([1, 11])

    # there is no model method for deleting seqhh.
    with pytest.raises(NotImplementedError):
        seqhh11.delete()


def test_constructor_delete_most_detailed_shh(two_sets_four_versions_sqlite):
    db = two_sets_four_versions_sqlite
    session = db.session

    version1 = session.query(SequelaSetVersion).get(1)

    seqhh11 = session.query(SequelaHierarchyHistory).get([1, 11])
    parent = version1.get_hierarchy_rows(seqhh11.parent_id)[0]

    siblings = parent.children
    number_of_siblings = len(siblings)

    constructor = RowConstructor.get_constructor_by_tablename(
        'sequela_hierarchy_history')(session)

    primary_keys = {'sequela_set_version_id': 1, 'sequela_id': 11}
    row = constructor.get_row(primary_keys)

    constructor.delete_row(row, sequela_set_version=version1)
    session.commit()

    siblings = parent.children

    assert seqhh11 not in siblings
    assert number_of_siblings - 1 == len(parent.children)
    assert session.query(SequelaHierarchyHistory).get((1, 11)) is None


def test_request_delete_most_detailed_shh(two_sets_four_versions_sqlite):
    db = two_sets_four_versions_sqlite
    session = db.session

    version1 = session.query(SequelaSetVersion).get(1)

    seqhh11 = session.query(SequelaHierarchyHistory).get([1, 11])
    parent = version1.get_hierarchy_rows(seqhh11.parent_id)[0]

    siblings = parent.children
    number_of_siblings = len(siblings)

    request = {'sequela_hierarchy_history': [
        {'sequela_id': 11, 'sequela_set_version_id': 1,
         'is_delete': True}]}

    handler = RequestHandler(session)
    handler.process_request(request)
    session.commit()

    siblings = parent.children

    assert seqhh11 not in siblings
    assert number_of_siblings == len(parent.children) + 1
    assert session.query(SequelaHierarchyHistory).get((1, 11)) is None


def test_model_delete_aggregate_shh(two_sets_four_versions_sqlite):
    db = two_sets_four_versions_sqlite
    session = db.session

    seqhh3 = session.query(SequelaHierarchyHistory).get([2, 3])

    with pytest.raises(NotImplementedError):
        seqhh3.delete()


def test_constructor_delete_aggregate_shh(two_sets_four_versions_sqlite):
    db = two_sets_four_versions_sqlite
    session = db.session

    version2 = session.query(SequelaSetVersion).get(2)

    seqhh3 = session.query(SequelaHierarchyHistory).get([2, 3])
    children = seqhh3.children
    parent = version2.get_hierarchy_rows(seqhh3.parent_id)[0]
    siblings = parent.children

    children_len_before = len(children)
    siblings_len_before = len(siblings)

    constructor = RowConstructor.get_constructor_by_tablename(
        'sequela_hierarchy_history')(session)

    primary_keys = {'sequela_set_version_id': 2, 'sequela_id': 3}
    row = constructor.get_row(primary_keys)

    constructor.delete_row(row, sequela_set_version=version2)
    session.commit()

    siblings = parent.children

    assert seqhh3 not in siblings
    assert len(siblings) == siblings_len_before - 1 + children_len_before
    assert session.query(SequelaHierarchyHistory).get((2, 3)) is None


def test_request_delete_aggregate_shh(two_sets_four_versions_sqlite):
    db = two_sets_four_versions_sqlite
    session = db.session

    version2 = session.query(SequelaSetVersion).get(2)

    seqhh3 = session.query(SequelaHierarchyHistory).get([2, 3])
    children = seqhh3.children

    parent = version2.get_hierarchy_rows(seqhh3.parent_id)[0]
    siblings = parent.children

    children_len_before = len(children)
    siblings_len_before = len(siblings)

    request = {'sequela_hierarchy_history': [
        {'sequela_set_version_id': 2, 'sequela_id': 3,
         'is_delete': True}]}

    handler = RequestHandler(session)
    handler.process_request(request)
    session.commit()

    siblings = parent.children

    assert seqhh3 not in siblings
    assert len(siblings) == siblings_len_before - 1 + children_len_before
    assert session.query(SequelaHierarchyHistory).get((2, 3)) is None
