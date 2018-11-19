from epic_db.requests import RequestHandler
from epic_db.models import (Sequela,
                            SequelaSet,
                            SequelaSetVersion,
                            SequelaHierarchyHistory)


def test_add_single_hierachy(two_sets_four_versions_sqlite):
    db = two_sets_four_versions_sqlite
    session = db.session

    version_1 = session.query(SequelaSetVersion).get(1)

    # create new aggregate
    # assign both most detailed 3 and 4 to new aggregate

    new_aggregate = {'sequela': [
                        {'sequela_id': None,
                         'sequela_name': 'aggregate of id 3 and 4',
                         'sequela_hierarchy_history': {
                             'sequela_set_version_id': 1,
                             'children': [3, 4],
                             'cause_id': 294,
                             'modelable_entity_id': 1109,
                             'healthstate_id': 4}}]}

    handler = RequestHandler(session)
    handler.process_request(new_aggregate)

    all_sequela = version_1.fk_sequela_hierarchy_history.all()
    new_agg_id = max([seq.sequela_id for seq in all_sequela])

    new_agg = session.query(SequelaHierarchyHistory).get([1, new_agg_id])
    children = new_agg.children

    assert len(children) == 2

    seq_3_parent = session.query(SequelaHierarchyHistory).get([1, 3]).parent_id
    seq_4_parent = session.query(SequelaHierarchyHistory).get([1, 4]).parent_id

    assert seq_3_parent == seq_4_parent == new_agg_id


def test_modify_hierarchy_history(two_sets_four_versions_sqlite):
    """
    Take the first sequela_set_version (id: 1) and alter the hierarchy so the
    parent sequela_1 is modified from children [11, 12, 13, 14] to [11, 12] and
    sequela_2 is modified from children [21, 22, 23, 24] to [21, 22].

    This should cause sequela_ids 13, 14, 23, 24 to have their parent and path
    to top parent attributes changed to their own id.
    """
    db = two_sets_four_versions_sqlite
    session = db.session

    version_1 = session.query(SequelaSetVersion).get(1)
    seq_1 = session.query(SequelaHierarchyHistory).filter(
        SequelaHierarchyHistory.sequela_id == 1).first()
    seq_1_children = seq_1.children
    num_seq_1_children_before = len(seq_1_children)

    seq_2 = session.query(SequelaHierarchyHistory).filter(
        SequelaHierarchyHistory.sequela_id == 2).first()
    seq_2_children = seq_2.children
    num_seq_2_children_before = len(seq_2_children)

    request = {'sequela_hierarchy_history': [
                  {'sequela_set_version_id': 1,
                   'sequela_id': 1, 'sequela_name': 'test sequela 1',
                   'cause_id': 294, 'modelable_entity_id': 1109,
                   'healthstate_id': 1, 'children': [11, 21]},
                  {'sequela_set_version_id': 1,
                   'sequela_id': 2, 'sequela_name': 'test sequela 2',
                   'cause_id': 295, 'modelable_entity_id': 1110,
                   'healthstate_id': 2, 'children': [12, 22]}]}

    handler = RequestHandler(session)
    handler.process_request(request)

    # We've modified the old hierarchies to take away two children
    assert (len(seq_1_children) == len(seq_2_children) ==
            num_seq_1_children_before - 2)
    # Since sequela 13, 14, 23, 24 have been removed from their parents,
    # each of them should now have the global parent as their parent id
    adj_children_ids = [13, 14, 23, 24]
    adjusted_children = version_1.fk_sequela_hierarchy_history.filter(
        SequelaHierarchyHistory.sequela_id.in_(adj_children_ids)).all()
    assert all(child.parent_id == 0 for child in adjusted_children)
