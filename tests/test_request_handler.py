from epic_db.requests import RequestHandler
from epic_db.models import (Sequela,
                            SequelaSet,
                            SequelaSetVersion,
                            SequelaHierarchyHistory,
                            SequelaReiHistory)


def test_add_single_version(two_sets_four_versions_sqlite):
    db = two_sets_four_versions_sqlite
    session = db.session
    versions = session.query(SequelaSetVersion).all()
    num_ver_before = len(versions)
    version_to_add = {
        'sequela_set_version': {
            'sequela_set_id': 1,
            'sequela_set_version_id': None,
            'sequela_set_version': 'dummy',
            'sequela_set_version_description': 'new',
            'sequela_set_version_justification': 'we want it'}}

    handler = RequestHandler(session)
    handler.process_request(version_to_add)
    versions = session.query(SequelaSetVersion).all()
    # there were 4 versions, we added one
    assert len(versions) == num_ver_before + 1
    # set 1 had 2 versions previously, now 3
    set_1 = session.query(SequelaSet).get(1)
    assert len(set_1.fk_sequela_set_version_id.all()) == 3
    # version 5 should have the name 'dummy'
    version_5 = session.query(SequelaSetVersion).get(5)
    assert version_5.sequela_set_version == 'dummy'


def test_add_single_hierarchy(two_sets_four_versions_sqlite):

    db = two_sets_four_versions_sqlite
    session = db.session
    version_2 = session.query(SequelaSetVersion).get(2)
    num_version_2_shh_before = len(
        version_2.fk_sequela_hierarchy_history.all())
    sequela = session.query(Sequela).all()
    num_seq_before = len(sequela)
    max_sequela_id = max([seq.sequela_id for seq in sequela])

    shh_to_add = {'sequela': [
                      {'sequela_id': 61,
                       'sequela_name': 'new sequela 1',
                       'sequela_hierarchy_history': {
                           'sequela_set_version_id': 2,
                           'cause_id': 295,
                           'modelable_entity_id': 1109,
                           'healthstate_id': 1}},
                      {'sequela_id': None,
                       'sequela_name': 'new sequela',
                       'sequela_hierarchy_history': {
                           'sequela_set_version_id': 2,
                           'cause_id': 295,
                           'modelable_entity_id': 1110,
                           'healthstate_id': 2}}]}

    handler = RequestHandler(session)
    handler.process_request(shh_to_add)

    version_2 = session.query(SequelaSetVersion).get(2)

    # there were 12 shh rows associated with version 2, we added two
    assert len(
        version_2.fk_sequela_hierarchy_history.all()) == (
        num_version_2_shh_before + 2)

    # one of the two new shh rows is also a new sequela
    assert len(session.query(Sequela).all()) == num_seq_before + 1

    # the name of the last sequela added should be 'new_sequela'
    test_seq = session.query(Sequela).get(max_sequela_id + 1)
    assert test_seq.sequela_name == 'new sequela'


def test_multiple(two_sets_four_versions_sqlite):

    db = two_sets_four_versions_sqlite
    session = db.session
    set_1 = session.query(SequelaSet).get(1)
    num_set_1_versions_before = len(set_1.fk_sequela_set_version_id.all())
    old_seqs = session.query(Sequela).all()
    old_seq_ids = [seq.sequela_id for seq in old_seqs]
    to_add = {'sequela_set_version': {
                  'sequela_set_id': 1,
                  'sequela_set_version_id': None,
                  'sequela_set_version': 'new version',
                  'sequela_set_version_description': 'a new version to use',
                  'sequela_set_version_justification': 'cuz we wants it',
                  'sequela': [
                      {'sequela_id': 61,
                       'sequela_name': 'new sequela 1',
                       'sequela_hierarchy_history': {
                           'cause_id': 295,
                           'modelable_entity_id': 1109,
                           'healthstate_id': 1}},
                      {'sequela_id': None,
                       'sequela_name': 'new sequela 2',
                       'sequela_hierarchy_history': {
                           'cause_id': 295,
                           'modelable_entity_id': 1110,
                           'healthstate_id': 2}}]}}

    handler = RequestHandler(session)
    handler.process_request(to_add)

    # we added one version to set_1
    assert len(set_1.fk_sequela_set_version_id.all()) == (
        num_set_1_versions_before + 1)

    # get that version by name
    new_version = session.query(SequelaSetVersion).filter(
        SequelaSetVersion.sequela_set_version == 'new version').one()
    new_shh = new_version.fk_sequela_hierarchy_history
    all_sequela = session.query(Sequela).all()
    # that version should have 2 shh rows associated with it
    assert len(new_shh.all()) == 2
    # one of those two new versions should have old sequela_id 61
    assert len(new_shh.filter(
        SequelaHierarchyHistory.sequela_id == 61).all()) == 1
    # we added one new sequela
    assert len(all_sequela) == (len(old_seqs) + 1)
    # one of the two we added should also have an shh row in the new version
    new_seq_ids = [seq.sequela_id for seq in all_sequela
                   if seq.sequela_id not in old_seq_ids]
    new_shh_new_seq = [shh.sequela_id for shh in new_shh
                       if shh.sequela_id in new_seq_ids]
    assert len(new_shh_new_seq) == 1


def test_add_rei(two_sets_four_versions_sqlite):
    db = two_sets_four_versions_sqlite
    session = db.session
    num_seqs_before = len(session.query(Sequela).all())

    request = {'sequela': [
        {'sequela_id': None,
         'sequela_name': 'this is my name',
         'sequela_rei_history': [
              {'sequela_set_version_id': 1,
               'rei_id': 82}]}],
        'sequela_rei_history': [
        {'sequela_id': 3,
         'sequela_set_version_id': 1,
         'rei_id': 86},
        {'sequela_id': 3,
         'sequela_set_version_id': 1,
         'rei_id': 87}]}

    handler = RequestHandler(session)
    handler.process_request(request)

    version = session.query(SequelaSetVersion).get(1)
    version_reis = version.fk_sequela_rei_history

    # version 1 should now have 3 rei rows
    assert len(version_reis.all()) == 3

    # version 1, sequela 3 should have 2 rei rows
    seq3 = version_reis.filter(SequelaReiHistory.sequela_id == 3).all()
    assert len(seq3) == 2

    # there should be one new sequela
    num_seqs_after = len(session.query(Sequela).all())
    assert num_seqs_after == num_seqs_before + 1

    # the new seq should have 1 rei row
    new_seq_id = session.query(Sequela).filter(
        Sequela.sequela_name == 'this is my name').one().sequela_id
    new_seq_rei = version_reis.filter(
        SequelaReiHistory.sequela_id == new_seq_id).all()
    assert len(new_seq_rei) == 1
