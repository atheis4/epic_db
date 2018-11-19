import pytest
from epic_db.activate import activate_sequela_set_version
from epic_db.models import (Sequela,
                            SequelaSetVersion,
                            SequelaSetVersionActive)
from epic_db.errors import SequelaSetVersionValidationError


def test_activate_version_new_round(two_sets_four_versions_sqlite):

    db = two_sets_four_versions_sqlite
    session = db.session

    # add some old rounds to the activate table
    version_1 = session.query(SequelaSetVersion).get(1)
    version_2 = session.query(SequelaSetVersion).get(2)
    version_3 = session.query(SequelaSetVersion).get(3)

    active_1 = SequelaSetVersionActive(
        sequela_set_id=version_1.sequela_set_id,
        gbd_round_id=3,
        sequela_set_version_id=version_1.sequela_set_version_id)
    active_2 = SequelaSetVersionActive(
        sequela_set_id=version_2.sequela_set_id,
        gbd_round_id=4,
        sequela_set_version_id=version_2.sequela_set_version_id)
    active_3 = SequelaSetVersionActive(
        sequela_set_id=version_3.sequela_set_id,
        gbd_round_id=4,
        sequela_set_version_id=version_3.sequela_set_version_id)

    session.add_all([active_1, active_2, active_3])
    session.flush()
    num_old_active = len(session.query(SequelaSetVersionActive).all())

    # activate version 4 for gbd_round_id 5
    activate_sequela_set_version(4, gbd_round_id=5)

    # there should now be 4 rows in the active table
    num_new_active = len(session.query(SequelaSetVersionActive).all())
    assert num_new_active == num_old_active + 1

    # active version for set 2, round 5 should be 4
    active_4 = session.query(SequelaSetVersionActive).get((2, 5))
    assert active_4.sequela_set_version_id == 4


def test_activate_version_replace_round(two_sets_four_versions_sqlite):
    db = two_sets_four_versions_sqlite
    session = db.session

    # add some active versions to the activate table
    version_1 = session.query(SequelaSetVersion).get(1)
    version_2 = session.query(SequelaSetVersion).get(2)
    version_3 = session.query(SequelaSetVersion).get(3)

    active_1 = SequelaSetVersionActive(
        sequela_set_id=version_1.sequela_set_id,
        gbd_round_id=4,
        sequela_set_version_id=version_1.sequela_set_version_id)
    active_2 = SequelaSetVersionActive(
        sequela_set_id=version_2.sequela_set_id,
        gbd_round_id=5,
        sequela_set_version_id=version_2.sequela_set_version_id)
    active_3 = SequelaSetVersionActive(
        sequela_set_id=version_3.sequela_set_id,
        gbd_round_id=5,
        sequela_set_version_id=version_3.sequela_set_version_id)

    session.add_all([active_1, active_2, active_3])
    session.flush()

    num_old_active = len(session.query(SequelaSetVersionActive).all())

    # activate version 4 for set 2 round 5; this should replace version 3
    activate_sequela_set_version(4, gbd_round_id=5)

    # refresh session
    session.close()
    db = two_sets_four_versions_sqlite
    session = db.session

    # there should be the same number of rows in the active table as before
    num_new_active = len(session.query(SequelaSetVersionActive).all())
    assert num_old_active == num_new_active

    # active version for set 2 round 5 should be 4
    active_4 = session.query(SequelaSetVersionActive).get((2, 5))
    assert active_4.sequela_set_version_id == 4

    # version 2 should remain active for set 1 round 5
    active_2 = session.query(SequelaSetVersionActive).get((1, 5))
    assert active_2.sequela_set_version_id == 2


def test_validation_fail(two_sets_four_versions_sqlite):
    with pytest.raises(SequelaSetVersionValidationError):
        db = two_sets_four_versions_sqlite
        session = db.session

        # add some rei rows; some are for sequela not in shh
        version_4 = session.query(SequelaSetVersion).get(4)
        sequela_11 = session.query(Sequela).get(11)
        sequela_21 = session.query(Sequela).get(21)
        sequela_33 = session.query(Sequela).get(33)

        version_4.add_sequela_rei(sequela_11, rei_id=82)
        version_4.add_sequela_rei(sequela_21, rei_id=88)
        version_4.add_sequela_rei(sequela_33, rei_id=92)
        session.commit()

        activate_sequela_set_version(4, gbd_round_id=5)
