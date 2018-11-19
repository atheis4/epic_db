from epic_db.models import (Sequela,
                            SequelaSet,
                            SequelaSetVersion,
                            SequelaHierarchyHistory,
                            SequelaReiHistory)


def test_backfill(two_sets_four_versions_sqlite):

    db = two_sets_four_versions_sqlite
    session = db.session

    set_2 = session.query(SequelaSet).get(2)

    # add some reis to make sure the rei backfill works
    version_4 = session.query(SequelaSetVersion).get(4)
    sequela_11 = session.query(Sequela).get(11)
    sequela_33 = session.query(Sequela).get(33)

    version_4.add_sequela_rei(sequela_11, 82)
    version_4.add_sequela_rei(sequela_33, 85)

    # create a new version and backfill with version 4
    set_2.add_version(sequela_set_version="new version",
                      gbd_round_id=5, backfill=4)

    new_version = session.query(SequelaSetVersion).filter(
        SequelaSetVersion.sequela_set_version == "new version").one()

    old_shh = version_4.fk_sequela_hierarchy_history.all()
    new_shh = new_version.fk_sequela_hierarchy_history.all()

    old_rei = version_4.fk_sequela_rei_history.all()
    new_rei = new_version.fk_sequela_rei_history.all()

    shh_oldver_62 = version_4.fk_sequela_hierarchy_history.filter(
        SequelaHierarchyHistory.sequela_id == 62).all()
    shh_newver_62 = new_version.fk_sequela_hierarchy_history.filter(
        SequelaHierarchyHistory.sequela_id == 62).all()

    rei_newver_82 = new_version.fk_sequela_rei_history.filter(
        SequelaReiHistory.rei_id == 82).all()

    # the new version should have the same number of shh rows as version 4
    assert len(old_shh) == len(new_shh)

    # the new version should have the same number of rei rows as version 4
    assert len(old_rei) == len(new_rei)

    # the new version should contain shh for seq 62
    assert len(shh_newver_62) == 1

    # the new version should contain rei for id 62
    assert len(rei_newver_82) == 1

    # the new versions insert dates should be newer than the old ones
    assert shh_newver_62[0].date_inserted > shh_oldver_62[0].date_inserted
