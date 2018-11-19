from epic_db.database import config, session_scope
from epic_db.models import SequelaSetVersion, SequelaSetVersionActive
from epic_db.errors import SequelaSetVersionValidationError
from gbd.constants import GBD_ROUND_ID
from db_tools.ezfuncs import get_engine


def activate_sequela_set_version(sequela_set_version_id,
                                 gbd_round_id=GBD_ROUND_ID,
                                 validate=True, conn_def=None):

    if conn_def is not None:
        config.engine = get_engine(conn_def=conn_def)

    with session_scope() as session:
        activate = ActivateSequelaVersion(
            session, sequela_set_version_id, gbd_round_id)
        if validate:
            activate.validate_version()
        activate.activate_version()


class ActivateSequelaVersion(object):

    def __init__(self, session, sequela_set_version_id,
                 gbd_round_id):
        self.session = session
        self.gbd_round_id = gbd_round_id
        self.version = self.session.query(
            SequelaSetVersion).get(sequela_set_version_id)
        if not self.version:
            raise ValueError(
                "Sequela_set_version_id {} not found in "
                "epic.sequela_set_version table.".format(
                    sequela_set_version_id))

    def validate_version(self):
        self.has_shh_for_rei()

    def has_shh_for_rei(self):

        rei_sequela_ids = set(
            [row.sequela_id for row in
             self.version.fk_sequela_rei_history.all()])
        shh_sequela_ids = set(
            [row.sequela_id for row in
             self.version.fk_sequela_hierarchy_history.all()])

        missing_in_shh = list(rei_sequela_ids - shh_sequela_ids)

        if missing_in_shh:
            raise SequelaSetVersionValidationError(
                "Sequela_ids {rei_rows} have row entries in "
                "sequela_rei_history but not corresponding "
                "row entries in sequela_hierarchy_history "
                "for sequela_set_version {version}".format(
                    rei_rows=missing_in_shh,
                    version=self.version))

    def sync_sequela_names(self):
        pass

    def activate_version(self):
        active_version = self.session.query(
            SequelaSetVersionActive).get(
            (self.version.sequela_set_id, self.gbd_round_id))

        if not active_version:
            active_version = SequelaSetVersionActive(
                sequela_set_id=self.version.sequela_set_id,
                gbd_round_id=self.gbd_round_id,
                sequela_set_version_id=self.version.sequela_set_version_id)
            self.session.add(active_version)
        else:
            active_version.sequela_set_version_id = (
                self.version.sequela_set_version_id)
        self.session.flush()
