from sqlalchemy import (Column,
                        DateTime,
                        Integer,
                        Float,
                        ForeignKey,
                        String,
                        ForeignKeyConstraint)
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import relationship

from datetime import datetime
from numpy import atleast_1d


class Base(object):

    def to_wire(self, columns=None, exclude_columns=False):
        wired = {c.name: getattr(self, c.name) for c in self.__table__.columns}
        if columns is None:
            return wired
        else:
            columns = list(atleast_1d(columns))

        if exclude_columns:
            return {key: wired[key] for key in wired if key not in columns}
        else:
            return {key: wired[key] for key in wired if key in columns}


Base = declarative_base(cls=Base)


class Sequela(Base):
    __tablename__ = 'sequela'

    sequela_id = Column(Integer, primary_key=True)
    sequela_name = Column(String(255), unique=True)
    active_start = Column(DateTime, default=datetime.utcnow)
    active_end = Column(DateTime, None)
    date_inserted = Column(DateTime, default=datetime.utcnow)
    inserted_by = Column(String(50), default='unknown')
    last_updated = Column(DateTime, default=datetime.utcnow)
    last_updated_by = Column(String(50), default='unknown')
    last_updated_action = Column(String(6), default='INSERT')

    fk_sequela_hierarchy_history = relationship(
        'SequelaHierarchyHistory',
        back_populates='fk_sequela')

    def __repr__(self):
        return ("<Sequela(sequela_id: {}, sequela_name: {}, active_start: {}, "
                "active_end: {}, date_inserted: {}, inserted_by: {}, "
                "last_updated: {}, last_updated_by: {}, "
                "last_updated_action: {})>".format(
                    self.sequela_id,
                    self.sequela_name,
                    self.active_start,
                    self.active_end,
                    self.date_inserted,
                    self.inserted_by,
                    self.last_updated,
                    self.last_updated_by,
                    self.last_updated_action))

    def delete(self):
        """Mark a Sequela row as deprecated."""
        self.active_end = datetime.utcnow()
        self.last_updated_action = 'DELETE'


class SequelaSet(Base):
    __tablename__ = 'sequela_set'

    sequela_set_id = Column(Integer, primary_key=True)
    sequela_set_name = Column(String(175), default=None, unique=True)
    sequela_set_description = Column(String(500), default=None)
    date_inserted = Column(DateTime, default=datetime.utcnow)
    inserted_by = Column(String(50), default='unknown')
    last_updated = Column(DateTime, default=datetime.utcnow)
    last_updated_by = Column(String(50), default='unknown')
    last_updated_action = Column(String(6), default='INSERT')

    fk_sequela_set_version_id = relationship("SequelaSetVersion",
                                             lazy="dynamic")

    def __repr__(self):
        return ("<SequelaSet(sequela_set_id: {}, sequela_set_name: {}, "
                "sequela_set_description: {}, date_inserted: {}, "
                "inserted_by: {}, last_updated: {}, last_updated_by: {}, "
                "last_updated_action: {})>".format(
                    self.sequela_set_id,
                    self.sequela_set_name,
                    self.sequela_set_description,
                    self.date_inserted,
                    self.inserted_by,
                    self.last_updated,
                    self.last_updated_by,
                    self.last_updated_action))

    def add_version(self, sequela_set_version=None,
                    sequela_set_version_description=None,
                    sequela_set_version_justification=None,
                    gbd_round_id=None, backfill=None):

        """
        Add a new SequelaSetVersion to the list of versions associated with
        this SequelaSet.

        Arguments:
            sequela_set_version (str): varchar(255)
            sequela_set_version_description (str): varchar(500)
            sequela_set_version_justification (str): varchar(500)
            gbd_round_id (int): GBD round to associate new set version id with.

        """
        row = SequelaSetVersion(
            sequela_set_version=sequela_set_version,
            sequela_set_version_description=sequela_set_version_description,
            sequela_set_version_justification=(
                sequela_set_version_justification),
            gbd_round_id=gbd_round_id)

        self.fk_sequela_set_version_id.append(row)

        row = self.fk_sequela_set_version_id[-1]
        if backfill:
            self.backfill_version(backfill, row.sequela_set_version_id)

        return row

    def backfill_version(self, old_version_id, new_version_id):
        old_version = self.fk_sequela_set_version_id.filter(
            SequelaSetVersion.sequela_set_version_id == old_version_id).one()

        new_version = self.fk_sequela_set_version_id.filter(
            SequelaSetVersion.sequela_set_version_id == new_version_id).one()

        new_version.backfill_hierarchy(old_version)
        new_version.backfill_rei(old_version)

        return self.fk_sequela_set_version_id[-1]

    def delete(self):
        """Mark a SequelaSet as deprecated."""
        self.last_updated_action = 'DELETE'


class SequelaSetVersion(Base):
    """Add in reasoning behind using SetVersion as a primary means for altering
    hierarchies and history tables."""
    __tablename__ = 'sequela_set_version'

    sequela_set_version_id = Column(Integer, primary_key=True)
    sequela_set_id = Column(
        Integer,
        ForeignKey('sequela_set.sequela_set_id'))
    sequela_set_version = Column(String(255), default=None)
    sequela_set_version_description = Column(String(500), default=None)
    sequela_set_version_justification = Column(String(500), default=None)
    gbd_round_id = Column(Integer, default=None)
    start_date = Column(DateTime, default=datetime.utcnow)
    end_date = Column(DateTime, default=None)
    date_inserted = Column(DateTime, default=datetime.utcnow)
    inserted_by = Column(String(50), default='unknown')
    last_updated = Column(DateTime, default=datetime.utcnow)
    last_updated_by = Column(String(50), default='unknown')
    last_updated_action = Column(String(6), default='INSERT')

    fk_sequela_hierarchy_history = relationship(
        "SequelaHierarchyHistory", lazy="dynamic",
        back_populates="fk_sequela_set_version_id")

    fk_sequela_rei_history = relationship(
        "SequelaReiHistory", lazy="dynamic",
        back_populates="fk_sequela_set_version_id")
    # relationship to sequela_set_version_active

    def __repr__(self):
        return ("<SequelaSetVersion(sequela_set_version_id: {}, "
                "sequela_set_id: {}, sequela_set_version: {}, "
                "sequela_set_version_description: {}, "
                "sequela_set_version_justification: {}, "
                "gbd_round_id: {}, start_date: {}, end_date: {}, "
                "date_inserted: {}, inserted_by: {}, last_updated: {}, "
                "last_updated_by: {}, last_updated_action: {})>".format(
                    self.sequela_set_version_id,
                    self.sequela_set_id,
                    self.sequela_set_version,
                    self.sequela_set_version_description,
                    self.sequela_set_version_justification,
                    self.gbd_round_id,
                    self.start_date,
                    self.end_date,
                    self.date_inserted,
                    self.inserted_by,
                    self.last_updated,
                    self.last_updated_by,
                    self.last_updated_action))

    @property
    def most_detailed(self):
        """Return all most detailed SequelaHierarchyHistory rows in this
        version."""
        return (self.fk_sequela_hierarchy_history.filter(
                SequelaHierarchyHistory.most_detailed == 1).all())

    def delete(self):
        """Mark SequelaSetVersion as deprecated."""
        self.end_date = datetime.utcnow()
        self.last_updated_action = 'DELETE'

    def hierarchy_add_most_detailed(self, sequela, cause_id=None,
                                    modelable_entity_id=None,
                                    healthstate_id=None):
        """
        Insert a most-detailed Sequela to this version's sequela hierarchy
        history table.

        Arguments:
            sequela (models.Sequela): Sequela row of a most detailed sequela to
                add to the SequelaHierarchyHistory table.

            cause_id (int): represents the cause this sequela is mapped to.
                Default None.

            modelable_entity_id (int): represents the modelable entity this
                sequela is mapped to. Default None.

            healthstate_id (int): represents the healthstate this sequela is
                mapped to. Default None.
        """
        row = SequelaHierarchyHistory(
            sequela_set_id=self.sequela_set_id,
            sequela_id=sequela.sequela_id,
            level=1,
            most_detailed=1,
            parent_id=0,
            sequela_name=sequela.sequela_name,
            modelable_entity_id=modelable_entity_id,
            cause_id=cause_id,
            healthstate_id=healthstate_id)
        self.fk_sequela_hierarchy_history.append(row)

        return row

    def get_hierarchy_rows(self, sequela_id):
        """
        Return a list of SequelaHierarchyHistory rows from sequela ids.

        Arguments:
            sequela_id (int or intlist): integer list of sequela_id to return.

        Returns:
            A list of SequelaHierarchyHistory rows.
        """
        if isinstance(sequela_id, list):
            sequela_ids = sequela_id
        else:
            sequela_ids = [sequela_id]
        return self.fk_sequela_hierarchy_history.filter(
            SequelaHierarchyHistory.sequela_id.in_(sequela_ids)).all()

    def get_child_rows(self, parent_id):
        """
        Return all children from a parent sequela id.

        Arguments:
            parent_id (int): The parent sequela id.

        Returns:
            A list of child SequelaHierarchyHistory rows associated with the
                parent sequela_id.
        """
        return self.fk_sequela_hierarchy_history.filter(
            SequelaHierarchyHistory.sequela_id == parent_id).first().children

    def hierarchy_add_aggregate(self, sequela, children, cause_id=None,
                                modelable_entity_id=None, healthstate_id=None):
        """
        Insert an aggregate sequela to the SequelaHierarchyHistory table
        associated with this version.

        Arguments:
            sequela (models.Sequela): Sequela row instance to be added to the
                SequelaHierarchyHistory table.

            children (intlist): list representing the child sequela ids to be
                associated with this new aggregate sequela.

            cause_id (Optional int): the cause_id that this new aggregate
                sequela will be associated with. Must be the same as the
                cause_ids for all of the sequela children. Default None.

            modelable_entity_id (Optional int): the modelable_entity_id that
                this new aggregate sequela will be associated with. Must be the
                same as the modelable_entity_ids for all of the sequela
                children. Default None.

            healthstate_id (Optional int): the healthstate_id that this new
                aggregate sequela will be associated with. Must be the same as
                the healthstate_ids for all of the sequela children. Default
                None.

        Returns:
            The newly created row.
        """
        row = SequelaHierarchyHistory(
            sequela_set_id=self.sequela_set_id,
            sequela_id=sequela.sequela_id,
            level=1,
            most_detailed=0,
            parent_id=0,
            path_to_top_parent='0,{sequela_id}'.format(
                sequela_id=sequela.sequela_id),
            sequela_name=sequela.sequela_name,
            modelable_entity_id=modelable_entity_id,
            cause_id=cause_id,
            healthstate_id=healthstate_id)

        self.fk_sequela_hierarchy_history.append(row)
        self.modify_hierarchy(row, children)

        return row

    def modify_hierarchy(self, parent, children, cascade=False):
        """
        Modify an existing hierarchy by reshuffling parent and child
        relationships.

        Only SequelaHierarchyHistory rows that aren't already children of the
        target parent will be added. Children to be added are removed from
        their old parent's list of children, their hierarchy attributes are
        modified and they are added to the new parent row's list of children.

        If any modified child rows have grandkids, their grandkid's hierarchy
        attributes are also updated.

        If the target parent is not the root node, any preexisting children
        that are excluded from the list of new children are removed and
        reassigned to the target parent's parent SequelaHierarchyHistory row.

        Arguments:
            parent (model.SequelaHierarchyHistory): SequelaHierarchyHistory row
                representing the aggregate sequela to associate children to.

            children (intlist): integer list representing the sequela ids to
                set as the children for the input parent.

            cascade (bool): whether to simply add the new children to the
                parent or continue to modify the hierarchy by removing already
                existing children from the parent if they are not specified in
                the input child list.
        """
        children_to_add = self.get_hierarchy_rows(children)
        for child in children_to_add:
            if child not in parent.children:
                old_parent = self.get_hierarchy_rows(child.parent_id)[0]
                old_parent.children.remove(child)
                child.modify_hierarchy_attributes(parent)
                parent.children.append(child)

                if child.children:
                    for grandchild in child.children:
                        grandchild.modify_hierarchy_attributes(child)

        if parent.sequela_id != 0 and cascade:
            ids_to_remove = [child.sequela_id for child in parent.children
                             if child not in children_to_add]

            grandparent = self.get_hierarchy_rows(parent.parent_id)[0]
            self.modify_hierarchy(grandparent, ids_to_remove, cascade=True)

    def add_sequela_rei(self, sequela, rei_id):
        """
        Add a sequela_id/rei_id mapping to the SequelaReiHistory table for this
        version.

        Arguments:
            sequela (models.Sequela): the row instance of the Sequela we wish
                to map with the input rei_id.

            rei_id (int): integer representing the risk, etiology or impairment
                the sequela_id will be mapped with.

        Returns:
            The newly inserted row instance.
        """
        new_seq_rei = SequelaReiHistory(
            sequela_id=sequela.sequela_id,
            rei_id=rei_id)
        self.fk_sequela_rei_history.append(new_seq_rei)
        return new_seq_rei

    def backfill_hierarchy(self, old_version):

        dont_backfill_cols = ['start_date', 'end_date', 'date_inserted',
                              'inserted_by', 'last_updated', 'last_updated_by',
                              'last_updated_action']
        old_rows = [row.to_wire(
            columns=dont_backfill_cols, exclude_columns=True) for row in
            old_version.fk_sequela_hierarchy_history.all()]
        old_rows = [SequelaHierarchyHistory(**row) for row in old_rows]
        self.fk_sequela_hierarchy_history.extend(old_rows)

    def backfill_rei(self, old_version):

        dont_backfill_cols = ['date_inserted', 'inserted_by', 'last_updated',
                              'last_updated_by', 'last_updated_action']

        old_rows = [row.to_wire(
            columns=dont_backfill_cols, exclude_columns=True)
            for row in old_version.fk_sequela_rei_history.all()]
        old_rows = [SequelaReiHistory(**row) for row in old_rows]
        self.fk_sequela_rei_history.extend(old_rows)

    def hierarchy_delete_aggregate(self, sequela):
        """
        Delete an aggregate SequelaHierarchyHistory row from this version.

        Retrieve the parent and the list of children from the aggregate to
        remove. Modify the hierarchy attributes of the children and reassign
        the child rows to the soon-to-be deleted aggregate's parent.

        Arguments:
            sequela (models.Sequela): instance of the SequelaHierarchyHistory
                table to remove from the hierarchy.
        """
        agg_parent = self.get_hierarchy_rows(sequela.parent_id)[0]
        children_ids = [child.sequela_id for child in sequela.children]
        self.modify_hierarchy(agg_parent, children_ids, cascade=False)
        return sequela


class SequelaSetVersionActive(Base):
    __tablename__ = 'sequela_set_version_active'

    sequela_set_id = Column(
        Integer,
        ForeignKey('sequela_set.sequela_set_id'),
        primary_key=True)
    gbd_round_id = Column(Integer, primary_key=True)
    sequela_set_version_id = Column(
        Integer,
        ForeignKey('sequela_set_version.sequela_set_version_id'))
    date_inserted = Column(DateTime, default=datetime.utcnow)
    inserted_by = Column(String(50), default='unknown')
    last_updated = Column(DateTime, default=datetime.utcnow)
    last_updated_by = Column(String(50), default='unknown')
    last_updated_action = Column(String(6), default='INSERT')

    def __repr__(self):
        return ("<SequelaSetVersionActive(sequela_set_id: {}, "
                "gbd_round_id: {}, sequela_set_version_id: {}, "
                "date_inserted: {}, inserted_by: {}, last_updated: {}, "
                "last_updated_by: {}, last_updated_action: {})>".format(
                    self.sequela_set_id,
                    self.gbd_round_id,
                    self.sequela_set_version_id,
                    self.date_inserted,
                    self.inserted_by,
                    self.last_updated,
                    self.last_updated_by,
                    self.last_updated_action))

    def delete(self):
        """Deprecate a SequelaSetVersionActive row."""
        self.last_updated_action = 'DELETE'


class SequelaHierarchyHistory(Base):
    __tablename__ = 'sequela_hierarchy_history'

    __table_args__ = (
        ForeignKeyConstraint(
            ['sequela_set_version_id', 'parent_id'],
            ['sequela_hierarchy_history.sequela_set_version_id',
             'sequela_hierarchy_history.sequela_id']),)

    sequela_set_version_id = Column(
        Integer,
        ForeignKey('sequela_set_version.sequela_set_version_id'),
        primary_key=True)
    sequela_set_id = Column(
        Integer,
        ForeignKey('sequela_set.sequela_set_id'))
    sequela_id = Column(
        Integer,
        ForeignKey('sequela.sequela_id'),
        primary_key=True)
    level = Column(Integer, default=0)
    most_detailed = Column(Integer)
    parent_id = Column(Integer)
    path_to_top_parent = Column(String(200), default=None)
    sort_order = Column(Float, default=0)
    sequela_name = Column(String(175))
    # lancet_label = Column(String(200), default=None)
    modelable_entity_id = Column(Integer, default=None)
    cause_id = Column(Integer, default=None)
    healthstate_id = Column(Integer, default=None)
    start_date = Column(DateTime, default=datetime.utcnow)
    end_date = Column(DateTime, default=None)
    date_inserted = Column(DateTime, default=datetime.utcnow)
    inserted_by = Column(String(50), default='unknown')
    last_updated = Column(DateTime, default=datetime.utcnow)
    last_updated_by = Column(String(50), default='unknown')
    last_updated_action = Column(String(6), default='INSERT')

    fk_sequela_set_version_id = relationship(
        'SequelaSetVersion',
        back_populates='fk_sequela_hierarchy_history')

    fk_sequela = relationship(
        'Sequela',
        back_populates='fk_sequela_hierarchy_history')

    parent = relationship("SequelaHierarchyHistory",
                          post_update=True,
                          backref="children",
                          remote_side=[sequela_set_version_id, sequela_id],
                          passive_deletes=True)

    def __repr__(self):
        return ("<SequelaHierarchyHistory(sequela_set_version_id: {}, "
                "sequela_set_id: {}, sequela_id: {}, level: {}, "
                "most_detailed: {}, parent_id: {}, path_to_top_parent: {}, "
                "sort_order: {}, sequela_name: {}, "
                "modelable_entity_id: {}, cause_id: {}, healthstate_id: {}, "
                "start_date: {}, end_date: {}, date_inserted: {}, "
                "inserted_by: {}, last_updated: {}, last_updated_by: {}, "
                "last_updated_action: {})>".format(
                    self.sequela_set_version_id,
                    self.sequela_set_id,
                    self.sequela_id,
                    self.level,
                    self.most_detailed,
                    self.parent_id,
                    self.path_to_top_parent,
                    self.sort_order,
                    self.sequela_name,
                    # self.lancet_label,
                    self.modelable_entity_id,
                    self.cause_id,
                    self.healthstate_id,
                    self.start_date,
                    self.end_date,
                    self.date_inserted,
                    self.inserted_by,
                    self.last_updated,
                    self.last_updated_by,
                    self.last_updated_action))

    def modify_hierarchy_attributes(self, parent):
        """
        Modify this SequelaHierarchyHistory's attributes to reflect its new
        location in the hierarchy.

        Arguments:
            parent (models.SequelaHierarchyHistory): instance of the parent
                sequela row to use to update the parent_id, level, and
                path_to_top_parent attributes.

        Returns:
            Itself.
        """
        self.parent_id = parent.sequela_id
        self.path_to_top_parent = '{parent_path},{sequela}'.format(
            parent_path=parent.path_to_top_parent, sequela=self.sequela_id)
        self.level = parent.level + 1
        return self

    def delete(self):
        raise NotImplementedError(
            "Model level deletes are not allowed on SequelaHierarchyHistory "
            "rows -- please use the RowConstructor or RequestHandler objects.")


class SequelaReiHistory(Base):
    __tablename__ = 'sequela_rei_history'

    sequela_set_version_id = Column(
        Integer, ForeignKey('sequela_set_version.sequela_set_version_id'),
        primary_key=True)
    sequela_id = Column(
        Integer, ForeignKey('sequela.sequela_id'),
        primary_key=True)
    rei_id = Column(Integer, primary_key=True)
    date_inserted = Column(DateTime, default=datetime.utcnow)
    inserted_by = Column(String(50), default='unknown')
    last_updated = Column(DateTime, default=datetime.utcnow)
    last_updated_by = Column(String(50), default='unknown')
    last_updated_action = Column(String(6), default='INSERT')

    fk_sequela_set_version_id = relationship(
        'SequelaSetVersion',
        back_populates='fk_sequela_rei_history')

    def __repr__(self):
        return ("<SequelaReiHistory(sequela_set_version_id: {}, "
                "sequela_id: {}, rei_id: {}, date_inserted: {}, "
                "inserted_by: {}, last_updated: {}, last_updated_by: {}, "
                "last_updated_action: {})>".format(
                    self.sequela_set_version_id,
                    self.sequela_id,
                    self.rei_id,
                    self.date_inserted,
                    self.inserted_by,
                    self.last_updated,
                    self.last_updated_by,
                    self.last_updated_action))

    def delete(self):
        raise NotImplementedError(
            "Model level deletes are not allowed on SequelaReiHistory rows "
            "-- please use the RowConstructor or RequestHandler objects.")


class Healthstate(Base):
    __tablename__ = 'healthstate'

    healthstate_id = Column(Integer, primary_key=True)
    healthstate = Column(String(100), default=None)
    healthstate_name = Column(String(255), default=None)
    healthstate_description = Column(String(512), default=None)
    healthstate_type_id = Column(
        Integer,
        ForeignKey('healthstate_type.healthstate_type_id'))
    date_inserted = Column(DateTime, default=datetime.utcnow)
    inserted_by = Column(String(50), default='unknown')
    last_updated = Column(DateTime, default=datetime.utcnow)
    last_updated_by = Column(String(50), default='unknown')
    last_updated_action = Column(String(6), default='INSERT')

    def __repr__(self):
        return ("<Healthstate(healthstate_id: {}, healthstate: {}, "
                "healthstate_name: {}, healthstate_description: {}, "
                "healthstate_type_id: {}, date_inserted: {}, "
                "inserted_by: {}, last_updated: {}, last_updated_by: {}, "
                "last_updated_action: {})>".format(
                    self.healthstate_id,
                    self.healthstate,
                    self.healthstate_name,
                    self.healthstate_description,
                    self.healthstate_type_id,
                    self.date_inserted,
                    self.inserted_by,
                    self.last_updated,
                    self.last_updated_by,
                    self.last_updated_action))


class HealthstateType(Base):
    __tablename__ = 'healthstate_type'

    UNKNOWN = 0
    CUSTOM = 1
    NONE = 2
    STANDARD = 3

    healthstate_type_id = Column(Integer, primary_key=True)
    healthstate_type = Column(String(64))
    healthstate_type_description = Column(String(500), default=None)
    date_inserted = Column(DateTime, default=datetime.utcnow)
    inserted_by = Column(String(50), default='unknown')
    last_updated = Column(DateTime, default=datetime.utcnow)
    last_updated_by = Column(String(50), default='unknown')
    last_updated_action = Column(String(6), default='INSERT')

    def __repr__(self):
        return ("<HealthstateType(healthstate_type_id: {}, "
                "healthstate_type: {}, healthstate_type_description: {}, "
                "date_inserted: {}, inserted_by: {}, last_updated: {}, "
                "last_updated_by: {}, last_updated_action: {})>".format(
                    self.healthstate_type_id,
                    self.healthstate_type,
                    self.healthstate_type_description,
                    self.date_inserted,
                    self.inserted_by,
                    self.last_updated,
                    self.last_updated_by,
                    self.last_updated_action))
