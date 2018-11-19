from gbd.constants import GBD_ROUND_ID

from epic_db import models
from epic_db.errors import RowNotFoundError


class RowConstructor(object):

    def __init__(self, session):
        """
        Abstract base class of the row constructor interface.

        A wrapper for managing all the primary keys, dependencies, and valid
        columns needed to insert, modify, or delete a row from the database.

        Arguments:
            session (sqlalchemy.orm.session): a session connecting to the
                database where updates will be applied.

        Attributes:
            model (sqlalchemy.Base.subclass): the sqlalchemy Base subclass
               representing the database table to be modified.

            primary_keys (str list): represents the primary keys of the
                database table.
        """
        self.session = session
        self.model = return_model_from_tablename(self.tablename)
        self.primary_keys = [
            col.name for col in self.model.__mapper__.primary_key]

    @classmethod
    def get_constructor_by_tablename(cls, tablename):
        """
        Returns a RowConstructor subclass.

        Arguments:
            tablename (str): identifies the RowConstructor subclass to return.

        Raises:
            ValueError: thrown if no sublcass exists for the provided input
                tablename.

        Returns:
            A sublcass of the RowConstructor abstract base class that acts as
                an interface for inserting, modifying, or deleting table rows.
        """
        for Constructor in cls.__subclasses__():
            if Constructor.tablename == tablename:
                return Constructor
        raise ValueError("No Constructor named {}".format(tablename))

    def _validate_insert_columns(self, column_map):
        """Checks input dictionary for any required insert columns that may be
        missing.

        Arguments:
            column_map (dict): represents the column - value mapping used for
                this transaction.

        Raises:
            KeyError: thrown if any of the columns required for inserting into
                the database are missing.
        """
        missing_cols = [col for col in self.req_insert_cols
                        if col not in column_map]
        if missing_cols:
            raise KeyError(
                "Columns {missing} missing from passed dictionary and "
                "required for insert into table {table}".format(
                    missing=missing_cols, table=self.tablename))

    def _valid_modify_columns(self, column_map):
        """Subset the column_map to include only viable insert columns.

        Arguments:
            column_map (dict): represents the column - value mapping used for
                this transaction.
        """
        to_modify = {col: value for (col, value) in column_map.items()
                     if col in self.insert_cols}
        return to_modify

    def _get_dependencies(self, column_map, **dependencies):
        """
        Queries the database for row instances of dependencies--assigns them to
        instance variables.

        Arguments:
            column_map (dict): represents the column - value mapping used for
                this transaction.

            dependencies (dict): kwarg dependencies.
        """
        for dep in dependencies:
            this_dep = dependencies.get(dep)
            if not this_dep:
                model = return_model_from_tablename(dep)
                primary_keys = [
                    col.name for col in model.__mapper__.primary_key]
                key = tuple(column_map.get(col) for col in primary_keys)
                this_dep = self.session.query(model).get(key)
            setattr(self, dep, this_dep)

    def get_column_names(self):
        """Returns a list of column names for the database table."""
        return list(self.model.__table__.columns.keys())

    def get_row(self, primary_keys):
        """
        Retrieves an existing row instance from the database.

        Arguments:
            primary_keys (dict): a mapping of all primary key names to values.

        Raises:
            KeyError: if the primary_key dictionary is missing any key value
                pairs, a row instance cannot be returned.

            RowNotFoundError: if the primary_key dictionary contains all
                necessary keys but a matching row is not found in the database,
                a row instance cannot be returned. This can occur when we are
                adding a row to a database table with a composite primary key.

        Returns:
            A row instance from the database.
        """
        missing_pk = [col for col in self.primary_keys
                      if col not in primary_keys]
        if missing_pk:
            raise KeyError(
                "Primary key {missing} missing from passed primary keys and "
                "requried for lookup of row from table {table}".format(
                    missing=missing_pk, table=self.tabelname))
        pk_ids = [primary_keys[key] for key in self.primary_keys]
        row = self.session.query(self.model).get(pk_ids)
        if row is None:
            raise RowNotFoundError(
                "Row doesn't exist for composite primary keys: {key_value} "
                "Must insert row instead".format(
                    key_value=primary_keys.items()))
        return row

    def insert_row(self, column_map):
        """
        Inserts a new row into the database.

        Arguments:
            column_map (dict): represents the column - value mapping used for
                this transaction.

        Returns:
            The newly created row instance.
        """
        self._validate_insert_columns(column_map)

        valid_columns = self.get_column_names()
        column_map = {key: column_map[key] for key in column_map
                      if key in valid_columns}
        row = self.model(**column_map)
        self.session.add(row)
        return row

    def modify_row(self, instance, column_map):
        """
        Modify an existing row in the database.

        Arguments:
            instance (sqlalchemy.Base): the row instance to modify.

            column_map (dict): represents the column - value mapping used for
                this transaction.

        Returns:
            The newly modified row instance.
        """
        modify_map = self._valid_modify_columns(column_map)
        for col in modify_map:
            current_val = getattr(instance, col)
            if modify_map[col] is not None and modify_map[col] != current_val:
                setattr(instance, col, modify_map[col])
        return instance

    def delete_row(self, instance, column_map=None):
        """Delete a row from the database."""
        # api level delete sends method to row instance to allow model to deal
        # with specific delete actions
        instance.delete()


class SequelaRow(RowConstructor):

    tablename = 'sequela'
    req_insert_cols = ['sequela_name']
    insert_cols = req_insert_cols
    dependency_map = None


class SequelaSetRow(RowConstructor):

    tablename = 'sequela_set'
    req_insert_cols = None
    insert_cols = ['sequela_set_name']
    dependency_map = None


class SequelaSetVersionRow(RowConstructor):

    tablename = 'sequela_set_version'
    req_insert_cols = ['sequela_set_version']
    insert_cols = req_insert_cols + ['sequela_set_version_description',
                                     'sequela_set_version_justification']
    dependency_map = {'sequela_set': ['sequela_set_id']}

    sequela_set = None

    def insert_row(self, column_map, sequela_set=None):
        """
        Inserts a new SequelaSetVersion row.

        Arguments:
            column_map (dict): represents the column - value mapping used for
                this transaction.

            sequela_set (models.SequelaSet): an instance of a SequelaSet row
                object.

        Raises:
            ValueError: thrown if no SequelaSet row instance is provided.

        Returns:
            The newly inserted row instance.
        """
        self._validate_insert_columns(column_map)

        self._get_dependencies(column_map, sequela_set=sequela_set)

        insert_cols = {col: column_map.get(col) for col
                       in self.req_insert_cols}
        gbd_round_id = column_map.get('gbd_round_id', GBD_ROUND_ID)
        try:
            instance = self.sequela_set.add_version(
                gbd_round_id=gbd_round_id, **insert_cols)
            return instance

        except AttributeError:
            raise ValueError(
                "Argument 'sequela_set' must be a row instance of SequelaSet")


class SequelaHierarchyHistoryRow(RowConstructor):

    tablename = 'sequela_hierarchy_history'
    req_insert_cols = ['cause_id']
    insert_cols = req_insert_cols + ['healthstate_id', 'modelable_entity_id',
                                     'children', 'sequela_name']
    dependency_map = {'sequela': ['sequela_id', 'sequela_name'],
                      'sequela_set_version': ['sequela_set_version_id']}

    sequela = None
    sequela_set_version = None

    def insert_row(self, column_map, sequela_set_version=None, sequela=None):
        """
        Inserts a new row into the SequelaHierarchyHistory table.

        Arguments:
            column_map (dict): represents the column - value mapping used for
                this transaction.

            sequela_set_version (models.SequelaSetVersion): instance of a
                SequelaSetVersion row.

            sequela (models.Sequela): instance of a Sequela row object.

        Returns:
            The newly inserted row.
        """
        self._validate_insert_columns(column_map)
        self._get_dependencies(column_map,
                               sequela=sequela,
                               sequela_set_version=sequela_set_version)

        children = column_map.get('children')
        insert_cols = {col: column_map.get(col) for col in self.insert_cols
                       if column_map.get(col) if col not in ['children']}
        if children:
            return self.sequela_set_version.hierarchy_add_aggregate(
                self.sequela, children, **insert_cols)
        else:
            return self.sequela_set_version.hierarchy_add_most_detailed(
                self.sequela, **insert_cols)

    def modify_row(self, instance, column_map, sequela_set_version=None):
        """
        Modify an existing row.

        Arguments:
            instance (models.SequelaHierarchyHistory): the row instance to
                modify in the table.

            column_map (dict): represents the column - value mapping used for
                this transaction.

            sequela_set_version (models.SequelaSetVersion): instance of a
                SequelaSetVersion row.
        """
        children = column_map.pop('children', None)

        if children:
            self._get_dependencies(column_map,
                                   sequela_set_version=sequela_set_version)
            self.sequela_set_version.modify_hierarchy(
                instance, children, cascade=True)

        mod_instance = super(SequelaHierarchyHistoryRow, self).modify_row(
            instance, column_map)

        return mod_instance

    def delete_row(self, instance, column_map=None, sequela_set_version=None):
        """
        Delete a row from the SequelaHierarchyHistory table.

        Arguments:
            instance (models.SequelaHierarchyHistoryRow): the row instance to
                delete from the table.

            sequela_set_version (models.SequelaSetVersion): instance of a
                SequelaSetVersion row.
        """
        self._get_dependencies(column_map,
                               sequela_set_version=sequela_set_version)

        if instance.most_detailed == 0:
            instance = self.sequela_set_version.hierarchy_delete_aggregate(
                instance)

        self.session.delete(instance)


class SequelaReiHistoryRow(RowConstructor):

    tablename = 'sequela_rei_history'
    req_insert_cols = ['rei_id']
    insert_cols = req_insert_cols
    dependency_map = {'sequela': 'sequela_id',
                      'sequela_set_version': 'sequela_set_version_id'}

    sequela = None
    sequela_set_version = None

    def insert_row(self, column_map, sequela_set_version=None,
                   sequela=None):
        """Insert a row into the SequelaReiHistory table.

        Arguments:
            column_map (dict): represents the column - value mapping used for
                this transaction.

            sequela_set_version (models.SequelaSetVersion): instance of a
                SequelaSetversion row object.

            sequela (models.Sequela): instance of a Sequela row object

        Returns:
            The newly inserted SequelaReiHistory row object.
        """
        self._get_dependencies(column_map,
                               sequela_set_version=sequela_set_version,
                               sequela=sequela)
        rei_id = column_map.get('rei_id')
        return self.sequela_set_version.add_sequela_rei(self.sequela, rei_id)

    def delete_row(self, instance, column_map=None):
        """Remove a row from the SequelaReiHistory table.

        Arguments:
            instance (models.SequelaReiHistory): row instance to be deleted.
        """
        self.session.delete(instance)


def return_model_from_tablename(tablename):
    """Return a subclass constructor for the sqlalchemy Base model classes."""
    for c in models.Base._decl_class_registry.values():
        if hasattr(c, '__tablename__') and c.__tablename__ == tablename:
            return c
    raise ValueError("No Model with tablename {}".format(tablename))
