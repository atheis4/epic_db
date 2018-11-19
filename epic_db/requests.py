import json
import numpy as np

from epic_db.constructors import RowConstructor
from epic_db.errors import RowNotFoundError


class RequestHandler(object):

    def __init__(self, session):
        """
        The RequestHandler is a class used to alter to the state of a database
        table.

        The RequestHandler connects to the database with a session object. The
        process_request method is the only public method used to submit the
        transaction and alter the state of the database.

        Arguments:
            session (sqlalchemy.orm.Session)
        """
        self.session = session

    def _unpack_request(self, request):
        """
        Unpack the request datatype.

        If the request is a python dictionary, it is returned. If the request
        is any other datatype, it is assumed to be a JSON compatible string and
        basic validation is outsourced to the json.loads() method.

        Arguments:
            request(dict or str): A dict or JSON compatible string specifying
                the database transaction to be processed.

        Returns:
            A dictionary containing the tablenames, dependencies, and primary
                keys needed to complete the transaction.
        """
        if isinstance(request, dict):
            return request
        else:
            return json.loads(request)

    def process_request(self, request):
        """
        Process the complete request.

        Creates the list of tables in the transaction and sequentially sends
        each table and the relevant nested data in the dictionary to the
        _process_table() method.

        The request will continue to be processed as long as a tablename exists
        in the 'tables' instance variable.

        Arguments:
            request (dict): A dictionary representing each of the database
                transactions to be completed in this request. The request
                dictionary's outermost keys represent the individual tables
                that will be processed over.
        """
        request = self._unpack_request(request)
        self.tables = list(request.keys())

        while self.tables:
            self._process_table(self.tables[0], request.get(self.tables[0]))

    def _process_table(self, tablename, table_dict, row_dict=None):
        """
        Submits each row request in the table_dict to the _process_row()
        method.

        The most recent row request and RowConstructor is returned and the
        row_dict is updated. Additional dependency tables that exist in the
        current table_dict are identified, added to the 'tables' instance
        variable, and recursively submitted to this same method.

        If no downstream tables are identified, the table's request is finished
        and the tablename is removed from the 'tables' instance variable.

        Arguments:
            tablename (str): A string specifying the name of the database table
                that will have a row inserted, modified, or deleted.

            table_dict (dict): A dictionary containing the necessary metadata
                to complete the transaction: primary keys, dependencies, etc.

            row_dict (dict): A dictionary containing the row instances that
                have already been processed in this request.
        """
        if row_dict is None:
            row_dict = {}

        table_iter = np.atleast_1d(table_dict)
        for entry in table_iter:
            row, constructor = self._process_row(tablename, entry, row_dict)
            row_dict.update({tablename: row})
            # create a list of keys that aren't db tables or have been run
            invalid_downstream = (constructor.insert_cols +
                                  row.__mapper__.columns.keys() +
                                  ['is_delete'])
            # identify downstream dependencies that haven't been processed
            downstream_tables = [col for col in entry.keys() if col
                                 not in invalid_downstream]
            for table in downstream_tables:
                self.tables.append(table)
                self._process_table(table, entry.get(table), row_dict=row_dict)

        self.tables.remove(tablename)

    def _process_row(self, tablename, table_dict, row_dict):
        """
        Process an individual row transaction.

        A RowConstructor object is retrieved for the specific tablename being
        processed. If all the primary keys exist in the table_dict, we are
        modifying or deleting an existing row, or we are creating a row for a
        table with a composite primary key.

        Arguments:
            tablename (str): A string specifying the name of the database table
                that will have a row added, modified, or deleted.

            table_dict (dict): A dictionary containing the necessary metadata
                to complete the transaction: primary keys, dependencies, etc.

            row_dict (dict): A dictionary containing the row instances that
                have already been processed in this request.

        Returns:
            The row instance after flushing and the RowConstructor subclass
                for the database table that was processed.
        """
        row_constructor = RowConstructor.get_constructor_by_tablename(
            tablename)(self.session)
        primary_keys = row_constructor.primary_keys
        table_dependencies = row_constructor.dependency_map

        if not table_dependencies:
            table_dependencies = {}
        dependencies = {key: row_dict[key] for key in row_dict
                        if key in table_dependencies.keys()}

        if all(table_dict.get(key) is not None for key in primary_keys):
            # directly referencing existing row with all primary keys
            primary_key_dict = {key: table_dict[key] for key in primary_keys}
            try:
                row = row_constructor.get_row(primary_key_dict)
                row_dict.update({tablename: row})

                if table_dict.get('is_delete'):
                    row_constructor.delete_row(row, table_dict, **dependencies)
                else:
                    row_constructor.modify_row(row, table_dict, **dependencies)
            except RowNotFoundError:
                row = row_constructor.insert_row(table_dict, **dependencies)
        else:
            # new row
            row = row_constructor.insert_row(table_dict, **dependencies)

        self.session.flush()
        return row, row_constructor
