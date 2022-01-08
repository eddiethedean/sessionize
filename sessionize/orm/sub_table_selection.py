from sessionize.orm.value_selection import ValueSelection
from sessionize.utils.custom_types import Record
from sessionize.orm.session_table import SessionTable
from sessionize.orm.record_selection import RecordSelection
from sessionize.orm.sub_column_selection import SubColumnSelection
from sessionize.utils.select import select_records_by_primary_keys
from sessionize.utils.update import update_records_session
from sessionize.utils.insert import insert_records_session
from sessionize.utils.delete import delete_records_by_values_session


class SubTableSelection:
    # returned when a subset of records are selecected
    def __init__(self, session_table: SessionTable, primary_key_values: list[Record]):
        self.session_table = session_table
        self.primary_key_values = primary_key_values
        self.name = session_table.name
        self.session = session_table.session

    def records(self):
        records = select_records_by_primary_keys(self.sa_table,
                                                 self.session,
                                                 self.primary_key_values)
        if len(records) == 0:
            raise KeyError('Primary key values do not exist in table.')
        else:
            return records

    def __getitem__(self, key):
        if isinstance(key, int):
            # SubTableSelection[index]
            return RecordSelection(self.primary_key_values[key], self.session_table)
        if isinstance(key, slice):
            # SubTableSelection[slice]
            _slice = key
            return SubTableSelection(self.session_table, self.primary_key_values[_slice])
        if isinstance(key, str):
            # SubTableSelection[column_name]
            column_name = key
            return SubColumnSelection(column_name, self.session_table, self.primary_key_values)
        if isinstance(key, tuple):
            # SubTableSelection[column_name, <index or slice>]
            column_name = key[0]
            if isinstance(key[1], int):
                # SubTableSelection[column_name, index]
                index = key[1]
                return ValueSelection(self.session_table, self.primary_key_values[index])
            if isinstance(key[1], slice):
                # SubColumnSelection[column_name, slice]
                _slice = key[1]
                return SubColumnSelection(self.name, self.session_table, self.primary_key_values[_slice])

    def __setitem__(self, key, value):
        if isinstance(key, int):
            # SubTableSelection[index] = value
            index = key
            record_selection = self[index]
            record_selection.update(value)
        if isinstance(key, slice):
            # SubTableSelection[slice]
            _slice = key
            sub_table_selection = self[_slice]
            sub_table_selection.update(value)
        if isinstance(key, str):
            # SubTableSelection[column_name]
            column_name = key
            sub_column_selection = self[column_name]
            sub_column_selection.update(value)
        if isinstance(key, tuple):
            # SubTableSelection[column_name, <index or slice>]
            column_name = key[0]
            if isinstance(key[1], int):
                # SubTableSelection[column_name, index]
                index = key[1]
                value_selection = self[column_name, index]
                value_selection.update(value)
            if isinstance(key[1], slice):
                # SubColumnSelection[column_name, slice]
                _slice = key[1]
                sub_column_selection = self[column_name, _slice]
                sub_column_selection.update(value)

    def __delitem__(self, key):
        if isinstance(key, int):
            # SubTableSelection[index] = value
            index = key
            record_selection = self[index]
            record_selection.delete()
        if isinstance(key, slice):
            # SubTableSelection[slice]
            _slice = key
            sub_table_selection = self[_slice]
            sub_table_selection.delete()
        if isinstance(key, str):
            # SubTableSelection[column_name]
            sub_column_selection = self[key]
            sub_column_selection.delete()
        if isinstance(key, tuple):
            # SubTableSelection[column_name, <index or slice>]
            column_name = key[0]
            if isinstance(key[1], int):
                # SubTableSelection[column_name, index]
                index = key[1]
                value_selection = self[column_name, index]
                value_selection.delete()
            if isinstance(key[1], slice):
                # SubColumnSelection[column_name, slice]
                _slice = key[1]
                sub_column_selection = self[column_name, _slice]
                sub_column_selection.delete()

    def update(self, records: list[Record]) -> None:
        # TODO: check if records match primary key values
        update_records_session(self.sa_table, records, self.session)

    def insert(self, records: list[Record]) -> None:
        # TODO: check if records don't match any primary key values
        insert_records_session(self.sa_table, records, self.session)

    def delete(self) -> None:
        # delete all records in sub table
        delete_records_by_values_session(self.sa_table, self.primary_key_values, self.session)
        