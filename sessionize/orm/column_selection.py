from sessionize.utils.custom_types import Record, SqlConnection
from sessionize.orm.session_table import SessionTable
from sessionize.orm.value_selection import ValueSelection
from sessionize.orm.sub_column_selection import SubColumnSelection

from sessionize.utils.select import select_column_values_all


class ColumnSelection:
    # returned when a column is selected
    def __init__(self, session_table: SessionTable, column_name: str):
        self.column_name = column_name
        self.table_name = session_table.name
        self.session_table = session_table
        self.session = session_table.session
        self.sa_table = session_table.sa_table

    def records(self):
        return select_column_values_all(self.sa_table, self.session, self.column_name)

    def __repr__(self):
        return f'SessionColumn({self.records})'

    def __getitem__(self, key):
        if isinstance(key, int):
            # ColumnSelection[int]
            index = key
            return ValueSelection(self.session_table, self.column_name, self.primary_key_values[index])
        
        if isinstance(key, slice):
            # ColumnSelection[slice]
            _slice = key
            return SubColumnSelection(self.session_table, self.column_name, self.primary_key_values[_slice])

    def __setitem__(self, key, value):
        if isinstance(key, int):
            # ColumnSelection[int] = value
            index = key
            value_selection = self[index]
            value_selection.update(value)
        
        if isinstance(key, slice):
            # ColumnSelection[slice] = value
            _slice = key
            sub_column_selection = self[_slice]
            sub_column_selection.update(value)