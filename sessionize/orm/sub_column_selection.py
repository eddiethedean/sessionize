from sessionize.utils.custom_types import Record, SqlConnection
from sessionize.orm.session_table import SessionTable
from sessionize.orm.record_selection import RecordSelection
from sessionize.utils.select import select_record_by_primary_key, select_records_by_primary_keys
from sessionize.utils.select import select_column_values_by_primary_keys, select_value_by_primary_keys


class SubColumnSelection:
    # returned when a subset of a column is selecected
    def __init__(self, session_table: SessionTable, column_name: str, primary_key_values: list[Record]):
        self.column_name = column_name
        self.table_name = session_table.name
        self.session_table = session_table
        self.session = session_table.session
        self.primary_key_values = primary_key_values

    def records(self):
        return select_column_values_by_primary_keys(self.sa_table, self.session, self.primary_key_values)

    def __repr__(self):
        return f'SessionSubColumn({self.records})'

    def update(self, value):
        if isinstance(value, list):
            pass
        else:
            pass