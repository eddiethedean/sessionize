from sessionize.utils.custom_types import Record, SqlConnection
from sessionize.orm.session_table import SessionTable

from sessionize.utils.select import select_value_by_primary_keys
from sessionize.utils.update import update_records_session

            
class ValueSelection:
    # returned when a single value in a column is selecected
    def __init__(self, session_table: SessionTable, column_name: str, primary_key_values: Record):
        self.session_table = session_table
        self.session = session_table.session
        self.sa_table = session_table.sa_table
        self.column_name = column_name
        self.primary_key_values = primary_key_values

    @property
    def value(self):
        return select_value_by_primary_keys(self.sa_table, self.session, self.column_name, self.primary_key_values)

    def __repr__(self):
        return f'SessionValue({self.value})'

    def __eq__(self, other):
        return self.value == other

    def __gt__(self, other):
        return self.value > other

    def __lt__(self, other):
        return self.value < other

    def __sub__(self, value):
        # update value subtracting value
        record = self.primary_key_values.copy()
        record[self.column_name] = value - self.value
        update_records_session(self.sa_table, [record], self.session)

    def __add__(self, value):
        # update value adding value
        record = self.primary_key_values.copy()
        record[self.column_name] = value + self.value
        update_records_session(self.sa_table, [record], self.session)

    def update(self, value):
        # update the value in the table.
        record = self.primary_key_values.copy()
        record[self.column_name] = value
        update_records_session(self.sa_table, [record], self.session)