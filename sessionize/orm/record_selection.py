from sessionize.utils.custom_types import Record, SqlConnection
from sessionize.orm.session_table import SessionTable
from sessionize.utils.select import select_record_by_primary_key
from sessionize.utils.update import update_records_session
from sessionize.utils.delete import delete_record_by_values_session


class RecordSelection:
    # returned when a single record is selected with SessionTable
    def __init__(self, session_table: SessionTable, primary_key_values: Record):
        self.primary_key_values = primary_key_values
        self.session_table = session_table
        self.table_name = session_table.name
        self.session = session_table.session
        self.sa_table = session_table.sa_table

    @property
    def record(self):
        records = select_record_by_primary_key(self.sa_table, self.session, self.primary_key_values)
        if len(records) == 0:
            raise KeyError('Primary key values do not exist in table.')
        else:
            return records[0]

    def __repr__(self):
        return f'SessionRecord({self.record})'

    def __eq__(self, other):
        return self.record == other

    def update(self, record: Record) -> None:
        # update record with new values
        update_records_session(self.sa_table, [record], self.session)

    def delete(self) -> None:
        # delete the record
        delete_record_by_values_session(self.sa_table, self.primary_key_values, self.session)

