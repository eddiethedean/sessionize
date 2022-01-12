from collections.abc import Iterable
from numbers import Number

from sessionize.utils.custom_types import Record

from sessionize.utils.select import select_primary_key_record_by_index
from sessionize.utils.select import select_primary_key_records_by_slice
from sessionize.utils.select import select_records_by_primary_keys
from sessionize.utils.select import select_record_by_primary_key
from sessionize.utils.select import select_records_by_primary_keys
from sessionize.utils.select import select_column_values_all
from sessionize.utils.select import select_column_values_by_primary_keys
from sessionize.utils.select import select_primary_key_values
from sessionize.utils.select import select_value_by_primary_keys
from sessionize.utils.select import select_records

from sessionize.utils.update import update_records_session

from sessionize.utils.insert import insert_records_session

from sessionize.utils.delete import delete_records_by_values_session
from sessionize.utils.delete import delete_record_by_values_session


class Selection:
    def __init__(self, session_table):
        self.session_table = session_table
        self.session = session_table.session
        self.table_name = session_table.name
        self.sa_table = session_table.sa_table


class TableSelection(Selection):
    def __init__(self, session_table):
        Selection.__init__(self, session_table)

    def __repr__(self):
        return f"TableSelection(name='{self.table_name}', records={self.records})"

    def __getitem__(self, key):
        if isinstance(key, int):
            # TableSelection[index] -> RecordSelection
            primary_key_values = self.get_primary_key_values()
            return RecordSelection(self.session_table, primary_key_values[key])
        if isinstance(key, slice):
            # TableSelection[slice] -> SubTableSelection
            _slice = key
            primary_key_values = self.get_primary_key_values()
            if key.start is None and key.stop is None:
                # TableSelection[:] -> TableSelection
                return TableSelection(self.session_table)
            else:
                return SubTableSelection(self.session_table, primary_key_values[_slice])
        if isinstance(key, str):
            # TableSelection[column_name] -> ColumnSelection
            column_name = key
            return ColumnSelection(self.session_table, column_name)
        if isinstance(key, tuple):
            # TableSelection[column_name, <index, slice, or filter>] -> ValueSelection or SubColumnSelection
            if isinstance(key[0], str):
                column_name = key[0]
                primary_key_values = self.get_primary_key_values()
                if isinstance(key[1], int):
                    # TableSelection[column_name, index] -> ValueSelection
                    index = key[1]
                    return ValueSelection(self.session_table, column_name, primary_key_values[index])
                if isinstance(key[1], slice):
                    # TableSelection[column_name, slice] -> SubColumnSelection
                    _slice = key[1]
                    return SubColumnSelection(self.session_table, column_name, primary_key_values[_slice])
                if isinstance(key[1], Iterable) and all(isinstance(item, bool) for item in key[1]):
                    # TableSelection[column_name, filter] -> SubColumnSelection
                    filter = key[1]
                    primary_keys = self.get_primary_keys_by_filter(filter)
                    return SubColumnSelection(self.session_table, column_name, primary_keys)
            if isinstance(key[0], Iterable) and all(isinstance(item, str) for item in key):
                # TableSelection[column_names, <index, slice, or filter>] -> SubRecordSelection or SubTableSubColumnSelection
                column_names = key[0]
                primary_key_values = self.get_primary_key_values()
                if isinstance(key[1], int):
                    # TableSelection[column_names, index] -> SubRecordSelection
                    index = key[1]
                    return SubRecordSelection(self.session_table, primary_key_values[index], column_names)
                if isinstance(key[1], slice):
                    # TableSelection[column_names, slice] -> SubTableSubColumnSelection
                    _slice = key[1]
                    return SubColumnSelection(self.session_table, column_name, primary_key_values[_slice])
                if isinstance(key[1], Iterable) and all(isinstance(item, bool) for item in key[1]):
                    # TableSelection[column_names, filter] -> SubTableSubColumnSelection
                    filter = key[1]
                    primary_keys = self.get_primary_keys_by_filter(filter)
                    return SubColumnSelection(self.session_table, column_name, primary_keys)

        if isinstance(key, Iterable) and all(isinstance(item, bool) for item in key):
            # TableSelection[Iterable[bool]]
            filter = key
            primary_keys = self.get_primary_keys_by_filter(filter)
            return SubTableSelection(self.session_table, primary_keys)
        if isinstance(key, Iterable) and all(isinstance(item, str) for item in key):
            # TableSelection[Iterable[str]]
            column_names = key
            return TableSubColumnSelection(self.session_table, column_names)

    def __setitem__(self, key, value):
        if isinstance(key, int):
            # TableSelection[index] = value
            index = key
            record_selection = self[index]
            record_selection.update(value)
        if isinstance(key, slice):
            # TableSelection[slice] = value
            _slice = key
            sub_table_selection = self[_slice]
            sub_table_selection.update(value)
        if isinstance(key, str):
            # TableSelection[column_name] = value
            column_name = key
            sub_column_selection = self[column_name]
            sub_column_selection.update(value)
        if isinstance(key, tuple):
            # TableSelection[column_name, <index or slice>] = value
            column_name = key[0]
            if isinstance(key[1], int):
                # TableSelection[column_name, index] = value
                index = key[1]
                value_selection = self[column_name, index]
                value_selection.update(value)
            if isinstance(key[1], slice):
                # TableSelection[column_name, slice] = value
                _slice = key[1]
                sub_column_selection = self[column_name, _slice]
                sub_column_selection.update(value)
            if isinstance(key[1], Iterable) and all(isinstance(item, bool) for item in key[1]):
                # TableSelection[column_name, Iterable[bool]] = value
                filter = key[1]
                sub_column_selection = self[column_name, filter]
                sub_column_selection.update(value)
        if isinstance(key, Iterable) and all(isinstance(item, bool) for item in key):
            # TableSelection[Iterable[bool]] = value
            filter = key
            sub_table_selection = self[filter]
            sub_table_selection.update(value)

    def __delitem__(self, key):
        if isinstance(key, int):
            # del TableSelection[index]
            index = key
            record_selection = self[index]
            record_selection.delete()
        if isinstance(key, slice):
            # del TableSelection[slice]
            _slice = key
            sub_table_selection = self[_slice]
            sub_table_selection.delete()
        if isinstance(key, str):
            # del TableSelection[column_name]
            sub_column_selection = self[key]
            sub_column_selection.delete()
        if isinstance(key, tuple):
            # del TableSelection[column_name, <index or slice>]
            column_name = key[0]
            if isinstance(key[1], int):
                # del TableSelection[column_name, index]
                index = key[1]
                value_selection = self[column_name, index]
                value_selection.delete()
            if isinstance(key[1], slice):
                # del TableSelection[column_name, slice]
                _slice = key[1]
                sub_column_selection = self[column_name, _slice]
                sub_column_selection.delete()
            if isinstance(key[1], Iterable) and all(isinstance(item, bool) for item in key[1]):
                # del TableSelection[column_name, Iterable[bool]]
                filter = key[1]
                sub_column_selection = self[column_name, filter]
                sub_column_selection.delete()
        if isinstance(key, Iterable) and all(isinstance(item, bool) for item in key):
            # del TableSelection[Iterable[bool]]
            filter = key
            sub_table_selection = self[filter]
            sub_table_selection.delete()

    @property
    def records(self):
        return select_records(self.sa_table, self.session)

    def get_primary_keys_by_index(self, index: int):
        return select_primary_key_record_by_index(self.sa_table, self.session, index)

    def get_primary_keys_by_slice(self, _slice: slice):
        return select_primary_key_records_by_slice(self.sa_table, self.session, _slice)

    # TODO: select_primary_key_values_by_filter function
    def get_primary_keys_by_filter(self, filter: Iterable[bool]):
        primary_key_values = self.get_primary_key_values()
        return [record for record, b in zip(primary_key_values, filter) if b]

    def get_primary_key_values(self):
        return select_primary_key_values(self.sa_table, self.session)

    def update(self, records: list[Record]) -> None:
        # TODO: check if records match primary key values
        update_records_session(self.sa_table, records, self.session)

    def insert(self, records: list[Record]) -> None:
        # TODO: check if records don't match any primary key values
        insert_records_session(self.sa_table, records, self.session)

    def delete(self) -> None:
        # delete all records in sub table
        delete_records_by_values_session(self.sa_table, self.primary_key_values, self.session)


class TableSubColumnSelection(TableSelection):
    # returned when all records are selected but a subset of columns are selected
    def __init__(self, session_table, column_names: Iterable[str]):
        TableSelection.__init__(self, session_table)
        self.column_names = column_names

    def __repr__(self):
        return f"TableSubColumnSelection(name='{self.table_name}', records={self.records})"

    @property
    def records(self) -> list[Record]:
        return select_records(self.sa_table, self.session, include_columns=self.column_names)

    def __getitem__(self, key):
        if isinstance(key, int):
            # TableSubColumnSelection[index]
            index = key
            primary_key_values = self.get_primary_key_values()
            return SubRecordSelection(self.session_table, primary_key_values[index], self.column_names)
        if isinstance(key, slice):
            # TableSubColumnSelection[slice]
            _slice = key
            primary_key_values = self.get_primary_key_values()
            if _slice.start is None and _slice.stop is None:
                return TableSubColumnSelection(self.session_table, self.column_names)
            else:
                return SubTableSubColumnSelection(self.session_table, primary_key_values[_slice], self.column_names)
        if isinstance(key, str):
            # TableSubColumnSelection[column_name]
            column_name = key
            primary_key_values = self.get_primary_key_values()
            _type = type(self)
            if isinstance(self, SubTableSelection):
                return SubColumnSelection(self.session_table, column_name, primary_key_values)
            else:
                return ColumnSelection(self.session_table, column_name)
        if isinstance(key, tuple):
            # TableSubColumnSelection[column_name, <index or slice>]
            column_name = key[0]
            primary_key_values = self.get_primary_key_values()
            if isinstance(key[1], int):
                # TableSubColumnSelection[column_name, index]
                index = key[1]
                return ValueSelection(self.session_table, column_name, primary_key_values[index])
            if isinstance(key[1], slice):
                # TableSubColumnSelection[column_name, slice]
                _slice = key[1]
                return SubColumnSelection(self.session_table, column_name, primary_key_values[_slice])
            if isinstance(key[1], Iterable) and all(isinstance(item, bool) for item in key[1]):
                filter = key[1]
                primary_keys = self.get_primary_keys_by_filter(filter)
                return SubColumnSelection(self.session_table, column_name, primary_keys)
        if isinstance(key, Iterable) and all(isinstance(item, bool) for item in key):
            # TableSubColumnSelection[Iterable[bool]]
            filter = key
            primary_keys = self.get_primary_keys_by_filter(filter)
            return SubTableSubColumnSelection(self.session_table, primary_keys, self.column_names)
        if isinstance(key, Iterable) and all(isinstance(item, str) for item in key):
            # TableSubColumnSelection[Iterable[str]]
            column_names = key
            return TableSubColumnSelection(self.session_table, column_names)
     

class SubTableSelection(TableSelection):
    # returned when a subset of records is selecected
    def __init__(self, session_table, primary_key_values: list[Record]):
        TableSelection.__init__(self, session_table)
        self.primary_key_values = primary_key_values

    def __repr__(self):
        return f"SubTableSelection(name='{self.table_name}', records={self.records})"

    def __len__(self):
        return len(self.primary_key_values)

    @property
    def records(self) -> list[Record]:
        return select_records_by_primary_keys(self.sa_table, self.session, self.primary_key_values)

    def get_primary_key_values(self):
        return self.primary_key_values

    def get_primary_keys_by_index(self, index: int):
        return self.primary_key_values[index]

    def get_primary_keys_by_slice(self, _slice: slice):
        return self.primary_key_values[_slice]

    def get_primary_keys_by_filter(self, filter: Iterable[bool]):
        return [record for record, b in zip(self.primary_key_values, filter) if b]

class SubTableSubColumnSelection(SubTableSelection):
    # returned when a subset of records is selected and a subset of columns is selected
    def __init__(self, session_table, primary_key_values, column_names):
        SubTableSelection.__init__(self, session_table, primary_key_values)
        self.column_names = column_names

    def __repr__(self):
            return f"SubTableSubColumnSelection(name='{self.table_name}', records={self.subrecords})" 

    def __getitem__(self, key):
        if isinstance(key, int):
            # SubTableSubColumnSelection[index]
            primary_key_values = self.get_primary_key_values()
            return SubRecordSelection(self.session_table, primary_key_values[key], self.column_names)
        if isinstance(key, slice):
            # SubTableSubColumnSelection[slice]
            _slice = key
            primary_key_values = self.get_primary_key_values()
            if key.start is None and key.stop is None:
                return SubTableSubColumnSelection(self.session_table, primary_key_values, self.column_names)
            else:
                return SubTableSubColumnSelection(self.session_table, primary_key_values[_slice], self.column_names)
        if isinstance(key, str):
            # SubTableSubColumnSelection[column_name]
            column_name = key
            primary_key_values = self.get_primary_key_values()
            _type = type(self)
            return SubColumnSelection(self.session_table, column_name, primary_key_values)

        if isinstance(key, tuple):
            # SubTableSubColumnSelection[column_name, <index or slice>]
            column_name = key[0]
            primary_key_values = self.get_primary_key_values()
            if isinstance(key[1], int):
                # SubTableSubColumnSelection[column_name, index]
                index = key[1]
                return ValueSelection(self.session_table, column_name, primary_key_values[index])
            if isinstance(key[1], slice):
                # SubTableSubColumnSelection[column_name, slice]
                _slice = key[1]
                return SubColumnSelection(self.session_table, column_name, primary_key_values[_slice])
            if isinstance(key[1], Iterable) and all(isinstance(item, bool) for item in key[1]):
                filter = key[1]
                primary_keys = self.get_primary_keys_by_filter(filter)
                return SubColumnSelection(self.session_table, column_name, primary_keys)
        if isinstance(key, Iterable) and all(isinstance(item, bool) for item in key):
            # SubTableSubColumnSelection[Iterable[bool]]
            filter = key
            primary_keys = self.get_primary_keys_by_filter(filter)
            return SubTableSubColumnSelection(self.session_table, primary_keys, self.column_names)
        if isinstance(key, Iterable) and all(isinstance(item, str) for item in key):
            # SubTableSubColumnSelection[Iterable[str]]
            column_names = key
            return SubTableSubColumnSelection(self.session_table, column_names, column_names)

    @property
    def subrecords(self):
        return select_records_by_primary_keys(self.sa_table,
                                              self.session,
                                              self.primary_key_values,
                                              include_columns=self.column_names)


class ColumnSelection(Selection):
    # returned when a column is selected
    def __init__(self, session_table, column_name: str):
        Selection.__init__(self, session_table)
        self.column_name = column_name

    def __repr__(self):
        return f"""ColumnSelection(table_name='{self.table_name}', column_name='{self.column_name}', values={self.values})"""

    def __len__(self):
        return len(self.session_table)

    def __getitem__(self, key):
        if isinstance(key, int):
            # ColumnSelection[int]
            index = key
            primary_key_values = self.get_primary_key_values()
            return ValueSelection(self.session_table, self.column_name, primary_key_values[index])
        
        if isinstance(key, slice):
            # ColumnSelection[slice]
            _slice = key
            primary_key_values = self.get_primary_key_values()
            return SubColumnSelection(self.session_table, self.column_name, primary_key_values[_slice])

        if isinstance(key, Iterable) and all(isinstance(item, bool) for item in key):
            # ColumnSelection[Iterable[bool]]
            filter = key
            primary_key_values = self.get_primary_key_values_by_filter(filter)
            return SubColumnSelection(self.session_table, self.column_name, primary_key_values)

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

        if isinstance(key, Iterable) and all(isinstance(item, bool) for item in key):
            # ColumnSelection[Iterable[bool]] = value
            filter = key
            sub_column_selection = self[filter]
            sub_column_selection.update(value)

    def __add__(self, value):
        # update values by adding value
        records = self.get_records()
        if isinstance(value, Number):
            for record in records:
                record[self.column_name] += value
        elif isinstance(value, Iterable) and not isinstance(value, str):
            for record, val in zip(records, value):
                record[self.column_name] += val
        update_records_session(self.sa_table, records, self.session)

    def __sub__(self, value):
        # update values by subtracting value
        records = self.get_records()
        if isinstance(value, Number):
            for record in records:
                record[self.column_name] -= value
        elif isinstance(value, Iterable) and not isinstance(value, str):
            for record, val in zip(records, value):
                record[self.column_name] -= val
        update_records_session(self.sa_table, records, self.session)

    def __eq__(self, other):
        # ColumnSelection == value
        # Returns filter
        if isinstance(other, Iterable) and not isinstance(other, str):
            return [value == item for value, item in zip(self.values, other)]
        return [item == other for item in self.values]

    def __ne__(self, other):
        # ColumnSelection != value
        # Return filter
        if isinstance(other, Iterable) and not isinstance(other, str):
            return [value != item for value, item in zip(self.values, other)]
        return [item != other for item in self.values]

    def __ge__(self, other):
        # ColumnSelection >= value
        # Return filter
        if isinstance(other, Iterable) and not isinstance(other, str):
            return [value >= item for value, item in zip(self.values, other)]
        return [value >= other for value in self.values]

    def __le__(self, other):
        # ColumnSelection <= value
        # Return filter
        if isinstance(other, Iterable) and not isinstance(other, str):
            return [value <= item for value, item in zip(self.values, other)]
        return [value <= other for value in self.values]

    def __lt__(self, other):
        # ColumnSelection < value
        # Return filter
        if isinstance(other, Iterable) and not isinstance(other, str):
            return [value < item for value, item in zip(self.values, other)]
        return [item < other for item in self.values]

    def __gt__(self, other):
        # ColumnSelection > value
        # Return filter
        if isinstance(other, Iterable) and not isinstance(other, str):
            return [value > item for value, item in zip(self.values, other)]
        return [item > other for item in self.values]

    @property
    def values(self):
        return select_column_values_all(self.sa_table, self.session, self.column_name)

    def get_records(self):
        return select_records(self.sa_table, self.session)

    def get_primary_key_values(self):
        return select_primary_key_values(self.sa_table, self.session)

    def get_primary_keys_by_filter(self, filter: Iterable[bool]):
        primary_key_values = self.get_primary_key_values()
        return [record for record, b in zip(primary_key_values, filter) if b]

    def get_primary_keys_by_index(self, index):
        primary_keys = self.get_primary_key_values()
        return primary_keys[index]

    def get_primary_keys_by_slice(self, _slice):
        primary_keys = self.get_primary_key_values()
        return primary_keys[_slice]

    def get_primary_keys_by_filter(self, filter: Iterable[bool]):
        primary_key_values = self.get_primary_key_values()
        return [record for record, b in zip(primary_key_values, filter) if b]

    def update(self, values):
        primary_key_values = self.get_primary_key_values()
        if isinstance(values, Iterable) and not isinstance(values, str):
            for record, value in zip(primary_key_values, values):
                record[self.column_name] = value
        else:
            for record in primary_key_values:
                record[self.column_name] = values
        update_records_session(self.sa_table, primary_key_values, self.session)


class SubColumnSelection(ColumnSelection):
    # returned when a subset of a column is selecected
    def __init__(self, session_table, column_name: str, primary_key_values: list[Record]):
        ColumnSelection.__init__(self, session_table, column_name)
        self.primary_key_values = primary_key_values

    def __repr__(self):
        return f"SubColumnSelection(table_name='{self.table_name}', column_name='{self.column_name}', values={self.values})"

    def __len__(self):
        return len(self.primary_key_values)

    @property
    def values(self):
        return select_column_values_by_primary_keys(self.sa_table, self.session, self.column_name, self.primary_key_values)

    def get_primary_key_values(self):
        return self.primary_key_values

    def get_records(self):
        return select_records_by_primary_keys(self.sa_table, self.session, self.primary_key_values)


class RecordSelection(Selection):
    # returned when a single record is selected with SessionTable
    def __init__(self, session_table, primary_key_values: Record):
        Selection.__init__(self, session_table)
        self.primary_key_values = primary_key_values

    def __repr__(self):
        return f"RecordSelection(table_name='{self.table_name}', record={self.record})"

    def __getitem__(self, key):
        column_name = key
        return ValueSelection(self.session_table, column_name, self.primary_key_values)

    def __setitem__(self, key, value):
        column_name = key
        value_selection = self[column_name]
        value_selection.update(value)

    @property
    def record(self):
        records = select_record_by_primary_key(self.sa_table, self.session, self.primary_key_values)
        if len(records) == 0:
            raise KeyError('Primary key values do not exist in table.')
        else:
            return records[0]

    def update(self, record: Record) -> None:
        # update record with new values
        update_records_session(self.sa_table, [record], self.session)

    def delete(self) -> None:
        # delete the record
        delete_record_by_values_session(self.sa_table, self.primary_key_values, self.session)


class SubRecordSelection(Record):
    # returned when a record is selected and a subset of columns is selected
    def __init__(self, session_table, primary_key_values, column_names):
        RecordSelection.__init__(self, session_table, primary_key_values)
        self.column_names = column_names

    def __repr__(self):
        return f"SubRecordSelection(table_name='{self.table_name}', subrecord={self.subrecord})"

    @property
    def subrecord(self):
        return select_records_by_primary_keys(self.sa_table,
                                              self.session,
                                              [self.primary_key_values],
                                              include_columns=self.column_names)[0]

            
class ValueSelection(Selection):
    # returned when a single value in a column is selecected
    def __init__(self, session_table, column_name: str, primary_key_values: Record):
        Selection.__init__(self, session_table)
        self.column_name = column_name
        self.primary_key_values = primary_key_values

    def __repr__(self):
        return f"ValueSelection(table_name='{self.table_name}', column_name='{self.column_name}', primary_key_values={self.primary_key_values}, value={self.value})"

    def __eq__(self, other):
        return self.value == other

    def __ne__(self, other):
        return self.value != other

    def __le__(self, other):
        return self.value <= other

    def __gt__(self, other):
        return self.value > other

    def __ge__(self, other):
        return self.value >= other

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

    @property
    def value(self):
        return select_value_by_primary_keys(self.sa_table, self.session, self.column_name, self.primary_key_values)

    def update(self, value):
        # update the value in the table.
        record = self.primary_key_values.copy()
        record[self.column_name] = value
        update_records_session(self.sa_table, [record], self.session)