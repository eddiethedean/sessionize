from typing import Any, Optional, Union
from dataclasses import dataclass

from sqlalchemy.orm import Session
import sqlalchemy as sa

from sessionize.utils.delete import delete_records_session
from sessionize.utils.insert import insert_records_session
from sessionize.utils.update import update_records_session

from sessionize.utils.select import select_first_record, select_records, select_records_all
from sessionize.utils.select import check_slice_primary_keys_match, check_index_keys_match
from sessionize.utils.select import select_records_slice, select_record_by_index, select_column_values_all
from sessionize.utils.select import select_column_values_by_slice, select_column_value_by_index
from sessionize.utils.select import select_column_values_by_primary_keys, select_value_by_primary_keys
from sessionize.utils.select import select_primary_key_record_by_index, select_primary_key_records_by_slice
from sessionize.utils.sa_orm import get_table, has_primary_key, get_column_names
from sessionize.utils.sa_orm import get_column_types, get_row_count, primary_keys
from sessionize.utils.custom_types import Record, SqlConnection
from sessionize.exceptions import MissingPrimaryKey, SliceError

from sessionize.orm.sub_table_selection import SubTableSelection
from sessionize.orm.column_selection import ColumnSelection
from sessionize.orm.sub_column_selection import SubColumnSelection
from sessionize.orm.record_selection import RecordSelection
from sessionize.orm.value_selection import ValueSelection


class SessionTable:
    def __init__(self, name: str, engine: sa.engine.Engine, schema: Optional[str] = None):
        self.name = name
        self.engine = engine
        self.schema = schema
        self.session = Session(engine)
        self.sa_table = get_table(self.name, self.session, self.schema)
        if not has_primary_key(self.sa_table):
            raise MissingPrimaryKey(
            'sessionize requires sql table to have primary key to work properly.\n' +
            'Use sessionize.create_primary_key to add a primary key to your table.')

    def __repr__(self) -> str:
        return repr_session_table(self.sa_table, self.session)

    def __getitem__(self, key: Union[int, slice, str, tuple[str, int, int]]) -> Union[Record, list[Record]]:
        if isinstance(key, int):
            # SessionTable[index]
            index = key
            primary_key_values = select_primary_key_record_by_index(self.sa_table, self.session, index)
            return RecordSelection(self, primary_key_values)

        if isinstance(key, slice):
            # SessionTable[slice]
            _slice = key
            primary_key_values = select_primary_key_records_by_slice(self.sa_table, self.session, _slice)
            return SubTableSelection(self, primary_key_values)

        if isinstance(key, str):
            # SessionTable[column_name]
            column_name = key
            return ColumnSelection(self, column_name)

        if isinstance(key, tuple):
            # SessionTable[column_name, <index or slice>]
            column_name = key[0]

            if isinstance(key[1], int):
                # SessionTable[column_name, index]
                index = key[1]
                primary_key_values = select_primary_key_record_by_index(self.sa_table, self.session, index)
                return ValueSelection(self.session_table, primary_key_values)

            if isinstance(key[1], slice):
                # SessionTable[column_name, slice]
                _slice = key[1]
                primary_key_values = select_primary_key_records_by_slice(self.sa_table, self.session, _slice)
                return SubColumnSelection(self.name, self.session_table, primary_key_values)
        

    def __setitem__(
        self,
        key: Union[int, slice],
        value: Union[Record, list[Record]]
    ) -> None:
        keys = self.primary_keys
        if isinstance(key, slice):
            # SessionTable[slice] = value
            if not isinstance(value, list):
                raise TypeError('value must be type list when setting slice item')
            if not check_slice_primary_keys_match(self.sa_table, self.session, key.start, key.stop, value):
                raise ValueError('All primary key values must match slice records.')
            # SessionTable[slice] = [Record]
            self.update_records(value)
            return
        if isinstance(key, int):
            # SessionTable[index] = Record
            if not isinstance(value, dict):
                raise TypeError('value must be type dict when setting index item')
            if not check_index_keys_match(self.sa_table, self.session, key, value):
                raise ValueError('Primary key values must match index record.')
            self.update_records([value])
            return

        if isinstance(key, int):
            # SessionTable[index] = Record
            index = key
            record_selection = self[index]
            record_selection.update(value)
            
        elif isinstance(key, slice):
            # SessionTable[slice] = list[Record]
            if not isinstance(value, list):
                raise TypeError('value must be type list when setting slice item')
            if not check_slice_primary_keys_match(self.sa_table, self.session, key.start, key.stop, value):
                raise ValueError('All primary key values must match slice records.')
            _slice = key
            sub_table_selection = self[_slice]
            sub_table_selection.update(value)

        elif isinstance(key, str):
            # SessionTable[column_name] = list[value] or value
            column_name = key
            column_selection = self[column_name]
            column_selection.update(value)

        elif isinstance(key, tuple):
            # SessionTable[column_name, <index or slice>] = value
            column_name = key[0]
            if isinstance(key[1], int):
                # SessionTable[column_name, index] = value
                index = key[1]
                value_selection = self[column_name, index]
                value_selection.update(value)

            elif isinstance(key[1], slice):
                # SessionTable[column_name, slice] = list[value] or value
                _slice = key[1]
                sub_column_selection = self[column_name, _slice]
                sub_column_selection.update(value)

    def __eq__(self, other) -> list[bool]:
        # create filter
        pass

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        if exc_type:
            self.session.rollback()
        else:
            self.commit()

    def __len__(self):
        return get_row_count(self.sa_table, self.session)

    def __add__(self, value: Union[Record, list[Record]]):
        # insert a record or list of records into table
        if isinstance(value, dict):
            self.insert_one_record(value)
        elif isinstance(value, list):
            self.insert_records(value)
        else:
            raise TypeError('value must be a dictionary or list')
        return self

    def __iadd__(self, value: Union[Record, list[Record]]) -> None:
        return self.__add__(value)

    @property
    def columns(self):
        return get_column_names(self.sa_table)

    @property
    def records(self):
        return select_records_all(self.sa_table, self.session)

    @property
    def primary_keys(self):
        return primary_keys(self.sa_table)

    def info(self):
        return select_table_info(self.sa_table, self.session)

    def commit(self):
        self.session.commit()

    def rollback(self):
        self.session.rollback()

    def insert_records(self, records: list[Record]) -> None:
        insert_records_session(self.sa_table, records, self.session, schema=self.schema)

    def insert_one_record(self, record: Record) -> None:
        self.insert_records([record])

    def update_records(self, records: list[Record]) -> None:
        update_records_session(self.sa_table, records, self.session, schema=self.schema)

    def update_one_record(self, record: Record) -> None:
        self.update_records([record])

    def delete_records(self, column_name: str, values: list[Any]) -> None:
        delete_records_session(self.sa_table, column_name, values, self.session, schema=self.schema)

    def delete_one_record(self, column_name: str, value: Any) -> None:
        self.delete_records(column_name, [value])

    def select_records(self, chunksize=None) -> list[Record]:
        return select_records(self.sa_table, self.session, chunksize=chunksize, schema=self.schema)

    def head(self, size=5):
        if size < 0:
            raise ValueError('size must be a positive number')
        return self[:size]

    def tail(self, size=5):
        if size < 0:
            raise ValueError('size must be a positive number')
        return self[-size:]


def select_session_record_by_index(
    table: sa.Table,
    session_table: SessionTable,
    index: int
) -> RecordSelection:
    session = session_table.session
    primary_key_values = select_primary_key_record_by_index(table, session, index)


@dataclass
class TableInfo():
    name: str
    types: dict[str, sa.sql.sqltypes]
    row_count: int
    keys: list[str]
    first_record: Record
    schema: Optional[str] = None


def select_table_info(table: sa.Table, connection: SqlConnection) -> TableInfo:
    types = get_column_types(table)
    row_count = get_row_count(table, connection)
    keys = primary_keys(table)
    first_record = select_first_record(table, connection)
    return TableInfo(table.name, types, row_count, keys, first_record, table.schema)


def repr_session_table(
    table: sa.Table,
    connection: SqlConnection
) -> str:
    table_info = select_table_info(table, connection)
    types = table_info.types
    row_count = table_info.row_count
    keys = table_info.keys
    first_record = table_info.first_record
    name = table.name if table.schema is None else f"{table.name}', schema='{table.schema}"
    return f"""SessionTable(name='{name}', keys={keys}, row_count={row_count},
             first_record={first_record},
             sql_data_types={types})"""
