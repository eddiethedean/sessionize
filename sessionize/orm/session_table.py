from typing import Any, Dict, Generator, Optional, Union
from dataclasses import dataclass
from collections.abc import Iterable

import sqlalchemy as sa
import sqlalchemy.engine as sa_engine
from sqlalchemy.engine import Engine
from sqlalchemy.orm.session import Session
from chaingang import selection_chaining

import sessionize.utils.delete as delete
import sessionize.utils.insert as insert
import sessionize.utils.update as update
import sessionize.utils.select as select
import sessionize.utils.features as features
import sessionize.exceptions as exceptions
import sessionize.orm.session_parent as parent
import sessionize.utils.types as types
import sessionize.orm.selection as selection


@selection_chaining
class SessionTable(parent.SessionParent):
    def __init__(self, name: str, engine: sa_engine.Engine, schema: Optional[str] = None):
        parent.SessionParent.__init__(self, engine)
        self.name = name
        self.schema = schema
        self.sa_table = features.get_table(self.name, self.session, self.schema)
        if not features.has_primary_key(self.sa_table):
            raise exceptions.MissingPrimaryKey(
            'Sessionize requires sql table to have a primary key to work properly.\n' +
            'Use sessionize.create_primary_key to add a primary key to your table.')
        # Used when SessionTable is selected (__getitem__, __setitem__, __delitem__)
        self.table_selection = selection.TableSelection(self, self.name, schema=self.schema)

    def __repr__(self) -> str:
        return repr_session_table(self.sa_table, self.session)

    def __iter__(self):
        return iter(self.table_selection)

    def __getitem__(self, key) -> selection.Selection:
        return self.table_selection[key]
    
    def __setitem__(self, key, value) -> None:
        self.table_selection[key] = value

    def __delitem__(self, key):
        del self.table_selection[key]

    def __len__(self):
        return features.get_row_count(self.sa_table, self.session)

    def __add__(self, value: Union[types.Record, list[types.Record]]):
        # insert a record or list of records into table
        if isinstance(value, dict):
            self.insert_one_record(value)
        elif isinstance(value, Iterable) and not isinstance(value, str):
            self.insert_records(value)
        else:
            raise TypeError('value must be a dictionary or list')
        return self

    # TODO: += feature
    # def __iadd__(self, value: Union[Record, list[Record]]) -> None:
        # return self.__add__(value)

    @property
    def columns(self):
        return features.get_column_names(self.sa_table)

    @property
    def records(self):
        return select.select_records_all(self.sa_table, self.session)

    @property
    def primary_keys(self):
        return features.primary_keys(self.sa_table)

    def info(self):
        return select_table_info(self.sa_table, self.session)

    def insert_records(self, records: list[types.Record]) -> None:
        insert.insert_records_session(self.sa_table, records, self.session, schema=self.schema)

    def insert_one_record(self, record: types.Record) -> None:
        self.insert_records([record])

    def update_records(self, records: list[types.Record]) -> None:
        update.update_records_session(self.sa_table, records, self.session, schema=self.schema)

    def update_one_record(self, record: types.Record) -> None:
        self.update_records([record])

    def delete_records(self, column_name: str, values: list[Any]) -> None:
        delete.delete_records_session(self.sa_table, column_name, values, self.session, schema=self.schema)

    def delete_one_record(self, column_name: str, value: Any) -> None:
        self.delete_records(column_name, [value])

    def select_records(
        self,
        chunksize=None) -> list[types.Record] | Generator[list[types.Record], None, None]:
        return select.select_records(self.sa_table, self.session, chunksize=chunksize, schema=self.schema)

    def head(self, size=5):
        return self[:size]

    def tail(self, size=5):
        return self[-size:]


@dataclass
class TableInfo():
    name: str
    types: dict
    row_count: int
    keys: list[str]
    first_record: Dict[str, Any] | None
    schema: Optional[str] = None


def select_table_info(table: sa.Table, connection: Engine | Session) -> TableInfo:
    types = features.get_column_types(table)
    row_count = features.get_row_count(table, connection)
    keys = features.primary_keys(table)
    first_record = select.select_first_record(table, connection)
    return TableInfo(table.name, types, row_count, keys, first_record, table.schema)


def repr_session_table(
    sa_table: sa.Table,
    connection: Engine | Session
) -> str:
    table_info = select_table_info(sa_table, connection)
    types = table_info.types
    row_count = table_info.row_count
    keys = table_info.keys
    first_record = table_info.first_record
    name = sa_table.name if sa_table.schema is None else f"{sa_table.name}', schema='{sa_table.schema}"
    return f"""SessionTable(name='{name}', keys={keys}, row_count={row_count},
             first_record={first_record},
             sql_data_types={types})"""