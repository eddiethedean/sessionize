from typing import Union

import sqlalchemy as sa
from sqlalchemy.engine import Engine
from alembic.runtime.migration import MigrationContext
from alembic.operations import Operations
from sqlalchemy.sql.schema import PrimaryKeyConstraint

from sessionize.utils.sa_orm import get_table, get_primary_key_constraints
from sessionize.utils.insert import insert_from_table
from sessionize.utils.drop import drop_table


def rename_column(
    table_name: Union[str, sa.Table],
    old_col_name: str,
    new_col_name: str,
    engine: Engine
) -> sa.Table:
    """
        Renames a table column.

        Returns newly reflected SqlAlchemy Table.
    """
    if isinstance(table_name, sa.Table):
        table_name = table_name.name
    conn = engine.connect()
    ctx = MigrationContext.configure(conn)
    op = Operations(ctx)
    with op.batch_alter_table(table_name) as batch_op:
        batch_op.alter_column(old_col_name, nullable=True, new_column_name=new_col_name)
    return get_table(table_name, engine)


def drop_column(
    table_name: Union[str, sa.Table],
    col_name: str,
    engine: Engine
) -> sa.Table:
    """
        Drops a table column.

        Returns newly reflected SqlAlchemy Table.
    """
    if isinstance(table_name, sa.Table):
        table_name = table_name.name
    conn = engine.connect()
    ctx = MigrationContext.configure(conn)
    op = Operations(ctx)
    with op.batch_alter_table(table_name) as batch_op:
        batch_op.drop_column(col_name)
    return get_table(table_name, engine)


# TODO: add column function
def add_column(
    table_name: Union[str, sa.Table],
    column_name: str,
    engine: Engine
) -> sa.Table:
    pass

def rename_table(
    old_table_name: Union[str, sa.Table],
    new_table_name: str,
    engine: Engine
) -> sa.Table:
    """
        Renames a table.

        Returns newly reflected SqlAlchemy Table.
    """
    if isinstance(old_table_name, sa.Table):
        table_name = old_table_name.name
    conn = engine.connect()
    ctx = MigrationContext.configure(conn)
    op = Operations(ctx)
    op.rename_table(old_table_name, new_table_name)
    return get_table(new_table_name, engine)


def copy_table(
    table: Union[sa.Table, str],
    new_table_name: str,
    engine: Engine,
    if_exists: str = 'replace'
) -> sa.Table:
    """
        Creates a copy of the given table with new_table_name.
        Drops a table with same new_table_name if_exists='replace'

        Returns the new SqlAlchemy Table.
    """
    if isinstance(table, str):
        table = get_table(table, engine)
    conn = engine.connect()
    ctx = MigrationContext.configure(conn)
    op = Operations(ctx)
    if if_exists == 'replace':
        drop_table(new_table_name, engine)
    op.create_table(new_table_name, *table.c, table.metadata)
    new_table = get_table(new_table_name, engine)
    insert_from_table(table, new_table, engine)
    return new_table


def replace_primary_key(
    table: Union[sa.Table, str],
    column_name: str,
    engine: Engine,
)-> sa.Table:
    """
        Drops primary key from table columns
        then adds primary key to column_name of table.

        Returns newly reflected SqlAlchemy Table.
    """
    if isinstance(table, str):
        table = get_table(table, engine)
    table_name = table.name
    conn = engine.connect()
    ctx = MigrationContext.configure(conn)
    op = Operations(ctx)
    keys = get_primary_key_constraints(table)
    
    with op.batch_alter_table(table_name) as batch_op:
        # name primary key constraint if not named (sqlite)
        if keys[0] is None:
            pk_name = f'pk_{table_name}'
            batch_op.create_primary_key(pk_name, keys[1])
        else:
            pk_name = keys[0]
        batch_op.drop_constraint(pk_name, type_='primary')
        batch_op.create_primary_key(f'pk_{table_name}', [column_name])
    return get_table(table_name, engine)


def create_primary_key(
    table_name: Union[str, sa.Table],
    column_name: str,
    engine: Engine
) -> sa.Table:
    """
        Adds a primary key constraint to table.
        Only use on a table with no primary key.
        Use replace_primary_key on tables with a primary key.

        Returns newly reflected SqlAlchemy Table.
    """
    if isinstance(table_name, sa.Table):
        table_name = table_name.name
    conn = engine.connect()
    ctx = MigrationContext.configure(conn)
    op = Operations(ctx)
    with op.batch_alter_table(table_name) as batch_op:
        batch_op.create_primary_key(f'pk_{table_name}', [column_name])
    return get_table(table_name, engine)


def name_primary_key(
    table_name: Union[str, sa.Table],
    column_name: str,
    engine: Engine
) -> sa.Table:
    """
        Names the primary key constraint for a given sql table column.

        Returns newly reflected SqlAlchemy Table.
    """
    if isinstance(table_name, sa.Table):
        table_name = table_name.name
    create_primary_key(table_name, column_name, engine)
    return get_table(table_name, engine)