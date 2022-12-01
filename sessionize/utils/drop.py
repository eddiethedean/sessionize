from typing import Optional, Union

# TODO: replace with interfaces
from sqlalchemy import Table
from sqlalchemy.engine import Engine
import sqlalchemize.drop as drop


def drop_table(
    table: Union[Table, str],
    engine: Engine,
    if_exists: bool = True,
    schema: Optional[str] = None
) -> None:
    drop.drop_table(table, engine, if_exists, schema)