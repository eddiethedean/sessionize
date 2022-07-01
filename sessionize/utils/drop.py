from typing import Optional, Union

# TODO: replace with interfaces
from sqlalchemy import Table
from sqlalchemy.engine import Engine

from sessionize.sa import sa_functions


def drop_table(
    table: Union[Table, str],
    engine: Engine,
    if_exists: Optional[bool] = True,
    schema: Optional[str] = None
) -> None:
    sa_functions.drop_table(table, engine, if_exists, schema)