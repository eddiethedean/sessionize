from typing import Optional, Union

import sqlalchemy as sa
from sqlalchemy.schema import DropTable
from sqlalchemy.engine import Engine

from sessionize.utils.sa_orm import get_table


def drop_table(
    table: Union[sa.Table, str],
    engine: Engine,
    if_exists: Optional[bool] = True
) -> None:
    if isinstance(table, str):
        if table not in sa.inspect(engine).get_table_names():
            if if_exists:
                return
        table = get_table(table, engine)
    sql = DropTable(table, if_exists=if_exists)
    engine.execute(sql)