from typing import Any, Dict, Union

import sqlalchemy.engine as sa_engine
import sqlalchemy.orm.session as sa_session


Record = Dict[str, Any]
SqlConnection = Union[sa_engine.Engine, sa_session.Session, sa_engine.Connection]