from typing import Any

import sqlalchemy.engine as sa_engine
import sqlalchemy.orm.session as sa_session


Record = dict[str, Any]
SqlConnection = sa_engine.Engine | sa_session.Session | sa_engine.Connection