import sessionize.orm.selection as selection
import sessionize.orm.session_parent as parent
import sqlalchemize.features as features


class SessionDatabase(parent.SessionParent):
    def __init__(self, engine):
        parent.SessionParent.__init__(self, engine)
        self.tables = {}

    def __repr__(self) -> str:
        return f"""SessionDatabase(table_names={self.table_names()})"""

    def __getitem__(self, key: str) -> selection.TableSelection:
        # SessionDataBase[table_name]
        if isinstance(key, str):
            # Pull out schema if key has period.
            if '.' in key:
                schema, name = key.split('.')
            else:
                schema, name = None, key
            if key not in self.tables:
                self.tables[key] = selection.TableSelection(self, name, schema=schema)
            return self.tables[key]
        raise KeyError('SessionDataBase key type can only be str.')

    def table_names(self, schema=None):
        if schema is not None:
            names = features.get_table_names(self.engine, schema)
            return [f'{schema}.{name}' for name in names]
        out = []
        for schema in features.get_schemas(self.engine):
            names = features.get_table_names(self.engine, schema)
            names = [f'{schema}.{name}' for name in names]
            out.extend(names)
        return out