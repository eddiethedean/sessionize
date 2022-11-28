from typing import Optional

import sqlalchemy as sa
import sqlalchemy.schema as sa_schema
import sqlalchemy.ext.declarative as sa_declarative
import sqlalchemy.engine as sa_engine
import sqlalchemy.orm.session as sa_session

# from creds import postgres_url, mysql_url

postgres_url = ''
mysql_url = ''


def setup(connection_string: str, schema: Optional[str] = None) -> tuple[sa_engine.Engine, sa.Table, sa.Table]:
    Base = sa_declarative.declarative_base()

    engine = sa.create_engine(connection_string, echo=False)

    if schema is not None:
        insp = sa.inspect(engine)
        if schema not in insp.get_schema_names(engine.connect()):
               engine.execute(sa_schema.CreateSchema(schema))

    class People(Base):
        __tablename__ = 'people'
        id = sa.Column(sa.Integer, primary_key=True, autoincrement=True)
        name = sa.Column(sa.String(20))
        age = sa.Column(sa.Integer)
        address_id = sa.Column(sa.Integer)
        if schema is not None:
            __table_args__ = {'schema': schema}

    class Places(Base):
        __tablename__ = 'places'
        id = sa.Column(sa.Integer, primary_key=True, autoincrement=True)
        address = sa.Column(sa.String(100))
        city = sa.Column(sa.String(30))
        state = sa.Column(sa.String(2))
        zipcode = sa.Column(sa.Integer)

        if schema is not None:
            __table_args__ = {'schema': schema}

    Base.metadata.reflect(bind=engine, schema=schema)
    Base.metadata.drop_all(bind=engine)
    Base.metadata.create_all(bind=engine, tables=[People.__table__, Places.__table__])

    people = [
        People(name='Olivia', age=17, address_id=1),
        People(name='Liam', age=18, address_id=1),
        People(name='Emma', age=19, address_id=2),
        People(name='Noah', age=20, address_id=2),
    ]

    places = [
        Places(address='1600 Pennsylvania Avenue NW', city='San Antonio', state='TX', zipcode=78205),
        Places(address='300 Alamo Plaza', city='Washington', state='DC', zipcode=20500),
    ]

    with sa_session.Session(engine) as session, session.begin():
        session.add_all(people)
        session.add_all(places)
   
    return engine, People.__table__, Places.__table__


def sqlite_setup(path='sqlite:///data/test.db', schema=None) -> tuple[sa_engine.Engine, sa.Table, sa.Table]:
    return setup(path, schema=schema)


def postgres_setup(postgres_url=postgres_url, schema=None) -> tuple[sa_engine.Engine, sa.Table, sa.Table]:
    path = postgres_url
    return setup(path, schema=schema)


def mysql_setup(shema=None) -> tuple[sa_engine.Engine, sa.Table, sa.Table]:
    path = mysql_url
    return setup(path, schema=shema)