from sqlalchemy import create_engine, Table
from sqlalchemy.engine import Engine
from sqlalchemy.orm import declarative_base
from sqlalchemy.orm import Session
from sqlalchemy import Column, Integer, String

from creds import postgres_url, mysql_url
from sessionize.utils.sa_orm import get_table


def setup(connection_string: str) -> tuple[Engine, Table]:
    Base = declarative_base()

    engine = create_engine(connection_string, echo=False)

    class People(Base):
        __tablename__ = 'people'
        id = Column(Integer, primary_key=True, autoincrement=True)
        name = Column(String(20))
        age = Column(Integer)

    Base.metadata.drop_all(bind=engine, tables=[People.__table__])
    Base.metadata.create_all(bind=engine, tables=[People.__table__])

    people = [
        People(name='Olivia', age=17),
        People(name='Liam', age=18),
        People(name='Emma', age=19),
        People(name='Noah', age=20),
    ]

    with Session(engine) as session, session.begin():
        session.add_all(people)
   
    return engine, get_table('people', engine)


def sqlite_setup(path='sqlite:///data/test.db') -> tuple[Engine, Table]:
    return setup(path)


def postgres_setup() -> tuple[Engine, Table]:
    path = postgres_url
    return setup(path)


def mysql_setup() -> tuple[Engine, Table]:
    path = mysql_url
    return setup(path)