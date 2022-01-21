![Sessionize Logo](https://raw.githubusercontent.com/eddiethedean/sessionize/main/docs/logo_name.svg)
-----------------

# Sessionize: intutive Python SQL table manipulation toolkit
[![PyPI Latest Release](https://img.shields.io/pypi/v/sessionize.svg)](https://pypi.org/project/sessionize/)

## What is it?

**Sessionize** is a Python package that has an intuitive API that utilizes SqlAlchemy to connect to and manipulate records in SQL tables.

## Main Features
Here are just a few of the things that Sessionize does well:

  - Quickly and easily start a SQL session to insert, delete, and update records in SQL tables.
  - Roll back any changes durring SQL session inside a context manager.
  - Additional SQL migration functions for changing or adding primary keys, adding or dropping columns, renaming column and more.

## Where to get it
The source code is currently hosted on GitHub at:
https://github.com/eddiethedean/sessionize

```sh
# PyPI
pip install sessionize
```

## Dependencies
- [sqlalchemy - Python SQL toolkit and Object Relational Mapper that gives application developers the full power and flexibility of SQL](https://www.sqlalchemy.org/)
- [alembic - a lightweight database migration tool for usage with the SQLAlchemy Database Toolkit for Python](https://alembic.sqlalchemy.org/)

## Example
```sh
import sqlalchemy as sa
from sessionize import SessionTable 

# Create SqlAlchemy engine to connect to database.
engine = sa.create_engine('sqlite:///foo.db')

# Create SessionTable to start session of table changes.
st = SessionTable('people', engine)

# Make changes to SessionTable:
# Add 1 to each value in the age column.
st['age'] + 1
# Update the first record with new values.
st[0] = {'id': 1, 'name': 'Olive', 'age': 18}
# Delete the last record.
del st[-1]

# Commit SessionTable to push changes to SQL table.
st.commit()
```