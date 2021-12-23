import pathlib
from setuptools import setup

# The directory containing this file
HERE = pathlib.Path(__file__).parent

# The text of the README file
README = (HERE / "README.md").read_text()

# This call to setup() does all the work
setup(
    name="sessionize",
    version="0.0.0",
    description="Make changes to sql tables using sessions",
    long_description=README,
    long_description_content_type="text/markdown",
    url="",
    author="Odos Matthews",
    author_email="odosmatthews@gmail.com",
    license="MIT",
    classifiers=[
        "License :: OSI Approved :: MIT License",
        "Programming Language :: Python :: 3",
        "Programming Language :: Python :: 3.7",
    ],
    packages=["sessionize"],
    include_package_data=True,
    install_requires=["sqlalchemy"]
)