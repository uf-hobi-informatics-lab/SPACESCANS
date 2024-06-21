
"""

    Boilerplate functions for getting the SQLite engine and defining a session
    for the ZIP_9 database.

"""
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from .models import exposome_base


def get_engine():
    # Returns a new engine using the exposomes.db file.
    # Echo set to False to hush logging.
    return create_engine('sqlite:////ABSOLUTE/PATH/TO/db_files/zip9_exposomes.db', echo=False)

def get_session():
    # Returns a session for interfacing with the database. Other scripts
    # use this function to interact with the database.
    engine = get_engine()
    Session = sessionmaker(bind=engine)
    return Session()

def init():
    engine = get_engine()
    exposome_base.metadata.create_all(engine)