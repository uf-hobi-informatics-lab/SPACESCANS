
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
    #return create_engine('sqlite:////Users/allison.burns/Desktop/exposome/TEST_DB/zip9_exposomes.db', echo=False)
    return create_engine('sqlite:////data/exposome_db/zip9_exposomes.db', echo=False)

def get_session():
    # Returns a session for interfacing with the database. Other scripts
    # use this function to interact with the database.
    engine = get_engine()
    Session = sessionmaker(bind=engine)
    return Session()

def create_tables():
    # Create all tables based on the models defined in models.py
    engine = get_engine()
    exposome_base.metadata.create_all(engine)

def drop_table(table_class):
    # Drop a specific table based on the class passed from models
    engine = get_engine()
    table_class.__table__.drop(engine, checkfirst=True)

def create_table(table_class):
    # Create a specific table based on the class passed from models
    engine = get_engine()
    table_class.__table__.create(engine, checkfirst=True)



def init():
    engine = get_engine()
    exposome_base.metadata.create_all(engine)