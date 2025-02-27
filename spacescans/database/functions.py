"""

    Script for general functions to hit against that databases. Mainly just using it for insertion right now. 
    Most other functionality requires directly interfacing with the DB from the calling script. 

"""

import csv
import pandas as pd


# Database imports
# Zip_9
from .zip_9 import db_session as z9_session
from .zip_9 import models as z9_models

# Zip_5
from .zip_5 import db_session as z5_session
from .zip_5 import models as z5_models

# Dictionary Definitions

#valid_geoids = ['zip_9','zip_5','FIPS']

zip_9_tables = {

    'test':z9_models.TEST,
    'acag': z9_models.acag,
    'caces': z9_models.caces,
    'epa_nata':z9_models.epa_nata,
    'us_hud': z9_models.us_hud,
    'national_walkability_index': z9_models.national_walkability_index,
    'usda_fara': z9_models.usda_fara,
    'acs': z9_models.acs,
    'cbp': z9_models.cbp,
    'ucr': z9_models.ucr
}

zip_5_tables = {

    'acag': z5_models.acag,
    'caces': z5_models.caces,
    'epa_nata':z5_models.epa_nata,
    'us_hud': z5_models.us_hud,
    'national_walkability_index': z5_models.national_walkability_index,
    'usda_fara': z5_models.usda_fara,
    'acs': z5_models.acs,
    'cbp': z5_models.cbp,
    'ucr': z5_models.ucr
}


# *******************************************************************************************************
# USING THIS FUNCTION FOR TESTING ONLY  - IT WORKS WELL BUT MAY NEED CHANGES BASED ON EXPOSOME DATA FILES
# this is the new function that I created to work only with zip_9 data and for testing only.  
# Once we move to using zip 5, we'll need to re-work this code.    
def insert_from_csv(source, target, session):
    """
    Function to insert a CSV file into a specified table in the exposome database.
    
    source: the path to the CSV file to be uploaded. The source file headers must match the target headers exactly.
    target: the desired table to insert into.
    """   

    # Set batch_size to reduce commits
    tables = zip_9_tables
    target_table = tables[target]
    batch_size = 10000   
    
    total_records = 0  # running total counter

    # Insert the DataFrame into the target table in batches
    for start in range(0, len(source), batch_size):
        batch = source.iloc[start:start + batch_size]
        records = batch.to_dict(orient='records')
            
        for record in records:
            new_record = target_table(**record)
            session.add(new_record)
        
        total_records += len(batch)
        print(f"Loaded {total_records} records\n")
        session.commit()

# *******************************************************************************************************


    # Clean up commits after insert loop finishes
    session.commit()
    print("Data fully loaded to database!")

# Initialization functions to create the db files. 
# These really shouldn't be called ever.
def init_zip_9():
    z9_session.init()

def init_zip_5():
    z5_session.init()