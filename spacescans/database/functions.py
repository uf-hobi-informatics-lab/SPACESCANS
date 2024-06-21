"""

    Script for general functions to hit against that databases. Mainly just using it for insertion right now. 
    Most other functionality requires directly interfacing with the DB from the calling script. 

"""

import csv


# Database imports
# Zip_9
from .zip_9 import db_session as z9_session
from .zip_9 import models as z9_models

# Zip_5
from .zip_5 import db_session as z5_session
from .zip_5 import models as z5_models

# Dictionary Definitions

valid_geoids = ['zip_9','zip_5']

zip_9_tables = {

    'test':z9_models.TEST,
    'acag': z9_models.acag,
    'cases': z9_models.caces,
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
    'cases': z5_models.caces,
    'epa_nata':z5_models.epa_nata,
    'us_hud': z5_models.us_hud,
    'national_walkability_index': z5_models.national_walkability_index,
    'usda_fara': z5_models.usda_fara,
    'acs': z5_models.acs,
    'cbp': z5_models.cbp,
    'ucr': z5_models.ucr
}


def insert_from_csv(source, target, geoid):
    """
    
        Function to insert a csv file into a specified table in the exposome database.

        source: the path to the csv file to be uploaded. The source file headers must match the target headers exactly.
        target: the desired table to insert into.
        geoid: the method of geographical identification.* 
        
            *Currently only zip_9 is supported. But zip_5 code has been added for that data down the line.

    """

    # Value checking to ensure proper values are called from the script.
    if geoid.lower() not in valid_geoids:
        raise ValueError(f"{geoid} is not a valid geoidentifier.")
    elif geoid.lower()=='zip_5':
        session = z5_session.get_session()
        tables = zip_5_tables
    else:
        session = z9_session.get_session()
        tables = zip_9_tables

    if target not in tables:
        raise ValueError(f"{target} is not a valid table!")
    else: 
        target_table = tables[target]

    # Set batch_size and count to reduce commits
    batch_size = 1000
    count = 0

    with open(source, 'r') as file:
        csv_read = csv.DictReader(file)
        for row in csv_read:
            new_record = target_table(**row)
            session.add(new_record)
            count += 1

            # Only commit every 1,000 records. May need to make this larger.
            if count % batch_size == 0:
                session.commit()
                session.flush()
    
    # Clean up commits after insert loop finishes
    session.commit()

# Initialization functions to create the db files. 
# These really shouldn't be called ever.
def init_zip_9():
    z9_session.init()

def init_zip_5():
    z5_session.init()