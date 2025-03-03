import argparse
import logging
import os
import json
import pandas as pd
from datetime import datetime
from sqlalchemy import create_engine, MetaData, select, func
from sqlalchemy.types import Float
import time

from spacescans.dataclean import address_cleaning as addr
from spacescans import csv_linkage as link
from spacescans.linkage import db_linkage_fara_cbp as db_fara_cbp
from spacescans.linkage import db_linkage_ucr as db_ucr

#========= GLOBAL VARIABLES ===========
logger = logging.getLogger()

valid_log_levels = {
    'DEBUG':logging.DEBUG,  
    'INFO':logging.INFO,
    'WARN':logging.WARN,
    'ERROR':logging.ERROR,
    'CRITICAL':logging.CRITICAL
}

project_name = ''



VALID_TARGETS = ['acag', 'caces', 'epa_nata', 'us_hud', 'national_walkability_index', 'usda_fara', 'cbp', 'ucr']
DB_URL='/Users/jasonglover/Documents/data/exposomes/zip9_exposomes.db'
#=========== End Global Variables =========


#============ Helper Functions =================
def get_project_name(name='new_project'):
    if name == '':
        name = 'new_project'
    
    if not os.path.exists(os.getcwd()+f'/projects/{name}.json'):
        return f'{name}'
    else:
        i = 1
        # Iterate until a project name does not exist
        while True:
            if not os.path.exists(os.getcwd()+f'/projects/{name}_{i}.json'):
                return f'{name}_{i}'
            i+=1

def get_output_name(name='new_project'):
    if name == '':
        name = 'new_project'
    
    if not os.path.exists(os.getcwd()+f'/output/linked_{name}.csv'):
        return f'{name}'
    else:
        i = 1
        # Iterate until a project name does not exist
        while True:
            if not os.path.exists(os.getcwd()+f'/output/linked_{name}_{i}.csv'):
                return f'{name}_{i}'
            i+=1

def is_valid_date(date):
    # Verify the correctness of the date
    #datetime.strptime(date, '%m/%d/%Y')
    try:
        datetime.strptime(date, '%Y-%m-%d')
        return True
    except ValueError:
        return False

def is_valid_project(project_name):
    try:
        with open(f'projects/{project_name}.json','r') as f:
            return True
    except Exception as e:
        return False
    
def get_vars():
    target = input("Which data source would you like to link to? (Options: usda_fara, cbp, ucr)\n")
    valid_target = is_valid_target(target)
    while not valid_target:
        print(f"{target} is not a valid selection")
        target = input("Which data source would you like to link to? (Options: usda_fara, cbp, ucr)\n")
        valid_target = is_valid_target(target)
        
    selection_input = input("Which variables would you like to link against? (Please separate each selection by comma)\n")

    selection = [var.strip() for var in selection_input.split(',') if var.strip()]

    return target, selection

def is_valid_target(input_target):
    if input_target in VALID_TARGETS:
        return True
    return False

#============ End Helper Functions ================
def list_projects():
    # Either the projects variable will be set or an exception will be raised
    
    try:
        if not os.listdir(os.getcwd()+'/projects'):
            raise "Directory is empty"
        projects = os.listdir(os.getcwd()+'/projects')
    except Exception as e:
        print("No projects found! Create a new project with 'python3 run_spacescans.py create_project'.")
        logger.debug(f"Caught the following exception when attempting to list projects: {e}")
        quit(0)
        # Safely exit when no project files are found

    #Iterate over the projects and list them out
    i = 1
    for proj in projects:
        proj = proj.replace(".json", "")
        print(f'{i}. {proj}')
        i+=1

def build_project(project_name, start_date, end_date, geoid, filepath):

    if project_name is None:
        project_name = input("Please enter a name for your project or hit enter to give a default name: ")
    project_name = get_project_name(project_name)
    # Pass the name to the function to handle duplicate project names
    

    # Get necessary values or validate passed in values
    if start_date is None:
        start_date = input("Please input a start date for the study period in the format yyyy-mm-dd: ")
        while not is_valid_date(start_date):
            start_date = input("Date invalid! Please input a start date for the study period in the format yyyy-mm-dd: ")
    else:
        # Then the user passed in a date, we still have to validate it
        while not is_valid_date(start_date):
            start_date = input("Date invalid! Please input a start date for the study period in the format yyyy-mm-dd: ")
    
    if end_date is None:
        end_date = input("Please input an end date for the study period in the format yyyy-mm-dd: ")
        while not is_valid_date(end_date):
            end_date = input("Date invalid! Please input an end date for the study period in the format yyyy-mm-dd: ")
    else:
        # Then the user passed in a date, we still have to validate it
        while not is_valid_date(end_date):
            end_date = input("Date invalid! Please input an end date for the study period in the format yyyy-mm-dd: ")

    if geoid is None:
        geoid = input("Please give the geoidentifier for this dataset. (Options: zip9,): ")
        while geoid not in ['zip9','zip5']:
            geoid = input("Invalid input! Please give the geoidentifier for this dataset. (Options: zip9,): ")
    else:
        while geoid not in ['zip9','zip5']:
            geoid = input("Invalid input! Please give the geoidentifier for this dataset. (Options: zip9,): ")

    if filepath is None:
        filepath = input("Please enter the location of the patient dataset. Ex: /path/to/data/my_data.csv: ")
        while not os.path.isfile(filepath):
            filepath = input(f"Invalid input! {filepath} does not exist. Please enter the location of the patient dataset. Ex: /path/to/data/my_data.csv: ")
    else:
        while not os.path.isfile(filepath):
            filepath = input(f"Invalid input! {filepath} does not exist. Please enter the location of the patient dataset. Ex: /path/to/data/my_data.csv: ")

    logger.debug('Writing the project file out with the following parameters:')
    logger.debug(f'Start date: {start_date}')
    logger.debug(f'End date: {end_date}')
    logger.debug(f'Geoid: {geoid}')
    logger.debug(f'File path: {filepath}')
    
    print('\nWriting the project file out with the following parameters:')
    print(f'Start date: {start_date}')
    print(f'End date: {end_date}')
    print(f'Geoid: {geoid}')
    print(f'File path: {filepath}')

    data = {
        "project_name": project_name,
        "start_date": start_date,
        "end_date": end_date,
        "geoid": geoid,
        "filepath": filepath
    }

    with open(f'projects/{project_name}.json','w') as f:
        json.dump(data, f, indent=4)

    print(f'Sucessfully created {project_name}.json in projects folder!')
    logger.info("Successfully wrote out the project file")


def run_address_cleaning(project_name):
    
    logger.info(f'Performing data_clean for {project_name}...')
    with open(f'projects/{project_name}.json','r') as f:
        project = json.load(f)
    

    start_date = project['start_date']
    end_date = project['end_date']
    geoid = project['geoid']
    file_path = project['filepath']

    start_date = addr.parse_date(start_date)
    end_date = addr.parse_date(end_date)
    patient_file = pd.read_csv(file_path, converters = {'ADDRESS_ZIP9': str})
    zip9_file = pd.read_csv('test_data/combined_zip9s.csv', converters = {'AREAKEY': str})


    addr.validate_csvs(patient_file, zip9_file)
    ldszip9 = addr.filter_good_zip9s(patient_file, zip9_file)
    ids_with_missingness = addr.find_ids_with_missingness(ldszip9)

    ldsz9_no_nulls = addr.fix_nulls(ldszip9, ids_with_missingness, start_date, end_date)
    ldsz9_continuous = addr.fix_gaps_overlaps_dupes(ldsz9_no_nulls)
    ldsz9_in_daterange = addr.limit_timeframe(ldsz9_continuous, start_date, end_date)

    logger.info('Success!')
    outfile = project_name + '_cleaned_patient_data.csv'

    outpath = os.path.join('output', outfile)
    logger.info(f'Writing the output to {outpath}')
    ldsz9_in_daterange.to_csv(outpath, index=False)

def run_linkage(project_name):
    target_db, selection = get_vars()

    logger.info(f'Performing linkage for {project_name} with the following variables:')
    for var in selection:
        print(var)
    

    with open(f'projects/{project_name}.json', 'r') as f:
        project = json.load(f)

    start_date  = project['start_date']
    end_date = project['end_date']
    geoid = project['geoid']
    file_path = project['filepath']

    output_name = get_output_name(project_name)
    output_path = f"output/linked_{output_name}.csv"

    start_date = pd.Timestamp(start_date)
    end_date = pd.Timestamp(end_date)

    # Load dataset
    logger.info("Loading the patient dataset")
    df_address = pd.read_csv(file_path) 
    logger.info(f"Loaded patient dataset with {len(df_address)} records")

    logger.info("Casting patient dates")
    df_address.loc[:, 'ADDRESS_PERIOD_START'] = pd.to_datetime(df_address['ADDRESS_PERIOD_START'])
    df_address.loc[:, 'ADDRESS_PERIOD_END'] = pd.to_datetime(df_address['ADDRESS_PERIOD_END'])


    logger.info("Performing linkage")
    linkage_start = time.time()
    if target_db=='ucr':
        result = db_ucr.db_linkage(df_address, DB_URL, target_db, start_date, end_date, selection)
    else:
        result = db_fara_cbp.db_linkage(df_address, DB_URL, target_db, start_date, end_date, selection)
    linkage_end = time.time()

    run_time = linkage_end - linkage_start
    hours, rem = divmod(run_time, 3600)
    minutes, seconds = divmod(rem, 60)
    formatted_time = "{:0>2}:{:0>2}:{:05.2f}".format(int(hours),int(minutes),seconds)
    logger.info('Linkage finished in {}'.format(formatted_time))

    #result = link.process_data(start_date, end_date, selection_dict, geoid, file_path)
    logger.info(f"Writing the result to output/linked_{output_name}.csv")
    output_name = get_output_name(project_name)
    output_path = f"output/linked_{output_name}.csv"
    result.to_csv(output_path, index=False)

def show_available_exposomes():
    '''
    Displays all exposomes able to be linked against. Should output to the command line available variables in the Database
    '''
    engine = create_engine(f"sqlite:///{DB_URL}")

    # Reflect the database schema
    metadata = MetaData()
    metadata.reflect(bind=engine)
    print("-------------Available Exposomes-------------")
    
    with engine.connect() as connection:
        # Iterate through each table and print out float-type columns
        for table_name, table in metadata.tables.items():

            count = connection.execute(select(func.count()).select_from(table)).scalar()
            if count == 0:
                continue


            float_columns = [column.name for column in table.columns if isinstance(column.type, Float)]
            if float_columns:
                print(f"{table_name}:")
                for col in float_columns:
                    print(f"     {col}")
                    
    print("---------------------------------------------")

def main():

    #============= PARSER DECLARATIONS ==============
    # Define parser and command line arguments
    parser = argparse.ArgumentParser(prog='run_spacescans.py')
    subparser = parser.add_subparsers(dest='command')


    show_parse = subparser.add_parser('show')


    # Parsing for the patient dataset preprocess (Address_cleaning, etc)
    clean_parse = subparser.add_parser('clean_data', help='Process to perform data cleans on the patient dataset')
    clean_parse.add_argument(
        '-p','--project_name',
        default = '',
        required = True,
        help='Name of the project to run'
    )

    # Parsing for linkage
    link_parse = subparser.add_parser('link', help='Link the chosen exposomes to the submitted dataset')
    link_parse.add_argument(
        '-g','--geoid',
        required=False,
        help='Pass the geoidentifier used in the patient dataset (9-digit zipcode, 5-digit zipcode, etc)'
    )

    link_parse.add_argument(
        '-p','--project_name',
        default=None,
        required=False,
        help='Name of the project'
    )

    # Parsing for listing projects
    projects_parser = subparser.add_parser('projects', help='List all user created projects')

    # Parsing for creating a project
    create_proj_parser = subparser.add_parser('create_project', help='Create a new project to run linkage against')
    create_proj_parser.add_argument(
        '-p','--project_name',
        default=None,
        required=False,
        help='Name of the project'
    )
    create_proj_parser.add_argument(
        '-s','--start_date',
        default=None,
        required=False,
        help='Start date of the study period'
    )
    
    create_proj_parser.add_argument(
        '-e', '--end_date',
        default=None,
        required=False,
        help='End date of the study period'
    )

    create_proj_parser.add_argument(
        '-g','--geoid',
        default=None,
        required=False,
        help='The geoidentifier used in the patient dataset (9-digit zipcode, 5-digit zipcode, etc)'
    )

    create_proj_parser.add_argument(
        '-f','--filepath',
        default=None,
        required=False,
        help='The location of your patient dataset.'
    )
    #### NEED TO ADD FLAGS AND SUCH TO  LINK PARSER
    show_catalog = subparser.add_parser('catalog', help='Display the full exposome catalog available in the databases')
    #=========== END PARSER DECLARATIONS ============
    
    #================ BUILD LOGGER ==================
    log_directory = 'log_files'
    if not os.path.exists(log_directory):
        os.makedirs(log_directory)
    
    logger.setLevel(valid_log_levels['INFO'])

    now = datetime.now()
    formatted_date_time = now.strftime("%m-%d-%Y %H:%M:%S")
    log_file_path = os.path.join(log_directory, f'{formatted_date_time}.log')

    # Create a file handler
    file_handler = logging.FileHandler(log_file_path, 'w')
    file_handler.setLevel(logging.DEBUG)

    # Create a console handler
    console_handler = logging.StreamHandler()
    console_handler.setLevel(logging.DEBUG)

    # Create a formatter and add it to the handlers
    formatter = logging.Formatter('%(asctime)s %(levelname)s - %(message)s', datefmt='%Y-%m-%d %H:%M:%S')
    file_handler.setFormatter(formatter)
    console_handler.setFormatter(formatter)

    # Add the handlers to the logger
    logger.addHandler(file_handler)
    logger.addHandler(console_handler)
    #=============== END LOGGER BUILD ===============




    args = parser.parse_args()
    logger.debug(f'The following arguments were passed in: {args}')

    if args.command=='clean_data':
        if is_valid_project(args.project_name):
            run_address_cleaning(args.project_name)
        else:
            logger.error(f'Could not find project named \'{args.project_name}\'')
    elif args.command=='projects':
        list_projects()
    elif args.command=='create_project':
        build_project(args.project_name, args.start_date, args.end_date, args.geoid, args.filepath)
    elif args.command=='link':
        run_linkage(args.project_name)
    elif args.command=='show':
        show_available_exposomes()





if __name__=='__main__':
    main()