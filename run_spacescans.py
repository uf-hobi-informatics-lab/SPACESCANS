
import argparse
import logging
import os
import json
import pandas as pd
import subprocess

from datetime import datetime

from spacescans.dataclean import address_cleaning as addr

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
#=========== End Global Variables =========


#============ Helper Functions =================
def get_project_name(name='new_project'):
    if not os.path.exists(os.getcwd()+f'/projects/{name}.json'):
        return f'{name}'
    else:
        i = 1
        # Iterate until a project name does not exist
        while True:
            if not os.path.exists(os.getcwd()+f'/projects/{name}_{i}.json'):
                return f'{name}'
            i+=1

def is_valid_date(date):
    # Verify the correctness of the date
    #datetime.strptime(date, '%m/%d/%Y')
    try:
        datetime.strptime(date, '%m/%d/%Y')
        return True
    except ValueError:
        return False

def is_valid_project(project_name):
    

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
        print(f'{i}. {proj}')
        i+=1

def build_project(project_name, start_date, end_date, geoid, filepath):

    if project_name is None:
        project_name = input("Please enter a name for your project: ")
        project_name = get_project_name(project_name)
        # Pass the name to the function to handle duplicate project names

    # Get necessary values or validate passed in values
    if start_date is None:
        start_date = input("Please input a start date for the study period in the format mm/dd/yyyy: ")
        while not is_valid_date(start_date):
            start_date = input("Date invalid! Please input a start date for the study period in the format mm/dd/yyyy: ")
    else:
        # Then the user passed in a date, we still have to validate it
        while not is_valid_date(start_date):
            start_date = input("Date invalid! Please input a start date for the study period in the format mm/dd/yyyy: ")
    
    if end_date is None:
        end_date = input("Please input an end date for the study period in the format mm/dd/yyyy: ")
        while not is_valid_date(end_date):
            end_date = input("Date invalid! Please input an end date for the study period in the format mm/dd/yyyy: ")
    else:
        # Then the user passed in a date, we still have to validate it
        while not is_valid_date(end_date):
            end_date = input("Date invalid! Please input an end date for the study period in the format mm/dd/yyyy: ")

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

    print(f'Sucessfully created {project_name}.json in projects/!')
    logger.info("Successfully wrote out the project file")


def run_address_cleaning(project_name):

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

    outfile = project_name + '_cleaned_patient_data.csv'
    ldsz9_in_daterange.to_csv(os.path.join('output', outfile), index=False)



def main():
    # Set a default project name before loading an existing project from save
    project_name = get_project_name()

    # Define parser and command line arguments
    parser = argparse.ArgumentParser(prog='run_spacescans.py')
    subparser = parser.add_subparsers(dest='command')

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
        required=True,
        help='Pass the geoidentifier used in the patient dataset (9-digit zipcode, 5-digit zipcode, etc)'
    )

    # Parsing for listing projects
    projects_parser = subparser.add_parser('projects', help='List all user created projects')

    # Parsing for creating a project
    create_proj_parser = subparser.add_parser('create_project', help='Create a new project to run linkage against')
    create_proj_parser.add_argument(
        '-n','--project_name',
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
    print("")





    # Build logger with proper parameters - COME BACK TO THIS 
    log_directory = 'log_files'
    if not os.path.exists(log_directory):
        os.makedirs(log_directory)
    
    log_file_path = os.path.join(log_directory, f'{project_name}.log')
    args = parser.parse_args()
    # Handle args

    if args.command=='clean_data':
        run_address_cleaning(args.project_name)
    elif args.command=='projects':
        list_projects()
    elif args.command=='create_project':
        build_project(args.project_name, args.start_date, args.end_date, args.geoid, args.filepath)





if __name__=='__main__':
    main()