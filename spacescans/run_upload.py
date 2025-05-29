from database.zip_9.db_session import get_session, create_tables, drop_table, create_table
from database.functions import insert_from_csv
from database.zip_9.models import usda_fara

import argparse
import logging
import pandas as pd


#============== Globals ================
session = get_session()
logger = logging.getLogger()

valid_log_levels = {
    'DEBUG':logging.DEBUG,
    'INFO':logging.INFO,
    'WARN':logging.WARN,
    'ERROR':logging.ERROR,
    'CRITICAL':logging.CRITICAL
}


main():
    parser = argparse.ArgumentParser(prog='db_upload')

    parser.add_argument(
        '-l','--log_level',
        default = 'INFO',
        help='Set the logging level (DEBUG, INFO, WARN, ERROR, CRITICAL)'
    )

    parser.add_argument(
        '-s', '--server',
        required = True,
        help='Address of the database for upload'
    )

    parser.add_argument(
        '-f', '--file_name',
        required = True,
        help='The absolute path of the file being uploaded'
    )

    parser.add_argument(
        '-t', '--target',
        required=True,
        help='The name of the target table this file should be uploaded to'
    )
    args = parser.parse_args()
    log_level = args.log_level.upper()
    file_name = args.file_name
    target_table = args.target
    server_address = args.server

    #============ Build logger
    logger.setLevel(valid_log_levels[log_level])

    #Set a logging subdirectory and check if it exists
    log_directory = 'upload_logs'
    if not os.path.exists(log_directory):
        os.makedirs(log_directory)

    # Define the log file's path within the newly ensured directory
    log_file_path = os.path.join(log_directory, '{}.log'.format(session_id))

    # Create a file handler
    file_handler = logging.FileHandler(log_file_path, 'w')
    file_handler.setLevel(valid_log_levels[log_level])

    # Create a console handler
    console_handler = logging.StreamHandler()
    console_handler.setLevel(valid_log_levels[log_level])

    # Create a formatter and add it to the handlers
    formatter = logging.Formatter('%(asctime)s %(levelname)s - %(message)s', datefmt='%Y-%m-%d %H:%M:%S')
    file_handler.setFormatter(formatter)
    console_handler.setFormatter(formatter)

    # Add the handlers to the logger
    logger.addHandler(file_handler)
    logger.addHandler(console_handler)
    #=========================




if __name__=='__main__':
main()