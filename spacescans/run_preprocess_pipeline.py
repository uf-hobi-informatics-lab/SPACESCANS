import argparse
import logging
import subprocess
import sys
import os
from preprocess.args import parse_args_with_defaults

# Configure the logging
def setup_logging(logfile):
    logger = logging.getLogger(__name__)
    logger.setLevel(logging.DEBUG)

    # File handler for logging
    fh = logging.FileHandler(logfile)
    fh.setLevel(logging.DEBUG)

    # Console handler for printing to stdout
    ch = logging.StreamHandler(sys.stdout)
    ch.setLevel(logging.INFO)

    # Formatter for log messages
    formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
    fh.setFormatter(formatter)
    ch.setFormatter(formatter)

    logger.addHandler(fh)
    logger.addHandler(ch)

    return logger

# Run a command and log its output
def run_command(command, logger):
    try:
        logger.info(f"Running: {command}")
        result = subprocess.run(command, shell=True, check=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        logger.info(f"Output: {result.stdout.decode()}")
    except subprocess.CalledProcessError as e:
        logger.error(f"Error running {command}: {e.stderr.decode()}")

# Mapping exposome types to corresponding scripts
def run_exposome_preprocessing(raw_data_paths, output_dir, exposome_type, buffer_dir, logger):
    exposome_type = exposome_type.lower()
    
    if len(raw_data_paths) > 1:
        joined_paths = ' '.join([f'"{path}"' for path in raw_data_paths])
    else: 
        joined_paths = f'"{raw_data_paths[0]}"'
        
    if exposome_type == 'caces':
        run_command(f'python preprocessing/CACES_formatting.py --data_list {joined_paths} --output_dir {output_dir}', logger)
        run_command(f'python preprocessing/block_group_translator.py --exposome_type {exposome_type} --output_dir {output_dir} --buffer_dir {buffer_dir}', logger)
    elif exposome_type == 'wi':
        run_command(f'python preprocessing/WALK_INDEX_formatting_2021.py --data_list {joined_paths} --output_dir {output_dir}', logger)
        run_command(f'python preprocessing/block_group_translator.py --exposome_type {exposome_type} --output_dir {output_dir} --buffer_dir {buffer_dir}', logger)
    elif exposome_type == 'fara':
        run_command(f'python preprocessing/FARA_formatting.py --data_list {joined_paths} --output_dir {output_dir}', logger)
        run_command(f'python preprocessing/census_tract_translator.py --exposome_type {exposome_type} --output_dir {output_dir} --buffer_dir {buffer_dir}', logger)
    elif exposome_type == 'hud':
        run_command(f'python preprocessing/HUD_formatting.py --data_list {joined_paths} --output_dir {output_dir}', logger)
        run_command(f'python preprocessing/census_tract_translator.py --exposome_type {exposome_type} --output_dir {output_dir} --buffer_dir {buffer_dir}', logger)
    elif exposome_type == 'nata':
        run_command(f'python preprocessing/NATA_formatting.py --data_list {joined_paths} --output_dir {output_dir}', logger)
        run_command(f'python preprocessing/census_tract_translator.py --exposome_type {exposome_type} --output_dir {output_dir} --buffer_dir {buffer_dir}', logger)
    elif exposome_type == 'ucr':
        run_command(f'python preprocessing/UCR_formatting.py --data_list {joined_paths} --output_dir {output_dir}', logger)
        run_command(f'python preprocessing/county_translator.py --exposome_type {exposome_type} --output_dir {output_dir} --buffer_dir {buffer_dir}', logger)
    elif exposome_type == 'acs':
        run_command(f'python preprocessing/ACS_formatting.py --data_list {joined_paths} --output_dir {output_dir}', logger)
        run_command(f'python preprocessing/census_tract_translator.py --exposome_type {exposome_type} --output_dir {output_dir} --buffer_dir {buffer_dir}', logger)
    elif exposome_type == 'acag':
        run_command(f'python preprocessing/ACAG_formatting.py --data_list {joined_paths} --output_dir {output_dir}', logger)
        run_command(f'python preprocessing/long_lat_translator.py --exposome_type {exposome_type} --output_dir {output_dir} --buffer_dir {buffer_dir}', logger)
    elif exposome_type == 'zbp':
        run_command(f'python preprocessing/ZBP_formatting.py --data_list {joined_paths} --output_dir {output_dir}', logger)
        run_command(f'python preprocessing/zcta_translator.py --exposome_type {exposome_type} --output_dir {output_dir} --buffer_dir {buffer_dir}', logger)


# Main preprocessing pipeline function
def preprocess_pipeline(raw_data_paths, output_dir, exposome_type, buffer_dir):
    logger = setup_logging('preprocessing_pipeline.log')
    logger.info(f"Starting preprocessing pipeline for exposome type: {exposome_type}")

    if not isinstance(raw_data_paths, list):
        logger.error("raw_data_paths should be a list of file paths.")
        return
        
    for path in raw_data_paths:
        if not os.path.exists(path):
            logger.error(f"Raw data path does not exist: {path}")
            return

    run_exposome_preprocessing(raw_data_paths, output_dir, exposome_type, buffer_dir, logger)
    logger.info(f"Completed preprocessing pipeline for {exposome_type}")

if __name__ == '__main__':

    args = parse_args_with_defaults()
    preprocess_pipeline(args["data_list"], args["output_dir"], args["exposome_type"], args["buffer_dir"])