import argparse
import logging
import subprocess
import sys
import os

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
def run_exposome_preprocessing(exposome_type, raw_data_path,logger):
    exposome_type = exposome_type.lower()
    if exposome_type == 'caces':
        run_command(f'python CACES_formatting.py {raw_data_path}', logger)
        run_command(f'python block_group_translator.py {raw_data_path}', logger)
    elif exposome_type == 'walk_index':
        run_command(f'python WALK_INDEX_formatting.py {raw_data_path}', logger)
        run_command(f'python block_group_translator.py {raw_data_path}', logger)
    elif exposome_type == 'fara':
        run_command(f'python FARA_formatting.py {raw_data_path}', logger)
        run_command(f'python census_tract_translator.py {raw_data_path}', logger)
    elif exposome_type == 'hud':
        run_command(f'python HUD_formatting.py {raw_data_path}', logger)
        run_command(f'python census_tract_translator.py {raw_data_path}', logger)
    elif exposome_type == 'nata':
        run_command(f'python NATA_formatting.py {raw_data_path}', logger)
        run_command(f'python census_tract_translator.py {raw_data_path}', logger)
    elif exposome_type == 'ucr':
        run_command(f'python UCR_formatting.py {raw_data_path}', logger)
        run_command(f'python census_tract_translator.py {raw_data_path}', logger)
    elif exposome_type == 'acs':
        run_command(f'python ACS_formatting.py {raw_data_path}', logger)
        run_command(f'python census_tract_translator.py {raw_data_path}', logger)
    else:
        logger.error(f"Unknown exposome type: {exposome_type}")

# Main preprocessing pipeline function
def preprocess_pipeline(raw_data_path, exposome_type):
    # Set up logging to a file
    logger = setup_logging('preprocessing_pipeline.log')
    
    # Logging the start of the preprocessing
    logger.info(f"Starting preprocessing pipeline for exposome type: {exposome_type}")
    
    # Check if the raw data path exists
    if not os.path.exists(raw_data_path):
        logger.error(f"Raw data path does not exist: {raw_data_path}")
        return
    
    # Run the preprocessing and translation
    run_exposome_preprocessing(exposome_type,raw_data_path,  logger)
    
    logger.info(f"Completed preprocessing pipeline for {exposome_type}")
    


if __name__ == '__main__':
    # Argument parser for command line input
    parser = argparse.ArgumentParser(description="Preprocessing pipeline for exposome data.")
    parser.add_argument('raw_data_path', type=str, help="Path to the raw exposome data")
    parser.add_argument('exposome_type', type=str, help="Type of the exposome data (for now, CACES, FARA, HUD, NATA, UCR, WI.)")
    
    args = parser.parse_args()

    # Run the pipeline
    preprocess_pipeline(args.raw_data_path, args.exposome_type)
