import argparse 
import yaml
import json

def parse_args():
    """
    Parse command-line arguments for the application.
    Returns:
        argparse.Namespace: Parsed command-line arguments.
    """
    parser = argparse.ArgumentParser(description="Running applicatiion")

    # Configuration file argument
    parser.add_argument(
        "-c", 
        "--config", 
        type=str, 
        help="Path to the configuration file.")
    
    parser.add_argument(
        "--json", 
        type=str, 
        help="Path to JSON configuration file.")

    # Individual parameters
    parser.add_argument(
        '--data_list', 
        nargs='+', 
        help="List of raw data file paths or parent directory (overrides config file).")
    parser.add_argument(
        '--output_dir',
        type=str,
        help="Directory for output files (overrides config file).")
    parser.add_argument(
        '--exposome_type',
        type=str,
        choices=['caces', 'wi', 'fara', 'hud', 'nata', 'ucr', 'acag', 'zbp'], 
        help="Type of the exposome data. Choose from: caces, wi, fara, hud, nata, ucr, acs, acag, zbp")
    parser.add_argument(
        '--buffer_dir',
        type=str,
        help="Directory for buffer files (overrides config file).")
    parser.add_argument('--project_name', type=str, help="Project name for output.")
    parser.add_argument('--target_start', type=str, help="Start date (YYYY-MM-DD).")
    parser.add_argument('--target_end', type=str, help="End date (YYYY-MM-DD).")
    parser.add_argument('--select_var', nargs='+', help="List of variables to extract.")
    parser.add_argument('--db_url', help="URL of exposome database file.")

    return parser.parse_args()

def load_config(config_file):
    """
    Load configuration from a YAML file.
    Args:
        config_file (str): Path to the configuration file.
    Returns:
        dict: Configuration as a dictionary.
    """
    try:
        with open(config_file, "r") as file:
            return yaml.safe_load(file)
    except FileNotFoundError:
        raise FileNotFoundError(f"Configuration file {config_file} not found.")
    except yaml.YAMLError as e:
        raise ValueError(f"Error parsing YAML file: {e}")

def load_json_config(json_file):
    with open(json_file, "r") as file:
        return json.load(file)

def parse_args_with_defaults():
    """
    Combine command-line arguments with defaults from a configuration file.
    Command-line arguments take precedence over configuration values.
    Returns:
        dict: Final configuration combining command-line arguments and config file.
    """
    args = parse_args()

    # Load configuration file if provided
    config = {}
    if args.json:
        config = load_json_config(args.json)
    elif args.config:
        config = load_config(args.config)
    
    # Override config with command-line arguments if provided
    
    if args.data_list is not None:
        config["data_list"] = args.data_list
    elif "data_list" in config and config["data_list"] is not None:
        config["data_list"] = config["data_list"]
        
    # exposome_type is flexabl for formatted_exposome.py
    if args.exposome_type is not None:
        config["exposome_type"] = args.exposome_type
    elif "exposome_type" in config and config["exposome_type"] is not None:
        config["exposome_type"] = config["exposome_type"]

    if args.buffer_dir is not None:
        config["buffer_dir"] = args.buffer_dir
    elif "buffer_dir" in config and config["buffer_dir"] is not None:
        config["buffer_dir"] = config["buffer_dir"]
    

    if args.output_dir is not None:
        config["output_dir"] = args.output_dir

    if args.project_name is not None:
        config["project_name"] = args.project_name
    if args.target_start is not None:
        config["target_start"] = args.target_start
    if args.target_end is not None:
        config["target_end"] = args.target_end
    if args.select_var is not None:
        config["select_var"] = args.select_var
    if args.db_url is not None:
        config["db_url"] = args.db_url
        
    return config