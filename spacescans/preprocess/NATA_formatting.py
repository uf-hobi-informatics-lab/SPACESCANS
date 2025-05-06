from args import parse_args_with_defaults
import pandas as pd
import pyreadr
from pathlib import Path

def read_raw_exposome(raw_data_path: str) -> pd.DataFrame:
    """
    Read a single .rds file into a DataFrame and pivot to wide format.
    
    Args:
        raw_data_path: Path to the input .rds file.
        
    Returns:
        A DataFrame in wide format with pollutants as columns.
    """
    print(f"Loading RDS file from: {raw_data_path}")
    result = pyreadr.read_r(raw_data_path)
    nata_df = list(result.values())[0]

    if not {'TRACT', 'EXPTOT', 'polid'}.issubset(nata_df.columns):
        raise ValueError("Required columns ('TRACT', 'EXPTOT', 'polid') not found in the data.")

    nata_df = nata_df[['TRACT', 'EXPTOT', 'polid']]
    nata_df['YEAR'] = 2014

    df_wide = nata_df.pivot_table(index=['TRACT', 'YEAR'], columns='polid', values='EXPTOT').reset_index()
    df_wide.rename(columns={'TRACT': 'FIPS'}, inplace=True)
    return df_wide

def save_exposome(exposome: pd.DataFrame, output_dir: str) -> None:
    """
    Save the exposome DataFrame to CSV format.

    Args:
        exposome: The DataFrame to save.
        output_dir: The directory where the output CSV will be saved.
    """
    output_path = Path(output_dir) / "formatted_nata.csv"
    print(f"Saving formatted data to: {output_path}")
    exposome.to_csv(output_path, index=False)

def main(raw_data_path: str, output_dir: str) -> None:
    print(">>> Reading raw NATA data...")
    nata_df = read_raw_exposome(raw_data_path)

    print(">>> Saving final formatted data...")
    save_exposome(nata_df, output_dir)

    print(">>> Process completed successfully.")

if __name__ == '__main__':
    args = parse_args_with_defaults()
    print(">>> Application Starting...")
    main(args["data_list"][0], args["output_dir"])
    print(">>> Application Finished.")

'''
python /home/cwang6/scripts/NATA_formatting.py --data_list /home/cwang6/data/original/nata14_all175HAP.Rda --output_dir /home/cwang6/data/output/test/
'''