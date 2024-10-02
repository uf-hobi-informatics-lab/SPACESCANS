import pyreadr
import pandas as pd

# Path to read in .rda file, this will need to change
input_rda_file = '/Users/allison.burns/Desktop/exposome/CBP/zbp_nationwide_0521.rda'

# Read the .rda file using pyreadr
result = pyreadr.read_r(input_rda_file)

# Below will extract the dataframe 
for key in result.keys():
    df = result[key]
    
    # Column 0 in this df is zip 5 and needs to be sure to pad the zeros at the beginning.
    df.columns = df.columns.str.upper()
    column_name = df.columns[0]
    df[column_name] = df[column_name].astype(str).apply(lambda x: x.zfill(5))
    df = df.astype({'YEAR': int})

    # Output location for now, this will need to change
    output_csv_file = '/Users/allison.burns/Desktop/exposome/CBP/formatted_zbp.csv'
    
    # Save the DF as CSV 
    df.to_csv(output_csv_file, index=False, quoting=0)  

    #testing only, do we need this?
    print(f"Saved {output_csv_file}")
