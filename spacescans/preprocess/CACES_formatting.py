from itertools import product
import pandas as pd
import sys
import argparse
from args import parse_args_with_defaults

def read_raw_exposome(raw_data_path):
    raw_caces = pd.read_csv(raw_data_path) 
    return raw_caces

def translate_exposome(raw_caces):
    fips_list = list(raw_caces['fips'].unique())
    year_list = list(raw_caces['year'].unique())
    pol_list = list(raw_caces['pollutant'].unique())

    new_caces = pd.DataFrame(data=list(product(fips_list, year_list)), columns=['fips', 'year'])
    new_caces = new_caces.reindex(columns=['fips', 'year']+pol_list)

    def get_pols(fips, year, pol, df):
        y = df.loc[(df['fips']==fips) & (df['year']==year) & (df['pollutant']==pol), 'pred_wght']
        return y.values.tolist()[0]

    for pol in pol_list:
        new_caces[pol] = new_caces.apply(lambda x: get_pols(x['fips'], x['year'], pol, raw_caces), axis=1)
    new_caces.columns = new_caces.columns.str.upper()           
    return new_caces

def save_exposome(caces, csv_file_path):
    caces.to_csv(csv_file_path + 'formatted_caces.csv', index=False)

def main(raw_data_path, csv_file_path):

    raw_caces = read_raw_exposome(raw_data_path)
    new_caces = translate_exposome(raw_caces)
    save_exposome(new_caces, csv_file_path)

if __name__== '__main__':

    args = parse_args_with_defaults()

    print("\nApplication Running...")
    main(args["data_list"][0],args["output_dir"])   