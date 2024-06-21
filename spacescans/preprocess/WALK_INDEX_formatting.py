from itertools import product

import pandas as pd

def read_raw_exposome():
    file_path = '~/EPA_SmartLocationDatabase_V3_Jan_2021_Final.csv'
    raw_walk_index = pd.read_csv(file_path)
    selected_columns = ['GEOID10', 'GEOID20', 'NatWalkInd']
    new_walk_index = raw_walk_index[selected_columns]
    return new_walk_index

def save_exposome(walk_index):
    walk_index.to_csv('~/formatted_walk_index.csv', index=False)

def main():
    new_walk_index = read_raw_exposome()
    save_exposome(new_walk_index)

if __name__== '__main__':
    main()