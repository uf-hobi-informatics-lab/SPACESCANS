from itertools import product

import pandas as pd

def read_raw_exposome():
    file_path = '/Users/allison.burns/Desktop/exposome/WALKABILITY/EPA_SmartLocationDatabase_V3_Jan_2021_Final.csv'
    raw_walk_index = pd.read_csv(file_path)
    selected_columns = ['GEOID10', 'GEOID20', 'NatWalkInd']
    new_walk_index = raw_walk_index[selected_columns]

    #force the columns to be int only for the first two geo id columns
    #fillna() converts any empty strings to zero so that the int conversion works. 
    new_walk_index = new_walk_index.fillna(0).astype({'GEOID10': int, 'GEOID20': int})
    return new_walk_index

def save_exposome(walk_index):
    walk_index.to_csv('/Users/allison.burns/Desktop/exposome/WALKABILITY/formatted_walk_index.csv', index=False)

def main():
    new_walk_index = read_raw_exposome()
    save_exposome(new_walk_index)

if __name__== '__main__':
    main()