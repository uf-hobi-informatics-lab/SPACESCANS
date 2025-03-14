"""THE WALKABILITY INDEX FORMATTED FILE IS CREATED BY DOING THE FOLLOWING STEPS:
1 - DOWNLOAD THE RAW WALKABILITY INDEX CSV FILE FROM HERE: https://catalog.data.gov/dataset/walkability-index7
2 - USE THIS SCRIPT TO GENERATE THE GEOID AND NATWALKIND FROM THE RAW .CSV FILE. 
3 - THE RAW FILE HAS GEOID10 AND GEOID20 POPULATED IN SCIENTIFIC NOTATATION WHICH DOES NOT WORK FOR OUR NEEDS.  
    IN ORDER TO GET THE CORRECT GEOID OR BLOCK GROUP, THE SCRIPT BELOW
    COMBINES THE STATEFP (STATE FIPS), COUNTYFP (COUNTY FIPS), TRACTCE, AND BLKGRPCE INTO ONE GEOID. """

from itertools import product
import pandas as pd

def read_raw_exposome():
    file_path = '/Users/allison.burns/Desktop/exposome/WALKABILITY/EPA_SmartLocationDatabase_V3_Jan_2021_Final.csv'
    raw_walk_index = pd.read_csv(file_path)
    selected_columns = ['STATEFP', 'COUNTYFP', 'TRACTCE', 'BLKGRPCE', 'NatWalkInd']
    
    # add the approrpiate number of zeros for padding so that this creates the correctly formatted GEOID
    raw_walk_index['STATEFP'] = raw_walk_index['STATEFP'].astype(str).str.zfill(2)
    raw_walk_index['COUNTYFP'] = raw_walk_index['COUNTYFP'].astype(str).str.zfill(3)
    raw_walk_index['TRACTCE'] = raw_walk_index['TRACTCE'].astype(str).str.zfill(6)
    raw_walk_index['BLKGRPCE'] = raw_walk_index['BLKGRPCE'].astype(str).str.zfill(1)
    
    
    # Combine the FIPS info columns into one column to create the GEOID
    raw_walk_index['combined_columns'] = (
    raw_walk_index['STATEFP'] + 
    raw_walk_index['COUNTYFP'] + 
    raw_walk_index['TRACTCE'] + 
    raw_walk_index['BLKGRPCE']
)


    new_walk_index = raw_walk_index[['combined_columns', 'NatWalkInd']]
    new_walk_index = new_walk_index.copy()  
    new_walk_index.rename(columns={'combined_columns': 'GEOID10'}, inplace=True)

    return new_walk_index

def save_exposome(walk_index):
    walk_index.to_csv('/Users/allison.burns/Desktop/exposome/WALKABILITY/formatted_walk_index_TEST.csv', index=False)

def main():
    new_walk_index = read_raw_exposome()
    save_exposome(new_walk_index)

if __name__== '__main__':
    main()