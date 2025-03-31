import pandas as pd
import geopandas as gpd
import fiona
from args import parse_args_with_defaults

def read_gdb(raw_data_path):
    
    gdb_path = raw_data_path # Path to the .gdb file

    # List all layers in the file
    layers = fiona.listlayers(gdb_path)
    print("Layers:", layers)

    # Load a specific layer
    layer_name = layers[0]
    gdf = gpd.read_file(gdb_path, layer=layer_name)

    # Display the data
    print(gdf.head())
    
    return gdf


def read_raw_exposome(raw_data_path):

    raw_walk_index = read_gdb(raw_data_path)
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
    new_walk_index.rename(columns={'combined_columns': 'FIPS'}, inplace=True)
    
    new_walk_index['YEAR'] = 2021

    return new_walk_index

def save_exposome(walk_index, output_dir):
    walk_index.to_csv(output_dir + 'formatted_wi_2021.csv', index=False)

def main(raw_data_path,output_dir):
    
    new_walk_index = read_raw_exposome(raw_data_path)
    save_exposome(new_walk_index, output_dir)
    print("Walkability Index formatting completed!")

if __name__== '__main__':
    args = parse_args_with_defaults()
    main(args["data_list"][0],args["output_dir"])
