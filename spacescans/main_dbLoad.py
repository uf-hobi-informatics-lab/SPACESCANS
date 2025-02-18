from database.zip_9.db_session import get_session, create_tables, drop_table, create_table
from database.functions import insert_from_csv
from database.zip_9.models import usda_fara
import pandas as pd

session = get_session()


# example: '/Users/allison.burns/Desktop/exposome/UCR/preprocess_ucr.csv'
source = input("Please enter the location of the file you want to upload: ")
# example: 'ucr'
target = input("Please enter the exposome name: ")

if target == 'usda_fara':
    source = pd.read_csv(source, nrows=10000)
    # Drop and recreate User table
    drop_table(usda_fara)
    print("Dropped FARA table.")
    create_table(usda_fara)
    print("Created FARA table.")
    
    
    columns_to_read = ["ZIP_9", "YEAR", "LA1AND20", "LASENIORS20", "LASENIORSHALFSHARE", "LAHISP20SHARE", "LASNAP20SHARE", "LALOWI05_10",
                       "LATRACTS10","LAKIDS10SHARE","LAPOP1SHARE","LATRACTS_HALF","LAHUNV1SHARE","LAAIAN20SHARE","LASNAPHALFSHARE",
                       "LABLACKHALFSHARE","LASNAP1SHARE","TRACTAIAN","LAPOPHALFSHARE","TRACTASIAN","LAHISP1SHARE","LABLACK20SHARE","LANHOPI1",
                       "LATRACTS1","LAAIAN10","LAHISP20","TRACTNHOPI","LATRACTSVEHICLE_20","LASNAP10","LAPOP10SHARE","LAAIAN1","TRACTHISPANIC",
                       "LAHISP10","TRACTLOWI","LAWHITEHALF","LAHUNV20","LAPOP1_10","URBAN","LAPOP1_20","LAWHITE10","LAASIAN10SHARE","LALOWI20SHARE",
                       "LAWHITE20SHARE","LAOMULTIR1","LABLACK1SHARE","LANHOPI10","LAHISP1","LAPOP05_10","LASENIORS1","LAASIAN20","PCTGQTRS",
                       "LAOMULTIR10SHARE","LAKIDS10","LAPOPHALF","LALOWIHALFSHARE","LAAIAN10SHARE","LAHISPHALFSHARE","LAWHITEHALFSHARE","LALOWI20",
                       "LALOWI10SHARE","LANHOPIHALFSHARE","LAHUNV1","LAHUNVHALF","LAOMULTIR20SHARE","LABLACK10SHARE","LAWHITE1SHARE","LAKIDSHALFSHARE",
                       "LAASIANHALFSHARE","LAPOP1","LAASIAN10","LASENIORSHALF","POVERTYRATE","LALOWI1_20","MEDIANFAMILYINCOME","LALOWI1_10","OHU2010",
                       "LABLACK1","TRACTSNAP","LA1AND10","LALOWIHALF","LAHISPHALF","GROUPQUARTERSFLAG","LASNAPHALF","LAAIAN20","LALOWI1","LAHALFAND10",
                       "LANHOPI20","LASENIORS1SHARE","LAOMULTIR10","LAPOP10","LAKIDSHALF","LILATRACTS_VEHICLE","LAPOP20SHARE","LILATRACTS_1AND10",
                       "LANHOPIHALF","LAKIDS20SHARE","LANHOPI1SHARE","LAHUNV20SHARE","LAASIAN20SHARE","LAHUNV10SHARE","LAOMULTIRHALF","LAHUNVHALFSHARE",
                       "LANHOPI10SHARE","TRACTKIDS","TRACTBLACK","LASENIORS10","LASENIORS10SHARE","LILATRACTS_1AND20","TRACTWHITE","LAWHITE10SHARE",
                       "LILATRACTS_HALFAND10", "LAKIDS1","LASNAP1","LAASIAN1SHARE","LAHUNV10","LALOWI1SHARE","LALOWI10","TRACTOMULTIR","LASNAP20","LATRACTS20",
                       "LAAIAN1SHARE", "TRACTHUNV","LAOMULTIR20", "LAAIANHALFSHARE","LOWINCOMETRACTS","LAASIAN1","NUMGQTRS","LAPOP20","LAOMULTIR1SHARE",
                       "LAAIANHALF","LAOMULTIRHALFSHARE", "LAKIDS1SHARE","LABLACK10", "LAKIDS20","LASENIORS20SHARE","LAHISP10SHARE","LAWHITE1","TRACTSENIORS",
                       "HUNVFLAG","LANHOPI20SHARE","LASNAP10SHARE","LAASIANHALF","LAWHITE20", "LABLACK20","LABLACKHALF"]
    available_columns = [col for col in columns_to_read if col in source.columns]
    
    # If there are any columns that are not found in the DataFrame, log or handle it
    missing_columns = set(columns_to_read) - set(available_columns)
    if missing_columns:
        print(f"Warning: The following columns were not found in the DataFrame and will be ignored: {missing_columns}")
    
    source = source[available_columns]
    
    columns_to_convert = ["LA1AND20","LASENIORS20","LASENIORSHALFSHARE", "LAHISP20SHARE", "LASNAP20SHARE","LALOWI05_10",
                       "LATRACTS10","LAKIDS10SHARE","LAPOP1SHARE","LATRACTS_HALF","LAHUNV1SHARE","LAAIAN20SHARE","LASNAPHALFSHARE",
                       "LABLACKHALFSHARE","LASNAP1SHARE","TRACTAIAN","LAPOPHALFSHARE","TRACTASIAN","LAHISP1SHARE","LABLACK20SHARE","LANHOPI1",
                       "LATRACTS1","LAAIAN10","LAHISP20","TRACTNHOPI","LATRACTSVEHICLE_20","LASNAP10","LAPOP10SHARE","LAAIAN1","TRACTHISPANIC",
                       "LAHISP10","TRACTLOWI","LAWHITEHALF","LAHUNV20","LAPOP1_10","URBAN","LAPOP1_20","LAWHITE10","LAASIAN10SHARE","LALOWI20SHARE",
                       "LAWHITE20SHARE","LAOMULTIR1","LABLACK1SHARE","LANHOPI10","LAHISP1","LAPOP05_10","LASENIORS1","LAASIAN20","PCTGQTRS",
                       "LAOMULTIR10SHARE","LAKIDS10","LAPOPHALF","LALOWIHALFSHARE","LAAIAN10SHARE","LAHISPHALFSHARE","LAWHITEHALFSHARE","LALOWI20",
                       "LALOWI10SHARE","LANHOPIHALFSHARE","LAHUNV1","LAHUNVHALF","LAOMULTIR20SHARE","LABLACK10SHARE","LAWHITE1SHARE","LAKIDSHALFSHARE",
                       "LAASIANHALFSHARE","LAPOP1","LAASIAN10","LASENIORSHALF","POVERTYRATE","LALOWI1_20","MEDIANFAMILYINCOME","LALOWI1_10","OHU2010",
                       "LABLACK1","TRACTSNAP","LA1AND10","LALOWIHALF","LAHISPHALF","GROUPQUARTERSFLAG","LASNAPHALF","LAAIAN20","LALOWI1","LAHALFAND10",
                       "LANHOPI20","LASENIORS1SHARE","LAOMULTIR10","LAPOP10","LAKIDSHALF","LILATRACTS_VEHICLE","LAPOP20SHARE","LILATRACTS_1AND10",
                       "LANHOPIHALF","LAKIDS20SHARE","LANHOPI1SHARE","LAHUNV20SHARE","LAASIAN20SHARE","LAHUNV10SHARE","LAOMULTIRHALF","LAHUNVHALFSHARE",
                       "LANHOPI10SHARE","TRACTKIDS","TRACTBLACK","LASENIORS10","LASENIORS10SHARE","LILATRACTS_1AND20","TRACTWHITE","LAWHITE10SHARE",
                       "LILATRACTS_HALFAND10", "LAKIDS1","LASNAP1","LAASIAN1SHARE","LAHUNV10","LALOWI1SHARE","LALOWI10","TRACTOMULTIR","LASNAP20","LATRACTS20",
                       "LAAIAN1SHARE", "TRACTHUNV","LAOMULTIR20", "LAAIANHALFSHARE","LOWINCOMETRACTS","LAASIAN1","NUMGQTRS","LAPOP20","LAOMULTIR1SHARE",
                       "LAAIANHALF","LAOMULTIRHALFSHARE", "LAKIDS1SHARE","LABLACK10", "LAKIDS20","LASENIORS20SHARE","LAHISP10SHARE","LAWHITE1","TRACTSENIORS",
                       "HUNVFLAG","LANHOPI20SHARE","LASNAP10SHARE","LAASIANHALF","LAWHITE20", "LABLACK20","LABLACKHALF"]
    for column in columns_to_convert:
        if column in source.columns:
            source[column] = source[column].fillna(0).astype(float)

    # I added this code because the model requires a primary key and therefore we can't have duplicate zip 9's. 
      
    source = source.dropna(subset=['ZIP_9', 'YEAR'])
    



elif target == 'ucr':
    source = pd.read_csv(source, nrows=10000)
    columns_to_read = ["ZIP_9", "YEAR", "p_murder","p_fso","p_rob", "p_assault", "p_burglary", "p_larceny", "p_mvt"]
    available_columns = [col for col in columns_to_read if col in source.columns]
    
    # If there are any columns that are not found in the DataFrame, log or handle it
    missing_columns = set(columns_to_read) - set(available_columns)
    if missing_columns:
        print(f"Warning: The following columns were not found in the DataFrame and will be ignored: {missing_columns}")
    
    source = source[available_columns]
        
    
    columns_to_convert = ["p_murder","p_fso","p_rob", "p_assault", "p_burglary", "p_larceny", "p_mvt"]
    for column in columns_to_convert:
        if column in source.columns:
            source[column] = source[column].fillna(0).astype(float)
          
    #Drop rows with missing critical values
    source = source.dropna(subset=['ZIP_9', 'YEAR'])

elif target == 'cbp':
    source = pd.read_csv(source, nrows=10000)  
    
    columns_to_read = ["ZIP_9", "YEAR", "religious", "civic", "business", "political", "professional", "labor", "bowling", "recreational", "golf", "sports"]
    available_columns = [col for col in columns_to_read if col in source.columns]
    
    # If there are any columns that are not found in the DataFrame, log or handle it
    missing_columns = set(columns_to_read) - set(available_columns)
    if missing_columns:
        print(f"Warning: The following columns were not found in the DataFrame and will be ignored: {missing_columns}")
    
    source = source[available_columns]
        
    columns_to_convert = ["religious","civic","business", "political", "professional", "labor", "bowling", "recreational", "golf", "sports"]
    for column in columns_to_convert:
        if column in source.columns:
            source[column] = source[column].fillna(0).astype(float)
    source = source.dropna(subset=['ZIP_9', 'YEAR'])




insert_from_csv(source, target, session)


