from database.zip_9.db_session import get_session, create_tables, drop_table, create_table
from database.functions import insert_from_csv
from database.zip_9.models import *
import pandas as pd

session = get_session()


# example: '/Users/allison.burns/Desktop/exposome/UCR/preprocess_ucr.csv'
source_path = input("Please enter the location of the file you want to upload: ")
# example: 'ucr'
target = input("Please enter the exposome name: ")

def upload(target, file_path):
    source = pd.read_csv(file_path)

    if target == 'usda_fara':
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
    

        drop_table(ucr)
        print("Dropped UCR table.")
        create_table(ucr)
        print("Created UCR table.")

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

    elif target == 'caces':
        drop_table(caces)
        print("Dropped CACES table.")
        create_table(caces)
        print("Created CACES table.")

        columns_to_read = ["zip9", "year", "pm25", "pm10", "co", "no2", "so2", "o3"]
        available_columns = [col for col in columns_to_read if col in source.columns]

        # Identify missing columns and add them with default float 0.0
        missing_columns = set(columns_to_read) - set(available_columns)
        if missing_columns:
            print(f"Warning: The following columns were not found in the DataFrame and will be added with default 0.0: {missing_columns}")
            for col in missing_columns:
                source[col] = 0.0
            # Update available_columns list now that missing columns have been added
            available_columns = columns_to_read

        # Select and order columns as specified in columns_to_read
        source = source[available_columns]

        # Convert specific columns to float, ensuring missing values are replaced with 0.0
        columns_to_convert = ["pm25", "pm10", "co", "no2", "so2", "o3"]
        for column in columns_to_convert:
            if column in source.columns:
                source[column] = source[column].fillna(0).astype(float)
                
        # Ensure essential columns don't have NaN values
        source = source.dropna(subset=["zip9", "year"])

    elif target=='us_hud':
    
        drop_table(us_hud)
        print("Dropped US_HUD table.")
        create_table(us_hud)
        print("Created US_HUD table.")

        # Expected columns in the us_hud schema.
        columns_to_read = [
            "ZIP_9", "YEAR", "QUARTER", "VAC", "AVG_VAC", "VAC_3", "VAC_3TO6", "VAC_6TO12",
            "VAC_12TO24", "VAC_24TO36", "VAC_36", "PQV_IS", "PQV_NOSTAT", "NOSTAT", "AVG_NOSTAT",
            "NS_3", "NS_3TO6", "NS_6TO12", "NS_12TO24", "NS_24TO36", "NS_36", "PQNS_IS", "P_VAC",
            "P_VAC_3", "P_VAC_3TO6", "P_VAC_6TO12", "P_VAC_12TO24", "P_VAC_24TO36", "P_VAC_36",
            "P_PQV_IS", "P_PQV_NOSTAT", "P_NOSTAT", "P_NS_3", "P_NS_3TO6", "P_NS_6TO12",
            "P_NS_12TO24", "P_NS_24TO36", "P_NS_36", "P_PQNS_IS", "AMS_RES", "AMS_BUS", "AMS_OTH",
            "RES_VAC", "BUS_VAC", "OTH_VAC", "AVG_VAC_R", "AVG_VAC_B", "VAC_3_RES", "VAC_3_BUS",
            "VAC_3_OTH", "VAC_3_6_R", "VAC_3_6_B", "VAC_3_6_O", "VAC_6_12R", "VAC_6_12B", "VAC_6_12O",
            "VAC_12_24R", "VAC_12_24B", "VAC_12_24O", "VAC_24_36R", "VAC_24_36B", "VAC_24_36O",
            "VAC_36_RES", "VAC_36_BUS", "VAC_36_OTH", "PQV_IS_RES", "PQV_IS_BUS", "PQV_IS_OTH",
            "PQV_NS_RES", "PQV_NS_BUS", "PQV_NS_OTH", "NOSTAT_RES", "NOSTAT_BUS", "NOSTAT_OTH",
            "AVG_NS_RES", "AVG_NS_BUS", "NS_3_RES", "NS_3_BUS", "NS_3_OTH", "NS_3_6_RES",
            "NS_3_6_BUS", "NS_3_6_OTH", "NS_6_12_R", "NS_6_12_B", "NS_6_12_O", "NS_12_24_R",
            "NS_12_24_B", "NS_12_24_O", "NS_24_36_R", "NS_24_36_B", "NS_24_36_O", "NS_36_RES",
            "NS_36_BUS", "NS_36_OTH", "PQNS_IS_R", "PQNS_IS_B", "PQNS_IS_O", "P_AMS_RES",
            "P_AMS_BUS", "P_AMS_OTH", "P_RES_VAC", "P_BUS_VAC", "P_OTH_VAC", "P2_RES_VAC",
            "P2_BUS_VAC", "P2_OTH_VAC", "P_VAC_3_RES", "P_VAC_3_BUS", "P_VAC_3_OTH",
            "P2_VAC_3_RES", "P2_VAC_3_BUS", "P2_VAC_3_OTH", "P_VAC_3_6_R", "P_VAC_3_6_B",
            "P_VAC_3_6_O", "P2_VAC_3_6_R", "P2_VAC_3_6_B", "P2_VAC_3_6_O", "P_VAC_6_12R",
            "P_VAC_6_12B", "P_VAC_6_12O", "P2_VAC_6_12R", "P2_VAC_6_12B", "P2_VAC_6_12O",
            "P_VAC_12_24R", "P_VAC_12_24B", "P_VAC_12_24O", "P2_VAC_12_24R", "P2_VAC_12_24B",
            "P2_VAC_12_24O", "P_VAC_24_36R", "P_VAC_24_36B", "P_VAC_24_36O", "P2_VAC_24_36R",
            "P2_VAC_24_36B", "P2_VAC_24_36O", "P_VAC_36_RES", "P_VAC_36_BUS", "P_VAC_36_OTH",
            "P2_VAC_36_RES", "P2_VAC_36_BUS", "P2_VAC_36_OTH", "P_PQV_IS_RES", "P_PQV_IS_BUS",
            "P_PQV_IS_OTH", "P2_PQV_IS_RES", "P2_PQV_IS_BUS", "P2_PQV_IS_OTH", "P_PQV_NS_RES",
            "P_PQV_NS_BUS", "P_PQV_NS_OTH", "P2_PQV_NS_RES", "P2_PQV_NS_BUS", "P2_PQV_NS_OTH",
            "P_NOSTAT_RES", "P_NOSTAT_BUS", "P_NOSTAT_OTH", "P2_NOSTAT_RES", "P2_NOSTAT_BUS",
            "P2_NOSTAT_OTH", "P_NS_3_RES", "P_NS_3_BUS", "P_NS_3_OTH", "P2_NS_3_RES",
            "P2_NS_3_BUS", "P2_NS_3_OTH", "P_NS_3_6_RES", "P_NS_3_6_BUS", "P_NS_3_6_OTH",
            "P2_NS_3_6_RES", "P2_NS_3_6_BUS", "P2_NS_3_6_OTH", "P_NS_6_12_R", "P_NS_6_12_B",
            "P_NS_6_12_O", "P2_NS_6_12_R", "P2_NS_6_12_B", "P2_NS_6_12_O", "P_NS_12_24_R",
            "P_NS_12_24_B", "P_NS_12_24_O", "P2_NS_12_24_R", "P2_NS_12_24_B", "P2_NS_12_24_O",
            "P_NS_24_36_R", "P_NS_24_36_B", "P_NS_24_36_O", "P2_NS_24_36_R", "P2_NS_24_36_B",
            "P2_NS_24_36_O", "P_NS_36_RES", "P_NS_36_BUS", "P_NS_36_OTH", "P2_NS_36_RES",
            "P2_NS_36_BUS", "P2_NS_36_OTH", "P_PQNS_IS_R", "P_PQNS_IS_B", "P_PQNS_IS_O",
            "P2_PQNS_IS_R", "P2_PQNS_IS_B", "P2_PQNS_IS_O", "AVG_VAC_O", "AVG_NS_OTH"
        ]

        # Process the CSV file in chunks.
        for chunk in pd.read_csv(file_path, chunksize=100000):
            # Identify missing columns in the current chunk.
            available_columns = [col for col in columns_to_read if col in chunk.columns]
            missing_columns = set(columns_to_read) - set(available_columns)
            if missing_columns:
                # Create missing columns with default 0.0 in one go.
                new_cols = pd.DataFrame({col: 0.0 for col in missing_columns}, index=chunk.index)
                chunk = pd.concat([chunk, new_cols], axis=1).copy()
            
            # Reorder columns to match the expected schema.
            chunk = chunk[columns_to_read]
            
            # Convert non-key columns to float.
            key_columns = {"ZIP_9", "YEAR", "QUARTER"}
            float_columns = [col for col in columns_to_read if col not in key_columns]
            for col in float_columns:
                chunk[col] = chunk[col].fillna(0).astype(float)
            
            # Drop rows that are missing any primary key values.
            chunk = chunk.dropna(subset=["ZIP_9", "YEAR", "QUARTER"])
            
            insert_from_csv(chunk, target, session)
            print(f"Processed chunk with {len(chunk)} records.")

    elif target == 'national_walkability_index':
        
        drop_table(national_walkability_index)
        print("Dropped National Walkability Index table.")
        create_table(national_walkability_index)
        print("Created National Walkability Index table.")

        columns_to_read = ["ZIP_9", "YEAR", "WALKABILITY"]
        available_columns = [col for col in columns_to_read if col in source.columns]

        # Identify missing columns and add them with default float 0.0
        missing_columns = set(columns_to_read) - set(available_columns)
        if missing_columns:
            print(f"Warning: The following columns were not found in the DataFrame and will be added with default 0.0: {missing_columns}")
            for col in missing_columns:
                source[col] = 0.0
            # Update available_columns list now that missing columns have been added
            available_columns = columns_to_read

        # Select and order columns as specified in columns_to_read
        source = source[available_columns]

        # Convert specific columns to float, ensuring missing values are replaced with 0.0
        columns_to_convert = ["WALKABILITY"]
        for column in columns_to_convert:
            if column in source.columns:
                source[column] = source[column].fillna(0).astype(float)
                
        # Ensure essential columns don't have NaN values
        source = source.dropna(subset=["ZIP_9", "YEAR"])


        


    if target != 'us_hud':
        insert_from_csv(source, target, session)