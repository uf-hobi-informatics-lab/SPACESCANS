README: Running the Preprocessing Pipeline for Exposome Linkage
This document provides instructions on how to run the preprocess_pipeline.py script to transform a raw dataset into a unified, linkage-ready exposome dataset.

1. Command Line Usage
To run the preprocess_pipeline.py script, use the following command syntax:

python preprocess_pipeline.py "/raw_data_path" type

2. Command Explanation
Script Name: preprocess_pipeline.py
The first argument specifies the path to the raw data file.
The second argument indicates the type of exposome data being processed, which includes CACES, FARA, HUD, NATA, UCR, and WI.
Select the appropriate type based on the data being processed.

3. Output
Finally, there will be two datasets generated, formatted_{exposome}.csv and preprocess_{exposome}.csv

4. Example
Run the following command to process a NATA-type dataset:

python preprocess_pipeline.py "/blue/bianjiang/cwang6/exposome/original/nata2014v2_national_cancerrisk_by_tract_poll.xlsx" NATA

This command will initiate the preprocess_pipeline.py script, preprocess the specified raw data file, and generate a preprocess_nata.csv file.
