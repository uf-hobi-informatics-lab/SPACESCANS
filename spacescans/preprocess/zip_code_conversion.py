# Buffers
# Tract: HUD, FARA, ACS, NATA
# BG: NDI, ACS, CACES, WI
# County: Crime
# ZCTA: ZBP

# Standardize all buffers to use headers: zip9, GEOID, value
# Standardize all exposomes to use headers: year, GEOID

import os
import sqlite3

import numpy as np
import pandas as pd

BUFFER_TO_EXPOSOME_MAP = {
    'tract': ['hud', 'fara', 'nata'], # + ACS
    'bg': ['caces', 'wi'], # + ACS, NDI
    'county': ['ucr'],
    'zcta': ['zbp']
}

INPUT_PATH = ''

def main():
    def get_area_weighted_average(group, var):
            sum1 = (group['value'] * group[var]).sum()
            if np.isnan(group['value'] * group[var]).sum(min_count=1):
                sum1 = np.nan
            sum2 = group['value'].sum()
            if sum2 == 0:
                sum2 = np.nan
            return sum1/sum2
    
    for buffer_type, exposome_list in BUFFER_TO_EXPOSOME_MAP.items():
        buffer_path = os.path.join(INPUT_PATH, 'buffers', 'buffer250to' + buffer_type + '_all.csv')
        buffer = pd.read_csv(buffer_path)

        for exposome in exposome_list:
            exposome_path = os.path.join(INPUT_PATH, 'exposomes', exposome + '.csv')
            exposome = pd.read_csv(exposome_path)
            variables = [x for x in list(exposome.columns) if x not in {'year', 'GEOID', 'qrt'}]

            exposome = pd.merge(buffer, exposome, how='inner', on='GEOID')

            if 'qrt' in exposome.columns:
                exposome['pid'] = exposome.groupby(['year', 'qrt', 'zip9']).ngroup()
            else:
                exposome['pid'] = exposome.groupby(['year', 'zip9']).ngroup()

            zip_df = exposome[['pid', 'zip9', 'year']].drop_duplicates()
            for var in variables:
                avg = exposome.groupby('pid').apply(get_area_weighted_average, var=var)
                zip_df = pd.merge(zip_df, pd.DataFrame({'pid':avg.index, var: avg.values}), on='pid')

            # save exposome

if __name__ == '__main__':
    main()