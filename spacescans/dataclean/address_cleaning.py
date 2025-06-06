"""Script for cleaning address histories to prepare then for exposome linkage.

    Clean address histories by removing rows with null zip9 information,
    removing gaps and overlaps, removing duplicates, and covering the
    entire study period.

    Usage:
        clean_addresses.py [-x] -z=<zip_code_file> -a=<address_file> -o=<output_path> -n=<project_name> -s=<start_date> -e=<end_date>
        clean_addresses.py [-x] -c <config_file>

    Arguments:
        -x                  Extend address timeframes to cover project period.
        -z=<zip_code_file>  CSV file containing zip9s, longitude, and latitude.
                            Used for limiting the address history to only
                            addresses we have positional information for.
                            Must have the following headers: AREAKEY, X, Y
        -a=<address_file>   CSV file containing cohort address history.
                            Must have the following headers:
                            PATID, ADDRESSID, ADDRESS_ZIP9,
                            ADDRESS_PERIOD_START, ADDRESS_PERIOD_END
        -o=<output_path>    Path to where to save final output file.
        -n=<project_name>   Prefix used when naming the output file.
                            Must have no spaces.
        -s=<start_date>     Start date of linkage period. Use YYYY-MM-DD format.
                            Addresses from before this date will be deleted,
                            and the final output will start from this date.
        -e=<end_date>       End date of linkage period. Use YYYY-MM-DD format.
                            Addresses from after this date will be deleted,
                            and the final output will end at this date.
        -c <config_file>    Use config file for required arguments.
                            See config_template.yaml for file structure.
"""

import datetime
import os
import logging
import time

import numpy as np
import pandas as pd
from docopt import docopt

logger = logging.getLogger(__name__)

def validate_csvs(lds: pd.DataFrame, zip9: pd.DataFrame):
    headers = {'lds': {'PATID', 'ADDRESSID', 'ADDRESS_ZIP9',
                       'ADDRESS_PERIOD_START', 'ADDRESS_PERIOD_END'},
                'zip9': {'AREAKEY', 'X', 'Y'}}
    if not headers['lds'].issubset(lds.columns):
        raise ValueError('Address history file does not contain required headers. Must contain: ' +
                         ', '.join(headers['lds']))
    if not headers['zip9'].issubset(zip9.columns):
        raise ValueError('Zip code file does not contain required headers. Must contain: ' +
                         ', '.join(headers['zip9']))
    return


def parse_date(date_string):
    try:
        formatted_date = datetime.date(int(date_string.split('-')[0]),
                                       int(date_string.split('-')[1]),
                                       int(date_string.split('-')[2]))
    except (IndexError, ValueError):
        raise ValueError('Malformed date. Check start and end date.')

    return formatted_date

def indexer(data: pd.DataFrame):
    """Return lookup list for index of rows by patient id.

    Args:
        data: Address history for study cohort, sorted by patient id.

    Returns:
        A list of tuples containing patient id, index of first value for
        patient, and number of rows for patient.
    """
    # Add a null row to the dataframe so that the final patid is caught
    null_row = pd.Series([None] * len(data.columns), index=data.columns)
    data = pd.concat([data, null_row.to_frame().T], ignore_index=True)

    indices = []

    # Initialize the curr_patid value with the first patid
    curr_patid = data.iloc[0,0]

    # Initialize the indexing variable and the length tracking variable
    # num_entries intialized to 0 in case else executes on first iteration
    idx = 0
    num_entries = 0

    while idx < len(data):
        if data.iloc[idx,0] != curr_patid:
            # If the current entry does not match current patid, update
            indices.append([curr_patid, idx-num_entries, num_entries])
            num_entries = 1
            curr_patid = data.iloc[idx, 0]
        else:
            #Otherwise, record another valid entry
            num_entries = num_entries + 1

        idx += 1
    return indices


def filter_good_zip9s(lds: pd.DataFrame, zip9: pd.DataFrame):
    """Filter address history to rows with a zip9 with positional information.

    Args:
        lds: Address history for the study cohort.
        zip9: Zip9s with positional information.

    Returns:
        A filtered dataframe with patient ids and address information.
    """
    # Filter null zip9s
    lds.loc[lds['ADDRESS_ZIP9'] == 'NULL', 'ADDRESS_ZIP9'] = np.nan
    ldszip9 = lds[lds['ADDRESS_ZIP9'].notna()]

    # Remove zip9s without positional information
    ldszip9 = pd.merge(ldszip9,zip9,how="left",left_on="ADDRESS_ZIP9",
                           right_on="AREAKEY")
    ldszip9 = ldszip9[ldszip9['X'].notna()]

    # Remove extraneous columns and prepare columns for further processing
    ldszip9 = ldszip9.rename(columns={'ID':'PATID'})

    ldszip9 = ldszip9[['PATID', 'ADDRESSID', 'ADDRESS_ZIP9',
                       'ADDRESS_PERIOD_START', 'ADDRESS_PERIOD_END']]

    ldszip9.loc[lds['ADDRESS_PERIOD_START'] == 'NULL', 'ADDRESS_PERIOD_START'] = np.nan
    ldszip9.loc[lds['ADDRESS_PERIOD_END'] == 'NULL', 'ADDRESS_PERIOD_END'] = np.nan

    ldszip9["ADDRESS_PERIOD_START"] = pd.to_datetime(ldszip9["ADDRESS_PERIOD_START"]).dt.date
    ldszip9["ADDRESS_PERIOD_END"] = pd.to_datetime(ldszip9["ADDRESS_PERIOD_END"]).dt.date

    return ldszip9


def find_ids_with_missingness(ldszip9: pd.DataFrame):
    """Save on processing time by finding IDs that need correction.

        Args:
            ldszip9: Filtered dataframe with patient ids and address info.

        Returns:
            An array of IDs that have at least one start or end date
            missing in their address history.
    """
    ldsz9 = ldszip9[['PATID', 'ADDRESS_PERIOD_START', 'ADDRESS_PERIOD_END']].sort_values(by=['PATID', 'ADDRESS_PERIOD_START'])
    ldsz9["any_missing"] = ldsz9.apply(lambda row: int(pd.isnull(row['ADDRESS_PERIOD_START'])
                                                       or pd.isnull(row['ADDRESS_PERIOD_END'])), axis=1)

    missing_count = ldsz9.groupby(['PATID'])['any_missing'].sum()
    idv = missing_count.index[missing_count>0].tolist()

    return idv


def fix_nulls(ldszip9: pd.DataFrame, missing_ids: list, proj_start_date, proj_end_date):
    """Clean rows with null start or end dates.

        Args:
            ldszip9: Filtered dataframe with patient ids and address info.
            missing_ids: List of ids with missing date information.
            project: Object with information about the study.

        Returns:
            A dataframe with no null start or end dates.
    """
    ldsz9_no_nulls = ldszip9[~ldszip9['PATID'].isin(missing_ids)]
    logger.debug('DF initialized with non-missing ids.')

    # Index patids
    ldszip9 = ldszip9.sort_values(by=['PATID', 'ADDRESS_PERIOD_START', 'ADDRESS_PERIOD_END'])
    logger.debug('DF sorted')
    indexed_patids = indexer(ldszip9)
    logger.debug('DF indexed')
    id_set = set(missing_ids)
    logger.debug('List to set')
    indexed_patids_with_missingness = [x for x in indexed_patids if x[0] in id_set]
    logger.debug('ID list created')

    num_missing_patids = len(indexed_patids_with_missingness)
    i = 0
    start = time.time()
    logger.debug('ID with missingness count: ' + str(num_missing_patids))
    while i < num_missing_patids:
        # Build pt_history using indexer
        start = indexed_patids_with_missingness[i][1]
        end = indexed_patids_with_missingness[i][2] + start
        pt_history = ldszip9[start:end]

        for index, row in pt_history.iterrows():
            logger.debug('patid: ' + row['PATID'] + ', zip: ' + row['ADDRESS_ZIP9'] + ', start: ' + str(row['ADDRESS_PERIOD_START']) + ', end: ' + str(row['ADDRESS_PERIOD_END']))
            start_date = row['ADDRESS_PERIOD_START']
            end_date = row['ADDRESS_PERIOD_END']

            # Scenario 1 (née 3): Start and end are both missing
            if pd.isnull(start_date) and pd.isnull(end_date):
                if len(pt_history)==1:
                    # If it is the only entry, make it cover the study period
                    pt_history.loc[index, 'ADDRESS_PERIOD_START'] = proj_start_date
                    pt_history.loc[index, 'ADDRESS_PERIOD_END'] = proj_end_date
                else:
                    # Otherwise, mark the entry for deletion
                    pt_history.loc[index, 'PATID'] = None
            # Scenario 2: Start is missing, end is nonmissing
            elif pd.isnull(start_date):
                sooner_end_dates = list(pt_history.loc[pt_history['ADDRESS_PERIOD_END'] < end_date,
                                        'ADDRESS_PERIOD_END'])

                if len(sooner_end_dates) > 0:
                    #Check that this is not the earliest entry
                    pt_history.loc[index, 'ADDRESS_PERIOD_START'] = (max(sooner_end_dates) +
                                                                      datetime.timedelta(days = 1))
                else:
                    #If it is the earliest entry, just update to project timeframe
                    pt_history.loc[index, 'ADDRESS_PERIOD_START'] = proj_start_date
            # Scenario 3 (née 1): End is missing, start is nonmissing
            elif pd.isnull(end_date):
                later_start_dates = list(pt_history.loc[
                    (pt_history['ADDRESS_PERIOD_START'] > start_date) & (pt_history['ADDRESS_PERIOD_START'].notna()),
                    'ADDRESS_PERIOD_START'])

                if len(later_start_dates) > 0:
                    #Check that this is not the latest entry
                    pt_history.loc[index, 'ADDRESS_PERIOD_END'] = (min(later_start_dates) -
                                                                    datetime.timedelta(days = 1))
                else:
                    #If it is the latest entry, just update to project timeframe
                    pt_history.loc[index, 'ADDRESS_PERIOD_END'] = proj_end_date

        # Remove marked-for-deletion entries by filtering null patids
        pt_history = pt_history[pt_history['PATID'].notna()]

        ldsz9_no_nulls = pd.concat([ldsz9_no_nulls, pt_history], ignore_index = True)
        i += 1

    return ldsz9_no_nulls


def fix_gaps_overlaps_dupes(ldsz9_no_nulls: pd.DataFrame):
    """Remove gaps, remove duplicates, and fix gaps in history.

        Args:
            ldsz9_no_nulls: Dataframe with no null start or end dates.

        Returns:
            A dataframe with no duplicate addresses, no gaps, and no
            overlaps in address history.
    """
    idv = ldsz9_no_nulls['PATID'].unique()
    ldsz9_no_nulls = ldsz9_no_nulls.sort_values(by=['PATID', 'ADDRESS_PERIOD_START', 'ADDRESS_PERIOD_END'])
    # empty numpy array to hold final data
    ldsz9_continuous = np.empty((0, 4), dtype=object)

    for pid in idv:
        pt_history = ldsz9_no_nulls.loc[ldsz9_no_nulls['PATID'] == pid, ['PATID', 'ADDRESS_ZIP9', 'ADDRESS_PERIOD_START', 'ADDRESS_PERIOD_END']]
        pt_history = pt_history.to_numpy()

        if len(pt_history) > 1:
            i = 1
            while i < len(pt_history):
                logger.debug('patid: ' + pt_history[i-1,0] + ', zip: ' + str(pt_history[i-1,1]) + ' and ' + str(pt_history[i,1]) + ', start: ' + str(pt_history[i-1,2]) + ' and ' + str(pt_history[i,2]) + ', end: ' + str(pt_history[i-1,3]) + ' and ' + str(pt_history[i,3]))
                start1, start2 = np.datetime64(pt_history[i-1, 2]), np.datetime64(pt_history[i, 2])
                end1, end2 = np.datetime64(pt_history[i-1, 3]), np.datetime64(pt_history[i, 3])
                zip1, zip2 = pt_history[i-1, 1], pt_history[i, 1]

                if zip1 == zip2:
                    # Scenario 1: same zip code
                    pt_history[i-1, 2] = min(start1, start2)
                    pt_history[i-1, 3] = max(end1, end2)
                    pt_history = np.delete(pt_history, i, axis=0)
                elif start2 > start1 and start2 <= end1:
                    # Scenario 2: overlap of addresses periods
                    temp1 = np.array([pid, zip1, start1, start2 - np.timedelta64(1, 'D')], dtype=object)
                    temp2 = np.array([pid, zip2, start2, end2], dtype=object)
                    pt_history[i-1] = temp1
                    pt_history[i] = temp2
                    i += 1
                elif start2 > start1 and start2 > (end1 + 1):
                    # Scenario 3: A gap between the 2 periods
                    mid = start2 + (end1 - start2) / 2
                    temp1 = np.array([pid, zip1, start1, mid - np.timedelta64(1, 'D')], dtype=object)
                    temp2 = np.array([pid, zip2, mid, end2], dtype=object)
                    pt_history[i-1] = temp1
                    pt_history[i] = temp2
                    i += 1
                elif start1 == start2:
                    # Scenario 4: Same start
                    pt_history[i-1] = np.array([pid, zip2, start2, end2], dtype=object)
                    pt_history = np.delete(pt_history, i, axis=0)
                else:
                    i += 1

        ldsz9_continuous = np.vstack((ldsz9_continuous, pt_history))

    ldsz9_continuous = pd.DataFrame(ldsz9_continuous, columns=['PATID', 'ADDRESS_ZIP9', 'ADDRESS_PERIOD_START', 'ADDRESS_PERIOD_END'])
    return ldsz9_continuous


def limit_timeframe(ldsz9_continuous: pd.DataFrame, proj_start_date, proj_end_date, expand_patient_timeframe=False):
    """Limit time frame of history to study period.

        Args:
            ldsz9_continuous: Address history with no nulls, duplicates, gaps,
                              or overlaps.
            project: Object with information about the study.
            expand_patient_timeframe: Boolean indicating if earliest start and
                        latest end date should be expanded to cover the full
                        study period.

        Returns:
            A dataframe with a cleaned address history limited to the study
            period.
    """
    #print('Timeframe expansion: ' + str(expand_patient_timeframe))
    ldsz9_continuous = ldsz9_continuous.sort_values(by=['ADDRESS_PERIOD_START'])

    indices_to_drop = []
    # Check rows against start date
    for index, row in ldsz9_continuous.iterrows():
        if row['ADDRESS_PERIOD_END'] < proj_start_date:
            indices_to_drop.append(index)
        elif row['ADDRESS_PERIOD_START'] > proj_start_date:
            break

    # Check rows against end date
    for index, row in ldsz9_continuous[::-1].iterrows():
        if row['ADDRESS_PERIOD_START'] > proj_end_date:
            indices_to_drop.append(index)
        else:
            break

    ldsz9_in_daterange = ldsz9_continuous.drop(indices_to_drop)
    ldsz9_in_daterange = ldsz9_in_daterange.sort_values(by=['PATID', 'ADDRESS_PERIOD_START', 'ADDRESS_PERIOD_END'])
    indexed_patids = indexer(ldsz9_in_daterange)

    i=0
    while i < len(indexed_patids):
        start = indexed_patids[i][1]
        end = indexed_patids[i][2] + start - 1

        if ldsz9_in_daterange.iloc[start]['ADDRESS_PERIOD_START'] < proj_start_date:
                ldsz9_in_daterange.iloc[start]['ADDRESS_PERIOD_START'] = proj_start_date
        elif (expand_patient_timeframe and
            ldsz9_in_daterange.iloc[start]['ADDRESS_PERIOD_START'] > proj_start_date):
                ldsz9_in_daterange.iloc[start]['ADDRESS_PERIOD_START'] = proj_start_date

        if ldsz9_in_daterange.iloc[end]['ADDRESS_PERIOD_END'] > proj_end_date:
                ldsz9_in_daterange.iloc[end]['ADDRESS_PERIOD_END'] = proj_end_date
        elif (expand_patient_timeframe and
            ldsz9_in_daterange.iloc[end]['ADDRESS_PERIOD_END'] < proj_end_date):
                ldsz9_in_daterange.iloc[end]['ADDRESS_PERIOD_END'] = proj_end_date

        i += 1

    return ldsz9_in_daterange

'''
def main(args):
    if args['-c']:
        project = fill_from_config(args['-c'])
    else:
        project = fill_from_args(args)

    if args['-x']:
        expand_patient_timeframe = True
    else:
        expand_patient_timeframe = False

    if not os.path.isdir(project.output_path):
        raise ValueError('Output path does not exist (' + project.output_path + ')')

    try:
        lds = pd.read_csv(project.address_file, converters = {'ADDRESS_ZIP9': str})
    except FileNotFoundError:
        raise FileNotFoundError('Address history file does not exist (' + project.address_file + ')')
    try:
        zip9 = pd.read_csv(project.zip_code_file, converters = {'AREAKEY': str})
    except FileNotFoundError:
        raise FileNotFoundError('Zip code file does not exist (' + project.zip_code_file + ')')
    validate_csvs(lds, zip9)

    print('start log')
    logging.basicConfig(format= "%(asctime)s||%(name)s||%(message)s",filename='address_clean4.log', filemode='a', level=logging.INFO)
    ldszip9 = filter_good_zip9s(lds, zip9)
    ids_with_missingness = find_ids_with_missingness(ldszip9)

    logging.info('Data accepted. Starting cleaning.')
    ldsz9_no_nulls = fix_nulls(ldszip9, ids_with_missingness, project)
    logging.info('fix_nulls() finsihed.')
    ldsz9_continuous = fix_gaps_overlaps_dupes(ldsz9_no_nulls)
    logging.info('fix_gaps_overlaps() finsihed.')
    ldsz9_in_daterange = limit_timeframe(ldsz9_continuous, project, expand_patient_timeframe)
    logging.info('limit_timeframe() finsihed.')

    outfile = project.project_name + '_cleaned_lds.csv'
    ldsz9_in_daterange.to_csv(os.path.join(project.output_path, outfile), index=False)


if __name__ == '__main__':
    args = docopt(__doc__)
    main(args)
'''