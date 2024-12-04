'''

    Script to handle data validation
    INCLUDING: file validation, date parsing, zip code validation, DataFrame Headers Validation

'''

from dateutil import parser
from datetime import datetime, date
from pydantic import BaseModel, field_validator, ValidationError
from pathlib import Path
import pandas as pd 
import re
"""

FILE EXTENSTION VALIDATION

"""
class File_Validator(BaseModel):
    file_path: str = None

    @field_validator('file_path', mode='before')
    def must_be_csv(cls, value):
        if value is None:
            return value
        path = Path(value)
        if path.suffix != '.csv':
            raise ValueError('File must be a .csv')
        return value

def validate_file_extension(file_path: str) -> bool:
    try:
        # Only validate the file path for being a CSV
        File_Validator(file_path=file_path)
        return True
    except ValidationError as e:
        print("Validation error:", e)
        return False
#############################################################################################
"""

ZIP CODE VALIDATION

"""

def detect_zip_code_columns(df: pd.DataFrame, zip_code_type: str):
    """
    Detect columns that potentially contain ZIP codes based on the specified ZIP code type.

    :param df: The DataFrame containing the CSV data.
    :param zip_code_type: The ZIP code format to validate ('5-digit' or '9-digit').
    :return: List of column names that potentially contain ZIP codes.
    """
    zip_code_pattern = r'^\d{5}$' if zip_code_type == '5-digit' else r'^\d{9}$'
    zip_columns = []

    for column in df.columns:
        # Convert the column to string, remove spaces, and handle NaN values by replacing them with an empty string
        column_str = df[column].astype(str).str.strip().fillna('')

        # Remove any non-numeric characters
        column_str = column_str.str.replace(r'\D', '', regex=True)

        # Check if all values in the column match the ZIP code pattern
        if column_str.str.match(zip_code_pattern).all():
            zip_columns.append(column)

    return zip_columns

def validate_zip_codes_in_file(file_path: str, zip_code_type: str) -> bool:
    """
    Validate all ZIP codes in a CSV file according to the specified ZIP code type.
    
    :param file_path: Path to the CSV file.
    :param zip_code_type: Expected ZIP code format ('5-digit' or '9-digit').
    :return: True if all ZIP codes match the specified type, False otherwise.
    """
    
    
    try:
        # Read the CSV file into a DataFrame
        df = pd.read_csv(file_path)
        
        # Print the first few rows of the DataFrame for debugging
        print("DataFrame Preview:")
        print(df.head())

        # Detect ZIP code columns
        zip_columns = detect_zip_code_columns(df, zip_code_type)

        if not zip_columns:
            print("No ZIP code columns detected matching the specified format.")
            return False

        zip_code_pattern = r'^\d{5}$' if zip_code_type == '5-digit' else r'^\d{9}$'
        
        for column in zip_columns:
            # Convert the column to string, strip spaces, and handle NaN values by replacing them with an empty string
            column_str = df[column].astype(str).str.strip().fillna('')
            column_str = column_str.str.replace(r'\D', '', regex=True)
            
            invalid_rows = df[~column_str.str.match(zip_code_pattern)]
            
            if not invalid_rows.empty:
                # Raise a ValidationError if invalid ZIP codes are found
                raise ValidationError(
                    f"Invalid ZIP code format detected in column '{column}':\n{invalid_rows}"
                )

        print("All ZIP codes in detected columns are valid.")
        return True

    except ValidationError as ve:
        print("Validation error:", ve)
        return False
    except Exception as e:
        print(f"Error reading file or validating ZIP codes: {e}")
        return False

    

########################################################################################################################

"""

DATE VALIDATION/PARSER

"""

class AmbiguousDateFormatError(Exception):
    pass

class DateProcessor:
    def __init__(self, file_path):
        self.file_path = file_path
        self.df = self._read_csv()
        self.date_columns = self._identify_date_columns()

    def _read_csv(self):
        # Load all columns as strings to prevent any automatic type conversion
        return pd.read_csv(self.file_path, dtype=str)

    
    def _identify_date_columns(self):
        #create a list to hold the date columns
        date_columns = []
        for column in self.df.columns:
            # Check if the column should be considered a date column
            # this code take the first 10 non null values to test to 
            # determine if the columns are dates and uses the _is_date() method. 
            # If dates are found, the column name is added to the list. 
            sample_values = self.df[column].dropna().astype(str).head(10)
            if sample_values.apply(self._is_date).any():
                date_columns.append(column)
        print(f"Identified date columns: {date_columns}")
        return date_columns

    def _is_date(self, value):
        # this method is used in the _identify_date_columns method to test for date values
        # the _parse_date method is used here to parse dates. 
        # if a field is null, then return false
        if pd.isnull(value) or value == '':
            return False
        # Exclude values that are purely numeric and have lengths typical of zip codes (5 or 9 digits)
        if value.isdigit() and len(value) in [5, 9]:
            return False
        try:
            self._parse_date(value)
            return True
        except AmbiguousDateFormatError:
            return False

    def _parse_date(self, date_str):
        if pd.isnull(date_str) or date_str == '':
            return None  # Handle null or empty date strings gracefully

        try:
            parsed_date = parser.parse(date_str, dayfirst=False, fuzzy=False)
            return parsed_date
        except (ValueError, TypeError):
            raise AmbiguousDateFormatError(f"Ambiguous or invalid date format: {date_str}")

    

    

    def parse_dates(self):
        for column in self.date_columns:
            self.df[column] = self.df[column].apply(self._format_date)
        return self.df

    def _format_date(self, date_str):
        try:
            parsed_date = self._parse_date(date_str)
            if parsed_date:
                return parsed_date.strftime('%m/%d/%Y')
        except AmbiguousDateFormatError:
            pass
        return None

########################################################################################################################

"""


DF HEADER VALIDATION


"""


def validate_df_headers(file_path):
    
    valid_headers = ['PATID', 'GEOID', 'START_DATE', 'END_DATE']
    
    df = pd.read_csv(file_path)
    headers = df.columns.to_list()
    #print(headers)

    try:
        validate = all(e in headers for e in valid_headers)
        if validate:
            print('All headers are valid')
        else:
            missing_headers = [e for e in valid_headers if e not in headers]
            raise ValueError(f'Missing required headers: {missing_headers}')
    except ValueError as e:
        # Handling the raised error
        print(f"Error: {e}")

    


    

