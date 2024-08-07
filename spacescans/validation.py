'''

    Script to handle data validation

'''

from dateutil import parser
from datetime import datetime, date
from pydantic import BaseModel, field_validator, ValidationError
from pathlib import Path
import pandas as pd 


########################################################################################################################

"""file validation class"""

class File_Validator(BaseModel):
    file_path: str
    #zip_code: str

    @field_validator('file_path')
    def must_be_csv(cls, value):
        path = Path(value)
        if path.suffix != '.csv':
            raise ValueError('File must be a .csv')
        return value
    
    """@field_validator('zip_code')
    def validate_zip(cls, value):
        if value not in {'5-digit', '9-digit'}:
            raise ValueError("ZIP code type must be '5-digit' or '9-digit'")
        return value"""


def validate_file_extension(file_path: str) -> bool:
    try:
        File_Validator(file_path=file_path)
        return True
    except ValidationError as e:
        print("Validation error:", e)
        return False

"""def validate_zip_code(zip_code: str) -> bool:
    try:
        File_Validator(zip_code=zip_code)
        return True
    except ValidationError as e:
        print("Validation error: ", e)
        return False"""
########################################################################################################################

"""date validation classes

still in progress .... """

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

    def _parse_date(self, date_str):
        if pd.isnull(date_str) or date_str == '':
            return None  # Handle null or empty date strings gracefully

        try:
            parsed_date = parser.parse(date_str, dayfirst=False, fuzzy=False)
            return parsed_date
        except (ValueError, TypeError):
            raise AmbiguousDateFormatError(f"Ambiguous or invalid date format: {date_str}")

    def _is_date(self, value):
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

    def _identify_date_columns(self):
        date_columns = []
        for column in self.df.columns:
            # Check if the column should be considered a date column
            sample_values = self.df[column].dropna().astype(str).head(10)
            if sample_values.apply(self._is_date).any():
                date_columns.append(column)
        print(f"Identified date columns: {date_columns}")
        return date_columns

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




