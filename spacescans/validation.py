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

    @field_validator('file_path')
    def must_be_csv(cls, value):
        path = Path(value)
        if path.suffix != '.csv':
            raise ValueError('File must be a .csv')
        return value


def validate_file_extension(file_path: str) -> bool:
    try:
        File_Validator(file_path=file_path)
        return True
    except ValidationError as e:
        print("Validation error:", e)
        return False

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
            return None  # Gracefully handle null or empty date strings

        try:
            parsed_date = parser.parse(date_str, dayfirst=False, fuzzy=False)
            return parsed_date
        except (ValueError, TypeError):
            return None  # Return None for invalid date formats

    def _is_date(self, string):
        # Exclude pure numbers with specific length checks
        if string.isdigit() and len(string) in [5, 9]:  # Common lengths for zip codes or numeric IDs
            return False
        if not any(char.isdigit() for char in string):  # Exclude strings without any digits
            return False

        try:
            parsed_date = self._parse_date(string)
            if parsed_date is not None:
                return True
        except AmbiguousDateFormatError:
            pass
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
            for idx, date_str in self.df[column].items():
                try:
                    parsed_date = self._parse_date(str(date_str))
                    if parsed_date:
                        # Save the date in the desired format mm/dd/yyyy
                        self.df.at[idx, column] = parsed_date.strftime('%m/%d/%Y')
                    else:
                        self.df.at[idx, column] = None
                except AmbiguousDateFormatError as e:
                    # Handle exceptions, ensuring they are relevant
                    if not pd.isnull(date_str) and date_str != '':
                        print(e)
                    self.df.at[idx, column] = None  # Set to None if ambiguous
        return self.df

    def save_to_csv(self):
        # Save the modified DataFrame back to the original CSV file
        self.df.to_csv(self.file_path, index=False)
        print(f"Updated CSV file saved as {self.file_path}")

########################################################################################################################




