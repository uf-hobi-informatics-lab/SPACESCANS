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

class DateProcessor:
    def __init__(self, file_path):
        self.file_path = file_path
        self.df = self._read_csv()
        self.date_columns = self._identify_date_columns()

    def _read_csv(self):
        # Step 1: Read the CSV file
        return pd.read_csv(self.file_path)

    def _parse_date(self, date_str):
        formats = [
            '%Y-%m-%d',
            '%d-%m-%Y',
            '%m-%d-%Y',
            '%d/%m/%Y',
            '%m/%d/%Y',
            '%Y/%m/%d'
        ]
        for fmt in formats:
            try:
                return datetime.strptime(date_str, fmt)
            except (ValueError, TypeError):
                continue
        try:
            return parser.parse(date_str)
        except (ValueError, TypeError):
            return None

    def _identify_date_columns(self):
        date_columns = []
        for column in self.df.columns:
            if self.df[column].apply(lambda x: self._parse_date(str(x)) is not None).any():
                date_columns.append(column)
        return date_columns

    def parse_dates(self):
        # Step 2: Determine the date format
        for column in self.date_columns:
            self.df[column] = self.df[column].apply(lambda x: self._parse_date(str(x)))
        return self.df

    def get_date_formats(self):
        date_formats = {}
        for column in self.date_columns:
            formats = self.df[column].dropna().apply(lambda x: x.strftime('%m/%d/%Y')).unique().tolist()
            date_formats[column] = formats
        return date_formats
########################################################################################################################




