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

    def _read_csv(self):
        # Step 1: Read the CSV file
        return pd.read_csv(self.file_path)

    def _parse_date(self, date_str):
        try:
            return parser.parse(date_str)
        except ValueError:
            return None

    def parse_dates(self):
        # Step 2: Determine the date format
        self.df['date1'] = self.df['date1'].apply(self._parse_date)
        self.df['date2'] = self.df['date2'].apply(self._parse_date)
        return self.df
    



########################################################################################################################




