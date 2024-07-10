'''

    Script to handle data validation

'''

from dateutil import parser
from datetime import datetime

def get_date_object(date_string): 
    return parser.parse(date_string)