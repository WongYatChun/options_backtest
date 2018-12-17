import glob
import os
import sys
import pandas as pd 
from distutils.util import strtobool

# All recognized fields by the library are defined in the tuples below. Structs are used
# to map headers from source data to one of the recognized fields.
# The second item of each tuple defines if that field is required or not
# [The third item of each tuple defines the expected value type of the field. This
# is used internally in the library and should not be changed.
# The fourth item of each tuple defines if the field is affected by ratios] (not yet implemented)

fields = ( # required field for the imported dataset
    # basic information
    ('call_put',True), # `option_type'`
    ('date', True), # 'quote date
    ('maturity_date', True), # expiration
    ('option_symbol', False),
    ('underlying_symbol', False),
    ('strike', True),
    # market price
    ('bid', False),
    ('ask', False),
    ('last', False),
    ('underlying_price', True),
    ('implied_vol', False),
    # greeks, but maybe not necessary at this point
    ('delta', True),
    ('gamma', True),
    ('theta', True),
    ('vega', True),
    ('rho', False),
    ('event_day', False),
    ('day_to_event', False),
)

def timeFormatter(df): # format_option_df
    return(
        df.assign(
            date = lambda x : pd.to_datetime(x['date'], unit = 'D', format = "%Y-%m-%d"),
            
            maturity_date = lambda x : pd.to_datetime(x['maturity_date'], unit = 'D', format = "%Y-%m-%d"),
            call_put = lambda x : x['call_put'].str.lower().str[ : 1],
            dtm = lambda x : (x['maturity_date'] - x['date']).dt.days, # day to maturity
        )   
        .round(2)
    )

def _check_field_is_standard(struct):
    # Check:
    # 1) if the de-zipped field name == the original zipped field name
    # 2) if the value of the zipped field is valid
    std_fields = list(zip(*fields))[0]
    for f in struct:
        if f[0] not in std_fields or f[1] < 0:

            raise ValueError("Field names or field indices not valid!")
    return True

def _check_field_is_duplicated(cols):
    # if we have any duplicated column indices, then invalid
    if len(list(set(cols[1]))) != len(cols[1]):
        raise ValueError('We have duplicated indices, it does not make sense!')
    return True

def _check_fields_contains_required(cols):
    # Check if the struct provided contains all the required fields
    req_fields = [x[0] for x in fields if x[1] is True] # x[1] = True -> x[0] are required
    if not all (f in cols[0] for f in req_fields):
        raise ValueError('Required field(s) is missing!')
    return True

def _check_structs(struct, cols):
    # Check if the struct we input is valid
    return (_check_field_is_standard(struct) 
            and _check_field_is_duplicated(cols) 
            and _check_fields_contains_required(cols))

def _import_file(path, names, usecols, date_cols, skiprow):
    # import the file
    if not os.path.isdir(path): # if the path is not a directory
        data = pd.read_csv(
            path,
            names = names,
            usecols = usecols,
            parse_dates = date_cols,
            skiprows = skiprow,
            infer_datetime_format = True,
        )
    else:
        raise ValueError("Invalid path, please provide a valid path to a file")

    return data.pipe(timeFormatter) # standardise the format

def _preview(data):
    # preview the data see if there is any problem
    # if the user input 'yes', then return True, vice versa
    print(data.head())

    return _user_prompt("Is is what you want?")

def _user_prompt(question):
    while True:
        sys.stdout.write(question + " [y/n]: ")
        user_input = input().lower()
        try: 
            result = strtobool(user_input)
            return result
        except ValueError:
            sys.stdout.write(" Please user y/n or yes/no. \n")

def _import(path, struct, skiprow, preview):
    # check the imported struct and then import the file by calling _import_file
    cols = list(zip(*struct)) # de-zipped the struct

    if _check_structs(struct, cols): # check if the imported struct fulfils our standard requirements
        date_cols = [cols[0].index('date'), cols[0].index('maturity_date')] # find the index of the two columns containing dates
        # find the index of the two columns containing dates
        data = _import_file(path, names = cols[0], usecols = cols[1], date_cols = date_cols, skiprow = skiprow)
        # data['day_to_event'] = pd.to_timedelta(data['day_to_event']).dt.days
        if not preview or (preview & _preview(data)):
            return data
        else:
            print('Data is not correct')
            sys.exit()

def get_data(file_path, struct, skiprow = 1, preview = False):
    return _import(file_path, struct, skiprow, preview)
