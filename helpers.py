from data_import import fields
from definedClass import Period, CallPut

def _convert_if_Period(val):
    return val.value if isinstance(val, Period) else val

def _calc_abs_distance(row, column, val, absolute = True):
    # calculate the absolute distance
    col = abs(row[column]) if absolute else row[column]
    return abs(col - _convert_if_Period(val))

def calls(df):
    # return call options from the data frame
    return df[df.call_put.str.lower().str.startswith('c')]

def puts(df):
    # return put options from the data frame
    return df[df.call_put.str.lower().str.startswith('p')]

def callput(df, call_put):
    # return call or put options from the data frame
    if isinstance(call_put, CallPut):
        return df[df['call_put'] == call_put.value[0]]
    else:
        raise ValueError("Invalid input")

def underlying_price(df):
    # don't understand its behavior, to be tested
    if "underlying_price" in df:
        dates = df["underlying_price"].unique()
        return dates.mean()
    else:
        raise ValueError("Underlying Price column undefined!")

def lte(df, column, val, groupby = None):
    # less than or equal to
    return df[df[column] <= _convert_if_Period(val)]

def lt(df, column, val, groupby = None):
    # less than
    return df[df[column] < _convert_if_Period(val)]

def gte(df, column, val, groupby = None):
    # greater than or equal to
    return df[df[column] >= _convert_if_Period(val)]

def gt(df, column, val, groupby = None):
    # greater than
    return df[df[column] > _convert_if_Period(val)]

def eq(df, column, val, groupby = None):
    # equal
    return df[df[column] == _convert_if_Period(val)]

def ne(df, column, val, groupby = None):
    # not equal
    return df[df[column] != _convert_if_Period(val)]

def between(df, column, start, end, inclusive = True, absolute = False):
    # find the range of the data frame within the boundary
    if absolute: # create a temporary column if absolute = True
        temp_col = f"{column}_temp"
        df[temp_col] = abs(df[column])
    else:
        temp_col = column

    result = df[
        df[temp_col].between(_convert_if_Period(start), _convert_if_Period(end), inclusive = inclusive)
    ]
    return result.drop(temp_col, axis=1) if absolute else result

def nearest(df, column, val, groupby = None, absolute = True, tie = "roundup"):
    # we need to group by unique option columns so that we are
    # getting the min abs dist over multiple sets of option groups
    # instead of the absolute min of the entire data set.
    if groupby is None:
        groupby = ["date", "call_put", "maturity_date", "underlying_symbol"]

    on = groupby + ["abs_dist"]

    data = df.assign(abs_dist = lambda r: _calc_abs_distance(r, column, val, absolute))

    return (
        data.groupby(groupby)["abs_dist"]
        .min()
        .to_frame()
        .merge(data, on = on)
        .drop("abs_dist", axis = 1)
    )

cond_map = {
    "less_or_equal": {"func": lte},
    "less": {"func": lt},
    "greater_or_equal": {"func": gte},
    "greater": {"func": gt},
    "equal": {"func": eq},
    "not_equal": {"func": ne},
    "nearest": {"func": nearest},
}


def _process_tuples(data, column, groupby, value):
    if len(set(value)) == 1:
        return eq(data, column, value[1])
    else:
        # what is the format of the argument 'value'?
        return data.pipe(nearest, column, value[1], groupby = groupby).pipe(between, column, value[0], value[2], absolute = True)

def _process_values_gte(data, column, value, groupby = None, valid_types = (int, float, tuple)):
    if not isinstance(value, valid_types):
        raise ValueError("Invalid value passed to the filter")
    elif isinstance(value, tuple):
        return _process_tuples(data, column, groupby = groupby, value = value)
    else:
        return gte(data, column, value)

def _process_values(data, column, value, cond = "nearest", groupby = None, valid_types = (int, float, tuple)):
    if not isinstance(value, valid_types):
        raise ValueError("Invalid value passed to the filter")
    elif isinstance(value, tuple):
        return _process_tuples(data, column, groupby = groupby, value = value)
    else:
        return cond_map[cond]['func'](data, column, value, groupby = groupby)
