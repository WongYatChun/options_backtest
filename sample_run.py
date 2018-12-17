import os
from datetime import datetime
import pandas as pd

from optionStrategies import long_call, short_call, long_call_long_put, long_call_short_put, short_call_short_put
from data_import import get_data
from tradeStat import results
from pathlib import PurePath, Path

NKY_BUDGET = 3000000 # initial budget
NKY_TCOST = 0 # T cost is irrelevant for NKY
NKY_CONTRACT_SIZE = 1000
ASX_BUDGET = 100000
ASX_TCOST = 10 # T cost per contract ASX
ASX_CONTRACT_SIZE = 10
SPX_BUDGET = 70000
SPX_TCOST = 1
SPX_CONTRACT_SIZE = 100

pp = PurePath(Path.cwd()).parts[:]
pdir = PurePath(*pp)
infp=PurePath(pdir)

def run_strategy(data, strategy, after = True, timeLag = True, contract_size = SPX_CONTRACT_SIZE):
    """
    This programme is specifically designed to test event driven strategies
    'After': True-exit at the event date, False-exit at the nearest trading day before the event
    'timeLag': True-if we will exit at the next date to account for the time zone difference
    """
    if (after and timeLag):
        entry_day_to_event = -1 # greater
        exit_day_to_event = -1 # nearest
    elif (after and not timeLag) or (not after and timeLag):
        entry_day_to_event = 0 # greater
        exit_day_to_event = 0 # nearest
    elif (not after and not timeLag):
        entry_day_to_event = 1 # greater
        exit_day_to_event = 1 # nearest
    else:
        ValueError()
    # define the entry and exit filters to use for this strategy, full list of
    # filters will be listed in the documentation (WIP). 
    filters = {
        "start_date": {"value": datetime(2012, 1, 1), "cond": "greater"}, # cond has no effect 
        "end_date": {"value": datetime(2018, 12, 28), "cond": "less_or_equal"}, # cond has no effect
        "entry_dtm": {"value": 7, "cond": "greater"},
        "entry_day_to_event": {"value": entry_day_to_event, "cond": "greater"}, # after -1, before, 0
        "day_to_event": {"value": 3, "cond": "less_or_equal"},
        "leg1_delta": {"value": 0.5, "cond": "nearest"},
        "leg2_delta": {"value": 0.5, "cond": "nearest"},
        "contract_size": contract_size, 
        "exit_day_to_event": {"value": exit_day_to_event, "cond": "nearest"}, # after -1, before, 0
        #  "exit_dtm": {"value": 21, "cond": "nearest"},
    }
    # set the start and end dates for the backtest, the dates are inclusive,
    # start and end dates are python datetime objects.
    # strategy functions will return a dataframe containing all the simulated trades
    if strategy == "long_straddle":
        return long_call_long_put(data, filters, mode = "mid_price")
    elif strategy == "short_straddle":
        return short_call_short_put(data, filters, mode = "mid_price")
    else:
        return data

def store_and_get_data(file_name):
    # absolute file path to our input file
    curr_file = os.path.abspath(os.path.dirname(__file__))
    file = os.path.join(curr_file, "data", f"{file_name}.pkl")

    # check if we have a pickle store
    if os.path.isfile(file):
        print("pickle file found, retrieving...")
        return pd.read_pickle(file)
    else:
        print("no picked file found, retrieving csv data...")

        csv_file = os.path.join(curr_file, "data", f"{file_name}.csv")
        data = get_data(csv_file, SPX_FILE_STRUCT, preview = False)

        print("storing to pickle file...")
        pd.to_pickle(data, file)
        return data

def mass_get_data(strategy, infp = infp, category = "Monetary Policy", event = "Fed", market = "ASX",after = True, timeLag = True):
    # absolute file path to our input file
    # curr_file = os.path.abspath(os.path.dirname(__file__))
    path = os.path.join(infp , "data" , "event_dateframe" , category , event ,  market)
    entries = os.listdir(path)
    
    if market == "ASX":
        init_balance = ASX_BUDGET
        t_cost = ASX_TCOST
        contract_size = ASX_CONTRACT_SIZE
    elif market == "NKY":
        init_balance = NKY_BUDGET
        t_cost = NKY_TCOST
        contract_size = NKY_CONTRACT_SIZE
    elif market == "SPX":
        init_balance = SPX_BUDGET
        t_cost = SPX_TCOST
        contract_size = SPX_CONTRACT_SIZE
    else:
        ValueError()
    
    for entry in entries:
        csv_file = os.path.join(path, entry)
        data = get_data(csv_file, SPX_FILE_STRUCT, preview = False)
        
        r = data.pipe(run_strategy, strategy = strategy, after = after, timeLag = timeLag, contract_size = contract_size).pipe(results, init_balance = init_balance, t_cost = t_cost)
        # r[0] is the simple trade stats
        # print(r[0])
        # r[1] is a dataframe containing all the individual trades of the strategy
        # i.e. document each call and put transactions
        # print(r[1])
        r[1].to_excel(entry.replace(".csv","_details.xls" ))
        # r[2] is a dataframe containing the consolidated trades
        # i.e. documents each trade by a strategy as a whole
        r[2].to_excel(entry.replace(".csv","_trade.xls" ))
        # print(r[2])

if __name__ == "__main__":
    # Here we define the struct to match the format of our csv file
    # the struct indices are 0-indexed where first column of the csv file
    # is mapped to 0
    SPX_FILE_STRUCT = (
        ("date", 0),
        ("bid", 1),
        ("ask", 2),
        ('last',3),
        ("call_put", 5),
        ("maturity_date", 6),
        ("strike", 7),
        ("underlying_symbol", 8),
        ("underlying_price", 9),
        ("implied_vol", 11),
        ("delta", 12),
        ("gamma", 13),
        ("vega", 14), 
        ("theta", 15),
        ("rho", 16),
        ('event_day', 18),
        ('day_to_event', 19),
    )
    # r = store_and_get_data("SPX_2018").pipe(run_strategy).pipe(results)
    # pd.DataFrame.from_dict(r[0], orient = 'index').to_excel("result.xls")
    long_straddle = "long_straddle"
    short_straddle = "short_straddle"
    # Long straddle
    AFTER = True
    TIMELAG = True
    mass_get_data(strategy = long_straddle, infp = infp, category = "Geopolitical event", event = "Brexit", market = "ASX", after = AFTER, timeLag = TIMELAG)
    mass_get_data(strategy = long_straddle, infp = infp, category = "Geopolitical event", event = "USA Election", market = "ASX", after = AFTER, timeLag = TIMELAG)


