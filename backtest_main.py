from definedClass import CallPut
from functools import reduce
from helpers import callput
from tradeStat import calc_entry_price, calc_exit_price, assign_trade_num, calc_pnl
from filters import filter_data
import pandas as pd 

pd.set_option("display.expand_frame_repr", False)

on = ['underlying_symbol', 'call_put', 'maturity_date', 'strike']

output_cols = {
    "date_entry": "entry_date",
    "date_exit": "exit_date",
    "delta_entry": "entry_delta",
    "gamma_entry": "entry_gamma",
    "vega_entry": "entry_vega",
    "rho_entry": "entry_rho",
    "theta_entry": "entry_theta",
    "underlying_price_entry": "entry_underlying_price",
    "underlying_price_exit": "exit_underlying_price",
    "dtm_entry": "dtm",
}

output_format = [
    "entry_date",
    "exit_date",
    "maturity_date",
    "underlying_symbol",
    "dtm",
    "ratio",
    "contracts",
    "call_put",
    "strike",
    "entry_delta",
    "entry_gamma",
    "entry_vega",
    "entry_theta",
    "entry_rho",
    "entry_underlying_price",
    "exit_underlying_price",
    "entry_opt_price",
    "exit_opt_price",
    "entry_price",
    "exit_price",
    "cash_flow",
]

def _create_legs(data, leg):
    # select call or put, ratio ("long": 1, "short": -1 )
    return data.pipe(callput, call_put = leg[0]).assign(ratio = leg[1])

def _do_dedupe(spread, groupby, col, mode):
    # dedupe delta dist ties
    if groupby is None:
        groupby = ['date', 'maturity_date', 'underlying_symbol', 'ratio', 'call_put']
    on = groupby + [col]

    if mode == 'min':
        return spread.groupby(groupby)[col].min().to_frame().merge(spread, on = on)
    else:
        return spread.groupby(groupby)[col].max().to_frame().merge(spread, on = on)

def _dedup_rows_by_cols(spreads, cols, groupby = None, mode = "max"):
    return reduce(lambda x, col: _do_dedupe(spreads, groupby, col, mode), cols, spreads)

def create_spread(data, leg_structs, entry_filters, entry_spread_filters, mode):
    legs = [_create_legs(data, leg) for leg in leg_structs]
    return(
        filter_data(legs, filters = entry_filters)
        .rename(columns = {'bid': 'bid_entry', 'ask': 'ask_entry', 'last': 'last_entry'})
        .pipe(_dedup_rows_by_cols, cols = ['delta', 'strike'])
        .pipe(assign_trade_num, groupby = ['date', 'maturity_date', 'underlying_symbol'])
        .pipe(calc_entry_price, mode = mode)
        .pipe(filter_data, filters = entry_spread_filters)
    )
#-------------------------------------------------------------------------------------------------------------------------
# Main function that runs the backtest engine:
#   1. Mainly finding the exit position according to the exit filter
#   2. Calculate the PnL
#-------------------------------------------------------------------------------------------------------------------------
def simulate(spreads, data, exit_filters, exit_spread_filters, mode):
    # for each option to be traded, determine the historical price action    
    res = (
        pd.merge(spreads, data, on = on, suffixes = ("_entry", "_exit")) # Suffix to apply to overlapping column names in the left and right side, respectively
        .pipe(filter_data, filters = exit_filters)
        .rename(columns = {"bid": "bid_exit", "ask": "ask_exit", 'last': 'last_exit'}) # because the bid_entry and ask_entry have been specified in 'create_spread'
        .pipe(calc_exit_price, mode = mode)
        .pipe(calc_pnl)
        .pipe(filter_data, filters = exit_spread_filters)
        .rename(columns = output_cols)
        .sort_values(["entry_date", "maturity_date", "underlying_symbol", "strike"])
        .pipe(assign_trade_num, groupby = ["entry_date", "maturity_date", "underlying_symbol"])
    )

    return res[output_format]