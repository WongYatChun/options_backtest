import pandas as pd 
import numpy as np
from helpers import _process_values, nearest, gte
from functools import reduce
from datetime import datetime, timedelta

dayThres = timedelta(days = 1)

def _calc_strike_pct(data, strikePct, n, idx):
    """
    calculate the Strike Percentage (strike / stock price).
    """
    if not isinstance(strikePct, (int, float, tuple)):
        raise ValueError(f"Invalid value passed for leg{n+1} entry strike percentage")
    elif idx == n:
        return data.assign(
            strike_pct = lambda x: (x['strike'] / x['underlying_price']).round(2)
            ).pipe(_process_values, "strike_pct", strikePct)

# Initiation Filter
def start_date(data, date, _idx):
    """
    find the contracts with maturity date >= the specified
    i.e. contracts that are alive after the the specified date
    """
    if isinstance(date["value"], datetime):
        # return data[data["maturity_date"] > date["value"]]
        if date["cond"] == "greater":
            return data[data["maturity_date"] > date["value"]]
        else:
            raise ValueError("The condition does not make sense")
    else:
        raise ValueError("Start Dates must of Date type")

def day_to_event(data, day_to_event, _idx):
    """
    
    """
    if day_to_event["cond"] == "less_or_equal":
        return data[data["day_to_event"] <= day_to_event["value"]]
    elif day_to_event['cond'] == 'less':
        return data[data["day_to_event"] < day_to_event["value"]]
    elif day_to_event['cond'] == 'equal':
        return data[data["day_to_event"] == day_to_event["value"]]
    elif day_to_event['cond'] == 'greater_or_equal':
        return data[data["day_to_event"] >= day_to_event["value"]]
    elif day_to_event['cond'] == 'greater':
        return data[data["day_to_event"] > day_to_event["value"]]
    else:
        raise ValueError("The condition does not make sense")

def end_date(data, date, _idx):
    """
    find the contracts with maturity date <= the specified
    """
    if isinstance(date["value"], datetime):
        if date['cond'] == 'less_or_equal':
            return data[data["maturity_date"] <= date["value"]]
        elif date['cond'] == 'less':
            return data[data["maturity_date"] < date["value"]]
        elif date['cond'] == 'equal':
            return data[data["maturity_date"] == date["value"]]
        elif date['cond'] == 'greater_or_equal':
            return data[data["maturity_date"] >= date["value"]]
        elif date['cond'] == 'greater':
            return data[data["maturity_date"] > date["value"]]
        else:
            raise ValueError("The condition does not make sense")
    else:
        raise ValueError("End Dates must of Date type")

# Entry Filter
def contract_size(data, size, _idx):
    """
    assign the contract with the column 'contract size.'
    """
    if isinstance(size, int):
        return data.assign(contracts = size)
    else:
        raise ValueError("Contract sizes must of Int type")

def entry_dtm(data, betweenDays, _idx):
    """
    Days to maturity min and max for the trade to be considered.

    For example, it will search options that have days to expiration
    between and including 20 to 55.
    """
    groupby = ["call_put", "maturity_date", "underlying_symbol"]
    return _process_values(data, "dtm", betweenDays['value'], betweenDays['cond'], groupby = groupby)

def entry_day_to_event(data, days, _idx):
    
    groupby = ["call_put", "maturity_date", "underlying_symbol"]
    return _process_values(data, "day_to_event", days['value'], days['cond'], groupby = groupby)

def leg1_delta(data, value, idx):
    """
    Absolute value of a delta of an option.
    """
    return _process_values(data, "delta", value['value'], cond = value['cond']) if idx == 0 else data

def leg2_delta(data, value, idx):
    """
    Absolute value of a delta of an option.
    """
    return _process_values(data, "delta", value['value'], cond = value['cond']) if idx == 1 else data

def leg3_delta(data, value, idx):
    """
    Absolute value of a delta of an option.
    """
    return _process_values(data, "delta", value['value'], cond = value['cond']) if idx == 2 else data

def leg4_delta(data, value, idx):
    """
    Absolute value of a delta of an option.
    """
    return _process_values(data, "delta", value['value'], cond = value['cond']) if idx == 3 else data

def leg1_strike_pct(data, value, idx):
    """
    Stock Percentage (strike / stock price).
    """
    return _calc_strike_pct(data, value, 0, idx)

def leg2_strike_pct(data, value, idx):
    """
    Stock Percentage (strike / stock price).
    """
    return _calc_strike_pct(data, value, 1, idx)

def leg3_strike_pct(data, value, idx):
    """
    Stock Percentage (strike / stock price).
    """
    return _calc_strike_pct(data, value, 2, idx)

def leg4_strike_pct(data, value, idx):
    """
    Stock Percentage (strike / stock price).
    """
    return _calc_strike_pct(data, value, 3, idx)

# Entry Spread Filter
def entry_spread_price(data, value, _idx):
    """
    The net price of the spread.

    For example, it would set a min max of $0.10 to $0.20 and find only spreads with prices
    within that range.
    """
    return (
        data.groupby(["trade_num"])["entry_opt_price"]
        .sum()
        .to_frame(name = "entry_opt_price")
        .pipe(_process_values, "entry_opt_price", value['value'], cond = value['cond'], groupby = ["trade_num"])
        .merge(data, left_index = True, right_index = True)
        .drop(["entry_opt_price_x"], axis = 1)
        .rename(columns = {"entry_opt_price_y": "entry_opt_price"})
    )

# Exit Filter
def exit_dtm(data, days, _idx):
    """
    Exit the trade when the days to day to maturity left is equal to this.
    For example, it would exit a trade with 10 days to the date to maturity.
    """
    return data
    # if days is None:
    #     #print(data["date_exit"].astype('datetime64[D]').tolist())
    #     #print(set(abs(data["date_exit"].values - data["maturity_date"].values)))
    #     # return data[abs(data["date_exit"].astype('datetime64[D]') - data["maturity_date"].astype('datetime64[D]'))< dayThres]
    #     return data[data["dtm_exit"] < 1]
    #     # return data[data["date_exit"] == data["maturity_date"]]
    # else:
    #     groupby = ["call_put", "maturity_date", "underlying_symbol"]
    #     return _process_values(data, "dtm_exit", days['value'], cond = days['cond'], groupby=groupby)
    #     # return nearest(data, "dtm_exit", days, groupby = groupby)

def exit_day_to_event(data, day_to_event, _idx):
    """
    
    """
    return data[data["day_to_event_exit"] == day_to_event['value']]
    # groupby = ["call_put", "maturity_date", "underlying_symbol"]
    # return _process_values(data, "day_to_event_exit", day_to_event["value"], cond = day_to_event["cond"], groupby=groupby)

func_map = {
    "start_date": {"func": start_date, "type": "init"},                     # Find the contracts alive after a certain date
    "end_date": {"func": end_date, "type": "init"},                         # Find the contracts matured before a certain date 
    "day_to_event": {"func": day_to_event, "type": "entry"},                  
    "contract_size": {"func": contract_size, "type": "entry"},              # Specify the contract size
    "entry_dtm": {"func": entry_dtm, "type": "entry"},                      # Search the contracts with day to maturity between x and y
    "entry_day_to_event": {"func": entry_day_to_event, "type": "entry"},                     
    
    # "entry_days": {"func": entry_days, "type": "entry"},                  
    "leg1_delta": {"func": leg1_delta, "type": "entry"},                    # The 1st contract with certain delta
    "leg2_delta": {"func": leg2_delta, "type": "entry"},                    # The 2nd contract with certain delta
    "leg3_delta": {"func": leg3_delta, "type": "entry"},                    # The 3rd contract with certain delta
    "leg4_delta": {"func": leg4_delta, "type": "entry"},                    # The 4th contract with certain delta
    "leg1_strike_pct": {"func": leg1_strike_pct, "type": "entry"},          # The 1st contract with certain strike percentage
    "leg2_strike_pct": {"func": leg2_strike_pct, "type": "entry"},          # The 2nd contract with certain strike percentage
    "leg3_strike_pct": {"func": leg3_strike_pct, "type": "entry"},          # The 3rd contract with certain strike percentage
    "leg4_strike_pct": {"func": leg4_strike_pct, "type": "entry"},          # The 4th contract with certain strike percentage
    "entry_spread_price": {"func": entry_spread_price, "type": "entry_s"},
    # "entry_spread_delta": {"func": entry_spread_delta, "type": "entry_s"},
    # "entry_spread_yield": {"func": entry_spread_yield, "type": "entry_s"},
    "exit_dtm": {"func": exit_dtm, "type": "exit"},                         # Find the option that has day to maturity <= centain days
    "exit_day_to_event": {"func": exit_day_to_event, "type": "exit"},
    # "exit_hold_days": {"func": exit_hold_days, "type": "exit"},
    # "exit_leg_1_delta": {"func": exit_leg_1_delta, "type": "exit"},
    # "exit_leg_1_otm_pct": {"func": exit_leg_1_otm_pct, "type": "exit"},
    # "exit_profit_loss_pct": {"func": exit_profit_loss_pct, "type": "exit"},
    # "exit_spread_delta": {"func": exit_spread_delta, "type": "exit_s"},
    # "exit_spread_price": {"func": exit_spread_price, "type": "exit_s"},
    # "exit_strike_diff_pct": {"func": exit_strike_diff_pct, "type": "exit"},
}

def _apply_filters(legs, filters):
    if not filters:
        return legs
    else:
        return [
            # sequentially applying the filters
            reduce(lambda data, filter: func_map[filter]['func'](data, filters[filter], idx), filters, leg)
            for idx, leg in enumerate(legs)
        ]
#========================================================================================
# Main function
#========================================================================================
def filter_data(data, filters):
    data = data if isinstance(data, list) else [data]
    return pd.concat(_apply_filters(data, filters), sort = True)

#==============================================================================================================================
# NOT YET IMPLEMENTED
#==============================================================================================================================

# Entry
def entry_days(data, value, _idx):
    """
    Stagger trades every this many Entry Days.

    For example, there would be a new trade every 7 days from the last new trade.
    """
    pass

# Entry Spread
def entry_spread_delta(data, value, _idx):
    """
    The net delta of the spread.

    For example, it would set a min max of .30 to .40 and find only spreads with net deltas
    within that range.
    """
    pass

def entry_spread_yield(data, value, _idx):
    """
    Yield Percentage is (option entry price / stockprice).

    For example it would search options that have yldpct between
    and including .05 to .10 (5 and 10 percent).
    """
    pass

# Exit Filter
def exit_hold_days(data, value, _idx):
    """
    Exit the trade when the trade was held this many days.

    For example, it would exit a trade when the trade has been held for 20 days.
    """
    pass

def exit_leg_1_delta(data, value, idx):
    """
    Exit the trade when the delta of leg 1 is below the min or above the max.

    For example, it would exit when the delta of the
    first leg is below .10 or above .90 delta.
    """
    pass

def exit_leg_1_otm_pct(data, value, idx):
    """
    Exit the trade when the strike as a percent of stock price
    of leg 1 is below the min or above the max.

    For example, it would exit when the strike percentage
    of stock price is below 1.05 or above 1.20.
    """
    pass

def exit_profit_loss_pct(data, value, _idx):
    """
    Take profits and add stop loss to exit trade at these intervals.

    For example, set a stop loss of 50 (-50%) and a profit target at 200 (200%) on a long call.
    Set only a stop loss by leaving profit blank.
    """
    pass

def exit_spread_delta(data, value, _idx):
    """
    Exit and roll the trade if the spread total delta exceed the min or max value. For Example;

    spread delta = leg1ratio * leg1delta + leg2ratio * leg2delta
    """
    pass

def exit_spread_price(data, value, _idx):
    """
    Exit the trade when the trade price falls below the min or rises above the max.

    For example, it would exit if below 0.4 min or above $0.90 max price.
    """
    pass

def exit_strike_diff_pct(data, value, _idx):
    """
    Exit the trade when the trade price divided by the difference
    in strike prices falls below the min or rises above the max.

    For example, a $5 wide call spread would exit if the price of
    $1 divided by 5 is below the min of .20 or the price of 4.5
    that is above the max of .90.
    """
    pass