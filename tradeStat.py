import numpy as np 

COMMISION_RATE = 0.175 / 100

BUDGET = 3000000 # initial budget
# BUDGET = 100000
# TCOST = 10 # T cost per contract ASX
TCOST = 0 # T cost is irrelevant for NKY

NUN_OPTION = 2 # straddle


def calc_ending_balance(data, init_balance = BUDGET):
#    window = np.insert(data['cash_flow'].values, 0, init_balance, axis = 0)
#    return np.subtract.accumulate(window)[-1]
    return (init_balance + data['cash_flow'].sum())

def calc_total_trades(data):
    return data.index.max() + 1

def calc_total_profit(data):
    return data['cash_flow'].sum().round(2)

def _calc_with_groups(data):
    df = data.groupby('trade_num')['cash_flow'].sum()
    wins = df[df >= 0].count()
    losses = df[df < 0].count()
    return {
        "win_cnt": wins,
        "win_pct": round(wins / df.size, 2),
        "loss_cnt": losses,
        "loss_pct": round(losses / df.size, 2),
    }

def _calc_mkt_opt_price(data, action):
    ask = data[f"ask_{action}"] * data['ratio']
    bid = data[f"bid_{action}"] * data['ratio']

    if action == "entry":
        return np.where(data['ratio'] > 0, -1 * ask, -1 * bid)
    elif action == 'exit':
        return np.where(data['ratio'] > 0, bid , ask)

def _calc_mid_opt_price(data, action):
    # bid_ask = [f"bid_{action}", f"ask_{action}"]
    # if action == "entry":
    #     return data[bid_ask].mean(axis=1) * data["ratio"] * -1
    # elif action == "exit":
    #     return data[bid_ask].mean(axis=1) * data["ratio"]

    if action == "entry":
        return data['last_entry'] * data["ratio"] * -1
    elif action == "exit":
        return data['last_exit'] * data["ratio"]

def _assign_opt_price(data, mode, action):
    if mode == "mid_price":
        data[f"{action}_opt_price"] = _calc_mid_opt_price(data, action)
    elif mode == "market":
        data[f"{action}_opt_price"] = _calc_mkt_opt_price(data, action)
    return data

def assign_trade_num(data, groupby):
    data["trade_num"] = data.groupby(groupby).ngroup()
    data.set_index("trade_num", inplace=True)
    return data

def calc_entry_price(data, mode = "market"):
    return _assign_opt_price(data, mode, "entry")

def calc_exit_price(data, mode = "market"):
    return _assign_opt_price(data, mode, "exit")

def calc_pnl(data):
    # calculate the p/l for the trades
    data["entry_price"] = data["entry_opt_price"] * data["contracts"]
    data["exit_price"] = data["exit_opt_price"] * data["contracts"]
    data["cash_flow"] = data["exit_price"] + data["entry_price"]
    return data.round(2)

def results(data, init_balance=BUDGET, t_cost = TCOST, num_option = NUN_OPTION):
    
    priceStats = ['entry_delta','entry_gamma','entry_vega','entry_rho', 'entry_theta', 'entry_price', 'exit_price']
    df = data.groupby(['trade_num', 'entry_date' ,'exit_date', 'maturity_date'])[priceStats].sum().reset_index().set_index('trade_num')
    df['holding_period'] = df['exit_date'] - df['entry_date']
    df['num_contracts'] = round(init_balance / abs(df['entry_price']),0)
    df['total_t_cost'] = num_option * t_cost * df['num_contracts'] # maybe not always
    df['Actual_Profit'] = df['num_contracts'] * (df['exit_price'] + df['entry_price']) - df['total_t_cost']
    df['Average_Day_Profit'] = round((df['Actual_Profit'] / df['holding_period'].dt.days),2)
    df['Total_Return_pct'] = df['Actual_Profit'] / init_balance * 100
    df['Average_Daily_Return_pct']  = round((df['Total_Return_pct'] / df['holding_period'].dt.days),2) 
    # df['Average_Day_Profit'] = round((df['Actual_Profit'] / df['holding_period']),2)
    return (
        {
            "Total Profit": calc_total_profit(data),
            "Total Win Count": _calc_with_groups(data)["win_cnt"],
            "Total Win Percent": _calc_with_groups(data)["win_pct"],
            "Total Loss Count": _calc_with_groups(data)["loss_cnt"],
            "Total Loss Percent": _calc_with_groups(data)["loss_pct"],
            "Total Trades": calc_total_trades(data),
        },
        data,
        df,
        
    )
