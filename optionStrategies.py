from definedClass import CallPut
from backtest_main import create_spread, simulate
from filters import filter_data, func_map

default_entry_filters = {
    "contract_size": 10,
    "entry_dtm": (27, 30, 31),
    "exit_dtm": None,
}

def _prepare_filters(fil):
    f = {**default_entry_filters, **fil}
    init_fil = {k: v for (k, v) in f.items() if func_map[k]["type"] == "init"}
    entry_s_fil = {k: v for (k, v) in f.items() if func_map[k]["type"] == "entry_s"}
    exit_s_fil = {k: v for (k, v) in f.items() if func_map[k]["type"] == "exit_s"}
    entry_fil = {k: v for (k, v) in f.items() if func_map[k]["type"] == "entry"}
    exit_fil = {k: v for (k, v) in f.items() if func_map[k]["type"] == "exit"}
    return init_fil, entry_fil, exit_fil, entry_s_fil, exit_s_fil

def _process_legs(data, legs, fil, mode):

    f = _prepare_filters(fil)
    return (
        data.pipe(filter_data, f[0])
        .pipe(create_spread, legs, f[1], f[3], mode)
        .pipe(simulate, data, f[2], f[4], mode)
    )

def long_call(data, filters, mode = "market"):
    legs = [(CallPut.CALL, 1)]
    return _process_legs(data, legs, filters, mode)

def short_call(data, filters, mode = "market"):
    legs = [(CallPut.CALL, -1)]
    return _process_legs(data, legs, filters, mode)

def long_put(data, filters, mode = "market"):
    legs = [(CallPut.PUT, 1)]
    return _process_legs(data, legs, filters, mode)

def short_put(data, filters, mode = "market"):
    legs = [(CallPut.PUT, -1)]
    return _process_legs(data, legs, filters, mode)

def long_call_long_put(data, filters, mode = "market"):
    legs = [(CallPut.CALL, 1), (CallPut.PUT, 1)]
    return _process_legs(data, legs, filters, mode)

def long_call_short_put(data, filters, mode = "market"):
    legs = [(CallPut.CALL, 1), (CallPut.PUT, -1)]
    return _process_legs(data, legs, filters, mode)

def short_call_short_put(data, filters, mode = "market"):
    legs = [(CallPut.CALL, -1), (CallPut.PUT, -1)]
    return _process_legs(data, legs, filters, mode)

