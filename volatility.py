import pandas as pd
import numpy as np 

def getAnnualisedVol(df, column, span = 90, mulFactor = np.sqrt(252)):
    """
    Use the log returns to find the standard deviation and multiply the mulFactor to get the annualised vol
    :params df: dataframe
    :params column: string, the name of the column
    :params span: int, the rolling span(window), default = 90 data points
    :params mulFactor: float, multiplication factor to annualise the volaility, default = sqrt(252), i.e. daily volatility
    """
    std = np.log(df[column] / df[column].shift(1)).rolling(window = span).std(ddof = 1)
    return mulFactor * std
